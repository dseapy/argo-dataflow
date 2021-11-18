package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	// TODO: change back to github.com/riferrei/srclient after PR merges.
	dfv1 "github.com/argoproj-labs/argo-dataflow/api/v1alpha1"
	sharedkafka "github.com/argoproj-labs/argo-dataflow/runner/sidecar/shared/kafka"
	"github.com/argoproj-labs/argo-dataflow/runner/sidecar/sink"
	sharedutil "github.com/argoproj-labs/argo-dataflow/shared/util"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dseapy/srclient"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

var logger = sharedutil.NewLogger()

type kafkaSink struct {
	sinkName                             string
	producer                             *kafka.Producer
	topic                                string
	async                                bool
	confluentSchemaRegistryClient        *srclient.SchemaRegistryClient
	confluentSchemaRegistryMessageFormat dfv1.ConfluentSchemaRegistryMessageFormat
}

func New(ctx context.Context, sinkName string, secretInterface corev1.SecretInterface, x dfv1.KafkaSink, errorsCounter prometheus.Counter) (sink.Interface, error) {
	logger := logger.WithValues("sink", sinkName)
	config, err := sharedkafka.GetConfig(ctx, secretInterface, x.KafkaConfig)
	if err != nil {
		return nil, err
	}
	config["go.logs.channel.enable"] = true
	if x.MaxMessageBytes > 0 {
		config["message.max.bytes"] = x.GetMessageMaxBytes()
	}
	// https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html
	config["batch.size"] = x.GetBatchSize()
	config["linger.ms"] = x.GetLingerMs()
	config["compression.type"] = x.CompressionType
	config["acks"] = x.GetAcks()
	config["enable.idempotence"] = x.EnableIdempotence
	if x.Async { // this is meant to be set by `enable.idempotence` automatically, but I'm not sure it is
		config["retries"] = math.MaxInt32
	}

	logger.Info("kafka config", "config", sharedutil.MustJSON(sharedkafka.RedactConfigMap(config)))

	// https://github.com/confluentinc/confluent-kafka-go/blob/master/examples/producer_example/producer_example.go
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, err
	}
	go wait.JitterUntilWithContext(ctx, func(context.Context) {
		logger.Info("consuming Kafka logs")
		for e := range producer.Logs() {
			logger.WithValues("name", e.Name, "tag", e.Tag).Info(e.Message)
		}
	}, 3*time.Second, 1.2, true)

	go wait.JitterUntilWithContext(ctx, func(context.Context) {
		logger.Info("starting producer event consuming loop")
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if err := ev.TopicPartition.Error; err != nil {
					logger.Error(err, "Async to Kafka failed", "topic", x.Topic)
					errorsCounter.Inc()
				}
			}
		}
	}, time.Second, 1.2, true)

	confluentSchemaRegistryClient := srclient.CreateSchemaRegistryClient(x.ConfluentSchemaRegistryConfig.URL)

	s := &kafkaSink{
		sinkName:                             sinkName,
		producer:                             producer,
		topic:                                x.Topic,
		async:                                x.Async,
		confluentSchemaRegistryClient:        confluentSchemaRegistryClient,
		confluentSchemaRegistryMessageFormat: x.ConfluentSchemaRegistryConfig.MessageFormat,
	}
	return s, nil
}

func (h *kafkaSink) Sink(ctx context.Context, msg []byte) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, fmt.Sprintf("kafka-sink-%s", h.sinkName))
	defer span.Finish()
	m, err := dfv1.MetaFromContext(ctx)
	if err != nil {
		return err
	}
	var deliveryChan chan kafka.Event
	if !h.async {
		deliveryChan = make(chan kafka.Event)
		defer close(deliveryChan)
	}
	finalMsgValue := &msg
	if h.confluentSchemaRegistryClient != nil && h.confluentSchemaRegistryMessageFormat != dfv1.ConfluentSchemaRegistryMessageFormatRaw {
		// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work
		schema, err := h.confluentSchemaRegistryClient.GetLatestSchema(h.topic + "-value")
		if err != nil {
			return err
		}
		// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		var recordValue []byte
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)

		schemaType := srclient.Avro
		if schema.SchemaType() != nil {
			schemaType = *schema.SchemaType()
		}
		switch schemaType {
		case srclient.Avro:
			if h.confluentSchemaRegistryMessageFormat == dfv1.ConfluentSchemaRegistryMessageFormatNative {
				recordValue = append(recordValue, msg...)
			} else {
				native, _, err := schema.Codec().NativeFromTextual(msg)
				if err != nil {
					return err
				}
				nativeBytes, err := schema.Codec().BinaryFromNative(nil, native)
				if err != nil {
					return err
				}
				recordValue = append(recordValue, nativeBytes...)
			}
		case srclient.Json:
			var v interface{}
			if err := json.Unmarshal(msg, &v); err != nil {
				return err
			}
			if err := schema.JsonSchema().Validate(v); err != nil {
				return err
			}
			recordValue = append(recordValue, msg...)
		case srclient.Protobuf:
			return fmt.Errorf("protobuf schema type is not currently supported")
		default:
			return fmt.Errorf("unknown schema type '%v'", schemaType)
		}
		finalMsgValue = &recordValue
	}

	if err := h.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &h.topic, Partition: kafka.PartitionAny},
		Headers: []kafka.Header{
			{Key: "source", Value: []byte(m.Source)},
			{Key: "id", Value: []byte(m.ID)},
		},
		Value: *finalMsgValue,
	}, deliveryChan); err != nil {
		return err
	}
	if deliveryChan != nil {
		select {
		case <-ctx.Done():
			return fmt.Errorf("failed to get delivery: %w", ctx.Err())
		case e := <-deliveryChan:
			switch ev := e.(type) {
			case *kafka.Message:
				return ev.TopicPartition.Error
			default:
				return fmt.Errorf("failed to read delivery report: %s", e.String())
			}
		}
	}
	return nil
}

func (h *kafkaSink) Close() error {
	logger.Info("flushing producer")
	unflushedMessages := h.producer.Flush(15 * 1000)
	if unflushedMessages > 0 {
		logger.Error(fmt.Errorf("unflushed messagesd %d", unflushedMessages), "failed to flush producer", "sinkName", h.sinkName)
	}
	logger.Info("closing producer")
	h.producer.Close()
	logger.Info("producer closed")
	return nil
}
