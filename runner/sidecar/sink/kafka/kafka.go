package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	dfv1 "github.com/argoproj-labs/argo-dataflow/api/v1alpha1"
	sharedkafka "github.com/argoproj-labs/argo-dataflow/runner/sidecar/shared/kafka"
	"github.com/argoproj-labs/argo-dataflow/runner/sidecar/sink"
	sharedutil "github.com/argoproj-labs/argo-dataflow/shared/util"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/riferrei/srclient"
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
	confluentSchemaRegistryConverterType dfv1.ConfluentSchemaRegistryConverterType
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
		confluentSchemaRegistryConverterType: x.ConfluentSchemaRegistryConfig.ConverterType,
	}
	return s, nil
}

func convertToConfluentMessageValue(client *srclient.SchemaRegistryClient,
	converterType dfv1.ConfluentSchemaRegistryConverterType,
	topic string,
	msg []byte) ([]byte, error) {
	kafkaMessageValue := &msg
	// Require a schema exist even if writing the raw bytes with a None converter type
	// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work
	schema, err := client.GetLatestSchema(topic + "-value")
	if err != nil {
		return nil, err
	}
	if converterType != dfv1.ConfluentSchemaRegistryConverterTypeNone {
		// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		var kafkaMessageValueBytes []byte
		kafkaMessageValueBytes = append(kafkaMessageValueBytes, byte(0))
		kafkaMessageValueBytes = append(kafkaMessageValueBytes, schemaIDBytes...)

		schemaType := srclient.Avro
		if schema.SchemaType() != nil {
			schemaType = *schema.SchemaType()
		}
		switch schemaType {
		case srclient.Avro:
			if converterType == dfv1.ConfluentSchemaRegistryConverterTypeNative {
				kafkaMessageValueBytes = append(kafkaMessageValueBytes, msg...)
			} else if converterType == dfv1.ConfluentSchemaRegistryConverterTypeJSON {
				native, _, err := schema.Codec().NativeFromTextual(msg)
				if err != nil {
					return nil, err
				}
				nativeBytes, err := schema.Codec().BinaryFromNative(nil, native)
				if err != nil {
					return nil, err
				}
				kafkaMessageValueBytes = append(kafkaMessageValueBytes, nativeBytes...)
			} else {
				return nil, fmt.Errorf("unknown converter type '%v'", converterType)
			}
		case srclient.Json:
			var v interface{}
			if err := json.Unmarshal(msg, &v); err != nil {
				return nil, err
			}
			if err := schema.JsonSchema().Validate(v); err != nil {
				return nil, err
			}
			kafkaMessageValueBytes = append(kafkaMessageValueBytes, msg...)
		case srclient.Protobuf:
			return nil, fmt.Errorf("protobuf schema type is not currently supported")
		default:
			return nil, fmt.Errorf("unknown schema type '%v'", schemaType)
		}
		kafkaMessageValue = &kafkaMessageValueBytes
	}
	return *kafkaMessageValue, nil
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
	kafkaMessageValue := &msg

	if h.confluentSchemaRegistryClient != nil {
		b, err := convertToConfluentMessageValue(h.confluentSchemaRegistryClient, h.confluentSchemaRegistryConverterType, h.topic, msg)
		if err != nil {
			return nil
		}
		kafkaMessageValue = &b
	}

	if err := h.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &h.topic, Partition: kafka.PartitionAny},
		Headers: []kafka.Header{
			{Key: "source", Value: []byte(m.Source)},
			{Key: "id", Value: []byte(m.ID)},
		},
		Value: *kafkaMessageValue,
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
