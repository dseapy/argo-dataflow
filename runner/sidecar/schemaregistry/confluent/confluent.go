package confluent

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	dfv1 "github.com/argoproj-labs/argo-dataflow/api/v1alpha1"
	"github.com/argoproj-labs/argo-dataflow/runner/sidecar/schemaregistry"
	sharedutil "github.com/argoproj-labs/argo-dataflow/shared/util"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

var logger = sharedutil.NewLogger()

type confluentSchemaRegistry struct {
	name               string
	client             *srclient.SchemaRegistryClient
	valueConverterType dfv1.ConfluentSchemaRegistryConverterType
}

func New(ctx context.Context, schemaRegistryName string, secretInterface corev1.SecretInterface, x dfv1.ConfluentSchemaRegistry) (schemaregistry.Interface, error) {
	client := srclient.CreateSchemaRegistryClient(x.ConfluentSchemaRegistryConfig.URL)
	c := &confluentSchemaRegistry{
		name: ,
		client: client,
		valueConverterType: ,
	}
	return c, nil
}
func (c *confluentSchemaRegistry) FromKafkaMessage(ctx context.Context, kafkaMessage *kafka.Message) ([]byte, error) {
	msgToProcess := &kafkaMessage.Value
	// Require a schema exist even if writing the raw bytes with a None converter type
	// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work
	schemaID := binary.BigEndian.Uint32(kafkaMessage.Value[1:5])
	schema, err := c.client.GetSchema(int(schemaID))
	if err != nil {
		return nil, err
	}
	if c.valueConverterType != dfv1.ConfluentSchemaRegistryConverterTypeNone {
		schemaType := srclient.Avro
		if schema.SchemaType() != nil {
			schemaType = *schema.SchemaType()
		}
		var msgToProcessBytes []byte
		switch schemaType {
		case srclient.Avro:
			if c.valueConverterType == dfv1.ConfluentSchemaRegistryConverterTypeNative {
				msgToProcessBytes = kafkaMessage.Value[5:]
			} else if c.valueConverterType == dfv1.ConfluentSchemaRegistryConverterTypeJSON {
				native, _, err := schema.Codec().NativeFromBinary(kafkaMessage.Value[5:])
				if err != nil {
					return nil, err
				}
				msgToProcessBytes, err = schema.Codec().TextualFromNative(nil, native)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("unknown converter type '%v'", c.valueConverterType)
			}
		case srclient.Json:
			msgValueBytes := kafkaMessage.Value[5:]
			var v interface{}
			if err := json.Unmarshal(msgValueBytes, &v); err != nil {
				return nil, err
			}
			if err := schema.JsonSchema().Validate(v); err != nil {
				return nil, err
			}
		case srclient.Protobuf:
			return nil, fmt.Errorf("protobuf schema type is not currently supported")
		default:
			return nil, fmt.Errorf("unknown schema type '%v'", schemaType)
		}
		msgToProcess = &msgToProcessBytes
	}
	return *msgToProcess, nil
}

func (c *confluentSchemaRegistry) ToKafkaMessage(ctx context.Context, msg []byte, topic string, kafkaMessage *kafka.Message) error {
	// Require a schema exist even if writing the raw bytes with a None converter type
	// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work
	schema, err := c.client.GetLatestSchema(topic + "-value")
	if err != nil {
		return err
	}
	if c.valueConverterType != dfv1.ConfluentSchemaRegistryConverterTypeNone {
		// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
		var newKafkaMessageValue []byte
		newKafkaMessageValue = append(newKafkaMessageValue, byte(0))
		newKafkaMessageValue = append(newKafkaMessageValue, schemaIDBytes...)

		schemaType := srclient.Avro
		if schema.SchemaType() != nil {
			schemaType = *schema.SchemaType()
		}
		switch schemaType {
		case srclient.Avro:
			if c.valueConverterType == dfv1.ConfluentSchemaRegistryConverterTypeNative {
				newKafkaMessageValue = append(newKafkaMessageValue, msg...)
			} else if c.valueConverterType == dfv1.ConfluentSchemaRegistryConverterTypeJSON {
				native, _, err := schema.Codec().NativeFromTextual(msg)
				if err != nil {
					return err
				}
				nativeBytes, err := schema.Codec().BinaryFromNative(nil, native)
				if err != nil {
					return err
				}
				newKafkaMessageValue = append(newKafkaMessageValue, nativeBytes...)
			} else {
				return fmt.Errorf("unknown converter type '%v'", c.valueConverterType)
			}
		case srclient.Json:
			var v interface{}
			if err := json.Unmarshal(msg, &v); err != nil {
				return err
			}
			if err := schema.JsonSchema().Validate(v); err != nil {
				return err
			}
			newKafkaMessageValue = append(newKafkaMessageValue, msg...)
		case srclient.Protobuf:
			return fmt.Errorf("protobuf schema type is not currently supported")
		default:
			return fmt.Errorf("unknown schema type '%v'", schemaType)
		}
		kafkaMessage.Value = newKafkaMessageValue
	}
	return nil
}
