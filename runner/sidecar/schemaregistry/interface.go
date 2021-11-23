package schemaregistry

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// TODO: For Kafka, add support for msgKey []byte and msg or msgKeyValue []byte, instead of a single byte[] for value (support for keys will allow better control over message-partition mapping and support topic compaction
type Interface interface {
	// FromKafkaMessage Returns the value from the kafkaMessage, does not modify the kafkaMessage
	FromKafkaMessage(ctx context.Context, kafkaMessage *kafka.Message) ([]byte, error)
	// ToKafkaMessage Modifies the kafkaMessage to set any necessary value/headers, does not modify "msg"
	ToKafkaMessage(ctx context.Context, msg []byte, topic string, kafkaMessage *kafka.Message) error
}
