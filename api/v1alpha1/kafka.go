package v1alpha1

import (
	"fmt"
)

type KafkaNET struct {
	TLS  *TLS  `json:"tls,omitempty" protobuf:"bytes,1,opt,name=tls"`
	SASL *SASL `json:"sasl,omitempty" protobuf:"bytes,2,opt,name=sasl"`
}

func (in *KafkaNET) GetSecurityProtocol() string {
	if in.SASL == nil && in.TLS == nil {
		return "plaintext"
	} else if in.SASL == nil && in.TLS != nil {
		return "ssl"
	} else if in.TLS == nil {
		return "sasl_plaintext"
	}
	return "sasl_ssl"
}

type KafkaConfig struct {
	Brokers         []string  `json:"brokers,omitempty" protobuf:"bytes,1,rep,name=brokers"`
	NET             *KafkaNET `json:"net,omitempty" protobuf:"bytes,3,opt,name=net"`
	MaxMessageBytes int32     `json:"maxMessageBytes,omitempty" protobuf:"varint,4,opt,name=maxMessageBytes"`
}

func (m *KafkaConfig) GetMessageMaxBytes() int {
	return int(m.MaxMessageBytes)
}

// +kubebuilder:validation:Enum=Raw;Native;JSON
type ConfluentSchemaRegistryMessageFormat string

const (
	ConfluentSchemaRegistryMessageFormatRaw    ConfluentSchemaRegistryMessageFormat = "Raw"    // messages are same bytes stored on topic
	ConfluentSchemaRegistryMessageFormatNative ConfluentSchemaRegistryMessageFormat = "Native" // messages are the payload (ie. AVRO serialized byte array)
	ConfluentSchemaRegistryMessageFormatJSON   ConfluentSchemaRegistryMessageFormat = "JSON"   // messages are the JSON byte array representation of the payload
)

type ConfluentSchemaRegistryConfig struct {
	URL string `json:"url,omitempty" protobuf:"bytes,1,opt,name=url"`
	// +kubebuilder:default="Native"
	MessageFormat ConfluentSchemaRegistryMessageFormat `json:"messageFormat,omitempty" protobuf:"bytes,2,opt,name=messageFormat"`
}

type Kafka struct {
	// +kubebuilder:default=default
	Name                          string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	KafkaConfig                   `json:",inline" protobuf:"bytes,4,opt,name=kafkaConfig"`
	Topic                         string                         `json:"topic" protobuf:"bytes,3,opt,name=topic"`
	ConfluentSchemaRegistryConfig *ConfluentSchemaRegistryConfig `json:"confluentSchemaRegistry,omitempty" protobuf:"bytes,5,opt,name=confluentSchemaRegistry"`
}

func (in Kafka) GenURN(cluster, namespace string) string {
	return fmt.Sprintf("urn:dataflow:kafka:%s:%s", in.Brokers[0], in.Topic)
}
