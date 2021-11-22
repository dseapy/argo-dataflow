package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/riferrei/srclient"
	"io"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"net/http"
	"net/http/httptest"
	"sigs.k8s.io/structured-merge-diff/v4/schema"
	"testing"

	dfv1 "github.com/argoproj-labs/argo-dataflow/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func Test_kafkaFromSecret(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		err := kafkaFromSecret(&dfv1.Kafka{}, &corev1.Secret{})
		assert.NoError(t, err)
	})
	t.Run("Brokers", func(t *testing.T) {
		x := &dfv1.Kafka{}
		err := kafkaFromSecret(x, &corev1.Secret{
			Data: map[string][]byte{
				"brokers": []byte("a,b"),
			},
		})
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"a", "b"}, x.Brokers)
	})
	t.Run("NetTLS", func(t *testing.T) {
		x := &dfv1.Kafka{}
		err := kafkaFromSecret(x, &corev1.Secret{
			Data: map[string][]byte{
				"net.tls.caCert": []byte(""),
			},
		})
		assert.NoError(t, err)
		if assert.NotNil(t, x.NET) {
			assert.NotNil(t, x.NET.TLS)
		}
	})
	t.Run("NetSASL", func(t *testing.T) {
		x := &dfv1.Kafka{}
		err := kafkaFromSecret(x, &corev1.Secret{
			Data: map[string][]byte{
				"net.sasl.user":     []byte(""),
				"net.sasl.password": []byte(""),
			},
		})
		assert.NoError(t, err)
		if assert.NotNil(t, x.NET) {
			assert.NotNil(t, x.NET.SASL)
		}
	})
}

func Test_enrichKafka(t *testing.T) {
	t.Run("NotFound", func(t *testing.T) {
		k := fake.NewSimpleClientset()
		secretInterface = k.CoreV1().Secrets("")
		x := &dfv1.Kafka{}
		err := enrichKafka(context.Background(), x)
		assert.NoError(t, err)
	})
	t.Run("Found", func(t *testing.T) {
		k := fake.NewSimpleClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dataflow-kafka-foo",
			},
			Data: map[string][]byte{
				"commitN": []byte("123"),
			},
		})
		secretInterface = k.CoreV1().Secrets("")
		x := &dfv1.Kafka{Name: "foo"}
		err := enrichKafka(context.Background(), x)
		assert.NoError(t, err)
	})
}

func bodyToString(in io.ReadCloser) string {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(in)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func Test_confluentSchemaRegistry(t *testing.T) {
	type schemaResponse struct {
		Subject    string      `json:"subject"`
		Version    int         `json:"version"`
		Schema     string      `json:"schema"`
		SchemaType *srclient.SchemaType `json:"schemaType"`
		ID         int         `json:"id"`
		References []srclient.Reference `json:"references"`
	}
	type schemaRequest struct {
		Schema     string      `json:"schema"`
		SchemaType string      `json:"schemaType"`
		References []srclient.Reference `json:"references"`
	}
	jsonSchemaType := srclient.Json
	//avroSchemaType := srclient.Avro
	//protobufSchemaType := srclient.Protobuf
	t.Run("Produce with Schema of type JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			responsePayload := schemaResponse{
				Subject: "test1-value",
				Version: 1,
				Schema:  "{\"type\":\"object\",\"properties\":{\"f1\":{\"type\":\"string\"}}}",
				SchemaType: &jsonSchemaType,
				ID:      1,
			}
			response, _ := json.Marshal(responsePayload)
			switch req.URL.String() {
			case "/subjects/test1-value":
				requestPayload := schemaRequest{
					Schema:     "test1-value",
					SchemaType: srclient.Json.String(),
					References: []srclient.Reference{},
				}
				expected, _ := json.Marshal(requestPayload)
				// test payload
				assert.Equal(t, bodyToString(req.Body), string(expected))
				// response to be tested
				_, err := rw.Write(response)
				if err != nil {
					panic(err)
				}
			default:
				assert.Error(t, errors.New("unhandled request"))
			}
		}))
		srClient := srclient.CreateSchemaRegistryClient(server.URL)
		kafkaBrokers := ...
		kafkaConfig := dfv1.KafkaConfig{
			Brokers := []{kafkaBrokers},
			NET: nil,
			MaxMessageBytes: 1000000,
		}
		confluentSchemaRegistryConfig := dfv1.ConfluentSchemaRegistryConfig{
			URL: server.URL,
			ConverterType: dfv1.ConfluentSchemaRegistryConverterTypeJSON,
		}
		kafka := dfv1.Kafka{
			Name: "test-kafka",
			KafkaConfig: kafkaConfig,
			Topic: "test1-topic",
			ConfluentSchemaRegistryConfig: &confluentSchemaRegistryConfig,
		}
		batchSize := resource.MustParse("100Ki")
		acks := intstr.IntOrString{StrVal: "all"}
		kafkaSource := dfv1.KafkaSink{
			Kafka: kafka,
			Async: false,
			BatchSize: &batchSize,
			Linger: nil,
			CompressionType: "lz4",
			Acks: &acks,
			EnableIdempotence: true,
		}

		// Test response
		assert.NoError(t, err)
		assert.Equal(t, schema.ID(), 1)
		assert.Nil(t, schema.Codec())
		assert.Equal(t, schema.Schema(), "test2")
		assert.Equal(t, schema.Version(), 1)
	})
}