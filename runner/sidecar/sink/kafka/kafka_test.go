package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	dfv1 "github.com/argoproj-labs/argo-dataflow/api/v1alpha1"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func bodyToString(in io.ReadCloser) string {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(in)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
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

func Test_convertToConfluentMessageValue(t *testing.T) {
	jsonSchemaType := srclient.Json
	test1Topic := "test1"
	test1Subject := test1Topic + "-value"
	t.Run("Convert to Confluent-JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			responsePayload := schemaResponse{
				Subject: test1Subject,
				Version: 1,
				Schema:  "{\"type\":\"object\",\"properties\":{\"f1\":{\"type\":\"string\"}}}",
				SchemaType: &jsonSchemaType,
				ID:      1,
			}
			response, _ := json.Marshal(responsePayload)
			switch req.URL.String() {
			case "/subjects/" + test1Subject:
				requestPayload := schemaRequest{
					Schema:     test1Subject,
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

		// None should return the same bytes
		jsonMessage := "{\"f1\": \"v1\"}"

		kafkaMsgValue, err := convertToConfluentMessageValue(
			srClient,
			dfv1.ConfluentSchemaRegistryConverterTypeNone,
			test1Topic,
			[]byte(jsonMessage))
		assert.NoError(t, err)
		assert.Equal(t, string(kafkaMsgValue), jsonMessage)
	})
}