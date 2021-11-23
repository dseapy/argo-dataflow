package kafka

import (
	"bytes"
	"github.com/riferrei/srclient"
	"github.com/stretchr/testify/assert"
	"io"
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

func Test_convertFromConfluentMessage(t *testing.T) {
	assert.Equal(t, true, true)
}
