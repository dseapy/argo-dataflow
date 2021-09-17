package v1alpha1

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type meta struct{ string }

func (m meta) String() string { return m.string }

var (
	// MetaID is a unique ID for the message.
	// Required.
	// https://github.com/cloudevents/spec/blob/master/spec.md#id
	MetaID = meta{"id"}
	// MetaSource is the source of the messages as a Unique Resource Identifier (URI).
	// Required.
	// https://github.com/cloudevents/spec/blob/master/spec.md#source-1
	MetaSource = meta{"source"}
	// MetaTime is the time of the message. As meta-data, this might be different to the event-time (which might be within the message).
	// For example, it might be the last-modified time of a file, but the file itself was created at another time.
	// Optional.
	// https://github.com/cloudevents/spec/blob/master/spec.md#time
	MetaTime = meta{"time"}
)

func ContextWithMeta(ctx context.Context, source, id string, time time.Time) context.Context {
	return context.WithValue(
		context.WithValue(
			context.WithValue(
				ctx,
				MetaSource,
				source,
			),
			MetaID,
			id,
		),
		MetaTime,
		time,
	)
}

func MetaFromContext(ctx context.Context) (source, id string, t time.Time, err error) {
	source, ok := ctx.Value(MetaSource).(string)
	if !ok {
		return "", "", time.Time{}, fmt.Errorf("failed to get source from context")
	}
	id, ok = ctx.Value(MetaID).(string)
	if !ok {
		return "", "", time.Time{}, fmt.Errorf("failed to get id from context")
	}
	t, ok = ctx.Value(MetaTime).(time.Time)
	if !ok {
		return "", "", time.Time{}, fmt.Errorf("failed to get time from context")
	}
	return source, id, t, nil
}

func MetaInject(ctx context.Context, h http.Header) error {
	source, id, t, err := MetaFromContext(ctx)
	if err != nil {
		return err
	}
	h.Add(MetaSource.String(), source)
	h.Add(MetaID.String(), id)
	h.Add(MetaTime.String(), t.Format(time.RFC3339))
	return nil
}

func MetaExtract(ctx context.Context, h http.Header) context.Context {
	t, _ := time.Parse(time.RFC3339, h.Get(MetaTime.String()))
	return ContextWithMeta(ctx,
		h.Get(MetaSource.String()),
		h.Get(MetaID.String()),
		t,
	)
}
