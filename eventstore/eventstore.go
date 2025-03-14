package eventstore

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	repository "github.com/nbarbey/go-event-store/eventstore/repository"
)

type EventStore[E any] struct {
	*Stream[E]
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	r, err := repository.NewTypedRepository[E](context.Background(), connStr, codec.NewJSONCodecWithTypeHints[E](nil))
	if err != nil {
		return nil, err
	}
	tr, err := r.CreateTableAndTrigger(ctx)
	return &EventStore[E]{
		Stream: NewStream[E]("default-stream", tr),
	}, err
}

func (e *EventStore[E]) WithCodec(codec codec.TypedCodec[E]) {
	e.Stream = e.Stream.WithCodec(codec)
}
