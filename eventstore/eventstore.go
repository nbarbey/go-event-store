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
	r, err := repository.NewTypedRepository[E](ctx, connStr, codec.NewJSONCodecWithTypeHints[E](nil))
	return NewEventStoreFromRepository(r), err
}

func NewEventStoreFromRepository[E any](r *repository.TypedRepository[E]) *EventStore[E] {
	return &EventStore[E]{
		Stream: NewStream[E]("default-stream", r),
	}
}

func (e *EventStore[E]) WithCodec(codec codec.TypedCodec[E]) {
	e.Stream = e.Stream.WithCodec(codec)
}
