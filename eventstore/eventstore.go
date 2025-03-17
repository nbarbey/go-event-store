package eventstore

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	repository "github.com/nbarbey/go-event-store/eventstore/repository"
)

type EventStore[E any] struct {
	*Stream[E]
}

func NewPostgresEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	pg, err := repository.NewPostgres(ctx, connStr)
	r := repository.NewTypedRepository[E](pg, codec.NewGobCodecWithTypeHints[E](nil))
	return NewEventStoreFromRepository(r), err
}

func NewInMemoryEventStore[E any]() *EventStore[E] {
	return NewEventStoreFromRepository(
		repository.NewTypedRepository[E](
			repository.NewInMemory(), codec.NewGobCodecWithTypeHints[E](nil)))
}

func NewEventStoreFromRepository[E any](r *repository.TypedRepository[E]) *EventStore[E] {
	return &EventStore[E]{
		Stream: NewStream[E]("default-stream", r),
	}
}

func (e *EventStore[E]) WithCodec(codec codec.TypedCodec[E]) {
	e.Stream = e.Stream.WithCodec(codec)
}
