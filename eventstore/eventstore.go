package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type EventStore[E any] struct {
	*Stream[E]
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	repository := NewRepository[E](pool, NewJSONCodec[E]())
	err = repository.createTableAndTrigger(ctx)
	return &EventStore[E]{
		Stream: NewStream[E]("default-stream", repository, &pgxlisten.Listener{}),
	}, err
}

func (e *EventStore[E]) GetStream(name string) *Stream[E] {
	return e.Stream.New(name)
}

func (e *EventStore[E]) WithCodec(codec Codec[E]) {
	e.Stream = e.Stream.WithCodec(codec)
}
