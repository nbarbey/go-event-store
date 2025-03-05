package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type EventStore[E any] struct {
	*Stream[E]
	*Repository[E]
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	repository := NewRepository[E](pool, NewJSONCodec[E]())
	err = repository.createTableAndTrigger(ctx)
	return &EventStore[E]{
		Repository: repository,
		Stream:     NewStream[E]("default-stream", repository, &pgxlisten.Listener{}),
	}, err
}

func (e *EventStore[E]) All(ctx context.Context) ([]E, error) {
	return e.Stream.All(ctx)
}

func (e *EventStore[E]) GetStream(name string) *Stream[E] {
	return NewStream[E](name, e.Repository, e.Stream.listener)
}

func (e *EventStore[E]) WithCodec(codec Codec[E]) {
	e.Repository.WithCodec(codec)
	e.Stream = e.GetStream("default-stream")
}
