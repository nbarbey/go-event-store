package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
	"github.com/nbarbey/go-event-store/eventstore/codec"
)

type EventStore[E any] struct {
	*Stream[E]
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	repository := NewRepository[E](pool, codec.NewJSONCodecWithTypeHints[E](nil))
	err = repository.createTableAndTrigger(ctx)
	return &EventStore[E]{
		Stream: NewStream[E]("default-stream", repository, &pgxlisten.Listener{}),
	}, err
}

func (e *EventStore[E]) WithCodec(codec codec.Codec[E]) {
	e.Stream = e.Stream.WithCodec(codec)
}
