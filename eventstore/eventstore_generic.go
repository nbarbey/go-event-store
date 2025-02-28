package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
)

var defaultStream = "default-stream"

type GenericEventStore[E any] struct {
	*BaseStore
	codec         Codec[E]
	defaultStream *Stream[E]
}

func NewGenericEventStore[E any](ctx context.Context, connStr string) (*GenericEventStore[E], error) {
	store, err := NewBaseStore(ctx, connStr)
	if err != nil {
		return nil, err
	}
	eventStore := GenericEventStore[E]{
		BaseStore: store,

		codec: &JSONCodec[E]{},
	}
	eventStore.defaultStream = eventStore.Stream(defaultStream)
	return &eventStore, nil
}

func (e GenericEventStore[E]) Publish(ctx context.Context, event E) error {
	return e.defaultStream.Publish(ctx, event)
}

func (e GenericEventStore[E]) Subscribe(consumer Consumer[E]) {
	e.defaultStream.Subscribe(consumer)
}

func (e GenericEventStore[E]) All(ctx context.Context) ([]E, error) {
	rows, err := e.connection.Query(ctx, "select payload from events")
	if err != nil {
		return nil, err
	}
	ers, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	return UnmarshallAll[E](e.codec, eventRows(ers).Payloads())
}

func (e GenericEventStore[E]) Stream(name string) *Stream[E] {
	return NewStream[E](name, e.connection, e.codec, e.listener)
}

type eventRow struct {
	Payload []byte
}

type eventRows []eventRow

func (ers eventRows) Payloads() [][]byte {
	payloads := make([][]byte, 0)
	for _, e := range ers {
		payloads = append(payloads, e.Payload)
	}
	return payloads
}
