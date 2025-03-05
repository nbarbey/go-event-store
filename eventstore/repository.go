package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository[E any] struct {
	streamId   string
	connection *pgxpool.Pool
	codec      Codec[E]
}

func NewRepository[E any](connection *pgxpool.Pool, codec Codec[E]) *Repository[E] {
	return &Repository[E]{connection: connection, codec: codec}
}

func (r Repository[E]) Stream(name string) *Repository[E] {
	return &Repository[E]{connection: r.connection, codec: r.codec, streamId: name}
}

func (r Repository[E]) getEvent(ctx context.Context, eventId string) (event E, err error) {
	row := r.connection.QueryRow(ctx, "select event_type, payload from events where event_id=$1 and stream_id=$2", eventId, r.streamId)
	var er eventRow
	err = row.Scan(&er.EventType, &er.Payload)
	if err != nil {
		return event, err
	}
	return r.codec.UnmarshallWithType(er.EventType.String, er.Payload)
}

func (r Repository[E]) All(ctx context.Context) ([]E, error) {
	rows, err := r.connection.Query(ctx, "select event_type, payload from events where stream_id=$1", r.streamId)
	if err != nil {
		return nil, err
	}
	sliceOfEventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	ers := eventRows(sliceOfEventRows)
	return UnmarshallAllWithType[E](r.codec, ers.types(), ers.payloads())
}
