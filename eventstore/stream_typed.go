package eventstore

import (
	"context"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TypedPublisher[E any] struct {
	typeHint   string
	streamId   string
	connection *pgxpool.Pool
	codec      Codec[E]
}

func NewTypedPublisher[E any](typeHint string, streamId string, connection *pgxpool.Pool, codec Codec[E]) *TypedPublisher[E] {
	return &TypedPublisher[E]{typeHint: typeHint, streamId: streamId, connection: connection, codec: codec}
}

func (s TypedPublisher[E]) Publish(ctx context.Context, event E) error {
	data, err := s.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = s.connection.Exec(ctx,
		"insert into events (event_id, stream_id, event_type, payload) values ($1, $2, $3, $4)",
		guid.New(), s.streamId, s.typeHint, data)
	return err
}
