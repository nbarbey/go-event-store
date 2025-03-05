package eventstore

import (
	"context"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type VersionedPublisher[E any] struct {
	version    string
	streamId   string
	connection *pgxpool.Pool
	codec      Codec[E]
}

func (v VersionedPublisher[E]) Publish(ctx context.Context, event E) error {
	data, err := v.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = v.connection.Exec(ctx,
		"insert into events (event_id, stream_id, version, payload) values ($1, $2, $3, $4)",
		guid.New(), v.streamId, v.version, data)
	return err
}
