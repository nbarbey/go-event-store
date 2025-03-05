package eventstore

import (
	"context"
	"errors"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type VersionedPublisher[E any] struct {
	expectedVersion string
	streamId        string
	connection      *pgxpool.Pool
	codec           Codec[E]
}

var ErrVersionMismatch = errors.New("mismatched version")

func (v VersionedPublisher[E]) Publish(ctx context.Context, event E) (version string, err error) {
	if v.expectedVersion != "" {
		row := v.connection.QueryRow(ctx, "select event_id from events where stream_id=$1 and version=$2", v.streamId, v.expectedVersion)
		var eventIDWithExpectedVersion string
		err = row.Scan(&eventIDWithExpectedVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrVersionMismatch
		}
	}
	data, err := v.codec.Marshall(event)
	if err != nil {
		return
	}
	version = guid.New().String()
	_, err = v.connection.Exec(ctx,
		"insert into events (event_id, stream_id, version, payload) values ($1, $2, $3, $4)",
		guid.New(), v.streamId, version, data)
	return
}
