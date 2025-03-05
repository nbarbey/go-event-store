package eventstore

import (
	"context"
	"errors"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Publisher[E any] struct {
	typeHint        string
	expectedVersion string
	streamId        string
	connection      *pgxpool.Pool
	codec           Codec[E]
	*Repository[E]
}

func NewPublisher[E any](streamId string, connection *pgxpool.Pool, codec Codec[E]) *Publisher[E] {
	return &Publisher[E]{
		streamId:   streamId,
		connection: connection,
		codec:      codec,
		Repository: NewRepository[E](connection, codec),
	}
}

func (p *Publisher[E]) WithType(typeHint string) *Publisher[E] {
	return &Publisher[E]{
		streamId:        p.streamId,
		connection:      p.connection,
		codec:           p.codec,
		expectedVersion: p.expectedVersion,
		typeHint:        typeHint,
		Repository:      NewRepository[E](p.connection, p.codec),
	}
}

func (p *Publisher[E]) ExpectedVersion(version string) *Publisher[E] {
	return &Publisher[E]{
		streamId:        p.streamId,
		connection:      p.connection,
		codec:           p.codec,
		expectedVersion: version,
		typeHint:        p.typeHint,
		Repository:      NewRepository[E](p.connection, p.codec),
	}
}

var ErrVersionMismatch = errors.New("mismatched version")

func (p *Publisher[E]) Publish(ctx context.Context, event E) (version string, err error) {
	if p.expectedVersion != "" {
		row := p.connection.QueryRow(ctx,
			"select event_id from events where stream_id=$1 and version=$2",
			p.streamId, p.expectedVersion)
		var eventIDWithExpectedVersion string
		err = row.Scan(&eventIDWithExpectedVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrVersionMismatch
		}
	}

	data, err := p.codec.Marshall(event)
	if err != nil {
		return
	}
	version = guid.New().String()

	err = p.insertEvent(ctx, p.streamId, version, p.typeHint, data)
	return
}
