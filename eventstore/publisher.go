package eventstore

import (
	"context"
	"errors"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbarbey/go-event-store/eventstore/codec"
)

type Publisher[E any] struct {
	typeHint        string
	expectedVersion string
	streamId        string
	*Repository[E]
}

func NewPublisher[E any](streamId string, connection *pgxpool.Pool, codec codec.TypedCodec[E]) *Publisher[E] {
	return &Publisher[E]{
		streamId:   streamId,
		Repository: NewRepository[E](connection, codec).Stream(streamId),
	}
}

func (p *Publisher[E]) WithType(typeHint string) *Publisher[E] {
	return &Publisher[E]{
		streamId:        p.streamId,
		expectedVersion: p.expectedVersion,
		typeHint:        typeHint,
		Repository:      p.Repository,
	}
}

func (p *Publisher[E]) ExpectedVersion(version string) *Publisher[E] {
	return &Publisher[E]{
		streamId:        p.streamId,
		expectedVersion: version,
		typeHint:        p.typeHint,
		Repository:      p.Repository,
	}
}

var ErrVersionMismatch = errors.New("mismatched version")

func (p *Publisher[E]) Publish(ctx context.Context, event E) (err error) {
	data, err := p.Repository.codec.Marshall(event)
	if err != nil {
		return
	}
	err = p.insertEvent(ctx, guid.New().String(), p.typeHint, data, p.expectedVersion)
	return
}
