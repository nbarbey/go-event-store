package eventstore

import (
	"context"
	"errors"
	"github.com/beevik/guid"
)

type Publisher[E any] struct {
	typeHint        string
	expectedVersion string
	streamId        string
	*Repository[E]
}

func NewPublisher[E any](streamId string, repository *Repository[E]) *Publisher[E] {
	return &Publisher[E]{
		streamId:   streamId,
		Repository: NewRepository[E](repository.connection, repository.codec).Stream(streamId),
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
	err = p.InsertEvent(ctx, guid.New().String(), p.typeHint, event, p.expectedVersion)
	return
}
