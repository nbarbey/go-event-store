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

func NewPublisher[E any](streamId string, repo *Repository[E]) *Publisher[E] {
	return &Publisher[E]{
		streamId:   streamId,
		Repository: repo,
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

func (p *Publisher[E]) Publish(ctx context.Context, event E) (version string, err error) {
	data, err := p.Repository.codec.Marshall(event)
	if err != nil {
		return
	}
	version = guid.New().String()

	err = p.insertEvent(ctx, p.streamId, version, p.typeHint, data, p.expectedVersion)
	return
}
