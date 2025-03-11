package eventstore

import (
	"context"
	"github.com/beevik/guid"
	"github.com/nbarbey/go-event-store/eventstore/repository"
)

type Publisher[E any] struct {
	typeHint        string
	expectedVersion string
	*repository.Repository[E]
}

func NewPublisher[E any](streamId string, r *repository.Repository[E]) *Publisher[E] {
	return &Publisher[E]{
		Repository: r.Stream(streamId),
	}
}

func (p *Publisher[E]) WithType(typeHint string) *Publisher[E] {
	return &Publisher[E]{
		expectedVersion: p.expectedVersion,
		typeHint:        typeHint,
		Repository:      p.Repository,
	}
}

func (p *Publisher[E]) ExpectedVersion(version string) *Publisher[E] {
	return &Publisher[E]{
		expectedVersion: version,
		typeHint:        p.typeHint,
		Repository:      p.Repository,
	}
}

func (p *Publisher[E]) Publish(ctx context.Context, event E) (err error) {
	err = p.InsertEvent(ctx, guid.New().String(), p.typeHint, event, p.expectedVersion)
	return
}
