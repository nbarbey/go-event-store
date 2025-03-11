package eventstore

import (
	"context"
	"github.com/beevik/guid"
	"github.com/nbarbey/go-event-store/eventstore/repository"
)

type Publisher[E any] struct {
	typeHint        string
	expectedVersion string
	*repository.TypedRepository[E]
}

func NewPublisher[E any](streamId string, r *repository.TypedRepository[E]) *Publisher[E] {
	return &Publisher[E]{
		TypedRepository: r.Stream(streamId),
	}
}

func (p *Publisher[E]) WithType(typeHint string) *Publisher[E] {
	return &Publisher[E]{
		expectedVersion: p.expectedVersion,
		typeHint:        typeHint,
		TypedRepository: p.TypedRepository,
	}
}

func (p *Publisher[E]) ExpectedVersion(version string) *Publisher[E] {
	return &Publisher[E]{
		expectedVersion: version,
		typeHint:        p.typeHint,
		TypedRepository: p.TypedRepository,
	}
}

func (p *Publisher[E]) Publish(ctx context.Context, event E) (err error) {
	err = p.InsertEvent(ctx, guid.New().String(), p.typeHint, event, p.expectedVersion)
	return
}
