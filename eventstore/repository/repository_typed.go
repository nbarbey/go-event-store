package repository

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
)

type TypedRepository[E any] struct {
	*Repository
	codec codec.Versioned[E]
}

func NewTypedRepository[E any](connection *pgxpool.Pool, c codec.TypedCodec[E]) *TypedRepository[E] {
	return &TypedRepository[E]{Repository: NewRepository(connection), codec: codec.Versioned[E]{TypedCodec: c}}
}

func (r *TypedRepository[E]) WithCodec(c codec.TypedCodec[E]) *TypedRepository[E] {
	r.codec = codec.Versioned[E]{TypedCodec: c}
	return r
}

func (r *TypedRepository[E]) Stream(name string) *TypedRepository[E] {
	repository := NewRepository(r.connection)
	repository.streamId = name
	return &TypedRepository[E]{Repository: repository, codec: r.codec}
}

type VersionSetter interface {
	SetVersion(version string)
}

func (r *TypedRepository[E]) GetEvent(ctx context.Context, eventId string) (event E, err error) {
	payload, typeHint, version, err := r.GetPayload(ctx, eventId)
	if err != nil {
		return event, err
	}
	return r.codec.UnmarshallWithTypeAndVersion(version, typeHint, payload)
}

func (r *TypedRepository[E]) All(ctx context.Context) ([]E, error) {
	types, payloads, err := r.AllTypesAndPayloads(ctx)
	if err != nil {
		return nil, err
	}
	return codec.UnmarshallAllWithType[E](r.codec, types, payloads)
}

func (r *TypedRepository[E]) InsertEvent(ctx context.Context, version, typeHint string, event E, expectedVersion string) error {
	data, err := r.codec.Marshall(event)
	if err != nil {
		return err
	}
	return r.InsertPayload(ctx, version, typeHint, expectedVersion, data)
}

func (r *TypedRepository[E]) BuildListener(consumer consumer.Consumer[E]) *Listener {
	listener := NewListener(r.streamId, r.connection)

	listener.Handle(func(ctx context.Context, eventId string) error {
		event, err := r.GetEvent(ctx, eventId)
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	})
	return listener
}
