package repository

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
)

type TypedRepository[E any] struct {
	*Repository
	codec *codec.Versioned[E]
}

func NewTypedRepository[E any](connection *pgxpool.Pool, c codec.TypedCodec[E]) *TypedRepository[E] {
	return &TypedRepository[E]{Repository: NewRepository(connection), codec: &codec.Versioned[E]{TypedCodec: c}}
}

func (r *TypedRepository[E]) WithCodec(c codec.TypedCodec[E]) *TypedRepository[E] {
	r.codec = &codec.Versioned[E]{TypedCodec: c}
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
	raw, err := r.GetRawEvent(ctx, eventId)
	if err != nil {
		return
	}
	return r.rawToEvent(raw)
}

func (r *TypedRepository[E]) rawToEvent(raw *RawEvent) (event E, err error) {
	event, err = r.codec.UnmarshallWithType(raw.EventType, raw.Payload)
	versioned, ok := any(&event).(VersionSetter)
	if ok {
		versioned.SetVersion(raw.Version)
	}
	return event, err
}

func (r *TypedRepository[E]) All(ctx context.Context) ([]E, error) {
	raws, err := r.AllRawEvents(ctx)
	if err != nil {
		return nil, err
	}
	return r.rawsToEvents(raws)
}

func (r *TypedRepository[E]) rawsToEvents(raws []*RawEvent) (events []E, err error) {
	for _, raw := range raws {
		event, err := r.rawToEvent(raw)
		if err != nil {
			return events, err
		}
		events = append(events, event)
	}
	return
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
