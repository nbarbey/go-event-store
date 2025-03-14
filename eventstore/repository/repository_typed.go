package repository

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
)

type TypedRepository[E any] struct {
	*Repository
	codec *codec.Versioned[E]
}

func NewTypedRepository[E any](ctx context.Context, connStr string, c codec.TypedCodec[E]) (*TypedRepository[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	return &TypedRepository[E]{Repository: NewRepository(pool), codec: &codec.Versioned[E]{TypedCodec: c}}, nil
}

func (r *TypedRepository[E]) WithCodec(c codec.TypedCodec[E]) *TypedRepository[E] {
	r.codec = &codec.Versioned[E]{TypedCodec: c}
	return r
}

func (r *TypedRepository[E]) Stream(name string) *TypedRepository[E] {
	return &TypedRepository[E]{Repository: NewRepository(r.connection).Stream(name), codec: r.codec}
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

func (r *TypedRepository[E]) All(ctx context.Context) ([]E, error) {
	raws, err := r.AllRawEvents(ctx)
	if err != nil {
		return nil, err
	}
	return r.rawsToEvents(raws)
}

func (r *TypedRepository[E]) InsertEvent(ctx context.Context, version, typeHint string, event E, expectedVersion string) error {
	data, err := r.codec.Marshall(event)
	if err != nil {
		return err
	}
	return r.InsertRawEvent(ctx, RawEvent{EventType: typeHint, Version: version, Payload: data}, expectedVersion)
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

func (r *TypedRepository[E]) rawToEvent(raw *RawEvent) (event E, err error) {
	event, err = r.codec.UnmarshallWithType(raw.EventType, raw.Payload)
	versioned, ok := any(&event).(VersionSetter)
	if ok {
		versioned.SetVersion(raw.Version)
	}
	return event, err
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
