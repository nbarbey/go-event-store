package repository

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
)

type TypedRepository[E any] struct {
	Repository
	codec      *codec.Versioned[E]
	streamId   string
	connection *pgxpool.Pool
}

func NewTypedRepository[E any](ctx context.Context, connStr string, c codec.TypedCodec[E]) (*TypedRepository[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	return &TypedRepository[E]{
		Repository: NewPostgres(pool),
		codec:      &codec.Versioned[E]{TypedCodec: c},
		streamId:   "default-stream",
		connection: pool,
	}, nil
}

func (tr *TypedRepository[E]) WithCodec(c codec.TypedCodec[E]) *TypedRepository[E] {
	tr.codec = &codec.Versioned[E]{TypedCodec: c}
	return tr
}

func (tr *TypedRepository[E]) Stream(name string) *TypedRepository[E] {
	return &TypedRepository[E]{
		Repository: tr.Repository.Stream(name),
		codec:      tr.codec,
		connection: tr.connection,
		streamId:   name,
	}
}

type VersionSetter interface {
	SetVersion(version string)
}

func (tr *TypedRepository[E]) GetEvent(ctx context.Context, eventId string) (event E, err error) {
	raw, err := tr.GetRawEvent(ctx, eventId)
	if err != nil {
		return
	}
	return tr.rawToEvent(raw)
}

func (tr *TypedRepository[E]) All(ctx context.Context) ([]E, error) {
	raws, err := tr.AllRawEvents(ctx)
	if err != nil {
		return nil, err
	}
	return tr.rawsToEvents(raws)
}

func (tr *TypedRepository[E]) InsertEvent(ctx context.Context, version, typeHint string, event E, expectedVersion string) error {
	data, err := tr.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = tr.InsertRawEvent(ctx, RawEvent{EventType: typeHint, Version: version, Payload: data}, expectedVersion)
	return err
}

func (tr *TypedRepository[E]) BuildListener(consumer consumer.Consumer[E]) *Listener {
	listener := NewListener(tr.streamId, tr.connection)

	listener.Handle(func(ctx context.Context, eventId string) error {
		event, err := tr.GetEvent(ctx, eventId)
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	})
	return listener
}

func (tr *TypedRepository[E]) rawToEvent(raw *RawEvent) (event E, err error) {
	event, err = tr.codec.UnmarshallWithType(raw.EventType, raw.Payload)
	versioned, ok := any(&event).(VersionSetter)
	if ok {
		versioned.SetVersion(raw.Version)
	}
	return event, err
}

func (tr *TypedRepository[E]) rawsToEvents(raws []*RawEvent) (events []E, err error) {
	for _, raw := range raws {
		event, err := tr.rawToEvent(raw)
		if err != nil {
			return events, err
		}
		events = append(events, event)
	}
	return
}
