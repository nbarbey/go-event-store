package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	name             string
	connection       *pgxpool.Pool
	codec            Codec[E]
	listener         *pgxlisten.Listener
	defaultPublisher *TypedPublisher[E]
}

func NewStream[E any](name string, connection *pgxpool.Pool, codec Codec[E], listener *pgxlisten.Listener) *Stream[E] {
	return &Stream[E]{
		name:             name,
		connection:       connection,
		codec:            codec,
		listener:         listener,
		defaultPublisher: NewTypedPublisher[E]("", name, connection, codec),
	}
}

func (s Stream[E]) Publish(ctx context.Context, event E) error {
	return s.defaultPublisher.Publish(ctx, event)
}

func (s Stream[E]) Subscribe(consumer Consumer[E]) {
	s.listener.Handle(s.name, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		event, err := s.getEvent(ctx, notification.Payload)
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	}))
}

func (s Stream[E]) SubscribeFromBeginning(ctx context.Context, consumer Consumer[E]) (err error) {
	events, err := s.All(ctx)
	if err != nil {
		return err
	}
	for _, e := range events {
		consumer.Consume(e)
	}
	s.Subscribe(consumer)
	return nil
}

func (s Stream[E]) getEvent(ctx context.Context, eventId string) (event E, err error) {
	row := s.connection.QueryRow(ctx, "select event_type, payload from events where event_id=$1 and stream_id=$2", eventId, s.name)
	var er eventRow
	err = row.Scan(&er.EventType, &er.Payload)
	if err != nil {
		return event, err
	}
	return s.codec.UnmarshallWithType(er.EventType.String, er.Payload)
}

func (s Stream[E]) All(ctx context.Context) ([]E, error) {
	rows, err := s.connection.Query(ctx, "select event_type, payload from events where stream_id=$1", s.name)
	if err != nil {
		return nil, err
	}
	sliceOfEventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	ers := eventRows(sliceOfEventRows)
	return UnmarshallAllWithType[E](s.codec, ers.types(), ers.payloads())
}

func (s Stream[E]) WithType(typeHint string) *TypedPublisher[E] {
	return NewTypedPublisher[E](typeHint, s.name, s.connection, s.codec)
}

func (s Stream[E]) ExpectedVersion(version string) *VersionedPublisher[E] {
	return &VersionedPublisher[E]{
		expectedVersion: version,
		streamId:        s.name,
		connection:      s.connection,
		codec:           s.codec,
	}
}
