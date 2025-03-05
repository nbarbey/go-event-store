package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Listener[E any] struct {
	streamId   string
	listener   *pgxlisten.Listener
	connection *pgxpool.Pool
	codec      Codec[E]
}

func NewListener[E any](streamId string, listener *pgxlisten.Listener, connection *pgxpool.Pool, codec Codec[E]) *Listener[E] {
	return &Listener[E]{streamId: streamId, listener: listener, connection: connection, codec: codec}
}

func (l Listener[E]) Subscribe(consumer Consumer[E]) {
	l.listener.Handle(l.streamId, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		event, err := l.getEvent(ctx, notification.Payload)
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	}))
}

func (l Listener[E]) SubscribeFromBeginning(ctx context.Context, consumer Consumer[E]) (err error) {
	events, err := l.All(ctx)
	if err != nil {
		return err
	}
	for _, e := range events {
		consumer.Consume(e)
	}
	l.Subscribe(consumer)
	return nil
}

func (l Listener[E]) getEvent(ctx context.Context, eventId string) (event E, err error) {
	row := l.connection.QueryRow(ctx, "select event_type, payload from events where event_id=$1 and stream_id=$2", eventId, l.streamId)
	var er eventRow
	err = row.Scan(&er.EventType, &er.Payload)
	if err != nil {
		return event, err
	}
	return l.codec.UnmarshallWithType(er.EventType.String, er.Payload)
}

func (l Listener[E]) All(ctx context.Context) ([]E, error) {
	rows, err := l.connection.Query(ctx, "select event_type, payload from events where stream_id=$1", l.streamId)
	if err != nil {
		return nil, err
	}
	sliceOfEventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	ers := eventRows(sliceOfEventRows)
	return UnmarshallAllWithType[E](l.codec, ers.types(), ers.payloads())
}
