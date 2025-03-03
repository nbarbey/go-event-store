package eventstore

import (
	"context"
	"fmt"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	name       string
	connection *pgxpool.Pool
	codec      Codec[E]
	listener   *pgxlisten.Listener
}

func NewStream[E any](name string, connection *pgxpool.Pool, codec Codec[E], listener *pgxlisten.Listener) *Stream[E] {
	return &Stream[E]{name: name, connection: connection, codec: codec, listener: listener}
}

func (s Stream[E]) Publish(ctx context.Context, event E) error {
	data, err := s.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = s.connection.Exec(ctx, "insert into events (event_id, stream_id, payload) values ($1, $2, $3)", guid.New(), s.name, data)
	return err
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

func (s Stream[E]) getEvent(ctx context.Context, eventId string) (event E, err error) {
	row := s.connection.QueryRow(ctx, "select payload from events where event_id=$1 and stream_id=$2", eventId, s.name)
	var payload []byte
	err = row.Scan(&payload)
	if err != nil {
		return event, err
	}
	return s.codec.Unmarshall(payload)
}

func (s Stream[E]) All(ctx context.Context) ([]E, error) {
	rows, err := s.connection.Query(ctx, "select payload from events where stream_id=$1", s.name)
	if err != nil {
		return nil, err
	}
	ers, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	return UnmarshallAll[E](s.codec, eventRows(ers).payloads())
}
