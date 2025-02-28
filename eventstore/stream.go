package eventstore

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	name       string
	connection *pgx.Conn
	codec      Codec[E]
	listener   *pgxlisten.Listener
}

func NewStream[E any](name string, connection *pgx.Conn, codec Codec[E], listener *pgxlisten.Listener) *Stream[E] {
	return &Stream[E]{name: name, connection: connection, codec: codec, listener: listener}
}

func (s Stream[E]) Publish(ctx context.Context, event E) error {
	data, err := s.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = s.connection.Exec(ctx, "insert into events values ($1, $2)", s.name, data)
	return err
}

func (s Stream[E]) Subscribe(consumer Consumer[E]) {
	s.listener.Handle(s.name, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		event, err := s.codec.Unmarshall([]byte(notification.Payload))
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	}))
}
