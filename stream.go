package go_event_store

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	name       string
	eventStore *EventStore[E]
}

func (s Stream[E]) Publish(ctx context.Context, event E) error {
	data, err := s.eventStore.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = s.eventStore.connection.Exec(ctx, "insert into events values ($1, $2)", s.name, data)
	return err
}

func (s Stream[E]) Subscribe(consumer ConsumerFunc[E]) {
	s.eventStore.listener.Handle(s.name, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		event, err := s.eventStore.codec.Unmarshall([]byte(notification.Payload))
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	}))
}
