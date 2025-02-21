package go_event_store

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
)

type Stream struct {
	name       string
	eventStore *EventStore
}

func (s Stream) Publish(bytes []byte) error {
	_, err := s.eventStore.connection.Exec(context.Background(), "insert into events values ($1, $2)", s.name, bytes)
	return err
}

func (s Stream) Subscribe(consumer ConsumerFunc) {
	s.eventStore.listener.Handle(s.name, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		consumer.Consume([]byte(notification.Payload))
		return nil
	}))
}
