package go_event_store

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	name       string
	eventStore *EventStore[E]
}

func (s Stream[E]) Publish(event E) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = s.eventStore.connection.Exec(context.Background(), "insert into events values ($1, $2)", s.name, data)
	return err
}

func (s Stream[E]) Subscribe(consumer ConsumerFunc[E]) {
	s.eventStore.listener.Handle(s.name, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		var event E
		err := json.Unmarshal([]byte(notification.Payload), &event)
		if err != nil {
			return err
		}
		consumer.Consume(event)
		return nil
	}))
}
