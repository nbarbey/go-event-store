package go_event_store

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
	"os"
)

var defaultStream = "events-channel"

type EventStore struct {
	connection *pgx.Conn
	listener   *pgxlisten.Listener
	cancelFunc context.CancelFunc
}

func (e EventStore) Publish(bytes []byte) error {
	_, err := e.connection.Exec(context.Background(), "insert into events values ($1, $2)", defaultStream, bytes)
	return err
}

func (e EventStore) All() ([][]byte, error) {
	rows, err := e.connection.Query(context.Background(), "select payload from events")
	if err != nil {
		return nil, err
	}
	output := make([][]byte, 0)
	for rows.Next() {
		var current []byte
		rowErr := rows.Scan(&current)
		if rowErr != nil {
			return nil, rowErr
		}
		output = append(output, current)
	}
	return output, nil
}

func (e EventStore) Subscribe(consumer Consumer) {
	e.listener.Handle(defaultStream, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		consumer.Consume([]byte(notification.Payload))
		return nil
	}))
}

func (e EventStore) Start(ctx context.Context) error {
	cancellableContext, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	go func() { _ = e.listener.Listen(cancellableContext) }()
	return nil
}

func (e EventStore) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}

func NewEventStore(ctx context.Context, connStr string) (*EventStore, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		return nil, err
	}

	_, err = conn.Exec(ctx, "create table if not exists events (stream_id text, payload jsonb)")
	if err != nil {
		return nil, err
	}
	_, err = conn.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.payload::TEXT);
  		return new;
		end;
		$$ language plpgsql;`)
	if err != nil {
		return nil, err
	}
	_, err = conn.Exec(ctx, `create trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	if err != nil {
		return nil, err
	}
	return &EventStore{
		connection: conn,
		listener: &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, connStr) },
		},
	}, nil
}
