package go_event_store

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
	"os"
)

var defaultStream = "default-stream"

type EventStore[E any] struct {
	connection *pgx.Conn
	listener   *pgxlisten.Listener
	cancelFunc context.CancelFunc
	codec      Codec[E]
}

func (e EventStore[E]) Publish(ctx context.Context, event E) error {
	data, err := e.codec.Marshall(event)
	if err != nil {
		return err
	}
	_, err = e.connection.Exec(ctx, "insert into events values ($1, $2)", defaultStream, data)
	return err
}

func (e EventStore[E]) All(ctx context.Context) ([]E, error) {
	rows, err := e.connection.Query(ctx, "select payload from events")
	if err != nil {
		return nil, err
	}
	output := make([]E, 0)
	for rows.Next() {
		var payload []byte
		rowErr := rows.Scan(&payload)
		if rowErr != nil {
			return nil, rowErr
		}
		event, err := e.codec.Unmarshall(payload)
		if err != nil {
			return nil, err
		}
		output = append(output, event)
	}
	return output, nil
}

func (e EventStore[E]) Subscribe(consumer Consumer[E]) {
	e.listener.Handle(defaultStream, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		payload := notification.Payload
		event, err := e.codec.Unmarshall([]byte(payload))
		if err != nil {
			return err
		}
		consumer.Consume(event)
		return nil
	}))
}

func (e EventStore[E]) Start(ctx context.Context) error {
	cancellableContext, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	go func() { _ = e.listener.Listen(cancellableContext) }()
	return nil
}

func (e EventStore[E]) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
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
	_, err = conn.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	if err != nil {
		return nil, err
	}
	return &EventStore[E]{
		connection: conn,
		listener: &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, connStr) },
		},
		codec: &JSONCodec[E]{},
	}, nil
}

func (e EventStore[E]) Stream(name string) *Stream[E] {
	return &Stream[E]{name: name, eventStore: &e}
}
