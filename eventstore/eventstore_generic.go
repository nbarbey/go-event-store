package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgxlisten"
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

	payloads, err := scanAll(rows)
	if err != nil {
		return nil, err
	}

	return UnmarshallAll[E](e.codec, payloads)
}

func scanAll(rows pgx.Rows) ([][]byte, error) {
	payloads := make([][]byte, 0)
	for rows.Next() {
		var payload []byte
		rowErr := rows.Scan(&payload)
		if rowErr != nil {
			return nil, rowErr
		}
		payloads = append(payloads, payload)
	}
	return payloads, nil
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
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	eventStore := EventStore[E]{
		connection: conn,
		listener: &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, connStr) },
		},
		codec: &JSONCodec[E]{},
	}
	err = eventStore.createTableAndTrigger(ctx)
	return &eventStore, err
}

func (e EventStore[E]) createTableAndTrigger(ctx context.Context) error {
	_, err := e.connection.Exec(ctx, "create table if not exists events (stream_id text, payload jsonb)")
	if err != nil {
		return err
	}
	_, err = e.connection.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.payload::TEXT);
  		return new;
		end;
		$$ language plpgsql;`)
	if err != nil {
		return err
	}
	_, err = e.connection.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	return err
}

func (e EventStore[E]) Stream(name string) *Stream[E] {
	return NewStream[E](name, e.connection, e.codec, e.listener)
}
