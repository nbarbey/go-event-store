package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type EventStore[E any] struct {
	connection    *pgxpool.Pool
	listener      *pgxlisten.Listener
	cancelFunc    context.CancelFunc
	codec         Codec[E]
	defaultStream *Stream[E]
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	eventStore := EventStore[E]{
		connection: pool,
		listener: &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, connStr) },
		},
		codec: NewJSONCodec[E](),
	}
	eventStore.defaultStream = eventStore.Stream("default-stream")
	err = eventStore.createTableAndTrigger(ctx)
	return &eventStore, err
}

func (e *EventStore[E]) Publish(ctx context.Context, event E) error {
	return e.defaultStream.Publish(ctx, event)
}

func (e *EventStore[E]) Subscribe(consumer Consumer[E]) {
	e.defaultStream.Subscribe(consumer)
}

func (e *EventStore[E]) All(ctx context.Context) ([]E, error) {
	return e.defaultStream.All(ctx)
}

func (e *EventStore[E]) Start(ctx context.Context) error {
	cancellableContext, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	go func() { _ = e.listener.Listen(cancellableContext) }()
	return nil
}

func (e *EventStore[E]) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}

func (e *EventStore[E]) createTableAndTrigger(ctx context.Context) error {
	_, err := e.connection.Exec(ctx, "create table if not exists events (event_id text, stream_id text, event_type text, payload text)")
	if err != nil {
		return err
	}
	_, err = e.connection.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.event_id);
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

func (e *EventStore[E]) Stream(name string) *Stream[E] {
	return NewStream[E](name, e.connection, e.codec, e.listener)
}

func (e *EventStore[E]) WithCodec(codec Codec[E]) {
	e.codec = codec
	e.defaultStream = e.Stream("default-stream")
}

type eventRow struct {
	EventType string
	Payload   []byte
}

type eventRows []eventRow

func (ers eventRows) payloads() [][]byte {
	payloads := make([][]byte, 0)
	for _, e := range ers {
		payloads = append(payloads, e.Payload)
	}
	return payloads
}

func (ers eventRows) types() []string {
	types := make([]string, 0)
	for _, e := range ers {
		types = append(types, e.EventType)
	}
	return types
}
