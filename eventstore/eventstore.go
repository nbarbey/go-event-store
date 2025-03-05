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
	defaultStream *Stream[E]
	*Repository[E]
}

func NewEventStore[E any](ctx context.Context, connStr string) (*EventStore[E], error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	s := EventStore[E]{
		connection: pool,
		listener:   &pgxlisten.Listener{},
	}
	s.Repository = NewRepository[E](s.connection, NewJSONCodec[E]())
	s.defaultStream = NewStream[E]("default-stream", s.Repository, s.listener)
	err = s.createTableAndTrigger(ctx)
	return &s, err
}

func (e *EventStore[E]) Publish(ctx context.Context, event E) (version string, err error) {
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
	var conn *pgxpool.Conn
	e.listener.Connect = func(ctx context.Context) (*pgx.Conn, error) {
		var err error
		conn, err = e.connection.Acquire(ctx)
		return conn.Conn(), err
	}
	e.cancelFunc = func() {
		cancel()
		conn.Release()
	}

	go func() { _ = e.listener.Listen(cancellableContext) }()
	return nil
}

func (e *EventStore[E]) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}

func (e *EventStore[E]) Stream(name string) *Stream[E] {
	return NewStream[E](name, e.Repository, e.defaultStream.listener)
}

func (e *EventStore[E]) WithCodec(codec Codec[E]) {
	e.Repository.WithCodec(codec)
	e.defaultStream = e.Stream("default-stream")
}

func (e *EventStore[E]) SubscribeFromBeginning(ctx context.Context, consumer ConsumerFunc[E]) error {
	return e.defaultStream.SubscribeFromBeginning(ctx, consumer)
}
