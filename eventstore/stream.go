package eventstore

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	name string
	*Listener[E]
	defaultPublisher *Publisher[E]
}

func NewStream[E any](name string, connection *pgxpool.Pool, codec Codec[E], listener *pgxlisten.Listener) *Stream[E] {
	return &Stream[E]{
		name:             name,
		Listener:         NewListener[E](name, listener, connection, codec),
		defaultPublisher: NewPublisher[E](name, connection, codec),
	}
}

func (s Stream[E]) Publish(ctx context.Context, event E) (version string, err error) {
	return s.defaultPublisher.Publish(ctx, event)
}

func (s Stream[E]) WithType(typeHint string) *Publisher[E] {
	return NewPublisher[E](s.name, s.defaultPublisher.connection, s.defaultPublisher.codec).WithType(typeHint)
}

func (s Stream[E]) ExpectedVersion(version string) *Publisher[E] {
	return NewPublisher[E](s.name, s.defaultPublisher.connection, s.defaultPublisher.codec).ExpectedVersion(version)
}
