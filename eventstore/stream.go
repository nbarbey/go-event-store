package eventstore

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	*Listener[E]
	*Publisher[E]
}

func NewStream[E any](name string, connection *pgxpool.Pool, codec Codec[E], listener *pgxlisten.Listener) *Stream[E] {
	return &Stream[E]{
		Listener:  NewListener[E](name, listener, connection, codec),
		Publisher: NewPublisher[E](name, connection, codec),
	}
}
