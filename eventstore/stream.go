package eventstore

import (
	"context"
	"github.com/jackc/pgxlisten"
)

type Stream[E any] struct {
	*Listener[E]
	*Publisher[E]
}

func NewStream[E any](name string, repo *Repository[E], listener *pgxlisten.Listener) *Stream[E] {
	return &Stream[E]{
		Listener:  NewListener[E](name, listener, repo.connection, repo.codec),
		Publisher: NewPublisher[E](name, repo.connection, repo.codec),
	}
}

func (s Stream[E]) All(ctx context.Context) ([]E, error) {
	return s.Listener.All(ctx)
}
