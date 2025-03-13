package eventstore

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/repository"
)

type Stream[E any] struct {
	name string
	*Listener[E]
	*Publisher[E]
}

func NewStream[E any](name string, repo *repository.TypedRepository[E]) *Stream[E] {
	return &Stream[E]{
		name:      name,
		Listener:  NewListener[E](name, repo),
		Publisher: NewPublisher[E](name, repo),
	}
}

func (s Stream[E]) GetStream(name string) *Stream[E] {
	return NewStream[E](name, s.Listener.TypedRepository)
}

func (s Stream[E]) WithCodec(codec codec.TypedCodec[E]) *Stream[E] {
	return NewStream[E](s.name, s.Listener.TypedRepository.WithCodec(codec))
}

func (s Stream[E]) Version(ctx context.Context) (string, error) {
	return s.Listener.TypedRepository.Version(ctx)
}
