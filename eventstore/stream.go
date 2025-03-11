package eventstore

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore/codec"
)

type Stream[E any] struct {
	*Listener[E]
	*Publisher[E]
}

func NewStream[E any](name string, repo *Repository[E]) *Stream[E] {
	return &Stream[E]{
		Listener:  NewListener[E](name, repo),
		Publisher: NewPublisher[E](name, repo),
	}
}

func (s Stream[E]) All(ctx context.Context) ([]E, error) {
	return s.Listener.All(ctx)
}

func (s Stream[E]) GetStream(name string) *Stream[E] {
	return NewStream[E](name, s.Listener.Repository)
}

func (s Stream[E]) WithCodec(codec codec.TypedCodec[E]) *Stream[E] {
	return NewStream[E](s.Listener.streamId, s.Listener.Repository.WithCodec(codec))
}

func (s Stream[E]) Version(ctx context.Context) (string, error) {
	return s.Listener.Repository.Version(ctx)
}
