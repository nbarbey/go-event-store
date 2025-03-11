package eventstore

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
	"github.com/nbarbey/go-event-store/eventstore/repository"
)

type Listener[E any] struct {
	cancelFunc context.CancelFunc
	*repository.TypedRepository[E]
}

func NewListener[E any](streamId string, r *repository.TypedRepository[E]) *Listener[E] {
	return &Listener[E]{
		TypedRepository: r.Stream(streamId),
	}
}

type Subscription struct {
	cancel func()
}

func (l *Listener[E]) Subscribe(consumer consumer.Consumer[E]) (subscription *Subscription) {
	listener := l.BuildListener(consumer)

	subscription = &Subscription{}
	var ctx context.Context
	ctx, subscription.cancel = context.WithCancel(context.Background())
	go func() { _ = listener.Listen(ctx) }()
	return subscription
}

func (s Subscription) Cancel() {
	s.cancel()
}

func (l *Listener[E]) SubscribeFromBeginning(ctx context.Context, consumer consumer.Consumer[E]) (err error) {
	events, err := l.All(ctx)
	if err != nil {
		return err
	}
	for _, e := range events {
		consumer.Consume(e)
	}
	l.Subscribe(consumer)
	return nil
}
