package eventstore

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
	"github.com/nbarbey/go-event-store/eventstore/codec"
)

type Listener[E any] struct {
	streamId   string
	listener   *pgxlisten.Listener
	cancelFunc context.CancelFunc
	*Repository[E]
}

func NewListener[E any](streamId string, listener *pgxlisten.Listener, connection *pgxpool.Pool, codec codec.TypedCodec[E]) *Listener[E] {
	return &Listener[E]{
		streamId:   streamId,
		listener:   listener,
		Repository: NewRepository[E](connection, codec).Stream(streamId),
	}
}

type Subscription struct {
	cancel func()
}

func (l *Listener[E]) Subscribe(consumer Consumer[E]) (subscription *Subscription) {
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

func (l *Listener[E]) SubscribeFromBeginning(ctx context.Context, consumer Consumer[E]) (err error) {
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
