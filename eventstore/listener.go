package eventstore

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Listener[E any] struct {
	streamId   string
	listener   *pgxlisten.Listener
	cancelFunc context.CancelFunc
	*Repository[E]
}

func NewListener[E any](streamId string, listener *pgxlisten.Listener, connection *pgxpool.Pool, codec Codec[E]) *Listener[E] {
	return &Listener[E]{
		streamId:   streamId,
		listener:   listener,
		Repository: NewRepository[E](connection, codec).Stream(streamId),
	}
}

func (l *Listener[E]) Subscribe(consumer Consumer[E]) {
	var conn *pgxpool.Conn
	listener := &pgxlisten.Listener{}
	listener.Connect = func(ctx context.Context) (*pgx.Conn, error) {
		var err error
		conn, err = l.connection.Acquire(ctx)
		return conn.Conn(), err
	}
	listener.Handle(l.streamId, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		event, err := l.getEvent(ctx, notification.Payload)
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	}))

	go func() { _ = listener.Listen(context.Background()) }()
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

func (l *Listener[E]) Start(ctx context.Context) error {

	return nil
}

func (l *Listener[E]) Stop() {

}
