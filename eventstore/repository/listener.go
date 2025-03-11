package repository

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
)

type Listener struct {
	streamId string
	listener *pgxlisten.Listener
}

func NewListener(streamId string, connection *pgxpool.Pool) *Listener {
	listener := pgxlisten.Listener{}
	listener.Connect = func(ctx context.Context) (*pgx.Conn, error) {
		conn, err := connection.Acquire(ctx)
		return conn.Conn(), err
	}
	return &Listener{streamId: streamId, listener: &listener}
}

type handler func(ctx context.Context, eventID string) error

func (t *Listener) Handle(h handler) {
	t.listener.Handle(t.streamId, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		return h(ctx, notification.Payload)
	}))
}

func (t *Listener) Listen(ctx context.Context) error {
	return t.listener.Listen(ctx)
}
