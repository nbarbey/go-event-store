package eventstore

import (
	"context"
	"github.com/jackc/pgxlisten"
)

type EventStore struct {
	*BaseStore
	listener   *pgxlisten.Listener
	cancelFunc context.CancelFunc
}

func NewEventStore(ctx context.Context, connStr string) (*EventStore, error) {
	store, err := NewBaseStore(ctx, connStr)
	if err != nil {
		return nil, err
	}
	eventStore := EventStore{
		BaseStore: store,
	}
	return &eventStore, nil
}
