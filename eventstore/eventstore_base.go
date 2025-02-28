package eventstore

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgxlisten"
)

type BaseStore struct {
	connection *pgx.Conn
	listener   *pgxlisten.Listener
	cancelFunc context.CancelFunc
}

func NewBaseStore(ctx context.Context, connStr string) (*BaseStore, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	eventStore := BaseStore{
		connection: conn,
		listener: &pgxlisten.Listener{
			Connect: func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, connStr) },
		},
	}

	err = eventStore.createTableAndTrigger(ctx)
	return &eventStore, err
}

func (e BaseStore) Start(ctx context.Context) error {
	cancellableContext, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	go func() { _ = e.listener.Listen(cancellableContext) }()
	return nil
}

func (e BaseStore) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}

func (e BaseStore) createTableAndTrigger(ctx context.Context) error {
	_, err := e.connection.Exec(ctx, "create table if not exists events (stream_id text, payload jsonb)")
	if err != nil {
		return err
	}
	_, err = e.connection.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.payload::TEXT);
  		return new;
		end;
		$$ language plpgsql;`)
	if err != nil {
		return err
	}
	_, err = e.connection.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	return err
}
