package go_event_store

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
)

type EventStore struct {
	connection *pgx.Conn
}

func (e EventStore) Publish(bytes []byte) error {
	_, err := e.connection.Exec(context.Background(), "insert into events values ($1)", bytes)
	return err
}

func (e EventStore) All() ([][]byte, error) {
	rows, err := e.connection.Query(context.Background(), "select * from events")
	if err != nil {
		return nil, err
	}
	output := make([][]byte, 0)
	for rows.Next() {
		var current []byte
		rowErr := rows.Scan(&current)
		if rowErr != nil {
			return nil, rowErr
		}
		output = append(output, current)
	}
	return output, nil
}

func NewEventStore(ctx context.Context, connStr string) (*EventStore, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		return nil, err
	}
	_, err = conn.Exec(context.Background(), "create table events (events jsonb)")
	if err != nil {
		return nil, err
	}
	return &EventStore{
		connection: conn,
	}, nil
}
