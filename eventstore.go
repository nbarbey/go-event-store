package go_event_store

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
)

type Repository struct {
	conn *pgx.Conn
}

func NewEventStore(ctx context.Context, connStr string) (*Repository, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		return nil, err
	}
	return &Repository{
		conn: conn,
	}, nil
}
