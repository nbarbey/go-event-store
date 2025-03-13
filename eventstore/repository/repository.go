package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Repository struct {
	streamId   string
	connection *pgxpool.Pool
}

func NewRepository(connection *pgxpool.Pool) *Repository {
	return &Repository{connection: connection}
}

func (r *Repository) GetPayload(ctx context.Context, eventId string) ([]byte, string, string, error) {
	row := r.connection.QueryRow(ctx, "select event_type, version, payload from events where event_id=$1 and stream_id=$2", eventId, r.streamId)
	var er eventRow
	err := row.Scan(&er.EventType, &er.Version, &er.Payload)
	if err != nil {
		return nil, "", "", err
	}
	payload := er.Payload
	typeHint := er.EventType.String
	return payload, typeHint, er.Version.String, nil
}

var ErrVersionMismatch = errors.New("mismatched version")

func (r *Repository) InsertPayload(ctx context.Context, version string, typeHint string, expectedVersion string, data []byte) error {
	if expectedVersion != "" {
		row := r.connection.QueryRow(ctx,
			"select event_id from events where stream_id=$1 and version=$2",
			r.streamId, expectedVersion)
		var eventIDWithExpectedVersion string
		err := row.Scan(&eventIDWithExpectedVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVersionMismatch
		}
	}

	_, err := r.connection.Exec(ctx,
		"insert into events (event_id, stream_id, event_type, version, payload, created_at) values ($1, $2, $3, $4, $5, $6)",
		guid.New(), r.streamId, typeHint, version, data, time.Now())
	return err
}

func (r *Repository) Version(ctx context.Context) (version string, err error) {
	row := r.connection.QueryRow(ctx, "select version from events where stream_id = $1 order by created_at desc limit 1", r.streamId)
	err = row.Scan(&version)
	return
}

func (r *Repository) AllTypesAndPayloads(ctx context.Context) ([]string, []string, [][]byte, error) {
	rows, err := r.connection.Query(ctx, "select event_id, event_type, version, stream_id, payload from events where stream_id=$1 order by created_at desc", r.streamId)
	if err != nil {
		return nil, nil, nil, err
	}
	sliceOfEventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, nil, nil, fmt.Errorf("CollectRows error: %w", err)
	}
	ers := eventRows(sliceOfEventRows)
	return ers.types(), ers.versions(), ers.payloads(), nil
}

func (r *Repository) CreateTableAndTrigger(ctx context.Context) error {
	err := r.createEventsTable(ctx)
	if err != nil {
		return err
	}
	err = r.createNotificationFunction(ctx, err)
	if err != nil {
		return err
	}
	return r.createNewEventNotificationTrigger(ctx, err)
}

func (r *Repository) createEventsTable(ctx context.Context) error {
	_, err := r.connection.Exec(ctx,
		"create table if not exists events (event_id text, stream_id text, event_type text, version text, payload text, created_at timestamp )")
	return err
}

func (r *Repository) createNewEventNotificationTrigger(ctx context.Context, err error) error {
	_, err = r.connection.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	return err
}

func (r *Repository) createNotificationFunction(ctx context.Context, err error) error {
	_, err = r.connection.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.event_id);
  		return new;
		end;
		$$ language plpgsql;`)
	return err
}
