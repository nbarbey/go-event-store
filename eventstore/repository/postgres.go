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

type RawEvent struct {
	EventType string
	Version   string
	Payload   []byte
}

type Postgres struct {
	streamId   string
	connection *pgxpool.Pool
}

func NewPostgres(ctx context.Context, connStr string) (*Postgres, error) {
	connection, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	p := &Postgres{streamId: "default-stream", connection: connection}
	_, err = p.CreateTableAndTrigger(ctx)
	return p, err
}

func (r *Postgres) Stream(name string) Repository {
	r.streamId = name
	return r
}

func (r *Postgres) GetRawEvent(ctx context.Context, eventId string) (*RawEvent, error) {
	row := r.connection.QueryRow(ctx, "select event_type, version, payload from events where event_id=$1 and stream_id=$2", eventId, r.streamId)
	var er eventRow
	err := row.Scan(&er.EventType, &er.Version, &er.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrEventNotFound
	}
	if err != nil {
		return nil, err
	}
	return er.ToRawEvent(), nil
}

func (r *Postgres) InsertRawEvent(ctx context.Context, raw RawEvent, expectedVersion string) (string, error) {
	if expectedVersion != "" {
		row := r.connection.QueryRow(ctx,
			"select event_id from events where stream_id=$1 and version=$2",
			r.streamId, expectedVersion)
		var eventIDWithExpectedVersion string
		err := row.Scan(&eventIDWithExpectedVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrVersionMismatch
		}
	}

	eventId := guid.New().String()
	_, err := r.connection.Exec(ctx,
		"insert into events (event_id, stream_id, event_type, version, payload, created_at) values ($1, $2, $3, $4, $5, $6)",
		eventId, r.streamId, raw.EventType, raw.Version, raw.Payload, time.Now())
	return eventId, err
}

func (r *Postgres) AllRawEvents(ctx context.Context) ([]*RawEvent, error) {
	rows, err := r.connection.Query(ctx, "select event_id, event_type, version, stream_id, payload from events where stream_id=$1 order by created_at desc", r.streamId)
	if err != nil {
		return nil, err
	}
	sliceOfEventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	return eventRows(sliceOfEventRows).ToRawEvents(), nil
}

func (r *Postgres) NewListener() Listener {
	return NewPostgresListener(r.streamId, r.connection)
}

func (r *Postgres) CreateTableAndTrigger(ctx context.Context) (*Postgres, error) {
	err := r.createEventsTable(ctx)
	if err != nil {
		return r, err
	}
	err = r.createIndex(ctx)
	if err != nil {
		return r, err
	}
	err = r.createNotificationFunction(ctx)
	if err != nil {
		return r, err
	}
	return r, r.createNewEventNotificationTrigger(ctx)
}

func (r *Postgres) createEventsTable(ctx context.Context) error {
	_, err := r.connection.Exec(ctx,
		"create table if not exists events (event_id text, stream_id text, event_type text, version text, payload bytea, created_at timestamp )")
	return err
}

func (r *Postgres) createNewEventNotificationTrigger(ctx context.Context) error {
	_, err := r.connection.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	return err
}

func (r *Postgres) createNotificationFunction(ctx context.Context) error {
	_, err := r.connection.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.event_id);
  		return new;
		end;
		$$ language plpgsql;`)
	return err
}

func (r *Postgres) createIndex(ctx context.Context) error {
	_, err := r.connection.Exec(ctx, `create index if not exists stream_index on events (stream_id)`)
	if err != nil {
		return err
	}
	_, err = r.connection.Exec(ctx, `create index if not exists stream_version_index on events (stream_id, version)`)
	if err != nil {
		return err
	}
	_, err = r.connection.Exec(ctx, `create unique index if not exists stream_event_index on events (event_id, stream_id)`)
	return err
}
