package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Repository[E any] struct {
	streamId   string
	connection *pgxpool.Pool
	codec      Codec[E]
}

func NewRepository[E any](connection *pgxpool.Pool, codec Codec[E]) *Repository[E] {
	return &Repository[E]{connection: connection, codec: codec}
}

func (r *Repository[E]) WithCodec(codec Codec[E]) *Repository[E] {
	r.codec = codec
	return r
}

func (r *Repository[E]) Stream(name string) *Repository[E] {
	return &Repository[E]{connection: r.connection, codec: r.codec, streamId: name}
}

func (r *Repository[E]) getEvent(ctx context.Context, eventId string) (event E, err error) {
	row := r.connection.QueryRow(ctx, "select event_type, payload from events where event_id=$1 and stream_id=$2", eventId, r.streamId)
	var er eventRow
	err = row.Scan(&er.EventType, &er.Payload)
	if err != nil {
		return event, err
	}
	return r.codec.UnmarshallWithType(er.EventType.String, er.Payload)
}

func (r *Repository[E]) All(ctx context.Context) ([]E, error) {
	rows, err := r.connection.Query(ctx, "select event_id, event_type, version, stream_id, payload from events where stream_id=$1", r.streamId)
	if err != nil {
		return nil, err
	}
	sliceOfEventRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[eventRow])
	if err != nil {
		return nil, fmt.Errorf("CollectRows error: %w", err)
	}
	ers := eventRows(sliceOfEventRows)
	return UnmarshallAllWithType[E](r.codec, ers.types(), ers.payloads())
}

func (r *Repository[E]) insertEvent(ctx context.Context, streamId, version, typeHint string, data []byte, expectedVersion string) error {
	if expectedVersion != "" {
		row := r.connection.QueryRow(ctx,
			"select event_id from events where stream_id=$1 and version=$2",
			streamId, expectedVersion)
		var eventIDWithExpectedVersion string
		err := row.Scan(&eventIDWithExpectedVersion)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVersionMismatch
		}
	}

	_, err := r.connection.Exec(ctx,
		"insert into events (event_id, stream_id, event_type, version, payload, created_at) values ($1, $2, $3, $4, $5, $6)",
		guid.New(), streamId, typeHint, version, data, time.Now())
	return err
}

func (r *Repository[E]) createTableAndTrigger(ctx context.Context) error {
	_, err := r.connection.Exec(ctx,
		"create table if not exists events (event_id text, stream_id text, event_type text, version text, payload text, created_at timestamp )")
	if err != nil {
		return err
	}

	_, err = r.connection.Exec(ctx, `create or replace function "doNotify"()
  		returns trigger as $$
			declare 
			begin
  			perform pg_notify(new.stream_id, new.event_id);
  		return new;
		end;
		$$ language plpgsql;`)
	if err != nil {
		return err
	}
	_, err = r.connection.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	return err
}

func (r *Repository[E]) Version(ctx context.Context) (version string, err error) {
	row := r.connection.QueryRow(ctx, "select version from events where stream_id = $1 order by created_at desc limit 1", r.streamId)
	err = row.Scan(&version)
	return
}

type eventRow struct {
	EventID   string
	EventType sql.NullString
	Version   sql.NullString
	StreamID  sql.NullString
	Payload   []byte
}

type eventRows []eventRow

func (ers eventRows) payloads() [][]byte {
	payloads := make([][]byte, 0)
	for _, e := range ers {
		payloads = append(payloads, e.Payload)
	}
	return payloads
}

func (ers eventRows) types() []string {
	types := make([]string, 0)
	for _, e := range ers {
		types = append(types, e.EventType.String)
	}
	return types
}
