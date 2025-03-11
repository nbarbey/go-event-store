package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/beevik/guid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgxlisten"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
	"time"
)

type Repository[E any] struct {
	streamId   string
	connection *pgxpool.Pool
	codec      codec.TypedCodec[E]
}

func NewRepository[E any](connection *pgxpool.Pool, c codec.TypedCodec[E]) *Repository[E] {
	return &Repository[E]{connection: connection, codec: c}
}

func (r *Repository[E]) WithCodec(codec codec.TypedCodec[E]) *Repository[E] {
	r.codec = codec
	return r
}

func (r *Repository[E]) Stream(name string) *Repository[E] {
	return &Repository[E]{connection: r.connection, codec: r.codec, streamId: name}
}

func (r *Repository[E]) GetEvent(ctx context.Context, eventId string) (event E, err error) {
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
	return codec.UnmarshallAllWithType[E](r.codec, ers.types(), ers.payloads())
}

func (r *Repository[E]) InsertEvent(ctx context.Context, version, typeHint string, event E, expectedVersion string) error {
	data, err := r.codec.Marshall(event)
	if err != nil {
		return err
	}
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

	_, err = r.connection.Exec(ctx,
		"insert into events (event_id, stream_id, event_type, version, payload, created_at) values ($1, $2, $3, $4, $5, $6)",
		guid.New(), r.streamId, typeHint, version, data, time.Now())
	return err
}

func (r *Repository[E]) CreateTableAndTrigger(ctx context.Context) error {
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

func (r *Repository[E]) createEventsTable(ctx context.Context) error {
	_, err := r.connection.Exec(ctx,
		"create table if not exists events (event_id text, stream_id text, event_type text, version text, payload text, created_at timestamp )")
	return err
}

func (r *Repository[E]) createNewEventNotificationTrigger(ctx context.Context, err error) error {
	_, err = r.connection.Exec(ctx, `create or replace trigger "new-event-notifier"
								after insert on events
								for each row execute procedure "doNotify"()`)
	return err
}

func (r *Repository[E]) createNotificationFunction(ctx context.Context, err error) error {
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

func (r *Repository[E]) Version(ctx context.Context) (version string, err error) {
	row := r.connection.QueryRow(ctx, "select version from events where stream_id = $1 order by created_at desc limit 1", r.streamId)
	err = row.Scan(&version)
	return
}

func (r *Repository[E]) BuildListener(consumer consumer.Consumer[E]) *pgxlisten.Listener {
	listener := &pgxlisten.Listener{}
	listener.Connect = func(ctx context.Context) (*pgx.Conn, error) {
		conn, err := r.connection.Acquire(ctx)
		return conn.Conn(), err
	}

	listener.Handle(r.streamId, pgxlisten.HandlerFunc(func(ctx context.Context, notification *pgconn.Notification, conn *pgx.Conn) error {
		event, err := r.GetEvent(ctx, notification.Payload)
		if err != nil {
			return err
		}

		consumer.Consume(event)
		return nil
	}))
	return listener
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
