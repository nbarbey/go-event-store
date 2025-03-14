package repository_test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbarbey/go-event-store/eventstore/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"os"
	"testing"
	"time"
)

var postgresContainer *testPostgresContainer

func TestMain(m *testing.M) {
	var err error
	postgresContainer, err = runTestContainer()
	if err != nil {
		panic(err)
	}
	defer postgresContainer.Cancel()

	os.Exit(m.Run())
}

func TestRepository(t *testing.T) {
	connectionString := postgresContainer.ConnectionString("search_path=string_events")
	pool, err := pgxpool.New(context.Background(), connectionString)
	require.NoError(t, err)

	t.Run("Get not found", func(t *testing.T) {
		r, err := repository.NewRepository(pool).CreateTableAndTrigger(context.Background())
		require.NoError(t, err)

		_, err = r.GetRawEvent(context.Background(), "bad_id")

		require.ErrorIs(t, err, repository.ErrEventNotFound)
	})
	t.Run("Insert and Get", func(t *testing.T) {
		r, err := repository.NewRepository(pool).CreateTableAndTrigger(context.Background())
		require.NoError(t, err)

		eventId, err := r.InsertRawEvent(context.Background(), repository.RawEvent{EventType: "my_type", Version: "1", Payload: []byte("coucou")}, "")
		require.NoError(t, err)
		event, err := r.GetRawEvent(context.Background(), eventId)
		require.NoError(t, err)

		assert.Equal(t, "my_type", event.EventType)
		assert.Equal(t, "1", event.Version)
		assert.Equal(t, []byte("coucou"), event.Payload)
	})

	t.Run("Insert and Get All", func(t *testing.T) {
		r, err := repository.NewRepository(pool).CreateTableAndTrigger(context.Background())
		require.NoError(t, err)

		r.Stream("all")
		_, err = r.InsertRawEvent(context.Background(), repository.RawEvent{EventType: "my_type", Version: "1", Payload: []byte("coucou")}, "")
		require.NoError(t, err)
		_, err = r.InsertRawEvent(context.Background(), repository.RawEvent{EventType: "my_type", Version: "1", Payload: []byte("salut !")}, "")
		require.NoError(t, err)
		events, err := r.AllRawEvents(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 2)
	})
}

type testPostgresContainer struct {
	user, password string
	*postgres.PostgresContainer
}

func (c *testPostgresContainer) Cancel() {
	if err := testcontainers.TerminateContainer(c.PostgresContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
}

func (c *testPostgresContainer) Port() int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, err := c.PostgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		panic(err)
	}
	return p.Int()
}

func (c *testPostgresContainer) ConnectionString(options string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/events?%s", c.user, c.password, c.Port(), options)
}
func runTestContainer() (*testPostgresContainer, error) {
	user, password := "postgres", "password"
	postgresContainer, err := postgres.Run(context.Background(),
		"postgres:16-alpine",
		postgres.WithDatabase("events"),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		postgres.WithInitScripts("helper_init_script.sql"),
		testcontainers.WithLogger(log.Default()),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5*time.Second)),
	)

	return &testPostgresContainer{user: user, password: password, PostgresContainer: postgresContainer}, err
}
