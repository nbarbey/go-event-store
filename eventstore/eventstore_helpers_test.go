package eventstore_test

import (
	"context"
	"fmt"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"testing"
	"time"
)

type testPostgresContainer struct {
	user, password string
	*postgres.PostgresContainer
}

func (c *testPostgresContainer) Cancel() {
	if err := testcontainers.TerminateContainer(c.PostgresContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
}

func (c *testPostgresContainer) Port(t *testing.T) int {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, err := c.PostgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)
	return p.Int()
}

func (c *testPostgresContainer) ConnectionString(t *testing.T, options string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/events?%s", c.user, c.password, c.Port(t), options)
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

func makeTestConsumer[E any](received *E) eventstore.ConsumerFunc[E] {
	return func(e E) {
		*received = e
	}
}
