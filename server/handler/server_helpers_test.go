package handler_test

import (
	"context"
	"fmt"
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

func (c *testPostgresContainer) ConnectionString(t *testing.T) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres", c.user, c.password, c.Port(t))
}
func runTestContainer() (*testPostgresContainer, error) {
	user, password := "user", "password"
	postgresContainer, err := postgres.Run(context.Background(),
		"postgres:16-alpine",
		postgres.WithDatabase("events"),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5*time.Second)),
	)

	return &testPostgresContainer{user: user, password: password, PostgresContainer: postgresContainer}, err
}
