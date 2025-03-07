package banking_kata

import (
	"context"
	"fmt"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"testing"
	"time"
)

func TestAccount_printStatement(t *testing.T) {
	a := Account{}

	assert.Equal(t, a.PrintStatement(), "")
}

func TestAccount_Deposit(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	es, err := eventstore.NewEventStore[AccountEvent](ctx, postgresContainer.ConnectionString(t, ""))
	require.NoError(t, err)
	bank := NewBank(es)

	t.Run("deposit 1", func(t *testing.T) {
		a := bank.NewAccount()

		startTestEventStore(t, es)
		defer es.Stop()

		a.Deposit(1)

		assert.Eventually(t, func() bool { return a.PrintStatement() == "\n+1 1" }, time.Second, time.Millisecond)
	})

	t.Run("deposit twice", func(t *testing.T) {
		a := bank.NewAccount()

		startTestEventStore(t, es)
		defer es.Stop()

		a.Deposit(1)
		a.Deposit(1)

		assert.Eventually(t, func() bool { return a.PrintStatement() == "\n+1 1\n+1 2" }, time.Second, time.Millisecond)
	})

	t.Run("deposit and withdraw", func(t *testing.T) {
		a := bank.NewAccount()

		startTestEventStore(t, es)
		defer es.Stop()

		a.Deposit(1)
		a.Withdraw(1)

		assert.Eventually(t, func() bool { return a.PrintStatement() == "\n+1 1\n-1 0" }, time.Second, time.Millisecond)
	})
}

func runTestContainer() (*testPostgresContainer, error) {
	user, password := "postgres", "password"
	postgresContainer, err := postgres.Run(context.Background(),
		"postgres:16-alpine",
		postgres.WithDatabase("events"),
		postgres.WithUsername(user),
		postgres.WithPassword(password),
		testcontainers.WithLogger(log.Default()),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5*time.Second)),
	)

	return &testPostgresContainer{user: user, password: password, PostgresContainer: postgresContainer}, err
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

func startTestEventStore[E any](t *testing.T, es *eventstore.EventStore[E]) {
	require.NoError(t, es.Start(context.Background()))
	// give time for listener to be set-up properly
	time.Sleep(10 * time.Millisecond)
}
