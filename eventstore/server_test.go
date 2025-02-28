package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"testing"
	"time"
)

type TypicalEvent struct {
	SomeString string
	SomeInt    int
}

func TestServerPublish(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	es, err := eventstore.NewEventStore[TypicalEvent](ctx, postgresContainer.ConnectionString(t))
	require.NoError(t, err)
	server := eventstore.NewServerFromEventStore[TypicalEvent]("http://localhost:8080", es)

	testServer := httptest.NewServer(server)
	testHTTPClient := testServer.Client()
	client := eventstore.NewClient[TypicalEvent](testHTTPClient)
	client.SetBaseURL(testServer.URL)

	t.Run("publish and get all", func(t *testing.T) {
		event := TypicalEvent{SomeString: "foo", SomeInt: 42}
		require.NoError(t, client.Publish(context.Background(), event))
		events, err := client.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, event, events[0])
	})
}
