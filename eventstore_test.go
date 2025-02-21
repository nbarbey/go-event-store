package go_event_store_test

import (
	"context"
	ges "github.com/nbarbey/go-event-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEventStore(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	es, err := ges.NewEventStore[[]byte](ctx, postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	t.Run("publish and get all", func(t *testing.T) {
		require.NoError(t, es.Publish([]byte(`"my_event_data"`)))
		events, err := es.All()
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, []byte(`"my_event_data"`), events[0])
	})
	t.Run("subscribe then publish", func(t *testing.T) {
		var received []byte
		es.Subscribe(makeTestConsumer(&received))

		startTestEventStore(t, es)
		defer es.Stop()

		require.NoError(t, es.Publish([]byte(`"my_event_data"`)))

		assert.Eventually(t, func() bool {
			return assert.Equal(t, `"my_event_data"`, string(received))
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("publish to some stream and not others", func(t *testing.T) {
		var received []byte
		es.Stream("some-stream").Subscribe(makeTestConsumer(&received))
		var receivedOther []byte
		es.Stream("other-stream").Subscribe(makeTestConsumer(&receivedOther))

		startTestEventStore(t, es)
		defer es.Stop()

		require.NoError(t, es.Stream("some-stream").Publish([]byte(`"my_event_data"`)))

		assert.Eventually(t, func() bool {
			return `"my_event_data"` == string(received) && len(receivedOther) == 0
		}, time.Second, 10*time.Millisecond)
	})
}

func makeTestConsumer(received *[]byte) ges.ConsumerFunc[[]byte] {
	return func(e []byte) { *received = e }
}

func startTestEventStore(t *testing.T, es *ges.EventStore[[]byte]) {
	require.NoError(t, es.Start(context.Background()))
	// give time for listener to be set-up properly
	time.Sleep(10 * time.Millisecond)
}
