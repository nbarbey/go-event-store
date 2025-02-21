package go_event_store_test

import (
	"context"
	ges "github.com/nbarbey/go-event-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
	"time"
)

func TestEventStore(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	es, err := ges.NewEventStore[string](ctx, postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	t.Run("publish and get all", func(t *testing.T) {
		require.NoError(t, es.Publish(context.Background(), "my_event_data"))
		events, err := es.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, "my_event_data", events[0])
	})
	t.Run("subscribe then publish", func(t *testing.T) {
		var received string
		es.Subscribe(makeTestConsumer[string](&received))

		startTestEventStore(t, es)
		defer es.Stop()

		require.NoError(t, es.Publish(context.Background(), "my_event_data"))

		assert.Eventually(t, func() bool {
			log.Printf(string(received))
			return "my_event_data" == string(received)
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("publish to some stream and not others", func(t *testing.T) {
		var received string
		es.Stream("some-stream").Subscribe(makeTestConsumer[string](&received))
		var receivedOther string
		es.Stream("other-stream").Subscribe(makeTestConsumer[string](&receivedOther))

		startTestEventStore(t, es)
		defer es.Stop()

		require.NoError(t, es.Stream("some-stream").Publish(context.Background(), "my_event_data"))

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received) && len(receivedOther) == 0
		}, time.Second, 10*time.Millisecond)
	})
}

func TestEventStore_custome_events(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()
	type MyEvent struct {
		Name string
	}
	es, err := ges.NewEventStore[MyEvent](context.Background(), postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	t.Run("publish and subscribe to custom event", func(t *testing.T) {

		var received MyEvent
		myStream := es.Stream("my-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		startTestEventStore(t, es)
		defer es.Stop()

		require.NoError(t, myStream.Publish(context.Background(), MyEvent{Name: "John"}))

		assert.Eventually(t, func() bool {
			return received == MyEvent{Name: "John"}
		}, time.Second, 10*time.Millisecond)
	})
}

func makeTestConsumer[E any](received *E) ges.ConsumerFunc[E] {
	return func(e E) { *received = e }
}

func startTestEventStore[E any](t *testing.T, es *ges.EventStore[E]) {
	require.NoError(t, es.Start(context.Background()))
	// give time for listener to be set-up properly
	time.Sleep(10 * time.Millisecond)
}
