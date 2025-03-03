package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
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
	stringEventStore, err := eventstore.NewEventStore[string](ctx, postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	t.Run("publish and get all", func(t *testing.T) {
		require.NoError(t, stringEventStore.Publish(context.Background(), "my_event_data"))
		events, err := stringEventStore.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, "my_event_data", events[0])
	})
	t.Run("publish and get all of one stream", func(t *testing.T) {
		stream := stringEventStore.Stream("awesome-string-stream")

		require.NoError(t, stringEventStore.Publish(context.Background(), "default stream data"))
		require.NoError(t, stream.Publish(context.Background(), "some other stream data"))

		events, err := stream.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, "some other stream data", events[0])
	})
	t.Run("subscribe then publish", func(t *testing.T) {
		var received string
		stringEventStore.Subscribe(makeTestConsumer[string](&received))

		startTestEventStore(t, stringEventStore)
		defer stringEventStore.Stop()

		require.NoError(t, stringEventStore.Publish(context.Background(), "my_event_data"))

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received)
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("publish to some stream and not others", func(t *testing.T) {
		var received string
		stringEventStore.Stream("some-string-stream").Subscribe(makeTestConsumer[string](&received))
		var receivedOther string
		stringEventStore.Stream("other-string-stream").Subscribe(makeTestConsumer[string](&receivedOther))

		startTestEventStore(t, stringEventStore)
		defer stringEventStore.Stop()

		require.NoError(t, stringEventStore.Stream("some-string-stream").Publish(context.Background(), "my_event_data"))

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received) && len(receivedOther) == 0
		}, time.Second, 10*time.Millisecond)
	})

	type MyEvent struct {
		Name string
	}
	customEventStore, err := eventstore.NewEventStore[MyEvent](context.Background(), postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	t.Run("publish and subscribe to custom event", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.Stream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		startTestEventStore(t, customEventStore)
		defer customEventStore.Stop()

		require.NoError(t, myStream.Publish(context.Background(), MyEvent{Name: "John"}))

		assert.Eventually(t, func() bool {
			return received == MyEvent{Name: "John"}
		}, time.Second, 10*time.Millisecond)
	})
}

func makeTestConsumer[E any](received *E) eventstore.ConsumerFunc[E] {
	return func(e E) { *received = e }
}

func startTestEventStore[E any](t *testing.T, es *eventstore.EventStore[E]) {
	require.NoError(t, es.Start(context.Background()))
	// give time for listener to be set-up properly
	time.Sleep(10 * time.Millisecond)
}
