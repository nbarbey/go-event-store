package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type MyEvent struct {
	Name    string
	Version string
}

func (m *MyEvent) SetVersion(version string) {
	m.Version = version
}

func TestEventStore_with_custom_events(t *testing.T) {
	customEventStore, err := eventstore.NewPostgresEventStore[MyEvent](context.Background(), postgresContainer.ConnectionString("search_path=my_events"))
	require.NoError(t, err)

	t.Run("publish and subscribe to custom event", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.Publish(context.Background(), MyEvent{Name: "John"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return received.Name == "John" }, time.Second, 10*time.Millisecond)
	})

	t.Run("publish with type", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.WithType("my_event").Publish(context.Background(), MyEvent{Name: "John"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return "John" == received.Name }, time.Second, time.Millisecond)
	})

	t.Run("publish with expected Version and reject if not expected", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return "Rose" == received.Name }, 10*time.Second, time.Millisecond)

		err = myStream.ExpectedVersion("unexpected").Publish(context.Background(), MyEvent{Name: "Juan"})
		assert.Error(t, err)
	})

	t.Run("publish with type and expected Version", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-incredible-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.ExpectedVersion("").WithType("my_event_type").Publish(context.Background(), MyEvent{Name: "Felipe"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return "Felipe" == received.Name }, time.Second, time.Millisecond)
	})

	t.Run("publish with expected Version and accept if expected matches actual Version", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return "Rose" == received.Name }, 10*time.Second, time.Millisecond)
		assert.NotEmpty(t, received.Version)

		err = myStream.ExpectedVersion(received.Version).Publish(context.Background(), MyEvent{Name: "Juan"})
		assert.NoError(t, err)
		assert.Eventually(t, func() bool { return "Juan" == received.Name }, 10*time.Second, time.Millisecond)
	})
}
