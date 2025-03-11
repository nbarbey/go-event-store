package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEventStore_with_custom_events(t *testing.T) {
	type MyEvent struct {
		Name string
	}
	customEventStore, err := eventstore.NewEventStore[MyEvent](context.Background(), postgresContainer.ConnectionString(t, "search_path=my_events"))
	require.NoError(t, err)

	t.Run("publish and subscribe to custom event", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.Publish(context.Background(), MyEvent{Name: "John"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return received == MyEvent{Name: "John"} }, time.Second, 10*time.Millisecond)
	})

	t.Run("publish with type", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.WithType("my_event").Publish(context.Background(), MyEvent{Name: "John"})
		require.NoError(t, err)

		expected := MyEvent{Name: "John"}
		assert.Eventually(t, func() bool { return expected == received }, time.Second, time.Millisecond)
	})

	t.Run("publish with expected Version and reject if not expected", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		expected := MyEvent{Name: "Rose"}
		assert.Eventually(t, func() bool { return expected == received }, 10*time.Second, time.Millisecond)

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

		assert.Eventually(t, func() bool { return MyEvent{Name: "Felipe"} == received }, time.Second, time.Millisecond)
	})

	t.Run("publish with expected Version and accept if expected matches actual Version", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return MyEvent{Name: "Rose"} == received }, 10*time.Second, time.Millisecond)
		version, err := myStream.Version(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, version)

		err = myStream.ExpectedVersion(version).Publish(context.Background(), MyEvent{Name: "Juan"})
		assert.NoError(t, err)
		assert.Eventually(t, func() bool { return MyEvent{Name: "Juan"} == received }, 10*time.Second, time.Millisecond)
	})

	t.Run("Version retrieve the Version of the latest event", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return MyEvent{Name: "Rose"} == received }, 10*time.Second, time.Millisecond)
		version1, err := myStream.Version(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, version1)

		err = myStream.ExpectedVersion(version1).Publish(context.Background(), MyEvent{Name: "Tiphaine"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return MyEvent{Name: "Tiphaine"} == received }, 10*time.Second, time.Millisecond)
		version2, err := myStream.Version(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, version2)

		assert.NotEqual(t, version1, version2)
	})
}
