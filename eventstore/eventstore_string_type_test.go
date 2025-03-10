package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEventStore_with_string_type(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stringEventStore, err := eventstore.NewEventStore[string](ctx, postgresContainer.ConnectionString(t, "search_path=string_events"))
	require.NoError(t, err)

	stringEventStore.WithCodec(codec.NoopCodec[string]{})

	t.Run("publish and get all", func(t *testing.T) {
		err := stringEventStore.Publish(context.Background(), "my_event_data")
		require.NoError(t, err)
		events, err := stringEventStore.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, "my_event_data", events[0])
	})
	t.Run("publish and get all of one stream", func(t *testing.T) {
		stream := stringEventStore.GetStream("awesome-string-stream")

		err := stringEventStore.Publish(context.Background(), "default stream data")
		require.NoError(t, err)
		err = stream.Publish(context.Background(), "some other stream data")
		require.NoError(t, err)

		events, err := stream.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, "some other stream data", events[0])
	})
	t.Run("subscribe then publish", func(t *testing.T) {
		var received string
		stringEventStore.Subscribe(makeTestConsumer[string](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := stringEventStore.Publish(context.Background(), "my_event_data")
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received)
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("subscribe is cancellable", func(t *testing.T) {
		var received string
		subscription := stringEventStore.Subscribe(makeTestConsumer[string](&received))
		defer subscription.Cancel()

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := stringEventStore.Publish(context.Background(), "my_event_data")
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received)
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("subscribe from beginning", func(t *testing.T) {
		err := stringEventStore.Publish(context.Background(), "my_event_data")
		require.NoError(t, err)

		var received string
		require.NoError(t, stringEventStore.SubscribeFromBeginning(context.Background(), makeTestConsumer[string](&received)))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received)
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("publish to some stream and not others", func(t *testing.T) {
		var received string
		stringEventStore.GetStream("some-string-stream").Subscribe(makeTestConsumer[string](&received))
		var receivedOther string
		stringEventStore.GetStream("other-string-stream").Subscribe(makeTestConsumer[string](&receivedOther))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := stringEventStore.GetStream("some-string-stream").Publish(context.Background(), "my_event_data")
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received) && len(receivedOther) == 0
		}, time.Second, 10*time.Millisecond)
	})
}
