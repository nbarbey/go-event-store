package go_event_store_test

import (
	"context"
	ges "github.com/nbarbey/go-event-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func makeTestConsumer(received *[]byte) ges.ConsumerFunc {
	return func(e []byte) { *received = e }
}

func TestEventStore(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	es, err := ges.NewEventStore(ctx, postgresContainer.ConnectionString(t))
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
		require.NoError(t, es.Start(context.Background()))
		defer es.Stop()
		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		require.NoError(t, es.Publish([]byte(`"my_event_data"`)))

		assert.Eventually(t, func() bool {
			return assert.Equal(t, `"my_event_data"`, string(received))
		}, time.Second, 10*time.Millisecond)
	})
}
