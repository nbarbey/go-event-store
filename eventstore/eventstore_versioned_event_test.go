package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type item struct {
	Version     string
	Name        string
	Description string
}

func (i *item) SetVersion(version string) {
	i.Version = version
}

func TestEventStore_custom_event_with_version(t *testing.T) {

	customEventStore, err := eventstore.NewEventStore[item](context.Background(), postgresContainer.ConnectionString(t, "search_path=items_events"))
	require.NoError(t, err)

	t.Run("publish and subscribe to custom event", func(t *testing.T) {
		var received item
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[item](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		err := myStream.Publish(context.Background(), item{Name: "Pan", Description: "Carbon steel"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return received.Name == "Pan" && received.Description == "Carbon steel" }, time.Second, 10*time.Millisecond)
		assert.NotEmpty(t, received.Version)
	})

}
