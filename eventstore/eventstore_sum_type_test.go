package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type todoEvent interface {
	isTodoEvent()
}

type todoCreated struct{ Date time.Time }

func (c todoCreated) isTodoEvent() {}

type todoDone struct {
	TodoID int
	Date   time.Time
}

func (c todoDone) isTodoEvent() {}

type todoDeleted struct{ TodoID int }

func (c todoDeleted) isTodoEvent() {}

func TestEventStore_with_sum_type(t *testing.T) {
	todoEventStore, err := eventstore.NewEventStore[todoEvent](context.Background(), postgresContainer.ConnectionString(t, "search_path=todo_events"))
	require.NoError(t, err)
	todoEventStore.WithCodec(codec.NewJSONCodecWithTypeHints[todoEvent](map[string]codec.Unmarshaller[todoEvent]{
		"todoCreated": codec.UnmarshalerFunc[todoEvent](func(payload []byte) (event todoEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[todoCreated]()(payload)
		}),
		"todoDone": codec.UnmarshalerFunc[todoEvent](func(payload []byte) (event todoEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[todoDone]()(payload)
		}),
		"todoDeleted": codec.UnmarshalerFunc[todoEvent](func(payload []byte) (event todoEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[todoDeleted]()(payload)
		}),
	}))

	t.Run("publish multiple events with different types on same stream", func(t *testing.T) {
		var createdReceived todoCreated
		var doneReceived todoDone
		var deletedReceived todoDeleted
		s := todoEventStore.GetStream("todo-list-1")
		s.Subscribe(consumer.ConsumerFunc[todoEvent](func(e todoEvent) {
			switch e.(type) {
			case todoCreated:
				createdReceived = e.(todoCreated)
			case todoDone:
				doneReceived = e.(todoDone)
			case todoDeleted:
				deletedReceived = e.(todoDeleted)
			}
		}))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		christmas := time.Date(2025, 12, 24, 0, 0, 0, 0, time.UTC)
		created := todoCreated{Date: christmas}
		err := s.WithType("todoCreated").Publish(context.Background(), created)
		require.NoError(t, err)
		done := todoDone{TodoID: 1, Date: christmas}
		err = s.WithType("todoDone").Publish(context.Background(), done)
		require.NoError(t, err)
		deleted := todoDeleted{TodoID: 1}
		err = s.WithType("todoDeleted").Publish(context.Background(), deleted)
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return created == createdReceived }, time.Second, time.Millisecond)
		assert.Eventually(t, func() bool { return done == doneReceived }, time.Second, time.Millisecond)
		assert.Eventually(t, func() bool { return deleted == deletedReceived }, time.Second, time.Millisecond)

	})

}
