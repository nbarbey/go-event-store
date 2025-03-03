package eventstore_test

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
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

func TestEventStore(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stringEventStore, err := eventstore.NewEventStore[string](ctx, postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	stringEventStore.WithCodec(eventstore.NoopCodec{})

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

	t.Run("publish with type", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.Stream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		startTestEventStore(t, customEventStore)
		defer customEventStore.Stop()

		require.NoError(t, myStream.WithType("my_event").Publish(context.Background(), MyEvent{Name: "John"}))

		assert.Eventually(t, func() bool {
			return received == MyEvent{Name: "John"}
		}, time.Second, 10*time.Millisecond)
	})

	todoEventStore, err := eventstore.NewEventStore[todoEvent](context.Background(), postgresContainer.ConnectionString(t))
	require.NoError(t, err)
	codec := eventstore.NewJSONCodec[todoEvent]()
	codec.RegisterType("todoCreated", eventstore.UnmarshalerFunc[todoEvent](func(payload []byte) (event todoEvent, err error) {
		return eventstore.BuildJSONUnmarshalFunc[todoCreated]()(payload)
	}))
	codec.RegisterType("todoDone", eventstore.UnmarshalerFunc[todoEvent](func(payload []byte) (event todoEvent, err error) {
		return eventstore.BuildJSONUnmarshalFunc[todoDone]()(payload)
	}))
	codec.RegisterType("todoDeleted", eventstore.UnmarshalerFunc[todoEvent](func(payload []byte) (event todoEvent, err error) {
		return eventstore.BuildJSONUnmarshalFunc[todoDeleted]()(payload)
	}))
	todoEventStore.WithCodec(codec)

	t.Run("publish multiple events with different types on same stream", func(t *testing.T) {
		var createdReceived todoCreated
		var doneReceived todoDone
		var deletedReceived todoDeleted
		s := todoEventStore.Stream("todo-list-1")
		s.Subscribe(eventstore.ConsumerFunc[todoEvent](func(e todoEvent) {
			switch e.(type) {
			case todoCreated:
				createdReceived = e.(todoCreated)
			case todoDone:
				doneReceived = e.(todoDone)
			case todoDeleted:
				deletedReceived = e.(todoDeleted)
			}
		}))

		startTestEventStore(t, todoEventStore)
		defer customEventStore.Stop()

		christmas := time.Date(2025, 12, 24, 0, 0, 0, 0, time.UTC)
		created := todoCreated{Date: christmas}
		require.NoError(t, s.WithType("todoCreated").Publish(context.Background(), created))
		done := todoDone{TodoID: 1, Date: christmas}
		require.NoError(t, s.WithType("todoDone").Publish(context.Background(), done))
		deleted := todoDeleted{TodoID: 1}
		require.NoError(t, s.WithType("todoDeleted").Publish(context.Background(), deleted))

		eventuallyEqual(t, created, createdReceived)
		eventuallyEqual(t, done, doneReceived)
		eventuallyEqual(t, deleted, deletedReceived)
	})

}

func eventuallyEqual(t *testing.T, expected any, received any) {
	t.Helper()
	assert.Eventually(t, func() bool { return assert.Equal(t, expected, received) }, time.Second, time.Millisecond)
}

func makeTestConsumer[E any](received *E) eventstore.ConsumerFunc[E] {
	return func(e E) { *received = e }
}

func startTestEventStore[E any](t *testing.T, es *eventstore.EventStore[E]) {
	require.NoError(t, es.Start(context.Background()))
	// give time for listener to be set-up properly
	time.Sleep(10 * time.Millisecond)
}
