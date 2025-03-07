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
	stringEventStore, err := eventstore.NewEventStore[string](ctx, postgresContainer.ConnectionString(t, "search_path=string_events"))
	require.NoError(t, err)

	stringEventStore.WithCodec(eventstore.NoopCodec{})

	t.Run("publish and get all", func(t *testing.T) {
		_, err := stringEventStore.Publish(context.Background(), "my_event_data")
		require.NoError(t, err)
		events, err := stringEventStore.All(context.Background())
		require.NoError(t, err)

		assert.Len(t, events, 1)
		assert.Equal(t, "my_event_data", events[0])
	})
	t.Run("publish and get all of one stream", func(t *testing.T) {
		stream := stringEventStore.GetStream("awesome-string-stream")

		_, err := stringEventStore.Publish(context.Background(), "default stream data")
		require.NoError(t, err)
		_, err = stream.Publish(context.Background(), "some other stream data")
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

		_, err := stringEventStore.Publish(context.Background(), "my_event_data")
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

		_, err := stringEventStore.Publish(context.Background(), "my_event_data")
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received)
		}, time.Second, 10*time.Millisecond)
	})
	t.Run("subscribe from beginning", func(t *testing.T) {
		_, err := stringEventStore.Publish(context.Background(), "my_event_data")
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

		_, err := stringEventStore.GetStream("some-string-stream").Publish(context.Background(), "my_event_data")
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return "my_event_data" == string(received) && len(receivedOther) == 0
		}, time.Second, 10*time.Millisecond)
	})

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

		_, err := myStream.Publish(context.Background(), MyEvent{Name: "John"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return received == MyEvent{Name: "John"} }, time.Second, 10*time.Millisecond)
	})

	t.Run("publish with type", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		_, err := myStream.WithType("my_event").Publish(context.Background(), MyEvent{Name: "John"})
		require.NoError(t, err)

		expected := MyEvent{Name: "John"}
		assert.Eventually(t, func() bool { return expected == received }, time.Second, time.Millisecond)
	})

	t.Run("publish with expected version and reject if not expected", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		version, err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		expected := MyEvent{Name: "Rose"}
		assert.Eventually(t, func() bool { return expected == received }, 10*time.Second, time.Millisecond)
		assert.NotEmpty(t, version)

		_, err = myStream.ExpectedVersion("unexpected").Publish(context.Background(), MyEvent{Name: "Juan"})
		assert.Error(t, err)

	})

	t.Run("publish with type and expected version", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-incredible-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		_, err := myStream.ExpectedVersion("").WithType("my_event_type").Publish(context.Background(), MyEvent{Name: "Felipe"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return MyEvent{Name: "Felipe"} == received }, time.Second, time.Millisecond)
	})

	t.Run("publish with expected version and accept if expected matches actual version", func(t *testing.T) {
		var received MyEvent
		myStream := customEventStore.GetStream("my-custom-event-stream")
		myStream.Subscribe(makeTestConsumer[MyEvent](&received))

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		version, err := myStream.ExpectedVersion("").Publish(context.Background(), MyEvent{Name: "Rose"})
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return MyEvent{Name: "Rose"} == received }, 10*time.Second, time.Millisecond)
		assert.NotEmpty(t, version)

		_, err = myStream.ExpectedVersion(version).Publish(context.Background(), MyEvent{Name: "Juan"})
		assert.NoError(t, err)
		assert.Eventually(t, func() bool { return MyEvent{Name: "Juan"} == received }, 10*time.Second, time.Millisecond)
	})

	todoEventStore, err := eventstore.NewEventStore[todoEvent](context.Background(), postgresContainer.ConnectionString(t, "search_path=todo_events"))
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
		s := todoEventStore.GetStream("todo-list-1")
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

		// give time for listener to be set-up properly
		time.Sleep(10 * time.Millisecond)

		christmas := time.Date(2025, 12, 24, 0, 0, 0, 0, time.UTC)
		created := todoCreated{Date: christmas}
		_, err := s.WithType("todoCreated").Publish(context.Background(), created)
		require.NoError(t, err)
		done := todoDone{TodoID: 1, Date: christmas}
		_, err = s.WithType("todoDone").Publish(context.Background(), done)
		require.NoError(t, err)
		deleted := todoDeleted{TodoID: 1}
		_, err = s.WithType("todoDeleted").Publish(context.Background(), deleted)
		require.NoError(t, err)

		assert.Eventually(t, func() bool { return created == createdReceived }, time.Second, time.Millisecond)
		assert.Eventually(t, func() bool { return done == doneReceived }, time.Second, time.Millisecond)
		assert.Eventually(t, func() bool { return deleted == deletedReceived }, time.Second, time.Millisecond)

	})

}

func makeTestConsumer[E any](received *E) eventstore.ConsumerFunc[E] {
	return func(e E) {
		*received = e
	}
}
