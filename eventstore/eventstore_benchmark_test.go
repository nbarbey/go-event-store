package eventstore_test

import (
	context2 "context"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
	"github.com/nbarbey/go-event-store/eventstore/repository"
	"testing"
)

func BenchmarkPublisher(b *testing.B) {
	typedRepository, err := repository.NewTypedRepository[string](context2.Background(), postgresContainer.ConnectionString("search_path=string_events"), codec.NoopCodec[string]{})
	if err != nil {
		panic(err)
	}
	publisher := eventstore.NewPublisher[string]("my_stream", typedRepository)

	for i := 0; i < b.N; i++ {
		_ = publisher.Publish(context2.Background(), "coucou")
	}
}

type benchEvent struct {
	Benchmark string
	ID        uint
	BoolArray []bool
}

func BenchmarkPublisherJSON(b *testing.B) {
	typedRepository, err := repository.NewTypedRepository[benchEvent](context2.Background(), postgresContainer.ConnectionString("search_path=string_events"), codec.NewJSONCodecWithTypeHints[benchEvent](nil))
	if err != nil {
		panic(err)
	}
	publisher := eventstore.NewPublisher[benchEvent]("my_stream", typedRepository)

	for i := 0; i < b.N; i++ {
		_ = publisher.Publish(context2.Background(), benchEvent{Benchmark: "awesome bench", ID: 1, BoolArray: []bool{true, true, false, true, false, false}})
	}
}

func BenchmarkPublisherGob(b *testing.B) {
	typedRepository, err := repository.NewTypedRepository[benchEvent](context2.Background(), postgresContainer.ConnectionString("search_path=string_events"), codec.NewGobCodecWithTypeHints[benchEvent](nil))
	if err != nil {
		panic(err)
	}
	publisher := eventstore.NewPublisher[benchEvent]("my_stream", typedRepository)

	for i := 0; i < b.N; i++ {
		_ = publisher.Publish(context2.Background(), benchEvent{Benchmark: "awesome bench", ID: 1, BoolArray: []bool{true, true, false, true, false, false}})
	}
}

func BenchmarkSubscriber(b *testing.B) {
	es, err := eventstore.NewEventStore[string](context2.Background(), postgresContainer.ConnectionString("search_path=string_events"))
	if err != nil {
		panic(err)
	}
	es.Subscribe(consumer.ConsumerFunc[string](func(e string) {}))

	for i := 0; i < b.N; i++ {
		_ = es.Publish(context2.Background(), "Hey!")
	}
}
