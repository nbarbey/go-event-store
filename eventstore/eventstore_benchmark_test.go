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
	pg, err := repository.NewPostgres(context2.Background(), postgresContainer.ConnectionString("search_path=string_events"))
	typedRepository := repository.NewTypedRepository[string](pg, codec.NoopCodec[string]{})
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
	pg, err := repository.NewPostgres(context2.Background(), postgresContainer.ConnectionString("search_path=string_events"))
	if err != nil {
		panic(err)
	}
	typedRepository := repository.NewTypedRepository[benchEvent](pg, codec.NewJSONCodecWithTypeHints[benchEvent](nil))

	publisher := eventstore.NewPublisher[benchEvent]("my_stream", typedRepository)

	for i := 0; i < b.N; i++ {
		_ = publisher.Publish(context2.Background(), benchEvent{Benchmark: "awesome bench", ID: 1, BoolArray: []bool{true, true, false, true, false, false}})
	}
}

func BenchmarkPublisherGob(b *testing.B) {
	pg, err := repository.NewPostgres(context2.Background(), postgresContainer.ConnectionString("search_path=string_events"))
	if err != nil {
		panic(err)
	}
	typedRepository := repository.NewTypedRepository[benchEvent](pg, codec.NewJSONCodecWithTypeHints[benchEvent](nil))
	publisher := eventstore.NewPublisher[benchEvent]("my_stream", typedRepository)

	for i := 0; i < b.N; i++ {
		_ = publisher.Publish(context2.Background(), benchEvent{Benchmark: "awesome bench", ID: 1, BoolArray: []bool{true, true, false, true, false, false}})
	}
}

func BenchmarkSubscriber(b *testing.B) {
	es, err := eventstore.NewPostgresEventStore[string](context2.Background(), postgresContainer.ConnectionString("search_path=string_events"))
	if err != nil {
		panic(err)
	}
	es.Subscribe(consumer.ConsumerFunc[string](func(e string) {}))

	for i := 0; i < b.N; i++ {
		_ = es.Publish(context2.Background(), "Hey!")
	}
}
