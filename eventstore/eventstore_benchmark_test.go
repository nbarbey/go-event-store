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
