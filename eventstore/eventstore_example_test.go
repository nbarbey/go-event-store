package eventstore_test

import (
	"context"
	"fmt"
	"github.com/nbarbey/go-event-store/eventstore"
	"github.com/nbarbey/go-event-store/eventstore/consumer"
	"time"
)

func ExampleEventStore() {
	type eventStoreDeployed struct {
		Date time.Time
		Env  string
	}
	var log string
	eventStore := eventstore.NewInMemoryEventStore[eventStoreDeployed]()
	eventStore.Subscribe(consumer.ConsumerFunc[eventStoreDeployed](func(e eventStoreDeployed) {
		log = fmt.Sprintf("event store deployed in %s environment at %s", e.Env, e.Date.Format("2006-02-01"))
	}))

	_ = eventStore.Publish(context.Background(), eventStoreDeployed{Env: "production", Date: time.Date(2006, 01, 01, 0, 0, 0, 0, time.UTC)})
	fmt.Print(log)
	// Output: event store deployed in production environment at 2006-01-01
}
