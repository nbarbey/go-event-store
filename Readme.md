# An event store library in go on top of PostgreSQL

This library implements an event store on top of PostgreSQL.
It handles typing and versioning of events.
It handles marshalling and unmarshalling so that event payloads
are stored uniformly as byte arrays in PostgreSQL.

This is heavily inspired by Marten https://martendb.io/.
This is also inspired by a talk called "Let's build event store in one hour!" by Oskar Dudycz
which he gave at NDC Oslo in 2022. (see https://www.youtube.com/watch?v=gaoZdtQSOTo).

It has been used as a pedagogic tool to explain the event sourcing architecture
using the banking kata (https://kata-log.rocks/banking-kata) with good success.

This code is not production ready as it has not been tested in production yet.

## example

```go

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

```