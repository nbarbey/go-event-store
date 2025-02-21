package main

import (
	"context"
	"github.com/nbarbey/go-event-store/eventstore"
)

func main() {
	es, err := eventstore.NewEventStore[any](context.Background(), "postgres://user:password@localhost:5432/postgres")
	if err != nil {
		panic(err)
	}
	err = es.Start(context.Background())
	if err != nil {
		panic(err)
	}
	defer es.Stop()

	server := eventstore.NewServerFromEventStore[any]("localhost:8080", es)
	err = server.Start()
	if err != nil {
		panic(err)
	}

}
