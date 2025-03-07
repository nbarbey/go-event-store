package eventstore_test

import (
	"github.com/nbarbey/go-event-store/eventstore"
	"os"
	"testing"
)

var postgresContainer *testPostgresContainer

func TestMain(m *testing.M) {
	var err error
	postgresContainer, err = runTestContainer()
	if err != nil {
		panic(err)
	}
	defer postgresContainer.Cancel()

	os.Exit(m.Run())
}

func makeTestConsumer[E any](received *E) eventstore.ConsumerFunc[E] {
	return func(e E) {
		*received = e
	}
}
