package eventstore_test

import (
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
