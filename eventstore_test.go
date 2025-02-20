package go_event_store_test

import (
	"context"
	ges "github.com/nbarbey/go-event-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPublish(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()
	es, err := ges.NewEventStore(context.Background(), postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	assert.NotEmpty(t, es)
}
