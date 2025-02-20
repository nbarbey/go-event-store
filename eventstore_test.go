package go_event_store_test

import (
	"context"
	ges "github.com/nbarbey/go-event-store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPublish_and_list_all_events(t *testing.T) {
	postgresContainer, err := runTestContainer()
	require.NoError(t, err)
	defer postgresContainer.Cancel()
	es, err := ges.NewEventStore(context.Background(), postgresContainer.ConnectionString(t))
	require.NoError(t, err)

	require.NoError(t, es.Publish([]byte(`"my_event_data"`)))
	events, err := es.All()
	require.NoError(t, err)

	assert.Len(t, events, 1)
	assert.Equal(t, []byte(`"my_event_data"`), events[0])
}
