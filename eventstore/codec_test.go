package eventstore

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type carSold struct {
	Brand string
	Name  string
	Date  time.Time
}

func TestJSONCodec_Marshall_Unmarshall(t *testing.T) {
	c := JSONCodec[carSold]{}

	christmas := time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
	aMercedesForChristmas := carSold{Brand: "Mercedes", Name: "Class A", Date: christmas}
	payload, err := c.Marshall(aMercedesForChristmas)
	require.NoError(t, err)

	received, err := c.Unmarshall(payload)
	require.NoError(t, err)

	assert.Equal(t, aMercedesForChristmas, received)
}
