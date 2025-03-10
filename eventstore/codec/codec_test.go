package codec

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type carEvent interface {
	isCarEvent()
}

type carSold struct {
	Brand string
	Name  string
	Date  time.Time
}

func (s carSold) isCarEvent() {}

type carRepaired struct {
	CarID string
	Date  time.Time
}

func (s carRepaired) isCarEvent() {}

var christmas = time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
var soldAMercedesForChristmas = carSold{Brand: "Mercedes", Name: "Class A", Date: christmas}
var newYear = time.Date(2026, 01, 01, 0, 0, 0, 0, time.UTC)
var repairedAMercredForNewYear = carRepaired{CarID: "1", Date: newYear}

func TestJSONCodec_Marshall_Unmarshall(t *testing.T) {
	c := NewJSONCodec[carSold]()

	payload, err := c.Marshall(soldAMercedesForChristmas)
	require.NoError(t, err)

	received, err := c.Unmarshall(payload)
	require.NoError(t, err)

	assert.Equal(t, soldAMercedesForChristmas, received)
}

func TestJSONCodec_Marshall_UnmarshallWithType(t *testing.T) {
	c := NewJSONCodecWithTypeHints[carEvent](map[string]Unmarshaller[carEvent]{
		"carSold": UnmarshalerFunc[carEvent](func(payload []byte) (event carEvent, err error) {
			return BuildJSONUnmarshalFunc[carSold]()(payload)
		}),
		"carRepaired": UnmarshalerFunc[carEvent](func(payload []byte) (event carEvent, err error) {
			return BuildJSONUnmarshalFunc[carRepaired]()(payload)
		}),
	})

	payload, err := c.Marshall(soldAMercedesForChristmas)
	require.NoError(t, err)
	receivedSold, err := c.UnmarshallWithType("carSold", payload)
	require.NoError(t, err)
	payload, err = c.Marshall(repairedAMercredForNewYear)
	require.NoError(t, err)
	receivedRepaired, err := c.UnmarshallWithType("carRepaired", payload)
	require.NoError(t, err)

	assert.Equal(t, soldAMercedesForChristmas, receivedSold)
	assert.Equal(t, repairedAMercredForNewYear, receivedRepaired)
}
