package codec_test

import (
	"github.com/nbarbey/go-event-store/eventstore/codec"
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
}

func (s carRepaired) isCarEvent() {}

var christmas = time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
var soldAMercedesForChristmas = carSold{Brand: "Mercedes", Name: "Class A", Date: christmas}
var repairedAMercedes = carRepaired{CarID: "1"}

func testCodecMarshalUnmarshal(t *testing.T, c codec.Codec[carSold]) {
	payload, err := c.Marshall(soldAMercedesForChristmas)
	require.NoError(t, err)

	received, err := c.Unmarshall(payload)
	require.NoError(t, err)

	assert.Equal(t, soldAMercedesForChristmas.Brand, received.Brand)
	assert.Equal(t, soldAMercedesForChristmas.Name, received.Name)
	assert.True(t, soldAMercedesForChristmas.Date.Equal(received.Date))
}

func testCodecMarshalUnmarshalWithType(t *testing.T, c codec.TypedCodec[carEvent]) {
	payload, err := c.Marshall(soldAMercedesForChristmas)
	require.NoError(t, err)
	receivedSoldEvent, err := c.UnmarshallWithType("carSold", payload)
	require.NoError(t, err)
	payload, err = c.Marshall(repairedAMercedes)
	require.NoError(t, err)
	receivedRepairedEvent, err := c.UnmarshallWithType("carRepaired", payload)
	require.NoError(t, err)

	receivedSold := receivedSoldEvent.(carSold)
	assert.Equal(t, soldAMercedesForChristmas.Brand, receivedSold.Brand)
	assert.Equal(t, soldAMercedesForChristmas.Name, receivedSold.Name)
	assert.True(t, soldAMercedesForChristmas.Date.Equal(receivedSold.Date))
	receivedRepaired := receivedRepairedEvent.(carRepaired)
	assert.Equal(t, repairedAMercedes.CarID, receivedRepaired.CarID)
}
