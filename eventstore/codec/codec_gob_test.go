package codec_test

import (
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"testing"
	"time"
)

func TestGobCodec_Marshall_Unmarshall(t *testing.T) {
	testCodecMarshalUnmarshal(t, codec.NewGobCodec[carSold]())
}

func TestGobCodec_Marshall_UnmarshallWithType(t *testing.T) {
	testCodecMarshalUnmarshalWithType(t, codec.NewGobCodecWithTypeHints[carEvent](codec.NewUnmarshallerMap[carEvent]().
		AddFunc("carSold", func(payload []byte) (event carEvent, err error) {
			return codec.BuildGobUnmarshalFunc[carSold]()(payload)
		}).
		AddFunc("carRepaired", func(payload []byte) (event carEvent, err error) {
			return codec.BuildGobUnmarshalFunc[carRepaired]()(payload)
		})))
}

func BenchmarkGobCodec_Marshall(b *testing.B) {
	c := codec.NewGobCodec[carSold]()

	for i := 0; i < b.N; i++ {
		_, _ = c.Marshall(soldAMercedesForChristmas)
	}
}

type largeEvent struct {
	Name        string
	Count       uint
	Temperature int
	Date        time.Time
	Stuff       []string
}

var bigBlaBla = largeEvent{
	Name:        "big",
	Count:       10,
	Temperature: -15,
	Date:        time.Date(2025, 10, 22, 5, 3, 2, 1, time.UTC),
	Stuff:       []string{"bla", "bla", "bla"},
}

func BenchmarkGobCodec_Marshall_big(b *testing.B) {
	c := codec.NewGobCodec[largeEvent]()

	for i := 0; i < b.N; i++ {
		_, _ = c.Marshall(bigBlaBla)
	}
}

func BenchmarkGobCodec_MarshallUnmarshal_big(b *testing.B) {
	c := codec.NewGobCodec[largeEvent]()

	for i := 0; i < b.N; i++ {
		payload, _ := c.Marshall(bigBlaBla)
		_, _ = c.Unmarshall(payload)
	}
}
