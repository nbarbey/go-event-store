package codec_test

import (
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"testing"
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
