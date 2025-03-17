package codec_test

import (
	"github.com/nbarbey/go-event-store/eventstore/codec"
	"testing"
)

func TestJSONCodec_Marshall_Unmarshall(t *testing.T) {
	testCodecMarshalUnmarshal(t, codec.NewJSONCodec[carSold]())
}

func TestJSONCodec_Marshall_UnmarshallWithType(t *testing.T) {
	testCodecMarshalUnmarshalWithType(t, codec.NewJSONCodecWithTypeHints[carEvent](codec.NewUnmarshallerMap[carEvent]().
		AddFunc("carSold", func(payload []byte) (event carEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[carSold]()(payload)
		}).
		AddFunc("carRepaired", func(payload []byte) (event carEvent, err error) {
			return codec.BuildJSONUnmarshalFunc[carRepaired]()(payload)
		})))
}
