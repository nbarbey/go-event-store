package codec

import (
	"bytes"
	"encoding/gob"
)

func NewGobCodec[E any]() *GobCodec[E] {
	c := GobCodec[E]{}
	c.encoder = gob.NewEncoder(&c.encoderBuffer)
	c.decoder = gob.NewDecoder(&c.decoderBuffer)
	return &c
}

type GobCodec[E any] struct {
	encoderBuffer bytes.Buffer
	encoder       *gob.Encoder
	decoderBuffer bytes.Buffer
	decoder       *gob.Decoder
}

func (g *GobCodec[E]) Marshall(event E) ([]byte, error) {
	err := g.encoder.Encode(event)
	out := g.encoderBuffer.Bytes()
	g.encoderBuffer.Reset()
	return out, err
}

func (g *GobCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	g.decoderBuffer.Write(payload)
	err = g.decoder.Decode(&event)
	g.decoderBuffer.Reset()
	return event, err
}

type GobCodecWithTypeHints[E any] struct {
	*GobCodec[E]
	*UnmarshalerWithTypeHint[E]
}

func NewGobCodecWithTypeHints[E any](unmarshalers UnmarshallerMap[E]) *GobCodecWithTypeHints[E] {
	codec := NewGobCodec[E]()
	return &GobCodecWithTypeHints[E]{
		GobCodec:                codec,
		UnmarshalerWithTypeHint: NewUnmarshalerWithTypeHints[E](codec, unmarshalers),
	}
}

func BuildGobUnmarshalFunc[E any]() UnmarshalerFunc[E] {
	c := NewGobCodec[E]()
	return func(payload []byte) (event E, err error) {
		return c.Unmarshall(payload)
	}
}
