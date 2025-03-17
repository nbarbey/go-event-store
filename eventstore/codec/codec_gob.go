package codec

import (
	"bytes"
	"encoding/gob"
)

func NewGobCodec[E any]() *GobCodec[E] {
	return &GobCodec[E]{}
}

type GobCodec[E any] struct{}

func (GobCodec[E]) Marshall(event E) ([]byte, error) {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(event)
	return buffer.Bytes(), err
}

func (GobCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	buffer := bytes.NewBuffer(payload)
	decoder := gob.NewDecoder(buffer)
	err = decoder.Decode(&event)
	return event, err
}

type GobCodecWithTypeHints[E any] struct {
	GobCodec[E]
	*UnmarshalerWithTypeHint[E]
}

func NewGobCodecWithTypeHints[E any](unmarshalers UnmarshallerMap[E]) *GobCodecWithTypeHints[E] {
	return &GobCodecWithTypeHints[E]{UnmarshalerWithTypeHint: NewUnmarshalerWithTypeHints[E](GobCodec[E]{}, unmarshalers)}
}

func BuildGobUnmarshalFunc[E any]() UnmarshalerFunc[E] {
	c := NewGobCodec[E]()
	return func(payload []byte) (event E, err error) {
		return c.Unmarshall(payload)
	}
}
