package codec

import (
	"encoding/json"
)

func NewJSONCodec[E any]() *JSONCodec[E] {
	return &JSONCodec[E]{}
}

type JSONCodec[E any] struct{}

func (JSONCodec[E]) Marshall(event E) ([]byte, error) {
	return json.Marshal(event)
}

func (JSONCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	err = json.Unmarshal(payload, &event)
	return event, err
}

type JSONCodecWithTypeHints[E any] struct {
	JSONCodec[E]
	*UnmarshalerWithTypeHint[E]
}

func NewJSONCodecWithTypeHints[E any](unmarshalers map[string]Unmarshaller[E]) *JSONCodecWithTypeHints[E] {
	return &JSONCodecWithTypeHints[E]{UnmarshalerWithTypeHint: NewUnmarshalerWithTypeHints[E](JSONCodec[E]{}, unmarshalers)}
}

func BuildJSONUnmarshalFunc[E any]() UnmarshalerFunc[E] {
	return func(payload []byte) (event E, err error) {
		err = json.Unmarshal(payload, &event)
		return event, err
	}
}
