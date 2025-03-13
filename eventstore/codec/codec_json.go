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

type UnmarshallerMap[E any] map[string]Unmarshaller[E]

func NewUnmarshallerMap[E any]() UnmarshallerMap[E] {
	return make(UnmarshallerMap[E], 0)
}

func (um UnmarshallerMap[E]) Add(typeHint string, unmarshaller Unmarshaller[E]) UnmarshallerMap[E] {
	um[typeHint] = unmarshaller
	return um
}

func (um UnmarshallerMap[E]) AddFunc(typeHint string, f UnmarshalerFunc[E]) UnmarshallerMap[E] {
	um[typeHint] = f
	return um
}

func NewJSONCodecWithTypeHints[E any](unmarshalers UnmarshallerMap[E]) *JSONCodecWithTypeHints[E] {
	return &JSONCodecWithTypeHints[E]{UnmarshalerWithTypeHint: NewUnmarshalerWithTypeHints[E](JSONCodec[E]{}, unmarshalers)}
}

func BuildJSONUnmarshalFunc[E any]() UnmarshalerFunc[E] {
	return func(payload []byte) (event E, err error) {
		err = json.Unmarshal(payload, &event)
		return event, err
	}
}
