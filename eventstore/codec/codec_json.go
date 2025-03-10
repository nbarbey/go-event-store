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
	unmarshalers map[string]Unmarshaller[E]
}

func NewJSONCodecWithTypeHints[E any](unmarshalers map[string]Unmarshaller[E]) *JSONCodecWithTypeHints[E] {
	if unmarshalers == nil {
		unmarshalers = make(map[string]Unmarshaller[E])
	}
	return &JSONCodecWithTypeHints[E]{unmarshalers: unmarshalers}
}

func (j JSONCodecWithTypeHints[E]) UnmarshallWithType(typeHint string, payload []byte) (event E, err error) {
	u, ok := j.unmarshalers[typeHint]
	if ok {
		return u.Unmarshall(payload)
	}
	err = json.Unmarshal(payload, &event)
	return
}

func (j JSONCodecWithTypeHints[E]) RegisterType(s string, u Unmarshaller[E]) {
	j.unmarshalers[s] = u
}

func BuildJSONUnmarshalFunc[E any]() UnmarshalerFunc[E] {
	return func(payload []byte) (event E, err error) {
		err = json.Unmarshal(payload, &event)
		return event, err
	}
}
