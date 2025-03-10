package codec

import (
	"encoding/json"
)

type JSONCodec[E any] struct {
	unmarshalers map[string]Unmarshaller[E]
}

func NewJSONCodec[E any]() *JSONCodec[E] {
	return &JSONCodec[E]{
		unmarshalers: make(map[string]Unmarshaller[E]),
	}
}

func (JSONCodec[E]) Marshall(event E) ([]byte, error) {
	return json.Marshal(event)
}

func (JSONCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	err = json.Unmarshal(payload, &event)
	return event, err
}

func (j JSONCodec[E]) UnmarshallWithType(typeHint string, payload []byte) (event E, err error) {
	u, ok := j.unmarshalers[typeHint]
	if ok {
		return u.Unmarshall(payload)
	}
	err = json.Unmarshal(payload, &event)
	return
}

func (j JSONCodec[E]) RegisterType(s string, u Unmarshaller[E]) {
	j.unmarshalers[s] = u
}

func BuildJSONUnmarshalFunc[E any]() UnmarshalerFunc[E] {
	return func(payload []byte) (event E, err error) {
		err = json.Unmarshal(payload, &event)
		return event, err
	}
}
