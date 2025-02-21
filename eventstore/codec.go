package eventstore

import "encoding/json"

type Codec[E any] interface {
	Marshall(event E) ([]byte, error)
	Unmarshall(payload []byte) (event E, err error)
}

type JSONCodec[E any] struct{}

func (JSONCodec[E]) Marshall(event E) ([]byte, error) {
	return json.Marshal(event)
}

func (JSONCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	err = json.Unmarshal(payload, &event)
	return event, err
}
