package eventstore

import "encoding/json"

type Marshaller[E any] interface {
	Marshall(event E) ([]byte, error)
}
type Unmarshaller[E any] interface {
	Unmarshall(payload []byte) (event E, err error)
}

type Codec[E any] interface {
	Marshaller[E]
	Unmarshaller[E]
}

type JSONCodec[E any] struct{}

func (JSONCodec[E]) Marshall(event E) ([]byte, error) {
	return json.Marshal(event)
}

func (JSONCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	err = json.Unmarshal(payload, &event)
	return event, err
}

func UnmarshallAll[E any](u Unmarshaller[E], payloads [][]byte) (events []E, err error) {
	output := make([]E, 0)
	for _, payload := range payloads {
		event, err := u.Unmarshall(payload)
		if err != nil {
			return nil, err
		}
		output = append(output, event)
	}
	return output, nil
}
