package eventstore

type Marshaller[E any] interface {
	Marshall(event E) ([]byte, error)
}
type Unmarshaller[E any] interface {
	Unmarshall(payload []byte) (event E, err error)
}

type TypedUnmarshaller[E any] interface {
	UnmarshallWithType(typeHint string, payload []byte) (event E, err error)
}

type Codec[E any] interface {
	Marshaller[E]
	Unmarshaller[E]
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
