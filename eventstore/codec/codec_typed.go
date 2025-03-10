package codec

type TypeRegister[E any] interface {
	RegisterType(s string, u Unmarshaller[E])
}

type TypedUnmarshaller[E any] interface {
	UnmarshallWithType(typeHint string, payload []byte) (event E, err error)
}

type TypedCodec[E any] interface {
	Codec[E]
	TypeRegister[E]
	TypedUnmarshaller[E]
}

func UnmarshallAllWithType[E any](u TypedUnmarshaller[E], types []string, payloads [][]byte) (events []E, err error) {
	output := make([]E, 0)
	for i, payload := range payloads {
		event, err := u.UnmarshallWithType(types[i], payload)
		if err != nil {
			return nil, err
		}
		output = append(output, event)
	}
	return output, nil
}
