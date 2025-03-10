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

type UnmarshalerWithTypeHint[E any] struct {
	defaultUnmarshaler Unmarshaller[E]
	unmarshalers       map[string]Unmarshaller[E]
}

func NewUnmarshalerWithTypeHints[E any](defaultUnmarshaler Unmarshaller[E], unmarshalers map[string]Unmarshaller[E]) *UnmarshalerWithTypeHint[E] {
	if unmarshalers == nil {
		unmarshalers = make(map[string]Unmarshaller[E])
	}
	return &UnmarshalerWithTypeHint[E]{
		defaultUnmarshaler: defaultUnmarshaler,
		unmarshalers:       unmarshalers,
	}
}

func (j UnmarshalerWithTypeHint[E]) UnmarshallWithType(typeHint string, payload []byte) (event E, err error) {
	u, ok := j.unmarshalers[typeHint]
	if ok {
		return u.Unmarshall(payload)
	}
	return j.defaultUnmarshaler.Unmarshall(payload)
}

func (j UnmarshalerWithTypeHint[E]) RegisterType(s string, u Unmarshaller[E]) {
	j.unmarshalers[s] = u
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
