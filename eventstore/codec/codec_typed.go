package codec

type TypedUnmarshaller[E any] interface {
	UnmarshallWithType(typeHint string, payload []byte) (event E, err error)
}

type TypedCodec[E any] interface {
	Codec[E]
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
