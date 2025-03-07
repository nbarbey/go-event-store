package eventstore

type NoopCodec[E string | []byte] struct {
}

func (n NoopCodec[E]) Marshall(event E) ([]byte, error) {
	return []byte(event), nil
}

func (n NoopCodec[E]) Unmarshall(payload []byte) (event E, err error) {
	return E(payload), nil
}

func (n NoopCodec[E]) RegisterType(_ string, _ Unmarshaller[string]) {
}

func (n NoopCodec[E]) UnmarshallWithType(_ string, payload []byte) (event E, err error) {
	return E(payload), nil
}
