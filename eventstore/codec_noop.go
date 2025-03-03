package eventstore

type NoopCodec struct {
}

func (n NoopCodec) Marshall(event string) ([]byte, error) {
	return []byte(event), nil
}

func (n NoopCodec) Unmarshall(payload []byte) (event string, err error) {
	return string(payload), nil
}

func (n NoopCodec) RegisterType(s string, u Unmarshaller[string]) {
}

func (n NoopCodec) UnmarshallWithType(typeHint string, payload []byte) (event string, err error) {
	return "", nil
}
