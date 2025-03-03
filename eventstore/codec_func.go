package eventstore

type UnmarshalerFunc[E any] func(payload []byte) (event E, err error)

func (f UnmarshalerFunc[E]) Unmarshall(payload []byte) (event E, err error) {
	return f(payload)
}
