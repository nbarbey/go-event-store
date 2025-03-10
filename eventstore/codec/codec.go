package codec

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
