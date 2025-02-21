package go_event_store

type ConsumerFunc[E any] func(e []byte)

type Consumer[E any] interface {
	Consume(e []byte)
}

func (f ConsumerFunc[E]) Consume(e []byte) {
	f(e)
}
