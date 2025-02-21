package go_event_store

type ConsumerFunc func(e []byte)

type Consumer interface {
	Consume(e []byte)
}

func (f ConsumerFunc) Consume(e []byte) {
	f(e)
}
