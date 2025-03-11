package consumer

type ConsumerFunc[E any] func(e E)

type Consumer[E any] interface {
	Consume(e E)
}

func (f ConsumerFunc[E]) Consume(e E) {
	f(e)
}
