package go_event_store

type Stream struct {
	name       string
	eventStore EventStore
}

func (s Stream) Publish(bytes []byte) error {
	return s.eventStore.Publish(bytes)
}

func (s Stream) Subscribe(consumer ConsumerFunc) {
	s.eventStore.Subscribe(consumer)
}

func (e EventStore) Stream(name string) *Stream {
	return &Stream{name: name, eventStore: e}
}
