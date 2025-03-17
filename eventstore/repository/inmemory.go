package repository

import (
	"context"
	"strconv"
)

type internalEvent struct {
	eventId   string
	eventType *string
	version   *string
	streamId  *string
	payload   []byte
}

func (ie internalEvent) toRawEvent() (raw *RawEvent) {
	raw = &RawEvent{}
	if ie.eventType != nil {
		raw.EventType = *ie.eventType
	}
	if ie.version != nil {
		raw.Version = *ie.version
	}
	raw.Payload = ie.payload
	return
}

type InMemory struct {
	events   map[string][]internalEvent
	streamId string
}

func NewInMemory() *InMemory {
	return &InMemory{
		events:   make(map[string][]internalEvent),
		streamId: "default-stream",
	}
}

func (i *InMemory) Stream(name string) Repository {
	return &InMemory{
		events:   make(map[string][]internalEvent),
		streamId: name,
	}
}

func (i *InMemory) GetRawEvent(_ context.Context, eventId string) (*RawEvent, error) {
	pos, _ := strconv.Atoi(eventId)
	stream := i.currentStream()
	eventPos, _ := strconv.Atoi(eventId)
	if eventPos >= len(stream) {
		return nil, ErrEventNotFound
	}
	event := stream[pos]
	return event.toRawEvent(), nil
}

func (i *InMemory) InsertRawEvent(_ context.Context, raw RawEvent, expectedVersion string) (string, error) {
	previousEventPosition := len(i.currentStream()) - 1
	eventId := strconv.Itoa(previousEventPosition + 1)

	if expectedVersion != "" {
		if previousEventPosition >= 0 {
			lastEvent := i.currentStream()[previousEventPosition]
			if lastEvent.version != nil && *lastEvent.version != expectedVersion {
				return "", ErrVersionMismatch
			}
		}
	}

	i.events[i.streamId] = append(i.events[i.streamId], newInternalEventFromRawEvent(raw, i.streamId))

	return eventId, nil
}

func (i *InMemory) currentStream() []internalEvent {
	return i.events[i.streamId]
}

func (i *InMemory) AllRawEvents(_ context.Context) ([]*RawEvent, error) {
	out := make([]*RawEvent, 0)
	for _, event := range i.events[i.streamId] {
		if event.streamId != nil && *event.streamId == i.streamId {
			out = append(out, event.toRawEvent())
		}
	}
	return out, nil
}

func newInternalEventFromRawEvent(raw RawEvent, streamId string) (i internalEvent) {
	i.streamId = &streamId
	i.eventType = &raw.EventType
	if raw.Version != "" {
		i.version = &raw.Version
	}
	i.payload = raw.Payload
	return
}
