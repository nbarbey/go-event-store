package repository

import "database/sql"

type eventRow struct {
	EventID   string
	EventType sql.NullString
	Version   sql.NullString
	StreamID  sql.NullString
	Payload   []byte
}

func (er *eventRow) ToRawEvent() *RawEvent {
	return &RawEvent{
		EventType: er.EventType.String,
		Version:   er.Version.String,
		Payload:   er.Payload,
	}
}

type eventRows []eventRow

func (ers eventRows) ToRawEvents() []*RawEvent {
	raws := make([]*RawEvent, 0)
	for _, e := range ers {
		raws = append(raws, e.ToRawEvent())
	}
	return raws
}

func (ers eventRows) payloads() [][]byte {
	payloads := make([][]byte, 0)
	for _, e := range ers {
		payloads = append(payloads, e.Payload)
	}
	return payloads
}

func (ers eventRows) types() []string {
	types := make([]string, 0)
	for _, e := range ers {
		types = append(types, e.EventType.String)
	}
	return types
}

func (ers eventRows) versions() []string {
	versions := make([]string, 0)
	for _, e := range ers {
		versions = append(versions, e.Version.String)
	}
	return versions
}
