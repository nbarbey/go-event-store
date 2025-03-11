package repository

import "database/sql"

type eventRow struct {
	EventID   string
	EventType sql.NullString
	Version   sql.NullString
	StreamID  sql.NullString
	Payload   []byte
}

type eventRows []eventRow

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
