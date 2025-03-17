package repository

import (
	"context"
	"errors"
)

type Repository interface {
	Stream(name string) Repository
	GetRawEvent(ctx context.Context, eventId string) (*RawEvent, error)
	InsertRawEvent(ctx context.Context, raw RawEvent, expectedVersion string) (string, error)
	AllRawEvents(ctx context.Context) ([]*RawEvent, error)
}

var ErrEventNotFound = errors.New("event not found")
var ErrVersionMismatch = errors.New("mismatched version")
