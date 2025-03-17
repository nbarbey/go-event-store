package repository

import "context"

type Listener interface {
	Handle(h handler)
	Listen(ctx context.Context) error
}
