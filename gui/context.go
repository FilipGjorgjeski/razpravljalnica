package gui

import (
	"context"
	"time"
)

func TimeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*5)
}
