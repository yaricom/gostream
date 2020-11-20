// Package gostream implements data stream based on Redis stream
package gostream

import (
	"context"
	"errors"
	"time"
)

var (
	ErrStreamIsFull = errors.New("stream is full")
	ErrTimeout      = errors.New("timeout")
)

// DataEvent is to encapsulate stream data event
type DataEvent struct {
	RecordID string
	Data     interface{}
	raw      interface{}
}

// DataStream is the buffer stream for the data events
type DataStream interface {
	// AddEvent is to add given event to the stream. If stream has reached cap limit of pending messages
	// the ErrStreamIsFull error returned.
	AddEvent(event DataEvent) error
	// PopEvent is to read event from the stream blocking for specified timeout period if no event available. The result
	// can be pointer or value and should support marshalling/unmarshalling from JSON. If timeout occurred the ErrTimeout
	// will be returned.
	ReadEvent(result interface{}, timeout time.Duration) (DataEvent, error)
	// AckEvent is to acknowledge that data event was fully consumed
	AckEvent(event DataEvent) error

	// Open is to open the data stream. If ctx.Done() returns not nil while stream is opening the operation will be canceled.
	Open(ctx context.Context) error
	// Close is to close the data stream
	Close() error
}
