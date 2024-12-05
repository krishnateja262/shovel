package queue

import "context"

type Producer interface {
	SendMessage(ctx context.Context, topic string, data interface{}) error
	SendCompressedMessage(ctx context.Context, topic string, data interface{}) error
}

type Consumer interface {
	Start() error
	Stop() error
}
