package kafkaesqe

import (
	"context"
	"io"
	"log/slog"

	"github.com/indexsupply/shovel/queue"
	"github.com/segmentio/kafka-go"
)

type OnMessage interface {
	Exec(message kafka.Message) error
	ExecAsync(message kafka.Message, callback func(error))
}

type qsvc struct {
	reader    *kafka.Reader
	onMessage OnMessage
	logger    *slog.Logger
}

func NewConsumer(reader *kafka.Reader, onMessage OnMessage, logger *slog.Logger) queue.Consumer {
	return &qsvc{
		reader:    reader,
		onMessage: onMessage,
		logger:    logger,
	}
}

func (svc *qsvc) Start() error {
	for {
		m, err := svc.reader.ReadMessage(context.Background())
		if err == io.EOF {
			svc.logger.Error("reader has been closed", "err", err.Error())
			return nil
		}

		if err != nil {
			svc.logger.Error("unable to read message from kafka", "err", err.Error())
			continue
		}

		svc.onMessage.ExecAsync(m, func(err1 error) {
			if err1 != nil {
				svc.logger.Error("unable to execute message", "error", err1.Error(), "data", m.Value)
			}
		})
	}
}

func (svc qsvc) Stop() error {
	return svc.reader.Close()
}
