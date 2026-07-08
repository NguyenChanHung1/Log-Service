package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"log-service/internal/logevent"
)

type Producer struct {
	brokers []string
	writer  *kafka.Writer
}

func NewProducer(brokers string, topic string) *Producer {
	brokerList := splitBrokers(brokers)
	return &Producer{
		brokers: brokerList,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokerList...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, events []logevent.Event) error {
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			return err
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(event.Source),
			Value: value,
			Time:  event.ReceivedAt,
		})
	}

	return p.writer.WriteMessages(ctx, messages...)
}

func (p *Producer) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func splitBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
