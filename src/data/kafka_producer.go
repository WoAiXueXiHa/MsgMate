package data

import (
	"context"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/pkg/middlewares/mq"
	"github.com/segmentio/kafka-go"
	"time"
)

// timedKafkaProducer preserves the existing synchronous producer's settings.
type timedKafkaProducer struct{ writer *kafka.Writer }

func newTimedKafkaProducer(brokers []string, topic string, timeout time.Duration) mq.Producer {
	return &timedKafkaProducer{writer: &kafka.Writer{
		Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll, Async: false, AllowAutoTopicCreation: true,
		BatchTimeout: timeout,
	}}
}
func (p *timedKafkaProducer) SendMessage(message []byte) error {
	return p.writer.WriteMessages(context.Background(), kafka.Message{Value: message})
}
func (p *timedKafkaProducer) Close() {
	if err := p.writer.Close(); err != nil {
		log.Errorf("Error closing producer: %v", err)
	}
}
