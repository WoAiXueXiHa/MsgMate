package data

import (
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"
)

func TestTimedKafkaProducerPreservesConfirmation(t *testing.T) {
	p := newTimedKafkaProducer([]string{"localhost:9092"}, "test", 20*time.Millisecond).(*timedKafkaProducer)
	defer p.Close()
	if p.writer.BatchTimeout != 20*time.Millisecond || p.writer.RequiredAcks != kafka.RequireAll || p.writer.Async {
		t.Fatalf("unexpected writer settings: timeout=%v ack=%v async=%v", p.writer.BatchTimeout, p.writer.RequiredAcks, p.writer.Async)
	}
	if _, ok := p.writer.Balancer.(*kafka.LeastBytes); !ok {
		t.Fatal("balancer changed")
	}
	if !p.writer.AllowAutoTopicCreation {
		t.Fatal("topic creation changed")
	}
}
func TestRejectNegativeKafkaBatchTimeout(t *testing.T) {
	var cf config.TomlConfig
	cf.Kafka.BatchTimeoutMS = -1
	if _, err := NewData(&cf); err == nil {
		t.Fatal("negative timeout accepted")
	}
}
