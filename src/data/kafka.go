package data

import (
	"context"
	"fmt"
	"github.com/WoAiXueXiHa/MsgMate/src/concurrency"
	"time"

	conf "github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/segmentio/kafka-go"
)

// kafkaConsumer 并发处理有上限的小批次，整批完成后才提交 offset。
// 批内失败或取消时不提交；提交重试不会重新执行已成功的处理器。
type kafkaConsumer struct {
	reader     kafkaBatchReader
	workers    int
	batchWait  time.Duration
	retryPause time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
}

func newKafkaConsumer(brokers []string, topic, group string) *kafkaConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &kafkaConsumer{reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, StartOffset: kafka.FirstOffset, MaxWait: time.Second}), ctx: ctx, cancel: cancel}
}

type kafkaBatchReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

func (c *kafkaConsumer) ConsumeMessages(handler func([]byte) error) {
	workers := c.workers
	if workers < 1 {
		workers = 1
	}
	batchWait := c.batchWait
	if batchWait <= 0 {
		batchWait = 5 * time.Millisecond
	}
	for c.ctx.Err() == nil {
		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			if !pause(c.ctx) {
				return
			}
			continue
		}
		batch := []kafka.Message{msg}
		// At most workers messages can be in flight. A short fill deadline avoids
		// waiting indefinitely for a full batch at low arrival rates.
		fill, cancel := context.WithTimeout(c.ctx, batchWait)
		for len(batch) < workers {
			next, err := c.reader.FetchMessage(fill)
			if err != nil {
				break
			}
			batch = append(batch, next)
		}
		cancel()
		if !handleBatchAndCommit(c.ctx, batch, workers, handler, func() error { return c.reader.CommitMessages(c.ctx, batch...) }) {
			return
		}
		if c.retryPause > 0 && !delayContext(c.ctx, c.retryPause) {
			return
		}
	}
}

// No commit can cross an unfinished message, even if later offsets finish first.
func handleBatchAndCommit(ctx context.Context, batch []kafka.Message, workers int, handler func([]byte) error, commit func() error) bool {
	err := concurrency.Run(ctx, len(batch), workers, func(i int) error {
		if !retryHandler(ctx, batch[i].Value, handler) {
			return ctx.Err()
		}
		return nil
	})
	if err != nil {
		return false
	}
	return retryCommit(ctx, commit)
}
func retryHandler(ctx context.Context, body []byte, handler func([]byte) error) bool {
	for ctx.Err() == nil {
		if err := handler(body); err == nil {
			return true
		}
		if !pause(ctx) {
			return false
		}
	}
	return false
}
func retryCommit(ctx context.Context, commit func() error) bool {
	for ctx.Err() == nil {
		if err := commit(); err == nil {
			return true
		}
		if !pause(ctx) {
			return false
		}
	}
	return false
}
func delayContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// handleAndCommit 分开重试处理和提交：处理成功后，提交失败只重试 offset，
// 不再次执行渠道调用。进程崩溃后的重新消费仍依赖业务记录判断状态。
func handleAndCommit(ctx context.Context, body []byte, handler func([]byte) error, commit func() error) bool {
	return retryHandler(ctx, body, handler) && retryCommit(ctx, commit)
}

func pause(ctx context.Context) bool {
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (c *kafkaConsumer) Close() { c.cancel(); _ = c.reader.Close() }

// validateKafka 检查四种优先级配置，并确保 Topic 存在且 Leader 就绪。
// 创建单分区、单副本 Topic 适合当前本地部署，不提供副本容灾保证。
func validateKafka(cf *conf.TomlConfig) error {
	if len(cf.Kafka.Brokers) == 0 {
		return fmt.Errorf("Kafka brokers missing")
	}
	seen := map[int]bool{}
	for _, t := range cf.Kafka.Topics {
		if t.Priority < 1 || t.Priority > 4 || t.Name == "" || t.GroupID == "" || seen[t.Priority] {
			return fmt.Errorf("invalid or duplicate Kafka topic priority")
		}
		seen[t.Priority] = true
	}
	if len(seen) != 4 {
		return fmt.Errorf("Kafka requires four priorities")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", cf.Kafka.Brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	admin, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer admin.Close()
	_ = admin.SetDeadline(time.Now().Add(15 * time.Second))
	topics := []kafka.TopicConfig{}
	for _, t := range cf.Kafka.Topics {
		topics = append(topics, kafka.TopicConfig{Topic: t.Name, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := admin.CreateTopics(topics...); err != nil {
		return err
	}
	for _, t := range cf.Kafka.Topics {
		for {
			partitions, err := conn.ReadPartitions(t.Name)
			ready := err == nil && len(partitions) > 0
			for _, p := range partitions {
				if p.Leader.ID < 0 {
					ready = false
				}
			}
			if ready {
				break
			}
			if !pause(ctx) {
				return fmt.Errorf("Kafka topic %s has no ready leader", t.Name)
			}
		}
	}
	return nil
}
