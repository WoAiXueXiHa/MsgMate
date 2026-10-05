package data

import (
	"context"
	"fmt"
	"time"

	conf "github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/segmentio/kafka-go"
)

// kafkaConsumer 顺序执行拉取、处理、提交 offset，处理失败时不确认消息。
// 单条消息未处理完成会阻塞当前优先级消费者，以保留失败重试机会。
type kafkaConsumer struct {
	reader *kafka.Reader
	ctx    context.Context
	cancel context.CancelFunc
}

func newKafkaConsumer(brokers []string, topic, group string) *kafkaConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &kafkaConsumer{reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, StartOffset: kafka.FirstOffset, MaxWait: time.Second}), ctx: ctx, cancel: cancel}
}

func (c *kafkaConsumer) ConsumeMessages(handler func([]byte) error) {
	for c.ctx.Err() == nil {
		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			if !pause(c.ctx) {
				return
			}
			continue
		}
		if !handleAndCommit(c.ctx, msg.Value, handler, func() error { return c.reader.CommitMessages(c.ctx, msg) }) {
			return
		}
	}
}

// handleAndCommit 分开重试处理和提交：处理成功后，提交失败只重试 offset，
// 不再次执行渠道调用。进程崩溃后的重新消费仍依赖业务记录判断状态。
func handleAndCommit(ctx context.Context, body []byte, handler func([]byte) error, commit func() error) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		if err := handler(body); err == nil {
			break
		}
		if !pause(ctx) {
			return false
		}
	}
	for {
		if ctx.Err() != nil {
			return false
		}
		if err := commit(); err == nil {
			return true
		}
		if !pause(ctx) {
			return false
		}
	}
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
