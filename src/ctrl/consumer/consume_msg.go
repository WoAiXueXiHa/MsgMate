package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/BitofferHub/pkg/middlewares/lock"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/pkg/middlewares/mq"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/tools"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"gorm.io/gorm"
)

var consumePriority = []data.PriorityEnum{data.PRIORITY_HIGH, data.PRIORITY_MIDDLE, data.PRIORITY_LOW, data.PRIORITY_RETRY}

// MsgConsume 按优先级启动独立消费循环，共用取消信号并等待退出。
// 各队列独立推进，启动顺序不代表跨队列严格优先顺序。
type MsgConsume struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewMsgConsume() *MsgConsume {
	ctx, cancel := context.WithCancel(context.Background())
	return &MsgConsume{ctx: ctx, cancel: cancel}
}

func (s *MsgConsume) Consume() {
	for _, p := range consumePriority {
		s.wg.Add(1)
		go func(priority data.PriorityEnum) { defer s.wg.Done(); s.startConsumer(priority) }(p)
	}
}

func (s *MsgConsume) startConsumer(priority data.PriorityEnum) {
	if config.Conf.Common.MySQLAsMq {
		s.consumeFromMySQLWithLock(priority)
	} else {
		s.consumeFromMQ(data.GetData().GetConsumer(priority), priority)
	}
}

// wait 用可取消的定时等待，使退避过程中也能响应服务停止。
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// consumeFromMySQLWithLock 使用每个优先级独立的 Redis 锁协调消费。
// 看门狗负责续租，但此循环未检测持锁期间的续租丢失，不能据此保证多实例安全。
func (s *MsgConsume) consumeFromMySQLWithLock(priority data.PriorityEnum) {
	l := lock.NewRedisLock("MSG_LEADER_CONSUMER_"+data.GetPriorityStr(priority), lock.WithExpireSeconds(30), lock.WithWatchDogMode())
	for l.Lock(s.ctx) != nil {
		if !wait(s.ctx, time.Second) {
			return
		}
	}
	defer l.Unlock(context.Background())
	for s.ctx.Err() == nil {
		s.consumeMySQLMsg(priority)
		delay := RandNum(500)
		if priority == data.PRIORITY_RETRY {
			delay = 1000 + rand.Int63n(1001)
		}
		if !wait(s.ctx, time.Duration(delay)*time.Millisecond) {
			return
		}
	}
}

func (s *MsgConsume) consumeFromMQ(c mq.Consumer, priority data.PriorityEnum) {
	c.ConsumeMessages(func(body []byte) error {
		var req ctrlmodel.SendMsgReq
		// 无法识别的消息返回 nil，使 offset 可以提交；这里没有死信存储。
		if err := json.Unmarshal(body, &req); err != nil || req.MsgID == "" {
			log.Errorf("invalid Kafka payload; discarded")
			return nil
		}
		record, err := data.MsgRecordNsp.Find(data.GetData().GetDB(), req.MsgID)
		if err != nil {
			return err
		}
		if record.Status == int(data.MSG_STATUS_SUCC) || record.Status == int(data.MSG_STATUS_FAILED) {
			return nil
		}
		if record.RetryCount >= config.Conf.Common.MaxRetryCount {
			return data.MsgRecordNsp.UpdateStatus(data.GetData().GetDB(), req.MsgID, int(data.MSG_STATUS_FAILED))
		}
		if err := dealOneMsg(s.ctx, &req); err != nil {
			var sent *sentError
			if errors.As(err, &sent) {
				return err
			}
			return s.handleMqRetryAfterFailure(s.ctx, &req, body, data.GetPriorityStr(priority))
		}
		return nil
	})
}

// sentError 区分“渠道已接受，但状态未能保存”与真正的发送失败。
// 调用方不能把前者转入发送重试，否则可能重复触达。
type sentError struct{ err error }

func (e *sentError) Error() string { return e.err.Error() }
func (e *sentError) Unwrap() error { return e.err }
func (s *MsgConsume) handleMqRetryAfterFailure(ctx context.Context, req *ctrlmodel.SendMsgReq, body []byte, p string) error {
	db := data.GetData().GetDB()
	count, err := data.MsgRecordNsp.IncrementRetryCount(db, req.MsgID)
	if err != nil {
		return err
	}
	if count >= config.Conf.Common.MaxRetryCount {
		return data.MsgRecordNsp.UpdateStatus(db, req.MsgID, int(data.MSG_STATUS_FAILED))
	}
	// 一次发送失败只增加一次计数；重试队列发布失败属于转交失败，不能重复计数。
	reqCopy := *req
	reqCopy.Priority = int(data.PRIORITY_RETRY)
	for {
		if err := data.GetData().Publish(ctx, &reqCopy); err == nil {
			return nil
		}
		if !wait(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

// dealOneMsg 统一执行模板校验、正文渲染、渠道调用和成功状态持久化。
func dealOneMsg(ctx context.Context, req *ctrlmodel.SendMsgReq) error {
	dt := data.GetData()
	tp, err := dt.GetMsgTemplate(ctx, req.TemplateID)
	if err != nil {
		return err
	}
	if tp.Status != int(data.TEMPLATE_STATUS_NORMAL) {
		return errors.New("template not enabled")
	}
	var content string
	if tp.Channel == int(data.Channel_EMAIL) || tp.Channel == int(data.Channel_LARK) {
		content, err = tools.TemplateReplace(tp.Content, req.TemplateData)
		if err != nil {
			return err
		}
	}
	h, ok := msgProcMap[tp.Channel]
	if !ok {
		return errors.New("unsupported channel")
	}
	// 每条消息创建独立处理器，接收人和正文不会在并发消费之间共享。
	proc := h.NewProc()
	base := proc.Base()
	base.To = req.To
	base.Subject = tp.Subject
	base.Content = content
	base.Priority = req.Priority
	base.TemplateID = req.TemplateID
	base.TemplateData = req.TemplateData
	if err := proc.SendMsg(); err != nil {
		log.Errorf("message %s channel failed: %v", req.MsgID, err)
		return err
	}
	// 渠道接受后仅重试落库，不再次调用渠道。这个保证只在当前进程内成立；
	// 若接受后进程崩溃，重启恢复仍可能再次发送，不是严格一次投递。
	for {
		err = dt.GetDB().Transaction(func(tx *gorm.DB) error {
			if err := data.MsgRecordNsp.UpdateStatus(tx, req.MsgID, int(data.MSG_STATUS_SUCC)); err != nil {
				return err
			}
			if config.Conf.Common.MySQLAsMq {
				return data.FinishQueues(tx, req.MsgID, int(data.TASK_STATUS_SUCC))
			}
			return nil
		})
		if err == nil {
			data.InvalidateRecord(req.MsgID)
			log.Infof("message %s delivered", req.MsgID)
			return nil
		}
		if !wait(ctx, time.Second) {
			return &sentError{err}
		}
	}
}

// dealRetryMysqlQueue 将失败计数、原队列关闭和重试入队作为一个事务。
// 未达到阈值时保留相同消息 ID，达到阈值则结束相关队列任务。
func dealRetryMysqlQueue(db *gorm.DB, req *ctrlmodel.SendMsgReq) error {
	return db.Transaction(func(tx *gorm.DB) error {
		count, err := data.MsgRecordNsp.IncrementRetryCount(tx, req.MsgID)
		if err != nil {
			return err
		}
		if count >= config.Conf.Common.MaxRetryCount {
			if err := data.MsgRecordNsp.UpdateStatus(tx, req.MsgID, int(data.MSG_STATUS_FAILED)); err != nil {
				return err
			}
			return data.FinishQueues(tx, req.MsgID, int(data.TASK_STATUS_FAILED))
		}
		if req.Priority != int(data.PRIORITY_RETRY) {
			if err := data.MsgQueueNsp.SetStatus(tx, data.GetPriorityStr(data.PriorityEnum(req.Priority)), req.MsgID, int(data.TASK_STATUS_FAILED)); err != nil {
				return err
			}
		}
		copyReq := *req
		copyReq.Priority = int(data.PRIORITY_RETRY)
		if err := data.Enqueue(tx, &copyReq); err != nil {
			return err
		}
		return data.MsgQueueNsp.SetStatus(tx, "retry", req.MsgID, int(data.TASK_STATUS_PENDING))
	})
}

func (s *MsgConsume) consumeMySQLMsg(priority data.PriorityEnum) {
	dt := data.GetData()
	p := data.GetPriorityStr(priority)
	// 通过批量大小分配处理机会；各优先级循环并行，不是统一调度器。
	n := 60
	if priority == data.PRIORITY_MIDDLE {
		n = 30
	} else if priority == data.PRIORITY_LOW {
		n = 10
	}
	rows, err := data.MsgQueueNsp.GetMsgList(dt.GetDB(), p, int(data.TASK_STATUS_PENDING), n)
	if err != nil {
		log.Errorf("read queue: %v", err)
		return
	}
	for _, row := range rows {
		if s.ctx.Err() != nil {
			return
		}
		// 逐条按 pending 条件领取，领取失败跳过，避免整批任务都进入处理中。
		claim := dt.GetDB().Table("t_msg_queue_"+p).Where("msg_id = ? AND status = ?", row.MsgId, data.TASK_STATUS_PENDING).Update("status", data.TASK_STATUS_PROCESSING)
		if claim.Error != nil || claim.RowsAffected != 1 {
			continue
		}
		req := &ctrlmodel.SendMsgReq{MsgID: row.MsgId, Priority: row.Priority, To: row.To, Subject: row.Subject, TemplateID: row.TemplateID}
		record, err := data.MsgRecordNsp.Find(dt.GetDB(), row.MsgId)
		if err != nil {
			_ = data.MsgQueueNsp.SetStatus(dt.GetDB(), p, row.MsgId, int(data.TASK_STATUS_PENDING))
			continue
		}
		if record.Status == int(data.MSG_STATUS_SUCC) {
			_ = data.FinishQueues(dt.GetDB(), row.MsgId, int(data.TASK_STATUS_SUCC))
			continue
		}
		if record.Status == int(data.MSG_STATUS_FAILED) {
			_ = data.FinishQueues(dt.GetDB(), row.MsgId, int(data.TASK_STATUS_FAILED))
			continue
		}
		// 坏参数只终止当前消息，继续处理本批后续任务。
		if err := json.Unmarshal([]byte(row.TemplateData), &req.TemplateData); err != nil {
			_ = dt.GetDB().Transaction(func(tx *gorm.DB) error {
				if err := data.MsgRecordNsp.UpdateStatus(tx, row.MsgId, int(data.MSG_STATUS_FAILED)); err != nil {
					return err
				}
				return data.FinishQueues(tx, row.MsgId, int(data.TASK_STATUS_FAILED))
			})
			continue
		}
		if err := dealOneMsg(s.ctx, req); err != nil {
			var sent *sentError
			if errors.As(err, &sent) {
				return
			}
			if err := dealRetryMysqlQueue(dt.GetDB(), req); err != nil {
				log.Errorf("retry transition: %v", err)
				_ = data.MsgQueueNsp.SetStatus(dt.GetDB(), p, row.MsgId, int(data.TASK_STATUS_PENDING))
			}
		}
	}
}

func RandNum(n int64) int64 { return rand.Int63n(n) + 1 }

// UnlockAll 取消循环并关闭 Kafka Reader，等待后台协程释放各自资源。
func (s *MsgConsume) UnlockAll() {
	s.cancel()
	if !config.Conf.Common.MySQLAsMq {
		for _, p := range consumePriority {
			if c := data.GetData().GetConsumer(p); c != nil {
				c.Close()
			}
		}
	}
	s.wg.Wait()
}
