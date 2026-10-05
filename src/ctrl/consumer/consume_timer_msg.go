package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/BitofferHub/pkg/middlewares/lock"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/constant"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/tools"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"gorm.io/gorm"
)

// TimerMsgConsume 将到期任务转交给普通队列，本身不调用发送渠道。
// MySQL 保存任务本体，Redis 只提供时间索引，避免索引丢失导致任务永久遗漏。
type TimerMsgConsume struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	lastScan time.Time
}

func (s *TimerMsgConsume) Consume() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.consumeFromTimer(s.ctx) }()
}

// Unlock 先取消循环，再等待当前转交退出；锁由消费协程的 defer 释放。
func (s *TimerMsgConsume) Unlock() {
	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
	}
}

func (s *TimerMsgConsume) consumeFromTimer(ctx context.Context) {
	l := lock.NewRedisLock("TIMER_MSG_LEADER_CONSUMER", lock.WithExpireSeconds(5), lock.WithWatchDogMode())
	for l.Lock(ctx) != nil {
		if !wait(ctx, time.Second) {
			return
		}
	}
	defer l.Unlock(context.Background())
	for ctx.Err() == nil {
		s.consumeTimerMsg()
		if !wait(ctx, 100*time.Millisecond) {
			return
		}
	}
}

func (s *TimerMsgConsume) consumeTimerMsg() {
	ctx := s.ctx
	dt := data.GetData()
	now := time.Now()
	result, err := dt.GetCache().EvalResults(ctx, constant.LUA_ZRANGEBYSCORE_AND_REM, []string{"Timer_Msgs"}, []interface{}{"0", strconv.FormatInt(now.Unix(), 10)})
	times, _ := result.([]interface{})
	// Redis 索引用于触发扫描；即使没有索引或 Redis 出错，也定期查询 MySQL。
	// 索引先被移除后若数据库转交失败，pending 任务仍能被下一次补扫发现。
	if err == nil && len(times) == 0 && now.Sub(s.lastScan) < time.Second {
		return
	}
	s.lastScan = now
	rows, err := data.MsgTmpQueueTimerNsp.GetOnTimeMsgList(dt.GetDB(), int(data.TIMER_MSG_STATUS_PENDING), now.Unix())
	if err != nil {
		log.Errorf("timer database scan failed: %v", err)
		return
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		var req ctrlmodel.SendMsgReq
		if err := json.Unmarshal([]byte(row.Req), &req); err != nil {
			_ = finishTimer(dt.GetDB(), row.MsgId, int(data.TIMER_MSG_STATUS_FAILED))
			continue
		}
		req.MsgID = row.MsgId
		tp, err := dt.GetMsgTemplate(ctx, req.TemplateID)
		if errors.Is(err, gorm.ErrRecordNotFound) || err == nil && (tp.Status != int(data.TEMPLATE_STATUS_NORMAL) || req.Priority < 1 || req.Priority > 3) {
			_ = finishTimer(dt.GetDB(), row.MsgId, int(data.TIMER_MSG_STATUS_FAILED))
			continue
		}
		if err != nil {
			continue
		}
		req.Subject = tp.Subject
		// MySQL 模式将补建记录、入队、完成定时任务放在同一事务中。
		// 定时状态成功仅表示转交完成，实际发送结果由普通消费者写入。
		if config.Conf.Common.MySQLAsMq {
			err = dt.GetDB().Transaction(func(tx *gorm.DB) error {
				if err := ensureTimerRecord(tx, &req, tp); err != nil {
					return err
				}
				if err := data.Enqueue(tx, &req); err != nil {
					return err
				}
				return data.MsgTmpQueueTimerNsp.SetStatus(tx, req.MsgID, int(data.TIMER_MSG_STATUS_SUCC))
			})
		} else {
			if err := ensureTimerRecord(dt.GetDB(), &req, tp); err != nil {
				continue
			}
			if err = dt.Publish(ctx, &req); err == nil {
				// Broker 确认后只重试数据库状态，避免本进程重复发布。
				// 两个存储无法原子提交，进程在此崩溃仍可能导致再次发布。
				for {
					err = data.MsgTmpQueueTimerNsp.SetStatus(dt.GetDB(), req.MsgID, int(data.TIMER_MSG_STATUS_SUCC))
					if err == nil || !wait(ctx, time.Second) {
						break
					}
				}
			}
		}
		if err != nil {
			log.Errorf("timer %s forwarding failed; remains pending", row.MsgId)
		}
	}
}

// finishTimer 将无效定时任务及其已有消息记录一起标记失败。
func finishTimer(db *gorm.DB, id string, status int) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := data.MsgRecordNsp.UpdateStatus(tx, id, int(data.MSG_STATUS_FAILED)); err != nil {
			return err
		}
		return data.MsgTmpQueueTimerNsp.SetStatus(tx, id, status)
	})
}

// ensureTimerRecord 为缺少普通记录的历史定时任务补建记录。
// 已存在的记录保持原状态，不能在转交时覆盖为 pending。
func ensureTimerRecord(db *gorm.DB, req *ctrlmodel.SendMsgReq, tp *data.MsgTemplate) error {
	_, err := data.MsgRecordNsp.Find(db, req.MsgID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return tools.CreateMsgRecord(db, req.MsgID, req, tp, int(data.MSG_STATUS_PENDING))
	}
	return err
}
