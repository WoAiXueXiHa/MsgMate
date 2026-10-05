package data

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BitofferHub/pkg/utils"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func NewMsgID() string { return utils.NewUuid() }
func (p *Data) InvalidateTemplate(id string) {
	p.rdb.GetRedisBaseConn().Del(context.Background(), REDIS_KEY_TEMPLATE+id)
}

func InvalidateRecord(id string) {
	if data != nil {
		data.rdb.GetRedisBaseConn().Del(context.Background(), REDIS_KEY_MES_RECORD+id)
	}
}

func QueueModel(req *ctrlmodel.SendMsgReq) (*MsgQueue, error) {
	body, err := json.Marshal(req.TemplateData)
	if err != nil {
		return nil, err
	}
	return &MsgQueue{MsgId: req.MsgID, To: req.To, Subject: req.Subject, TemplateID: req.TemplateID, TemplateData: string(body), Priority: req.Priority, Status: int(TASK_STATUS_PENDING)}, nil
}

// Enqueue 支持传入事务句柄，让记录创建与入队由调用方统一提交。
// 同一队列表内遇到唯一键冲突时忽略插入，不等于 HTTP 提交具备幂等性。
func Enqueue(db *gorm.DB, req *ctrlmodel.SendMsgReq) error {
	if req.MsgID == "" || req.Priority < 1 || req.Priority > 4 {
		return fmt.Errorf("invalid queue request")
	}
	row, err := QueueModel(req)
	if err != nil {
		return err
	}
	return db.Table("t_msg_queue_" + GetPriorityStr(PriorityEnum(req.Priority))).Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error
}

func (p *Data) Publish(ctx context.Context, req *ctrlmodel.SendMsgReq) error {
	producer := p.GetProducer(PriorityEnum(req.Priority))
	if producer == nil {
		return fmt.Errorf("missing producer")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return producer.SendMessage(body)
}

// FinishQueues 关闭同一消息在普通队列和重试队列中的任务。
// 消息记录和队列表使用不同状态枚举，调用方必须传入队列状态。
func FinishQueues(db *gorm.DB, id string, status int) error {
	for _, priority := range []string{"high", "middle", "low", "retry"} {
		if err := MsgQueueNsp.SetStatus(db, priority, id, status); err != nil {
			return err
		}
	}
	return nil
}

// RecoverPending 仅用于单实例启动，将崩溃遗留的处理中任务恢复为待处理。
// 多实例启动会误恢复其他实例的任务；渠道已接受但未落库的消息也可能重复发送。
func RecoverPending(db *gorm.DB, mysqlQueue bool) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if mysqlQueue {
			for _, p := range []string{"high", "middle", "low", "retry"} {
				if err := tx.Table("t_msg_queue_"+p).Where("status = ?", TASK_STATUS_PROCESSING).Update("status", TASK_STATUS_PENDING).Error; err != nil {
					return err
				}
			}
		}
		return tx.Model(&MsgTmpQueueTimer{}).Where("status = ?", TIMER_MSG_STATUS_PROCESSING).Update("status", TIMER_MSG_STATUS_PENDING).Error
	})
}
