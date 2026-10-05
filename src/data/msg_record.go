package data

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var MsgRecordNsp MsgRecord

type MsgRecord struct {
	ID           int64
	Subject      string
	To           string
	MsgId        string
	TemplateID   string
	TemplateData string
	Channel      int
	SourceID     string
	Status       int        // 1 待处理，2 渠道接受，3 最终失败；与队列表状态枚举不同
	RetryCount   int        // 处理失败次数，达到阈值后终止重试
	CreateTime   *time.Time `gorm:"column:create_time;default:null"`
	ModifyTime   *time.Time `gorm:"column:modify_time;default:null"`
}

// TableName 表名
func (p *MsgRecord) TableName() string {
	return "t_msg_record"
}

// Find 查找记录
func (p *MsgRecord) Find(db *gorm.DB, msgID string) (*MsgRecord, error) {
	var data = &MsgRecord{}
	err := db.Where("msg_id= ?", msgID).First(data).Error
	return data, err
}

// Create 创建记录
func (p *MsgRecord) Create(db *gorm.DB, dt *MsgRecord) error {
	data := dt
	err := db.Create(data).Error
	return err
}

// UpdateStatus 更新消息记录状态
func (p *MsgRecord) UpdateStatus(db *gorm.DB, msgID string, status int) error {
	err := db.Model(&MsgRecord{}).Where("msg_id = ?", msgID).Update("status", status).Error
	if err == nil {
		InvalidateRecord(msgID)
	}
	return err
}

// UpdateRetryCount 更新消息记录的重试次数
func (p *MsgRecord) UpdateRetryCount(db *gorm.DB, msgID string, retryCount int) error {
	err := db.Model(&MsgRecord{}).Where("msg_id = ?", msgID).Update("retry_count", retryCount).Error
	return err
}

// IncrementRetryCount 增加消息记录的重试次数
func (p *MsgRecord) IncrementRetryCount(db *gorm.DB, msgID string) (int, error) {
	var count int
	err := db.Transaction(func(tx *gorm.DB) error {
		// 锁定记录后读取并递增，避免并发失败处理覆盖彼此的计数。
		var record MsgRecord
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("msg_id = ?", msgID).First(&record).Error; err != nil {
			return err
		}
		count = record.RetryCount + 1
		return tx.Model(&MsgRecord{}).Where("msg_id = ?", msgID).Update("retry_count", count).Error
	})
	return count, err
}
