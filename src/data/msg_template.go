package data

import (
	"gorm.io/gorm"
	"time"
)

var MsgTemplateNsp MsgTemplate

type MsgTemplate struct {
	ID            int64
	TemplateID    string
	RelTemplateID string
	Name          string
	Content       string
	Subject       string
	Channel       int
	SourceID      string
	SignName      string
	Status        int
	Ext           string
	CreateTime    *time.Time `gorm:"column:create_time;default:null"`
	ModifyTime    *time.Time `gorm:"column:modify_time;default:null"`
}

// TableName 表名
func (p *MsgTemplate) TableName() string {
	return "t_msg_template"
}

func (p *MsgTemplate) Find(db *gorm.DB, templateID string) (*MsgTemplate, error) {
	var data = &MsgTemplate{}
	err := db.Where("template_id= ?", templateID).First(data).Error
	return data, err
}

func (p *MsgTemplate) Create(db *gorm.DB, dt *MsgTemplate) error {
	data := dt
	err := db.Create(data).Error
	return err
}

func (p *MsgTemplate) Save(db *gorm.DB, dt *MsgTemplate) error {
	err := db.Save(dt).Error
	return err
}

func (p *MsgTemplate) Delete(db *gorm.DB, templateID string) error {
	var dt = new(MsgTemplate)
	err := db.Delete(dt).Where("template_id = ?", templateID).Limit(1).Error
	return err
}
