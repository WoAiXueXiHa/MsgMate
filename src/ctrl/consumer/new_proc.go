package consumer

import (
	"encoding/json"
	"fmt"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/msgpush"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
)

// MsgIntf 将队列处理与具体渠道解耦：消费者填充公共字段，适配器负责发送。
type MsgIntf interface {
	SendMsg() error
	Base() *MsgBase
}

// MsgHandler 保存处理器工厂，每次消费创建新实例，避免共享单条消息的状态。
type MsgHandler struct {
	Channel int
	NewProc func() MsgIntf
}

type MsgBase struct {
	To           string            `json:"to" form:"to"`
	Subject      string            `json:"subject" form:"subject"`
	Content      string            `json:"content" form:"content"`
	Priority     int               `json:"priority" form:"priority"`
	TemplateID   string            `json:"templateID" form:"templateID"`
	TemplateData map[string]string `json:"templateData" form:"templateData"`
	NotifyURL    string            `json:"notifyUrl" form:"notifyUrl"`
}

// Base 暴露嵌入的公共字段，供消费者统一填充。
func (p *MsgBase) Base() *MsgBase {
	return p
}

// InitMsgProc 必须在启动消费者前完成；注册表在消费期间只读。
func InitMsgProc() {
	emailMsgProc := MsgHandler{
		Channel: int(data.Channel_EMAIL),
		NewProc: func() MsgIntf { return new(EmailMsgProc) },
	}
	RegisterHandler(&emailMsgProc)
	smsMsgProc := MsgHandler{
		Channel: int(data.Channel_SMS),
		NewProc: func() MsgIntf { return new(SMSMsgProc) },
	}
	RegisterHandler(&smsMsgProc)
	larkProc := MsgHandler{
		Channel: int(data.Channel_LARK),
		NewProc: func() MsgIntf { return new(LarkProc) },
	}
	RegisterHandler(&larkProc)
}

var msgProcMap = make(map[int]*MsgHandler, 0)

// RegisterHandler 在初始化阶段注册渠道工厂。
func RegisterHandler(handler *MsgHandler) {
	msgProcMap[handler.Channel] = handler
}

type EmailMsgProc struct {
	MsgBase
}

func (p *EmailMsgProc) SendMsg() error {
	// 发送对应消息
	return msgpush.SendEmail(p.To, p.Subject, p.Content)
}

type SMSMsgProc struct {
	MsgBase
}

func (p *SMSMsgProc) SendMsg() error {
	// 发送对应消息
	dt := data.GetData()
	mt, err := data.MsgTemplateNsp.Find(dt.GetDB(), p.TemplateID)
	if err != nil {
		return err
	}
	templateParam, _ := json.Marshal(p.TemplateData)
	err = msgpush.SendSMS(p.To, mt.SignName, mt.RelTemplateID, string(templateParam))
	if err != nil {
		return err
	}
	return nil
}

type LarkProc struct {
	MsgBase
}

func (p *LarkProc) SendMsg() error {
	// 发送对应消息
	accessToken, err := msgpush.GetAccessToken()
	if err != nil {
		fmt.Println("Error getting access token:", err)
		return err
	}
	err = msgpush.SendMessage(accessToken, p.To, p.Content)
	if err != nil {
		return err
	}
	return nil
}
