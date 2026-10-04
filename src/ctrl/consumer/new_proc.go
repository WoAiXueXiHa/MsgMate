package consumer

import (
	"encoding/json"
	"fmt"
	"github.com/BitofferHub/msgcenter/src/ctrl/msgpush"
	"github.com/BitofferHub/msgcenter/src/data"
)

type MsgIntf interface {
	SendMsg() error
	Base() *MsgBase
}

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

// Base func get base struct
func (p *MsgBase) Base() *MsgBase {
	return p
}

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

// RegisterHandler func RegisterHandler
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
