package ctrlmodel

// RespComm 通用的响应消息
type RespComm struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// SendMsgReq 请求消息
type SendMsgReq struct {
	To            string            `json:"to" form:"to"`
	Subject       string            `json:"subject" form:"subject"`
	Priority      int               `json:"priority" form:"priority"`
	TemplateID    string            `json:"templateID" form:"templateID"`
	TemplateData  map[string]string `json:"templateData" form:"templateData"`
	SendTimestamp int64             `json:"sendTimestamp" form:"sendTimestamp"`
	MsgID         string
}

// SendMsgResp 响应消息
type SendMsgResp struct {
	RespComm
	MsgID string `json:"msgID"`
}

// GetMsgResult 请求消息
type GetMsgRecordReq struct {
	MsgID string `json:"msgID" form:"msgID"`
}

// GetMsgResult 响应消息
type GetMsgRecordResp struct {
	RespComm
	To           string            `json:"to" form:"to"`
	Subject      string            `json:"subject" form:"subject"`
	TemplateID   string            `json:"templateID" form:"templateID"`
	TemplateData map[string]string `json:"templateData" form:"templateData"`
}

type CreateTemplateReq struct {
	SourceID string `json:"sourceID" form:"sourceID"`
	Name     string `json:"name" form:"name"`
	Subject  string `json:"subject" form:"subject"`
	SignName string `json:"signName" form:"signName"`
	Channel  int    `json:"channel" form:"channel"`
	Content  string `json:"content" form:"content"`
}

type CreateTemplateResp struct {
	RespComm
	TemplateID string `json:"templateID"`
}

type GetTemplateReq struct {
	TemplateID string `json:"templateID" form:"templateID"`
}

type GetTemplateResp struct {
	RespComm
	RelTemplateID string `json:"relTemplateID"`
	SourceID      string `json:"sourceID" form:"sourceID"`
	SignName      string `json:"signName" form:"signName"`
	Name          string `json:"name" form:"name"`
	Subject       string `json:"subject" form:"subject"`
	Channel       int    `json:"channel" form:"channel"`
	Content       string `json:"content" form:"content"`
}

type UpdateTemplateReq struct {
	TemplateID string `json:"templateID"`
	Name       string `json:"name" form:"name"`
	SourceID   string `json:"sourceID" form:"sourceID"`
	Subject    string `json:"subject" form:"subject"`
	Channel    int    `json:"channel" form:"channel"`
	Content    string `json:"content" form:"content"`
}

type UpdateTemplateResp struct {
	RespComm
}

type DelTemplateReq struct {
	TemplateID string `json:"templateID"`
}

type DelTemplateResp struct {
	RespComm
}
