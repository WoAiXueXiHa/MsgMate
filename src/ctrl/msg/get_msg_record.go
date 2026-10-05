package msg

import (
	"encoding/json"
	"net/http"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/WoAiXueXiHa/MsgMate/src/constant"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/handler"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/gin-gonic/gin"
)

// GetMsgRecordHandler 接口处理handler
type GetMsgRecordHandler struct {
	Req    ctrlmodel.GetMsgRecordReq
	Resp   ctrlmodel.GetMsgRecordResp
	UserId string
}

// GetMsgRecord 接口
func GetMsgRecord(c *gin.Context) {
	var hd GetMsgRecordHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	// 解析请求包
	if err := c.ShouldBind(&hd.Req); err != nil {
		log.Errorf("GetMsgRecord shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	// 执行处理函数, 这里会调用对应的HandleInput和HandleProcess，往下看
	if err := handler.Run(&hd); err != nil && hd.Resp.Code == 0 {
		hd.Resp.Code = constant.ERR_INTERNAL
	}
}

// HandleInput 参数检查
func (p *GetMsgRecordHandler) HandleInput() error {
	if p.Req.MsgID == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *GetMsgRecordHandler) HandleProcess() error {
	log.Infof("into HandleProcess")
	dt := data.GetData()
	var record = new(data.MsgRecord)
	var err error
	record, err = data.MsgRecordNsp.Find(dt.GetDB(), p.Req.MsgID)
	if err != nil {
		return err
	}
	if record.SourceID != p.UserId {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}

	p.Resp.To = record.To
	p.Resp.Status = record.Status
	p.Resp.RetryCount = record.RetryCount
	p.Resp.Subject = record.Subject
	p.Resp.TemplateID = record.TemplateID
	p.Resp.TemplateData = make(map[string]string)
	err = json.Unmarshal([]byte(record.TemplateData), &p.Resp.TemplateData)
	if err != nil {
		log.Errorf("json.Unmarshal err %s", err.Error())
		return err
	}
	return nil
}
