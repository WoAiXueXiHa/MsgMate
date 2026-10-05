package msg

import (
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/WoAiXueXiHa/MsgMate/src/constant"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/handler"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/gin-gonic/gin"
	"net/http"
)

// GetTemplateHandler 接口处理handler
type GetTemplateHandler struct {
	Req    ctrlmodel.GetTemplateReq
	Resp   ctrlmodel.GetTemplateResp
	UserId string
}

// GetTemplate 接口
func GetTemplate(c *gin.Context) {
	var hd GetTemplateHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	// 解析请求包
	if err := c.ShouldBind(&hd.Req); err != nil {
		log.Errorf("GetTemplate shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	// 执行处理函数, 这里会调用对应的HandleInput和HandleProcess，往下看
	if err := handler.Run(&hd); err != nil && hd.Resp.Code == 0 {
		hd.Resp.Code = constant.ERR_INTERNAL
	}
}

// HandleInput 参数检查
func (p *GetTemplateHandler) HandleInput() error {
	if p.Req.TemplateID == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *GetTemplateHandler) HandleProcess() error {
	log.Infof("into HandleProcess")
	dt := data.GetData()
	mt, err := data.MsgTemplateNsp.Find(dt.GetDB(), p.Req.TemplateID)
	if err != nil {
		return err
	}
	p.Resp.SourceID = mt.SourceID
	p.Resp.Name = mt.Name
	p.Resp.Subject = mt.Subject
	p.Resp.Channel = mt.Channel
	p.Resp.SignName = mt.SignName
	p.Resp.Content = mt.Content
	p.Resp.RelTemplateID = mt.RelTemplateID
	return nil
}
