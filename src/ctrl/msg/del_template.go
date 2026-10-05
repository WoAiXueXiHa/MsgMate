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

// DelTemplateHandler 接口处理handler
type DelTemplateHandler struct {
	Req    ctrlmodel.DelTemplateReq
	Resp   ctrlmodel.DelTemplateResp
	UserId string
}

// DelTemplate 接口
func DelTemplate(c *gin.Context) {
	var hd DelTemplateHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	// 解析请求包
	if err := c.ShouldBind(&hd.Req); err != nil {
		log.Errorf("DelTemplate shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	// 执行处理函数, 这里会调用对应的HandleInput和HandleProcess，往下看
	if err := handler.Run(&hd); err != nil && hd.Resp.Code == 0 {
		hd.Resp.Code = constant.ERR_INTERNAL
	}
}

// HandleInput 参数检查
func (p *DelTemplateHandler) HandleInput() error {
	if p.Req.TemplateID == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *DelTemplateHandler) HandleProcess() error {
	log.Infof("into HandleProcess")
	dt := data.GetData()
	err := data.MsgTemplateNsp.Delete(dt.GetDB(), p.Req.TemplateID)
	if err != nil {
		return err
	}
	dt.InvalidateTemplate(p.Req.TemplateID)
	return nil
}
