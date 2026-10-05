package msg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/WoAiXueXiHa/MsgMate/src/config"
	"github.com/WoAiXueXiHa/MsgMate/src/constant"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/ctrlmodel"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/handler"
	"github.com/WoAiXueXiHa/MsgMate/src/ctrl/tools"
	"github.com/WoAiXueXiHa/MsgMate/src/data"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SendMsgHandler 接口处理handler
type SendMsgHandler struct {
	Req    ctrlmodel.SendMsgReq
	Resp   ctrlmodel.SendMsgResp
	UserId string
}

// SendMsg 接口
func SendMsg(c *gin.Context) {
	var hd SendMsgHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	// 解析请求包
	if err := c.ShouldBind(&hd.Req); err != nil {
		log.Errorf("SendMsg shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	// 执行处理函数, 这里会调用对应的HandleInput和HandleProcess，往下看
	if err := handler.Run(&hd); err != nil {
		log.Errorf("SendMsg handler.Run err %s", err.Error())
		// 如果Resp.Code未设置，设置为内部错误
		if hd.Resp.Code == 0 {
			hd.Resp.Code = constant.ERR_INTERNAL
		}
	}
}

// HandleInput 参数检查
func (p *SendMsgHandler) HandleInput() error {
	if p.UserId == "" || p.Req.SendTimestamp < 0 || p.Req.TemplateID == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	if p.Req.TemplateData == nil {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	if p.Req.To == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	if p.Req.Priority == 0 {
		p.Req.Priority = int(data.PRIORITY_LOW)
	}
	if p.Req.Priority < 1 || p.Req.Priority > 3 {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *SendMsgHandler) HandleProcess() error {
	sourceID := p.UserId
	ctx := context.Background()
	log.Infof("into HandleProcess")
	dt := data.GetData()

	// 获取消息模板
	mt, err := dt.GetMsgTemplate(ctx, p.Req.TemplateID)
	if err != nil {
		log.Errorf("get msg template err %s", err.Error())
		p.Resp.Code = constant.ERR_TEMPLATE_NOT_READY
		return err
	}

	// 模板状态检查
	if mt.Status != int(data.TEMPLATE_STATUS_NORMAL) || mt.SourceID != sourceID {
		p.Resp.Code = constant.ERR_TEMPLATE_NOT_READY
		return errors.New("template not ready")
	}

	// 获取配额
	var (
		limit, div int
		ready      bool
	)

	quatoCacheKey := fmt.Sprintf("%s%s:%d", data.REDIS_KEY_SOURCE_QUOTA, sourceID, mt.Channel)

	// 如果缓存开启，则从缓存中获取配额
	if config.Conf.Common.OpenCache {
		limitdiv, _, _ := dt.GetCache().Get(ctx, quatoCacheKey)
		if len(limitdiv) > 0 {
			ary := strings.Split(limitdiv, "_")
			if len(ary) == 2 {
				var e1, e2 error
				limit, e1 = strconv.Atoi(ary[0])
				div, e2 = strconv.Atoi(ary[1])
				ready = e1 == nil && e2 == nil && limit > 0 && div > 0
			}
		}
	}

	// 如果缓存未命中，则从数据库中获取配额
	if !ready {
		log.Infof("quota cache miss")
		// 获取全局配额
		globalQuota, err := data.GlobalQuotaNsp.Find(dt.GetDB(), mt.Channel)
		if err != nil {
			p.Resp.Code = constant.ERR_INTERNAL
			return err
		}
		limit = globalQuota.Num
		div = globalQuota.Unit
		// 业务配额覆盖渠道全局默认值，并非同时扣减两个配额层级。
		// 计数 key 按来源和渠道隔离，全局配置不是所有业务共享的总量限制。
		sourceQuota, err := data.SourceQuotaNsp.Find(dt.GetDB(), sourceID, mt.Channel)
		if err != nil {
			if err != gorm.ErrRecordNotFound {
				p.Resp.Code = constant.ERR_INTERNAL
				return err
			}
		} else {
			limit = sourceQuota.Num
			div = sourceQuota.Unit
		}
		value := fmt.Sprintf("%d_%d", limit, div)
		if config.Conf.Common.OpenCache {
			dt.GetCache().Set(ctx, quatoCacheKey, value, 30*time.Second)
		}
	}
	log.Infof("limit %d, div %d", limit, div)

	// 创建限流器
	lm := tools.NewRateLimiter(dt.GetCache().GetRedisBaseConn(), div, limit)
	keyID := fmt.Sprintf(data.REDIS_KEY_RATE_LIMIT_COUNT+":%s:%d", sourceID, mt.Channel)
	if p.Req.SendTimestamp > 0 {
		// 定时消息单独计数限频
		keyID = fmt.Sprintf(data.REDIS_KEY_RATE_LIMIT_COUNT_TIMER+":%s:%d", sourceID, mt.Channel)
	}

	// 判断用户的请求是否被允许
	allowed, err := lm.IsRequestAllowed(keyID)
	if err != nil {
		log.Errorf("IsRequestAllowed err %s", err.Error())
		p.Resp.Code = constant.ERR_SEND_MSG
		return err
	}
	if allowed {
		log.Infof("request allowed")
	} else {
		log.Infof("request denied")
		p.Resp.Code = constant.ERR_REQUEST_LIMIT
		return nil
	}

	p.Req.MsgID = data.NewMsgID()
	p.Resp.MsgID = p.Req.MsgID
	p.Req.Subject = mt.Subject
	createRecord := func(tx *gorm.DB) error {
		return tools.CreateMsgRecord(tx, p.Req.MsgID, &p.Req, mt, int(data.MSG_STATUS_PENDING))
	}
	if p.Req.SendTimestamp > 0 {
		body, err := json.Marshal(p.Req)
		if err != nil {
			return err
		}
		err = dt.GetDB().Transaction(func(tx *gorm.DB) error {
			if err := createRecord(tx); err != nil {
				return err
			}
			return data.MsgTmpQueueTimerNsp.Create(tx, &data.MsgTmpQueueTimer{MsgId: p.Req.MsgID, Req: string(body), SendTimestamp: p.Req.SendTimestamp, Status: int(data.TIMER_MSG_STATUS_PENDING)})
		})
		if err != nil {
			return err
		}
		// 任务已持久化到 MySQL；Redis 索引写入失败时，由定时消费者补扫恢复。
		if _, err = dt.GetCache().ZAdd(ctx, "Timer_Msgs", float64(p.Req.SendTimestamp), fmt.Sprint(p.Req.SendTimestamp)); err != nil {
			log.Errorf("timer index write failed; database scan will recover")
		}
		return nil
	}
	// 普通 MySQL 消息记录与入队同时提交，避免消费者先看到队列却查不到记录。
	if config.Conf.Common.MySQLAsMq {
		return dt.GetDB().Transaction(func(tx *gorm.DB) error {
			if err := createRecord(tx); err != nil {
				return err
			}
			return data.Enqueue(tx, &p.Req)
		})
	}
	// Kafka 与 MySQL 无法共用事务：先建记录再发布，发布失败尝试标记失败。
	// 两步之间崩溃仍可能遗留待处理记录，因此没有跨存储原子提交保证。
	if err := createRecord(dt.GetDB()); err != nil {
		return err
	}
	if err := dt.Publish(ctx, &p.Req); err != nil {
		_ = data.MsgRecordNsp.UpdateStatus(dt.GetDB(), p.Req.MsgID, int(data.MSG_STATUS_FAILED))
		p.Resp.Code = constant.ERR_SEND_MSG
		return err
	}
	return nil
}
