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

	"github.com/BitofferHub/msgcenter/src/config"
	"github.com/BitofferHub/msgcenter/src/constant"
	"github.com/BitofferHub/msgcenter/src/ctrl/ctrlmodel"
	"github.com/BitofferHub/msgcenter/src/ctrl/handler"
	"github.com/BitofferHub/msgcenter/src/ctrl/tools"
	"github.com/BitofferHub/msgcenter/src/data"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/pkg/utils"
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
	if p.Req.TemplateID == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return nil
	}
	if p.Req.TemplateData == nil {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return nil
	}
	if p.Req.To == "" {
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return nil
	}
	if p.Req.Priority == 0 {
		p.Req.Priority = int(data.PRIORITY_LOW)
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
	if mt.Status != int(data.TEMPLATE_STATUS_NORMAL) {
		p.Resp.Code = constant.ERR_TEMPLATE_NOT_READY
		return errors.New("template not ready")
	}

	// 获取配额
	var (
		limit, div int
		ready      bool
	)

	quatoCacheKey := fmt.Sprintf("%s%s%d", data.REDIS_KEY_SOURCE_QUOTA, mt.SourceID, mt.Channel)

	// 如果缓存开启，则从缓存中获取配额
	if config.Conf.Common.OpenCache {
		limitdiv, _, _ := dt.GetCache().Get(ctx, quatoCacheKey)
		if len(limitdiv) > 0 {
			ary := strings.Split(limitdiv, "_")
			limit, _ = strconv.Atoi(ary[0])
			div, _ = strconv.Atoi(ary[1])
			log.Infof("quota cache hit %d, %d", limit, div)
			ready = true
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
		// 获取业务配额
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

	// 定时消息
	if p.Req.SendTimestamp > 0 {
		return p.sendToTimer()
	}

	// 确保消息在响应前持久化
	var msgErr error
	if config.Conf.Common.MySQLAsMq {
		msgErr = p.sendToMySQL()
	} else {
		msgErr = p.sendToMQ()
	}

	status := int(data.MSG_STATUS_PENDING)
	if msgErr != nil {
		status = int(data.MSG_STATUS_FAILED)
	}

	// 创建消息记录，使用户可以立即查询到消息状态
	err = tools.CreateMsgRecord(dt.GetDB(), p.Resp.MsgID, &p.Req, mt, status)
	if err != nil {
		log.Errorf("创建消息记录失败：%s", err.Error())
		// 即使创建消息记录失败，我们也已经发送了消息到MQ，继续
	}

	// 如果持久化出错，则返回错误
	if msgErr != nil {
		log.Errorf("消息持久化失败: %s", msgErr.Error())
		p.Resp.Code = constant.ERR_SEND_MSG
		return msgErr
	}

	log.Infof("消息 %s 已成功持久化", p.Resp.MsgID)
	return nil
}

// sendToMySQL 将消息发送到MySQL数据库
func (p *SendMsgHandler) sendToMySQL() error {
	// 获取数据实例
	dt := data.GetData()

	// 生成唯一的消息ID
	msgID := utils.NewUuid()

	// 创建一个新的消息队列实例
	var md = new(data.MsgQueue)

	// 设置消息的主题
	md.Subject = p.Req.Subject

	// 设置消息的模板ID
	md.TemplateID = p.Req.TemplateID

	// 将模板数据转换为JSON格式
	td, err := json.Marshal(p.Req.TemplateData)
	if err != nil {
		p.Resp.Code = constant.ERR_JSON_MARSHAL
		return err
	}

	// 设置消息的模板数据
	md.TemplateData = string(td)

	// 设置消息的接收者
	md.To = p.Req.To

	// 设置消息的ID
	md.MsgId = msgID

	// 设置消息的初始状态为待处理状态
	// 消息状态流转: PENDING -> PROCESSING -> SUCC
	// 1. 初始状态为PENDING，表示消息已持久化，等待消费
	// 2. 消费者获取消息后，将状态更新为PROCESSING，表示消息正在处理中
	// 3. 消息成功处理后，将状态更新为SUCC，表示消息处理完成
	md.Status = int(data.TASK_STATUS_PENDING)

	// 设置消息的优先级
	md.Priority = p.Req.Priority

	// 获取消息优先级字符串
	priorityStr := data.GetPriorityStr(data.PriorityEnum(p.Req.Priority))

	// 将消息插入到MySQL数据库中
	err = data.MsgQueueNsp.Create(dt.GetDB(), priorityStr, md)
	if err != nil {
		p.Resp.Code = constant.ERR_INSERT
		return err
	}

	// 将消息ID赋值给响应结构体
	p.Resp.MsgID = msgID

	// 返回nil，表示发送成功
	return nil
}

// sendToMQ 将消息发送到消息队列
func (p *SendMsgHandler) sendToMQ() error {
	// 获取数据实例
	log.Infof("into sendToMQ")
	dt := data.GetData()

	// 生成唯一的消息ID
	msgID := utils.NewUuid()

	// 将消息ID赋值给请求结构体
	p.Req.MsgID = msgID
	// 将消息ID赋值给响应结构体
	p.Resp.MsgID = msgID

	// 将请求结构体转换为JSON格式
	msgJson, err := json.Marshal(p.Req)
	if err != nil {
		// 记录错误日志
		p.Resp.Code = constant.ERR_JSON_MARSHAL
		log.ErrorContextf(context.Background(), "json marshal err %s", err.Error())
		return err
	}

	// 消息队列处理流程：
	// 1. 消息发送到MQ并持久化
	// 2. 消费者从MQ获取消息并处理
	// 3. 处理成功后，消费者会更新MySQL中的消息状态为成功

	var sendErr error

	// 根据消息优先级选择对应的消息队列生产者
	producer := dt.GetProducer(data.PriorityEnum(p.Req.Priority))
	sendErr = producer.SendMessage(msgJson)
	if sendErr != nil {
		log.ErrorContextf(context.Background(), "发送消息到MQ失败: %s", sendErr.Error())
		return sendErr
	}

	log.Infof("消息 %s 已发送到%s优先级队列", msgID, data.GetPriorityStr(data.PriorityEnum(p.Req.Priority)))

	// 返回nil，表示发送成功
	return nil
}

// sendToTimer 将消息发送到定时队列
func (p *SendMsgHandler) sendToTimer() error {
	// 获取数据实例
	log.Infof("into sendToTimer")
	dt := data.GetData()
	ctx := context.Background()

	// 生成唯一的消息ID
	msgID := utils.NewUuid()

	// 将消息ID赋值给请求结构体
	p.Req.MsgID = msgID
	// 将消息ID赋值给响应结构体
	p.Resp.MsgID = msgID

	// 将请求结构体转换为JSON格式
	msgJson, err := json.Marshal(p.Req)
	if err != nil {
		// 记录错误日志
		p.Resp.Code = constant.ERR_JSON_MARSHAL
		log.ErrorContextf(context.Background(), "json marshal err %s", err.Error())
		return err
	}

	// 根据消息优先级选择对应的定时队列 Todo 是否处理优先级？
	// 存入 MySQL 临时队列；
	// 创建一个新的消息队列实例
	var md = new(data.MsgTmpQueueTimer)

	// 设置消息的发送时间
	md.SendTimestamp = p.Req.SendTimestamp

	// 设置消息
	md.Req = string(msgJson)

	// 设置消息的ID
	md.MsgId = msgID

	// 设置消息的初始状态
	md.Status = int(data.TIMER_MSG_STATUS_PENDING)

	// 将消息插入到MySQL数据库中
	err = data.MsgTmpQueueTimerNsp.Create(dt.GetDB(), md)
	if err != nil {
		p.Resp.Code = constant.ERR_INSERT_TIMER
		return err
	}

	// 存入 ZSET；
	timeSocre := float64(p.Req.SendTimestamp)
	member := fmt.Sprintf("%d", p.Req.SendTimestamp)
	_, err = dt.GetCache().ZAdd(ctx, "Timer_Msgs", timeSocre, member)
	if err != nil {
		p.Resp.Code = constant.ERR_INSERT_TIMER
		return err
	}

	// 返回nil，表示发送成功
	return nil
}
