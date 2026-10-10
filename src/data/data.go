package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/BitofferHub/pkg/middlewares/cache"
	"github.com/BitofferHub/pkg/middlewares/gormcli"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/pkg/middlewares/mq"
	conf "github.com/WoAiXueXiHa/MsgMate/src/config"
	_ "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// Data 汇总数据库、Redis 和各优先级队列的连接，供业务处理与消费者复用。
type Data struct {
	db        *gorm.DB
	rdb       *cache.Client
	producers map[PriorityEnum]mq.Producer
	consumers map[PriorityEnum]mq.Consumer
}

var data *Data

func GetData() *Data {
	return data
}

func (p *Data) GetDB() *gorm.DB {
	return p.db
}

func (p *Data) GetCache() *cache.Client {
	return p.rdb
}

// GetMsgTemplate 优先读取可选缓存，未命中时查询 MySQL。
func (p *Data) GetMsgTemplate(ctx context.Context, templateID string) (*MsgTemplate, error) {
	var template *MsgTemplate

	// 缓存只用于加速查询；读取失败或内容无效时回退数据库。
	if conf.Conf.Common.OpenCache {
		templateCacheKey := p.genTemplateCacheKey(templateID)
		cacheData, _, _ := p.GetCache().Get(ctx, templateCacheKey)
		if len(cacheData) > 0 {
			template = new(MsgTemplate)
			if err := json.Unmarshal([]byte(cacheData), template); err == nil {
				log.Debugf("template cache hit")
				return template, nil
			}
		}
	}

	// MySQL 是模板数据的真实来源。
	log.Infof("template cache miss")
	var err error
	template, err = MsgTemplateNsp.Find(p.GetDB(), templateID)
	if err != nil {
		log.Errorf("find msg template err %s", err.Error())
		return nil, err
	}

	// 缓存设置失败不影响本次查询结果，模板更新和删除时主动清理缓存。
	if conf.Conf.Common.OpenCache {
		if cacheData, err := json.Marshal(template); err == nil {
			templateCacheKey := p.genTemplateCacheKey(templateID)
			p.GetCache().Set(ctx, templateCacheKey, string(cacheData), 30*time.Second)
		}
	}

	return template, nil
}

func (p *Data) genTemplateCacheKey(templateID string) string {
	return fmt.Sprintf("%s%s", REDIS_KEY_TEMPLATE, templateID)
}

func (p *Data) GetProducer(key PriorityEnum) mq.Producer {
	return p.producers[key]
}

func (p *Data) GetConsumer(key PriorityEnum) mq.Consumer {
	return p.consumers[key]
}

func (p *Data) GetLowMQProducer() mq.Producer {
	return p.producers[PRIORITY_LOW]
}

func (p *Data) GetLowMQConsumer() mq.Consumer {
	return p.consumers[PRIORITY_LOW]
}

func (p *Data) GetMiddleMQProducer() mq.Producer {
	return p.producers[PRIORITY_MIDDLE]
}

func (p *Data) GetMiddleMQConsumer() mq.Consumer {
	return p.consumers[PRIORITY_MIDDLE]
}

func (p *Data) GetHighMQProducer() mq.Producer {
	return p.producers[PRIORITY_HIGH]
}

func (p *Data) GetHighMQConsumer() mq.Consumer {
	return p.consumers[PRIORITY_HIGH]
}

func (p *Data) GetRetryMQProducer() mq.Producer {
	return p.producers[PRIORITY_RETRY]
}

func (p *Data) GetRetryMQConsumer() mq.Consumer {
	return p.consumers[PRIORITY_RETRY]
}

// NewData
//
//	@Author <a href="https://bitoffer.cn">狂飙训练营</a>
//	@Description:
//	@param dt
//	@return *Data
//	@return error
func NewData(cf *conf.TomlConfig) (result *Data, initErr error) {
	if cf.Kafka.BatchTimeoutMS < 0 {
		return nil, fmt.Errorf("Kafka batch_timeout_ms must be nonnegative")
	}
	defer func() {
		if r := recover(); r != nil {
			result = nil
			initErr = fmt.Errorf("dependency initialization failed: %v", r)
		}
	}()

	gormcli.Init(
		gormcli.WithAddr(cf.MySQL.Url),
		gormcli.WithUser(cf.MySQL.User),
		gormcli.WithPassword(cf.MySQL.Pwd),
		gormcli.WithDataBase(cf.MySQL.Dbname),
		gormcli.WithMaxIdleConn(50),
		gormcli.WithMaxOpenConn(50),
		gormcli.WithMaxIdleTime(30),
		gormcli.WithSlowThresholdMillisecond(0),
	)
	cache.Init(
		cache.WithAddr(cf.Redis.Url),
		cache.WithPassWord(cf.Redis.Pwd),
		cache.WithDB(0),
	)
	var producers map[PriorityEnum]mq.Producer
	var consumers map[PriorityEnum]mq.Consumer
	if !cf.Common.MySQLAsMq {
		if err := validateKafka(cf); err != nil {
			return nil, err
		}
		producers = generateProducer(cf)
		consumers = generateConsumer(cf)
	}

	dta := &Data{
		db:        gormcli.GetDB(),
		rdb:       cache.GetRedisCli(),
		producers: producers,
		consumers: consumers,
	}
	data = dta

	return dta, nil
}

func generateProducer(cf *conf.TomlConfig) map[PriorityEnum]mq.Producer {
	producers := make(map[PriorityEnum]mq.Producer)

	for _, topicConfig := range cf.Kafka.Topics {
		var producer mq.Producer
		if cf.Kafka.BatchTimeoutMS > 0 {
			producer = newTimedKafkaProducer(cf.Kafka.Brokers, topicConfig.Name, time.Duration(cf.Kafka.BatchTimeoutMS)*time.Millisecond)
		} else {
			producer = mq.NewKafkaProducer(
				mq.WithBrokers(cf.Kafka.Brokers),
				mq.WithTopic(topicConfig.Name),
				mq.WithAck(-1),
				mq.WithGroupID(topicConfig.GroupID),
				mq.WithPartition(topicConfig.Partition))
		}

		if producer == nil {
			panic(fmt.Sprintf("nil producer for %s", topicConfig.Name))
		}
		producers[PriorityEnum(topicConfig.Priority)] = producer
	}

	return producers
}

func generateConsumer(cf *conf.TomlConfig) map[PriorityEnum]mq.Consumer {
	consumers := make(map[PriorityEnum]mq.Consumer)

	for _, topicConfig := range cf.Kafka.Topics {
		consumer := newKafkaConsumer(cf.Kafka.Brokers, topicConfig.Name, topicConfig.GroupID)
		if consumer == nil {
			panic(fmt.Sprintf("nil consumer for %s", topicConfig.Name))
		}
		consumers[PriorityEnum(topicConfig.Priority)] = consumer
	}

	return consumers
}

func (p *Data) Close() {
	for _, producer := range p.producers {
		producer.Close()
	}
	if p.db != nil {
		if db, err := p.db.DB(); err == nil {
			_ = db.Close()
		}
	}
	if p.rdb != nil {
		p.rdb.Close()
	}
}
