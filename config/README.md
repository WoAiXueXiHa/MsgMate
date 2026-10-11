# 本地配置说明

配置由 TOML 加载。建议显式使用 `go run ./src -config /absolute/path/to/config-local.toml`，默认配置从仓库根目录的 `config/config-test.toml` 加载。已有配置不覆盖，私有配置不提交。

以下是字段示例，全部连接和凭据值需要在本地填写。端口 `8081` 仅为文档示例。

```toml
[common]
port = 8081
mysql_as_mq = true
open_cache = false
max_retry_count = 5
email_account = "<QQ_EMAIL_ACCOUNT>"
email_auth_code = "<QQ_SMTP_AUTH_CODE>"

[mysql]
url = "127.0.0.1:3306"
user = "<MYSQL_USER>"
pwd = "<MYSQL_PASSWORD>"
db_name = "msgcenter_db"

[redis]
url = "127.0.0.1:6379"
pwd = "<REDIS_PASSWORD>"

[kafka]
brokers = ["127.0.0.1:9092"]

[kafka.topics.low]
name = "msgmate-low"
priority = 1
ack = -1
group_id = "msgmate-low-consumer"
partition = 0

[kafka.topics.middle]
name = "msgmate-middle"
priority = 2
ack = -1
group_id = "msgmate-middle-consumer"
partition = 0

[kafka.topics.high]
name = "msgmate-high"
priority = 3
ack = -1
group_id = "msgmate-high-consumer"
partition = 0

[kafka.topics.retry]
name = "msgmate-retry"
priority = 4
ack = -1
group_id = "msgmate-retry-consumer"
partition = 0
```

`mysql_as_mq=true` 选择 MySQL 队列表，`false` 选择 Kafka。MySQL 模式跳过 Kafka 初始化；Kafka 模式需要四个 Topic 配置，启动时检查 Broker、创建缺失 Topic 并等待 Leader 就绪。`open_cache=false` 不关闭 Redis：限流、锁、定时索引仍依赖它。

`max_retry_count` 是失败计数阈值，达到后终止重试；不是保证额外执行这么多次。配置为零或省略时默认 20；负数拒绝启动，建议显式填写正数。

Kafka 发布固定为同步、`acks=-1`；旧 `ack`、`async` 配置字段保留兼容，不再决定可靠性。消费者使用消费者组已提交位置；新组从最早消息开始。`offset`、`partition` 字段不用于消费者组模式。

## 与 Compose 对接

当前 Compose 中 MySQL 数据库为 `msgcenter_db`，宿主机端口为 MySQL 3306、Redis 6379、Kafka 默认 9092，可通过 `MSGMATE_KAFKA_PORT` 设置宿主端口，例如 19092；配置 brokers 需对应修改。密码按本地 Compose 配置填写，不在这里复制。Go 程序在宿主机运行时使用 `127.0.0.1:9092`；在 Compose 网络中运行时使用 Kafka 内部地址 `msgcenter_kafka:9093`。现有 Compose 已区分两种监听地址，无需按旧文档修改 hosts。

QQ SMTP 主机 `smtp.qq.com` 与端口 `465` 当前固定在源码中，配置只提供邮箱账号和授权码。飞书凭据通过环境变量 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 读取，使用飞书发送前需在本地设置；短信适配保留，但不作为本项目接入主线。

程序不再打印整份配置，私有配置不提交，授权码不要写入文档或聊天。集成验收配置必须使用名称以 `_test` 结尾的隔离数据库。真实 SMTP 结果与未验证边界见 `docs/REPAIR.md`。

### Kafka 批次等待

`[Kafka]` 可设置 `batch_timeout_ms = 20`，单位毫秒，用于缩短批次未满时的等待。未设置或为0时保留原默认约1000毫秒；负数拒绝启动。该参数不改变同步返回、ack=-1、分区策略、重试或发送超时。

### MySQL连接池

当前源码统一设置最大连接50、最大空闲连接50、连接寿命30秒。连接寿命由依赖的WithMaxIdleTime设置，实际调用SetConnMaxLifetime。空闲连接10→50的查询复测及完整wrk输出见[RESULTS.md](../reports/performance/RESULTS.md)。

### 消费并发与发送保护

新增 `[Consumer]` 配置，默认普通优先级各4个worker、重试1个，MySQL高/中/低批量60/30/30；每个渠道跨优先级最多4次在途发送，可设置 `channel_rate_per_second`（0不限速）。空队列逐步退避、普通队列忙时连续拉取、重试保留1000ms批间隔；Kafka采用整批完成后提交offset。并发消费不保证消息完成顺序，仍只支持单实例恢复。

```toml
[Consumer]
workers = 4
retry_workers = 1
high_batch = 60
middle_batch = 30
low_batch = 30
retry_batch = 10
idle_min_ms = 50
idle_max_ms = 500
error_backoff_ms = 500
retry_interval_ms = 1000
kafka_batch_wait_ms = 5
channel_concurrency = 4
channel_rate_per_second = 0
```

省略或零值采用上述默认值；发送速率0表示不限速。普通/重试worker范围1–32，渠道并发1–64，批量1–1000；空闲等待满足 `1 <= idle_min_ms <= idle_max_ms <= 60000`，错误退避及重试间隔1–60000ms，Kafka填充等待1–1000ms，发送速率0–100000条/s。无效配置拒绝启动。

渠道并发与速率对每个渠道分别生效，跨优先级共享，覆盖普通发送和重试；只在本进程有效，不提供跨实例发送限速。设置 `workers=1`、`retry_workers=1`、`channel_concurrency=1` 可回到顺序处理，但仍使用新版空闲退避。Kafka慢消息会拖住当前批次，提交失败只重试offset，不重复执行该批处理器。

新库已包含队列查询索引 `(status, create_time, id)`。已有数据库部署前先执行 `SHOW INDEX FROM t_msg_queue_low` 等检查四张队列表，仅在索引缺失时按[一次性迁移脚本](../sql/migrations/20261011_consumer_indexes.sql)添加；不要重复执行已有索引的迁移。本次没有自动修改现有业务数据库。
