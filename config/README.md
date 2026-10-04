# 本地配置说明

配置由 TOML 加载。建议显式使用 `go run ./src -config /absolute/path/to/config-local.toml`，避免默认相对路径 `../config/config-test.toml` 随工作目录变化。已有配置不覆盖，私有配置不提交。

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

`mysql_as_mq=true` 选择 MySQL 队列表，`false` 选择 Kafka。初始化没有按此开关跳过 Kafka 对象创建，因此上例仍列出四个 Topic。`open_cache=false` 不关闭 Redis：限流、锁、定时索引仍依赖它。

`max_retry_count` 是失败计数阈值，达到后终止重试；不是保证额外执行这么多次。源码注释提到默认值，不代表配置加载会自动补上，应显式填写正数。

Kafka `ack` 映射确认参数（0/1/-1）。配置结构虽然包含 `async` 和 `offset`，当前初始化不读取它们：生产者无条件传入 `WithAsync()`，消费者未传入配置的 offset。因此不要依赖填写 `async=false` 获得同步持久化确认。

## 与 Compose 对接

当前 Compose 中 MySQL 数据库为 `msgcenter_db`，宿主机端口为 MySQL 3306、Redis 6379、Kafka 9092。密码按本地 Compose 配置填写，不在这里复制。Go 程序在宿主机运行时使用 `127.0.0.1:9092`；在 Compose 网络中运行时使用 Kafka 内部地址 `msgcenter_kafka:9093`。现有 Compose 已区分两种监听地址，无需按旧文档修改 hosts。

QQ SMTP 主机 `smtp.qq.com` 与端口 `465` 当前固定在源码中，配置只提供邮箱账号和授权码。飞书凭据通过环境变量 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 读取，使用飞书发送前需在本地设置；短信适配保留，但不作为本项目接入主线。

程序初始化会打印配置，不要分享包含凭据的启动日志。上述配置仅作源码字段说明，未进行启动或真实发送验证。
