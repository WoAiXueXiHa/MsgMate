# 模拟渠道全链路测试

`../../../scripts/perf-e2e/main.go` 启动真实 HTTP 路由、普通消费者和定时消费者，只注册模拟邮件处理器，每次调用等待 10ms 后成功，不注册任何真实渠道。

负载发生器使用固定速率，避免普通 wrk 的闭环吞吐掩盖后台积压。每组低优先级请求持续 30 秒，每 5 秒查询消息记录的待处理、成功、失败数量；停止提交后最多等待 180 秒排空。测试不覆盖真实渠道、失败重试、高中低混合负载或长时间稳定性。

在仓库根目录运行，使用专属项目名 `msgmate-e2e`；不要与已有同名测试同时运行：

```bash
mkdir -p /tmp/msgmate-e2e
cp reports/performance/e2e/mysql.toml reports/performance/e2e/kafka.toml /tmp/msgmate-e2e/
CGO_ENABLED=0 go build -o /tmp/msgmate-e2e/bench ./scripts/perf-e2e
docker compose -p msgmate-e2e -f reports/performance/e2e/compose.yml up -d
# 等待 MySQL 初始化完成、Kafka 就绪；再运行负载。
python3 reports/performance/e2e/run.py
# JSONL 和服务日志写入 /tmp/msgmate-e2e/。
docker exec msgmate-e2e-runtime-1 pkill -TERM bench
docker compose -p msgmate-e2e -f reports/performance/e2e/compose.yml down -v
```

测试凭据仅用于本地独立测试。没有宿主端口映射或业务数据目录挂载。运行脚本检查容器属于专属 Compose 项目，仅清空该测试库的消息、队列表与专属 Redis；组间保留已完成消费的 Kafka offset。数据库名必须以 `_test` 结尾。

服务重启后未做消费预热，因此包含初始消费分配/启动过程。HTTP P99 只表示提交响应时间；完成速度依据消息记录 `status=2`，意味着模拟处理器成功且状态已持久化。排空时间按 5 秒轮询观测，是上界近似值。累计渠道调用数与状态查询并非同一事务，中间样本可能短暂相差 1；最终计数需完全对齐。

消费优化后的原始数据保存在 `optimized/`，旧基线不覆盖。复跑新版默认消费配置时，可以使用 `python3 reports/performance/e2e/run.py --output-dir /tmp/msgmate-e2e/optimized` 分开保存新输出；新版脚本在组间检查旧服务确实退出后才重置测试表。
