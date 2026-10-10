# MsgMate 渐进性能测试日志

测试日期与时区：20261010-121707 / Asia/Shanghai。
执行状态：complete。

**范围**：当前真实 HTTP 路由、MySQL/Redis/Kafka、生产消费者；外部渠道替换为进程内零延迟模拟发送器。不能据此宣称 SMTP/飞书吞吐、收件质量、多实例安全或生产 SLA。

**方法**：wrk 固定并发阶梯测试入口和记录查询；独立于响应完成的固定到达速率测试队列，均衡 High/Middle/Low。未改变生产消费者批量、轮询、连接池及 Kafka 写入策略。查询轮转 10,000 条记录，每次直接读 MySQL。入口测试关闭消费者，单独量化持久化受理；队列测试开启真实消费者。

**测试判据**：业务成功率至少 99%，且无业务/传输错误；查询 P99 ≤ 200ms、发送 P99 ≤ 2000ms；队列后半段积压增长 ≤ 提交速率的 5%，完成速率 ≥ 目标到达速率的 95%，停止后 60 秒内排空，全部受理消息最终成功且无失败/重试。判据是本次实验选定的容量边界，并非生产承诺。

**可复核性**：environment.json 记录资源、源码哈希、Git 状态、暂停容器、镜像及阈值；每轮 txt 为原始输出，json 为结构化结果，samples.json 为运行期间快照。固定速率测试的 missed 也计入未达标，不能隐藏负载生成器过载。

| 测试 | 模式 | 并发 / 目标RPS | 时长 | 成功响应RPS / 后半段完成RPS | P99 ms | 错误或漏发 | 积压增长/s | 排空秒 | 达标 | 复测 | 原始日志 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| record | mysql | 4 | 15s | 6728.0 | 1.14 | 0 | 0.0 | 0.0 | 是 | 否 | [001-record-mysql-c4](001-record-mysql-c4.txt) |
| record | mysql | 8 | 15s | 10283.2 | 1.56 | 0 | 0.0 | 0.0 | 是 | 否 | [002-record-mysql-c8](002-record-mysql-c8.txt) |
| record | mysql | 16 | 15s | 10098.1 | 4.90 | 0 | 0.0 | 0.0 | 是 | 否 | [003-record-mysql-c16](003-record-mysql-c16.txt) |
| record | mysql | 32 | 15s | 11276.3 | 11.18 | 0 | 0.0 | 0.0 | 是 | 否 | [004-record-mysql-c32](004-record-mysql-c32.txt) |
| record | mysql | 64 | 15s | 10867.4 | 22.43 | 1214 | 0.0 | 0.0 | 否 | 否 | [005-record-mysql-c64](005-record-mysql-c64.txt) |
| record | mysql | 32 | 30s | 9506.2 | 22.91 | 8478 | 0.0 | 0.0 | 否 | 是 | [006-record-mysql-c32](006-record-mysql-c32.txt) |
| record | mysql | 8 | 30s | 9921.3 | 1.66 | 0 | 0.0 | 0.0 | 是 | 是 | [007-record-mysql-c8](007-record-mysql-c8.txt) |
| record | mysql | 8 | 30s | 9963.7 | 1.64 | 0 | 0.0 | 0.0 | 是 | 是 | [008-record-mysql-c8](008-record-mysql-c8.txt) |
| send | mysql | 4 | 15s | 611.3 | 10.59 | 0 | 0.0 | 0.0 | 是 | 否 | [009-send-mysql-c4](009-send-mysql-c4.txt) |
| send | mysql | 8 | 15s | 1015.4 | 12.64 | 0 | 0.0 | 0.0 | 是 | 否 | [010-send-mysql-c8](010-send-mysql-c8.txt) |
| send | mysql | 16 | 15s | 1346.8 | 18.62 | 0 | 0.0 | 0.0 | 是 | 否 | [011-send-mysql-c16](011-send-mysql-c16.txt) |
| send | mysql | 32 | 15s | 2097.0 | 25.72 | 0 | 0.0 | 0.0 | 是 | 否 | [012-send-mysql-c32](012-send-mysql-c32.txt) |
| send | mysql | 64 | 15s | 2851.8 | 48.12 | 0 | 0.0 | 0.0 | 是 | 否 | [013-send-mysql-c64](013-send-mysql-c64.txt) |
| send | mysql | 128 | 15s | 3465.1 | 100.52 | 0 | 0.0 | 0.0 | 是 | 否 | [014-send-mysql-c128](014-send-mysql-c128.txt) |
| send | mysql | 256 | 15s | 3740.7 | 254.88 | 0 | 0.0 | 0.0 | 是 | 否 | [015-send-mysql-c256](015-send-mysql-c256.txt) |
| send | mysql | 512 | 15s | 3795.2 | 569.43 | 0 | 0.0 | 0.0 | 是 | 否 | [016-send-mysql-c512](016-send-mysql-c512.txt) |
| send | mysql | 1024 | 15s | 3806.9 | 1151.53 | 18 | 0.0 | 0.0 | 否 | 否 | [017-send-mysql-c1024](017-send-mysql-c1024.txt) |
| send | mysql | 512 | 30s | 3748.8 | 581.36 | 0 | 0.0 | 0.0 | 是 | 是 | [018-send-mysql-c512](018-send-mysql-c512.txt) |
| send | mysql | 512 | 30s | 3748.3 | 577.30 | 0 | 0.0 | 0.0 | 是 | 是 | [019-send-mysql-c512](019-send-mysql-c512.txt) |
| queue | mysql | 10 | 15s | 11.0 | 11.54 | 0 | -0.2 | 0.3 | 是 | 否 | [020-queue-mysql-r10](020-queue-mysql-r10.txt) |
| queue | mysql | 20 | 15s | 21.0 | 12.76 | 0 | 0.4 | 0.2 | 是 | 否 | [021-queue-mysql-r20](021-queue-mysql-r20.txt) |
| queue | mysql | 40 | 15s | 38.1 | 11.38 | 0 | -1.2 | 0.3 | 是 | 否 | [022-queue-mysql-r40](022-queue-mysql-r40.txt) |
| queue | mysql | 80 | 15s | 77.6 | 9.40 | 0 | -2.2 | 0.2 | 是 | 否 | [023-queue-mysql-r80](023-queue-mysql-r80.txt) |
| queue | mysql | 160 | 15s | 129.8 | 9.19 | 0 | 21.3 | 14.8 | 否 | 否 | [024-queue-mysql-r160](024-queue-mysql-r160.txt) |
| queue | mysql | 120 | 30s | 103.9 | 10.01 | 0 | 8.7 | 14.1 | 否 | 否 | [025-queue-mysql-r120](025-queue-mysql-r120.txt) |
| queue | mysql | 100 | 30s | 96.4 | 10.28 | 0 | 4.7 | 5.4 | 是 | 否 | [026-queue-mysql-r100](026-queue-mysql-r100.txt) |
| queue | mysql | 110 | 30s | 102.5 | 9.25 | 0 | 7.7 | 6.8 | 否 | 否 | [027-queue-mysql-r110](027-queue-mysql-r110.txt) |
| queue | mysql | 100 | 30s | 92.7 | 9.06 | 0 | 2.6 | 5.6 | 否 | 是 | [028-queue-mysql-r100](028-queue-mysql-r100.txt) |
| queue | mysql | 80 | 30s | 82.5 | 8.75 | 0 | -2.1 | 0.3 | 是 | 是 | [029-queue-mysql-r80](029-queue-mysql-r80.txt) |
| queue | mysql | 80 | 30s | 79.3 | 8.96 | 0 | 1.6 | 0.3 | 是 | 是 | [030-queue-mysql-r80](030-queue-mysql-r80.txt) |
| send | kafka | 4 | 15s | 3.7 | 1057.57 | 0 | 0.0 | 0.0 | 是 | 否 | [031-send-kafka-c4](031-send-kafka-c4.txt) |
| send | kafka | 8 | 15s | 7.5 | 1016.59 | 0 | 0.0 | 0.0 | 是 | 否 | [032-send-kafka-c8](032-send-kafka-c8.txt) |
| send | kafka | 16 | 15s | 14.9 | 1018.13 | 0 | 0.0 | 0.0 | 是 | 否 | [033-send-kafka-c16](033-send-kafka-c16.txt) |
| send | kafka | 32 | 15s | 29.8 | 1031.73 | 0 | 0.0 | 0.0 | 是 | 否 | [034-send-kafka-c32](034-send-kafka-c32.txt) |
| send | kafka | 64 | 15s | 59.5 | 1024.14 | 0 | 0.0 | 0.0 | 是 | 否 | [035-send-kafka-c64](035-send-kafka-c64.txt) |
| send | kafka | 128 | 15s | 3119.1 | 76.43 | 0 | 0.0 | 0.0 | 是 | 否 | [036-send-kafka-c128](036-send-kafka-c128.txt) |
| send | kafka | 256 | 15s | 4677.1 | 147.58 | 0 | 0.0 | 0.0 | 是 | 否 | [037-send-kafka-c256](037-send-kafka-c256.txt) |
| send | kafka | 512 | 15s | 4668.7 | 411.57 | 0 | 0.0 | 0.0 | 是 | 否 | [038-send-kafka-c512](038-send-kafka-c512.txt) |
| send | kafka | 1024 | 15s | 4764.5 | 893.83 | 8 | 0.0 | 0.0 | 否 | 否 | [039-send-kafka-c1024](039-send-kafka-c1024.txt) |
| send | kafka | 256 | 30s | 4680.6 | 147.41 | 0 | 0.0 | 0.0 | 是 | 是 | [040-send-kafka-c256](040-send-kafka-c256.txt) |
| send | kafka | 256 | 30s | 4625.0 | 151.11 | 0 | 0.0 | 0.0 | 是 | 是 | [041-send-kafka-c256](041-send-kafka-c256.txt) |
| queue | kafka | 10 | 15s | 9.4 | 1015.59 | 0 | -1.1 | 0.3 | 否 | 否 | [042-queue-kafka-r10](042-queue-kafka-r10.txt) |

## 可重复验证的边界

- record/mysql：8 并发，两次复测成功响应吞吐 9921.3–9963.7 req/s，P99 最大 1.66ms。
- send/mysql：512 并发，两次复测成功响应吞吐 3748.3–3748.8 req/s，P99 最大 581.36ms。
- send/kafka：256 并发，两次复测成功响应吞吐 4625.0–4680.6 req/s，P99 最大 151.11ms。
- queue/mysql：目标 80 条/s，两次复测均满足积压与完成判据。
- queue/kafka：尚未获得两次达标复测，不能写成稳定阈值。
