# MsgMate 渐进性能测试

在 WSL/Linux 的仓库根目录执行。需要 Go、Docker Compose、Python 3、make、C 编译器与静态 libc 开发库；优先使用仓库本地 `wrk/` 源码构建压测程序；目录缺失时，在临时目录获取并固定到 upstream commit `a211dd5a7050b1f9e8a9870b95513060e72ac4a0`。Ubuntu/WSL 缺少编译依赖时可运行 `sudo apt install build-essential`。

## 一次执行全部测试

```bash
python3 scripts/perf/run.py --suite all --pause-unrelated
```

四个独立入口：

```bash
bash scripts/perf/test-send.sh --pause-unrelated
bash scripts/perf/test-mysql.sh --pause-unrelated
bash scripts/perf/test-kafka.sh --pause-unrelated
bash scripts/perf/test-record.sh --pause-unrelated
```

默认每档 20 秒，阈值附近复测 40 秒。也可以指定 `--duration 30 --verify-duration 60`。首次拉取 Docker 镜像或获取 wrk 源码需要网络。首次执行会编译 Go 与 wrk，源码目录没有二进制也可以运行。

脚本创建独立 Compose project，使用 `msgmate_perf_test` 新数据库、独立 Redis、独立 Kafka/Zookeeper，不映射宿主端口、不加载 `config-local.toml`、不接触原业务数据。所有负载在 Docker 私网内运行，避免已观察到的宿主 loopback 异常。凭据为公开的、仅用于无宿主端口测试网络的合成值，不是任何已有服务密码。结束后删除测试容器和测试卷，结果保留在 `reports/performance/<北京时间>/`。

`--pause-unrelated` 只暂停当前运行且名字匹配 `devsupport-*`、`learnq_demo_20261003-*`、`ollama`、`kafka-ui` 的 Docker 容器，记录名单并在 finally 中恢复。不会停 MsgMate 原 MySQL/Redis/Zookeeper 或 IDE、WSL、Docker。中断使用 Ctrl+C 让脚本执行清理；强制杀死 Python 或关闭 WSL 会阻止自动恢复，届时按 environment.json 的 paused_containers 手动 `docker start`。

## 四类测试的含义

1. **发送接口**：分别测 MySQL / Kafka 模式下 `/msg/send_msg` 的真实路由、模板读取、Redis 配额计数、记录持久化及入队。关闭消费者，避免把消费争用混入入口极限。消息只写入隔离环境，不会调用真实渠道。
2. **MySQL 中转**：开启生产 MySQL 消费者，均衡向 High/Middle/Low 提交消息，观察固定到达速率下的实际完成速率、积压斜率和排空时间。包含数据库操作、模板渲染、模拟渠道调用及状态落库，属于整条队列处理链，不是纯 SQL 插入基准。
3. **Kafka 中转**：同样的负载与模拟渠道，使用生产 Kafka producer/consumer。沿用源码的同步发布、ack=-1，四个 Topic 分别单分区、单副本；未调整默认 batching、消费线程或提交 offset 策略。这是当前应用链路容量，不能当作 Kafka Broker 的吞吐上限。
4. **消息记录查询**：10,000 条合成记录轮转查询 `/msg/get_msg_record`，携带正确 Source-Id，读取真实 MySQL。该接口当前不读 Redis 缓存。数据规模很小，不能外推到千万行或磁盘冷读。

外部渠道由 `cmd/perf` 独立入口注册的进程内模拟处理器替代，默认零延迟；只接受 `@benchmark.invalid` 接收地址，没有 SMTP/飞书/SMS 网络请求，也不注册生产发送器。没有修改生产路由或消费者算法。真实第三方延迟、限额、失败与费用不在容量数据内。

日志保持 info 级别及 Gin 请求日志，输出到文件，避免交互式 IDE 终端刷屏成为瓶颈。主程序不启动定时消费者，本测试也不提交定时消息。原服务日志配置、定时任务干扰、外部渠道延迟均属于生产使用时需要另测的因素。

## 渐进与停止条件

wrk 从 4 并发开始，按 8、16、32、64、128、256、512、1024 增加。若业务/传输/延迟/监控数据库错误判据不满足，或者连续两档吞吐增益不超过前面峰值的 5%，停止扩档；选择达标档中吞吐最大的档位，在新建的 runtime 网络命名空间中连续复测两次（两次之间不重启）；复测失败则下降候选并发再验证，失败数据同样保留。重建只用于候选验证的初始状态，不能把连接耗尽问题视为已修复。最大档仍增长时，只能称已测范围，不能宣称绝对极限。wrk 的 Lua 响应校验与动态请求构建有开销，压测端 CPU 也写入资源快照。

中转测试以 10、20、40、80、160、320、640、1280 条/s 独立于响应完成调度。2048 个 HTTP worker 有界；排队满而未发出的请求计入 missed。若短档只有完成速率接近判据、受理数与完成数完全一致且停止观察时零积压，会先延长重测，避免 Kafka 批次波动导致误判。首个实际不达标档出现后，在最后达标与首个失败速率之间最多二分三次，再对候选速率做两次更长复测；复测不达标则验证此前更低的达标速率。可用 `--queue-rates 80,90,100` 指定已知区间继续细化。每档结束都先等待排空，避免上一档积压污染下一档；60 秒不排空立即停止。

本次实验判据（可在日志 environment.json 查看）：

- 至少 99% 的计划请求业务成功，且无业务/传输错误；入口记录查询 P99 ≤ 200ms，发送与中转入口 P99 ≤ 2000ms。
- 中转后半段积压增长 ≤ 目标速率的 5%，完成速率 ≥ 目标速率的 95%；控制器观察负载结束时仅允许 max(10,目标速率×0.25) 条尾部在途，再在 60 秒内排空，受理数=持久化数=最终成功数，无失败或重试增长。
- 不把 `HTTP 200` 当作成功：检查 JSON `code=0`，发送还检查非空 msgID。轮次日志包括业务成功数、传输错误、SQL 状态计数与 SQL 连接池等待。
- 按每档 2 秒间隔采样数据库状态；独立 monitor.py 在整个测试期间采样测试容器 CPU/内存，结果为 resource-samples.jsonl；每档末尾另留资源快照。采样查询有一定开销，并不等同持续系统 profiling。
- 入口测试每档结束清理合成发送记录和 MySQL 队列，消费者测试按模式隔离，Kafka 入口与消费使用不同 Topic，避免前一阶段残留消息影响后续测试。

P99 一律表示 HTTP 请求完整响应的延迟，不是最终投递延迟。排空秒数从控制器观察负载进程结束后开始计时，有约 2 秒的轮询检测间隔，不应当作单条消息的送达延迟。

这些延迟/成功率边界是实验约定，不是生产 SLA。复测失败的候选值不能写成稳定容量。固定到达速率实验仍受有界负载客户端、同机资源争用、日志和小数据集影响。

`wrk/` 作为固定版本上游子模块登记，可通过 `git submodule update --init wrk` 获取。未初始化子模块时，脚本也会在临时目录获取固定版本源码。编译二进制和对象文件由 wrk 自身的忽略规则排除。
