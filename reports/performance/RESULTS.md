# 六组直接 wrk 测试结果

测试日期：2026-10-10 23:24（Asia/Shanghai）。

本轮使用直接 wrk 命令，不使用 Python/Shell 调度脚本。每组预热5秒、正式30秒，4线程、32连接、超时5秒、输出延迟分布。每组单次有效样本，不代表稳定提升或性能极限。

## 环境与准备

单实例 WSL2，Intel i9-13900HX，32逻辑CPU；独立 Docker 项目 `msgmate-direct-wrk`，MySQL 8.0、Redis 7.2.14、Kafka/Zookeeper 7.6.1。通过容器私网访问 `http://runtime:18081`，没有宿主端口映射。其他项目保持原状，未暂停。

使用旧测试启动能力的临时二进制注册真实HTTP路由，消费者关闭，不发送真实渠道；测试完成删除二进制与测试专用启动源码。连接池最大50、空闲10、连接寿命30秒；模板/配额缓存TTL30秒；保留原有日志、Redis限流、数据库事务及Kafka同步ack=-1和默认批次等待。没有调整生产源码或私有配置。

测试模板为 `perf-template`，来源为 `perf-send`，收件人为 `sink@benchmark.invalid`。先在独立测试库执行：

```sql
UPDATE t_global_quota SET num=10000000, unit=1000 WHERE channel=1;
UPDATE t_source_quota SET num=10000000, unit=1000 WHERE source_id='perf-send' AND channel=1;
```

两项均已查询确认：10000000条/1000毫秒。每组重启测试服务加载 `open_cache`；发送各组清空测试消息及队列、清空独立测试Redis，Kafka开关组使用不同测试主题。预检发送/查询均返回code=0。查询两组只保留同一条有效记录，切换组时保留该记录并清空Redis。

查询共用记录ID：`95d14e92-a8e0-4601-a8c5-80603d35ebe7`；请求头 `Source-Id: perf-send`。查询接口为GET，源码直接从MySQL读取，不使用 `open_cache`。

最小请求文件：[scripts/wrk.lua](../../scripts/wrk.lua)。无参数发送POST JSON；传入记录ID时发送GET查询。容器中的 `/perf/wrk.lua` 是该文件的原样副本。

## 正式结果

| 组别 | 成功响应数 | 业务错误 | HTTP Requests/sec | 业务成功QPS | HTTP P99 ms |
|---|---:|---:|---:|---:|---:|
| MySQL发送／缓存OFF | 54928 | 0 | 1828.95 | 1828.95 | 26.99 |
| MySQL发送／缓存ON | 69046 | 0 | 2299.37 | 2299.37 | 21.54 |
| Kafka发送／缓存OFF | 928 | 0 | 30.87 | 30.87 | 1040.00 |
| Kafka发送／缓存ON | 928 | 0 | 30.88 | 30.88 | 1030.00 |
| 消息记录查询／缓存OFF | 253031 | 17303 | 9004.02 | 8427.71 | 22.08 |
| 消息记录查询／缓存ON | 256226 | 17399 | 9113.93 | 8534.40 | 21.45 |

六组正式输出均未报告Socket错误或非2xx/3xx；查询的业务失败仍返回HTTP 200，因此必须看Lua业务计数。P99为全部HTTP响应的延迟分布，不能作为成功请求专属P99。

## 结论

- MySQL发送：OFF→ON成功QPS 1828.95→2299.37，单次变化 +25.72%；P99 26.99→21.54ms。此次观察到提高，不能称稳定提升。
- Kafka发送：OFF→ON成功QPS 30.87→30.88，变化 +0.015%；请求延迟约1秒，此次未观察到明显缓存吞吐收益。没有独立验证批次等待的因果贡献。
- 消息记录查询：两组分别出现17303、17399次业务错误，均不作为有效性能对照。成功QPS的观测差异为 +1.27%，但不能解释为缓存收益；该接口不使用缓存。底层错误为 `dial tcp 172.28.0.3:3306: connect: cannot assign requested address`，即服务端建立MySQL连接时无法分配本地地址。没有调整连接池或操作系统参数。

原版wrk截止可能存在已提交但未读取响应的请求，业务QPS只计Lua收到的code=0响应；本轮不作受理数与落库数逐条一致声明。发送指标为入口受理与同步入队返回，不能推导消费者能力、真实渠道送达或生产SLA。

## 命令与完整原始输出

以下命令是实际执行命令；宿主可达接口时可直接使用 `wrk ... -s scripts/wrk.lua <接口地址>`。每段输出原样保留。

### 1. MySQL发送／缓存OFF

预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    18.14ms    5.26ms  75.50ms   82.08%
    Req/Sec   445.71     55.61   545.00     87.00%
  Latency Distribution
     50%   17.58ms
     75%   20.05ms
     90%   22.88ms
     99%   38.95ms
  8877 requests in 5.00s, 1.62MB read
Requests/sec:   1773.75
Transfer/sec:    330.85KB
Business success: 8877; business errors: 0; success QPS: 1773.75; duration: 5.004658s
```
正式：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    17.48ms    3.55ms  49.54ms   71.55%
    Req/Sec   459.36     35.95   560.00     75.17%
  Latency Distribution
     50%   17.25ms
     75%   19.51ms
     90%   21.90ms
     99%   26.99ms
  54928 requests in 30.03s, 10.01MB read
Requests/sec:   1828.95
Transfer/sec:    341.14KB
Business success: 54928; business errors: 0; success QPS: 1828.95; duration: 30.032590s
```

### 2. MySQL发送／缓存ON

预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    15.07ms    4.03ms  54.20ms   80.75%
    Req/Sec   534.78     59.65   640.00     78.50%
  Latency Distribution
     50%   14.65ms
     75%   16.63ms
     90%   18.78ms
     99%   31.03ms
  10656 requests in 5.00s, 1.94MB read
Requests/sec:   2129.55
Transfer/sec:    397.21KB
Business success: 10656; business errors: 0; success QPS: 2129.55; duration: 5.003885s
```
MySQL开启组首次正式轮与Go构建重叠，判为受干扰样本，不纳入上表；完整输出仍保留：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    27.90ms   15.67ms 182.33ms   78.66%
    Req/Sec   295.30    150.83   646.00     71.50%
  Latency Distribution
     50%   26.06ms
     75%   37.62ms
     90%   45.63ms
     99%   80.71ms
  35321 requests in 30.04s, 6.43MB read
Requests/sec:   1175.83
Transfer/sec:    219.32KB
Business success: 35321; business errors: 0; success QPS: 1175.83; duration: 30.039139s
```

重测前再次预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    34.19ms   25.06ms 255.51ms   89.17%
    Req/Sec   250.59    159.69   610.00     70.50%
  Latency Distribution
     50%   25.06ms
     75%   47.98ms
     90%   59.90ms
     99%  137.98ms
  4999 requests in 5.01s, 0.91MB read
Requests/sec:    998.11
Transfer/sec:    186.17KB
Business success: 4999; business errors: 0; success QPS: 998.11; duration: 5.008444s
```

正式：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    13.90ms    2.76ms  45.38ms   71.76%
    Req/Sec   577.60     40.85   680.00     70.75%
  Latency Distribution
     50%   13.76ms
     75%   15.37ms
     90%   17.29ms
     99%   21.54ms
  69046 requests in 30.03s, 12.58MB read
Requests/sec:   2299.37
Transfer/sec:    428.89KB
Business success: 69046; business errors: 0; success QPS: 2299.37; duration: 30.028218s
```

### 3. Kafka发送／缓存OFF

预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.04s    22.08ms   1.08s    75.00%
    Req/Sec     7.06      0.25     8.00     93.75%
  Latency Distribution
     50%    1.04s 
     75%    1.07s 
     90%    1.08s 
     99%    1.08s 
  128 requests in 5.01s, 23.88KB read
Requests/sec:     25.53
Transfer/sec:      4.76KB
Business success: 128; business errors: 0; success QPS: 25.53; duration: 5.013925s
```
正式：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.03s     5.48ms   1.04s    74.46%
    Req/Sec     7.03      0.16     8.00     97.41%
  Latency Distribution
     50%    1.02s 
     75%    1.03s 
     90%    1.04s 
     99%    1.04s 
  928 requests in 30.06s, 173.09KB read
Requests/sec:     30.87
Transfer/sec:      5.76KB
Business success: 928; business errors: 0; success QPS: 30.87; duration: 30.059191s
```

### 4. Kafka发送／缓存ON

预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.02s     5.60ms   1.03s    75.00%
    Req/Sec     7.25      0.45     8.00     75.00%
  Latency Distribution
     50%    1.02s 
     75%    1.03s 
     90%    1.03s 
     99%    1.03s 
  128 requests in 5.01s, 23.88KB read
Requests/sec:     25.55
Transfer/sec:      4.77KB
Business success: 128; business errors: 0; success QPS: 25.55; duration: 5.010065s
```
正式：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.02s     3.37ms   1.03s    76.19%
    Req/Sec     7.03      0.16     8.00     97.41%
  Latency Distribution
     50%    1.02s 
     75%    1.02s 
     90%    1.02s 
     99%    1.03s 
  928 requests in 30.05s, 173.09KB read
Requests/sec:     30.88
Transfer/sec:      5.76KB
Business success: 928; business errors: 0; success QPS: 30.88; duration: 30.054626s
```

### 5. 消息记录查询／缓存OFF

预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- 95d14e92-a8e0-4601-a8c5-80603d35ebe7
```

```text
Running 5s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.72ms    1.92ms  32.46ms   82.54%
    Req/Sec     3.13k   593.38    10.15k    97.51%
  Latency Distribution
     50%    2.29ms
     75%    3.46ms
     90%    5.05ms
     99%    8.43ms
  62546 requests in 5.10s, 17.54MB read
Requests/sec:  12265.64
Transfer/sec:      3.44MB
Business success: 62546; business errors: 0; success QPS: 12265.64; duration: 5.099285s
```
正式：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- 95d14e92-a8e0-4601-a8c5-80603d35ebe7
```

```text
Running 30s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.98ms    5.29ms  38.06ms   84.72%
    Req/Sec     2.26k   610.62     3.84k    60.67%
  Latency Distribution
     50%    2.63ms
     75%    6.37ms
     90%   13.71ms
     99%   22.08ms
  270334 requests in 30.02s, 74.95MB read
Requests/sec:   9004.02
Transfer/sec:      2.50MB
Business success: 253031; business errors: 17303; success QPS: 8427.71; duration: 30.023685s
```

### 6. 消息记录查询／缓存ON

预热：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- 95d14e92-a8e0-4601-a8c5-80603d35ebe7
```

```text
Running 5s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.74ms    2.23ms  40.13ms   89.42%
    Req/Sec     3.16k   621.60    10.14k    97.01%
  Latency Distribution
     50%    2.26ms
     75%    3.45ms
     90%    5.04ms
     99%    8.67ms
  63138 requests in 5.10s, 17.70MB read
Requests/sec:  12380.66
Transfer/sec:      3.47MB
Business success: 63138; business errors: 0; success QPS: 12380.66; duration: 5.099727s
```
正式：

```bash
docker exec msgmate-direct-wrk-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- 95d14e92-a8e0-4601-a8c5-80603d35ebe7
```

```text
Running 30s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.89ms    5.15ms  34.99ms   84.75%
    Req/Sec     2.29k   585.25     3.82k    60.50%
  Latency Distribution
     50%    2.62ms
     75%    6.23ms
     90%   13.39ms
     99%   21.45ms
  273625 requests in 30.02s, 75.87MB read
Requests/sec:   9113.93
Transfer/sec:      2.53MB
Business success: 256226; business errors: 17399; success QPS: 8534.40; duration: 30.022727s
```

## 清理与验证

旧scripts/perf框架、cmd/perf启动源码、旧JSON/压缩日志、历史归档、性能简历及对应/tmp测试配置、二进制、日志已删除。性能结果目录仅保留本文件；请求文件仅保留scripts/wrk.lua，wrk工具本身保留。README与功能验收指南已更新链接。

本轮独立Docker容器、匿名数据卷和网络已通过compose down -v删除；按项目标识检查，无本轮或旧msgmate-perf测试容器与具名卷残留。未删除正常业务库、私有配置、共享Go缓存或其他项目资源。

删除测试启动源码后，go test ./...和主程序构建通过。原始wrk秒级延迟行自带尾随空格，按原样保留；标准git diff --check对此有提示，忽略行尾空格后检查通过。未提交或推送。

## Kafka批次等待20ms复测

复测时间：2026-10-10 23:32（Asia/Shanghai）。

保持4线程、32并发、预热5秒、正式30秒、timeout5秒、消费者关闭、配额10000000条/1000ms。保留MySQL事务、Redis限流、30秒缓存TTL、50/10连接池及30秒连接寿命、info/Gin文件日志；Kafka同步返回、ack=-1、LeastBytes分区策略、重试和发送超时均保留。仅通过新参数显式设置批次等待：

```toml
[Kafka]
batch_timeout_ms = 20
```

原依赖没有该配置入口，因此在src/data增加兼容原生产者接口的适配器。配置未设置或为0仍使用原依赖、默认等待约1000ms；负数拒绝启动。未修改现有生产配置文件。独立Docker项目msgmate-kafka20，两组重启服务，清理测试消息和Redis，各用独立Kafka主题。两组预检均返回code=0，构建及测试在正式压测前完成。

| 缓存 | 业务成功数 | 业务错误 | HTTP/成功QPS | P99 ms |
|---|---:|---:|---:|---:|
| OFF | 23848 | 0 | 793.55 | 53.28 |
| ON | 26813 | 0 | 892.41 | 45.02 |

两组均未报告Socket错误或非2xx/3xx。20ms条件下缓存开启的单次QPS变化为 +12.46%，P99变化为 -15.50%。不能称稳定缓存收益。

与前文1000ms历史结果相比，OFF约30.87→793.55 QPS（约25.71倍），ON约30.88→892.41 QPS（约28.90倍）；P99从约1秒降至53.28/45.02ms。这支持原批次等待是主要瓶颈的判断，不代表Kafka容量极限。旧新样本不在同一次运行环境内，且新样本使用等价设置的本地适配器，因此上述倍数是历史样本比较，不是严格重复验证的因果提升。批次调整效果不能记为缓存收益。

### 20ms／缓存OFF：完整输出

预热：

```bash
docker exec msgmate-kafka20-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    41.33ms    8.18ms 106.76ms   90.04%
    Req/Sec   194.45     48.18   272.00     94.00%
  Latency Distribution
     50%   39.73ms
     75%   44.76ms
     90%   48.01ms
     99%   73.36ms
  3877 requests in 5.01s, 723.15KB read
Requests/sec:    774.38
Transfer/sec:    144.44KB
Business success: 3877; business errors: 0; success QPS: 774.38; duration: 5.006608s
```
正式：

```bash
docker exec msgmate-kafka20-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    40.26ms    5.25ms  87.35ms   74.54%
    Req/Sec   199.23     40.58   313.00     53.08%
  Latency Distribution
     50%   40.01ms
     75%   43.30ms
     90%   46.00ms
     99%   53.28ms
  23848 requests in 30.05s, 4.34MB read
Requests/sec:    793.55
Transfer/sec:    148.02KB
Business success: 23848; business errors: 0; success QPS: 793.55; duration: 30.052274s
```

### 20ms／缓存ON：完整输出

预热：

```bash
docker exec msgmate-kafka20-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 5s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    37.49ms    6.04ms  84.37ms   85.26%
    Req/Sec   214.04     45.71   323.00     64.00%
  Latency Distribution
     50%   37.11ms
     75%   39.64ms
     90%   42.69ms
     99%   50.83ms
  4267 requests in 5.01s, 795.90KB read
Requests/sec:    852.30
Transfer/sec:    158.97KB
Business success: 4267; business errors: 0; success QPS: 852.30; duration: 5.006449s
```
正式：

```bash
docker exec msgmate-kafka20-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/send_msg
```

```text
Running 30s test @ http://runtime:18081/msg/send_msg
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    35.78ms    3.87ms  59.43ms   69.14%
    Req/Sec   224.05     37.10   323.00     73.33%
  Latency Distribution
     50%   35.51ms
     75%   38.20ms
     90%   40.86ms
     99%   45.02ms
  26813 requests in 30.05s, 4.88MB read
Requests/sec:    892.41
Transfer/sec:    166.46KB
Business success: 26813; business errors: 0; success QPS: 892.41; duration: 30.045624s
```

复测收尾：专属测试容器、卷、网络与/tmp临时配置、源码、二进制和日志均已删除。Go测试、vet和构建通过；保留原始输出的尾随空格，忽略该项后diff检查通过。未提交或推送。

## MySQL空闲连接10→50：查询接口复测

测试时间：2026-10-10 23:42（Asia/Shanghai）。

本轮只将src/data/data.go的WithMaxIdleConn(10)改为WithMaxIdleConn(50)。最大连接仍为50，连接寿命仍为30秒，不调整数据库持久化、日志、限流或其他参数。代码修改应用于公共数据库初始化，后续MySQL/Kafka模式启动均使用该连接池设置。

复用同版本wrk和scripts/wrk.lua，4线程、32并发、timeout5秒，每组预热5秒、正式30秒。独立Docker项目msgmate-idle50，MySQL8.0、Redis7.2.14，关闭消费者，无真实渠道发送。测试来源及渠道默认配额均10000000条/1000ms。构建、Go测试与vet在压测前完成，正式窗口不运行其他检查。

两组使用同一条有效记录 `c4bd2258-a2c9-4846-9ea9-8ea43a182e0a`，Source-Id为perf-send，预检均code=0。切换缓存开关后重启服务、清理独立Redis并保留该记录。查询源码直接读MySQL，不使用缓存。

| 配置/查询组 | 业务成功数 | 业务错误 | 成功QPS | HTTP P99 ms |
|---|---:|---:|---:|---:|
| 原空闲10／OFF（历史） | 253031 | 17303 | 8427.71 | 22.08 |
| 原空闲10／ON（历史） | 256226 | 17399 | 8534.40 | 21.45 |
| 空闲50／OFF | 442256 | 0 | 14737.88 | 5.72 |
| 空闲50／ON | 448866 | 0 | 14956.97 | 5.60 |

本轮两组无业务错误、无wrk报告的Socket错误或非2xx/3xx，HTTP Requests/sec等于业务成功QPS。OFF较历史成功QPS提高 74.87%，ON提高 75.26%。历史两组包含数据库建连失败，所以不能把上述比例宣传为无错误容量提升或重复验证的稳定收益。

在本次相同并发和时长下，修改后不再复现原cannot assign requested address业务错误，且查询成功吞吐提高、延迟下降，支持保留空闲连接50以减少反复建连的方向。未采集连接创建/关闭计数，故不是对根因的完整证明；没有进一步延长连接寿命。

本轮ON/OFF单次QPS差异 +1.49%，查询接口不使用缓存，不将该波动解释为缓存收益。发送接口未在此连接设置下复测，不能直接更新此前发送QPS。


### 空闲50／查询缓存OFF：完整输出

预热：

```bash
docker exec msgmate-idle50-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- c4bd2258-a2c9-4846-9ea9-8ea43a182e0a
```

```text
Running 5s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.21ms    2.31ms  50.16ms   96.56%
    Req/Sec     3.91k   363.41     4.53k    77.50%
  Latency Distribution
     50%    1.91ms
     75%    2.68ms
     90%    3.52ms
     99%    6.12ms
  77829 requests in 5.00s, 21.82MB read
Requests/sec:  15553.53
Transfer/sec:      4.36MB
Business success: 77829; business errors: 0; success QPS: 15553.53; duration: 5.003946s
```
正式：

```bash
docker exec msgmate-idle50-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- c4bd2258-a2c9-4846-9ea9-8ea43a182e0a
```

```text
Running 30s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.20ms    1.13ms  14.59ms   70.12%
    Req/Sec     3.70k   247.45     4.41k    66.83%
  Latency Distribution
     50%    2.02ms
     75%    2.83ms
     90%    3.69ms
     99%    5.72ms
  442256 requests in 30.01s, 124.00MB read
Requests/sec:  14737.88
Transfer/sec:      4.13MB
Business success: 442256; business errors: 0; success QPS: 14737.88; duration: 30.008106s
```

### 空闲50／查询缓存ON：完整输出

预热：

```bash
docker exec msgmate-idle50-load-1 /perf/wrk -t4 -c32 -d5s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- c4bd2258-a2c9-4846-9ea9-8ea43a182e0a
```

```text
Running 5s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.22ms    2.26ms  46.35ms   96.18%
    Req/Sec     3.90k   470.40     5.72k    90.59%
  Latency Distribution
     50%    1.90ms
     75%    2.70ms
     90%    3.54ms
     99%    6.46ms
  78413 requests in 5.10s, 21.99MB read
Requests/sec:  15373.71
Transfer/sec:      4.31MB
Business success: 78413; business errors: 0; success QPS: 15373.71; duration: 5.100460s
```
正式：

```bash
docker exec msgmate-idle50-load-1 /perf/wrk -t4 -c32 -d30s --timeout 5s --latency -s /perf/wrk.lua http://runtime:18081/msg/get_msg_record -- c4bd2258-a2c9-4846-9ea9-8ea43a182e0a
```

```text
Running 30s test @ http://runtime:18081/msg/get_msg_record
  4 threads and 32 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     2.17ms    1.11ms  12.60ms   70.17%
    Req/Sec     3.76k   232.98     4.42k    70.75%
  Latency Distribution
     50%    1.99ms
     75%    2.78ms
     90%    3.62ms
     99%    5.60ms
  448866 requests in 30.01s, 125.85MB read
Requests/sec:  14956.97
Transfer/sec:      4.19MB
Business success: 448866; business errors: 0; success QPS: 14956.97; duration: 30.010488s
```



## 开启消费者的模拟渠道全链路测试

测试日期：2026-10-11（Asia/Shanghai）。业务代码基线：`4137857`；仅新增隔离测试入口，没有修改生产消费参数。

### 机器状态与测试条件

- 宿主机 Intel Core i9-13900HX，32 逻辑 CPU，WSL2。WSL 可见内存 7783MiB，测试启动后快照 available 2717MiB、swap 已使用约 500MiB，load average 5.16/1.53/0.68。该快照包含测试依赖，并非全程资源记录。
- 其他 DevSupport、LearnQ、Ollama 等容器保持运行。独立 Compose 项目 `msgmate-e2e` 使用 MySQL 8.0、Redis 7.2.14、Kafka/Zookeeper 7.6.1；容器未单独设置 CPU/内存限额，共享约 7.6GiB 内存环境。无宿主端口映射，无业务数据目录挂载。
- 服务与负载发生器在同一运行容器内，通过私网 HTTP 访问真实路由；开启真实普通消费者与定时消费者，仅注册模拟邮件渠道。每次模拟发送等待 **10ms 后固定成功**，不使用渠道凭据、不调用 SMTP、飞书或短信。
- 开启模板/配额缓存，保留入口 Redis 限流、数据库事务、原有日志、最大/空闲连接 50/50 与连接寿命设置；Kafka 同步发布、acks=-1、batch_timeout_ms=20。Broker 单实例单副本。
- 全部请求为**低优先级、立即发送**。使用固定速率 Go HTTP 发生器，分别以 20/50/100 条每秒持续提交 30 秒，每 5 秒采样消息记录状态；HTTP 超时 5 秒，停止提交后最多等待 180 秒排空。每组单次样本；没有消费预热，包含初始消费者启动/分配过程。
- 每组重启服务、清空专属测试库中的消息与队列表、清空专属 Redis；Kafka 保留已消费完成的 offset。负载停止后先等排空，再开始下一组。

### 结果

| 模式 | 目标提交速率（条/s） | 成功受理数 | 提交结束时完成数 | 负载阶段平均完成速度（条/s） | 提交结束时积压 | 观测排空耗时（s） | 提交响应 P99（ms） |
|---|---:|---:|---:|---:|---:|---:|---:|
| MYSQL | 20 | 600 | 581 | 19.4 | 19 | 5.0 | 15.73 |
| MYSQL | 50 | 1500 | 624 | 20.8 | 876 | 45.0 | 11.90 |
| MYSQL | 100 | 3000 | 670 | 22.3 | 2330 | 120.1 | 10.87 |
| KAFKA | 20 | 600 | 599 | 19.9 | 1 | 5.0 | 35.81 |
| KAFKA | 50 | 1500 | 1499 | 49.9 | 1 | 5.0 | 32.14 |
| KAFKA | 100 | 3000 | 1701 | 56.7 | 1299 | 25.0 | 28.92 |

所有组请求发出数均等于成功受理数；业务错误和 HTTP 传输错误均为 0。最终**受理数 = 落库数 = status=2 数 = 模拟渠道调用数**，待处理及最终失败数均为 0。累计成功 10200 条，逐组计数对齐；没有逐条收件人送达验证，status=2 在此仅代表模拟渠道成功后状态落库。

平均完成速度按「提交结束时成功记录数 / 实际负载阶段耗时」计算，包含启动过程及尾部未完成任务，不等于稳态容量。排空耗时按每 5 秒轮询观测：5 秒意味着在下一次采样时已排空，不表示处理最后一条实际花了 5 秒。P99 是提交响应延迟，不是通知从提交到完成的端到端延迟。

### 判断

- **MySQL 低优先级队列：**20 条/s 时积压在本轮采样中保持较小；50、100 条/s 时积压随时间增长。两组超载情况下负载阶段完成速度约 21–22 条/s，停止提交后分别再花约 45、120 秒排空。说明原有入口上千 QPS 不能代表消费吞吐。
- **Kafka 低优先级队列：**20、50 条/s 时初始积压能消退；100 条/s 时持续积压。100 条/s 组在约第 10–30 秒完成速度约 62 条/s，提交结束时积压 1299 条，再花约 25 秒排空。当前样本支持 50 条/s 能追上、100 条/s 会积压，不能据此宣布稳定容量极限。
- **源码解释与后续方向：**MySQL 低优先级每批拉取 10 条、逐条发送，批间随机等待 1–500ms；Kafka 该优先级单个消费者顺序处理。10ms 模拟发送延迟之外，还包含数据库读写、状态事务、Redis 与日志等开销。这些机制与观测相符，但本轮未逐项隔离开销，不能给出精确因果占比。后续可对比 MySQL 批量/轮询间隔或受控消费并发，当前未调参。
- 本轮只验证成功路径下的积压与排空，不覆盖模拟失败重试、真实渠道耗时与限流、混合优先级、公平性、长时间稳定性或多实例。

### 原始证据与复跑

完整 JSONL：[MySQL 20](e2e/mysql-20.jsonl)、[MySQL 50](e2e/mysql-50.jsonl)、[MySQL 100](e2e/mysql-100.jsonl)、[Kafka 20](e2e/kafka-20.jsonl)、[Kafka 50](e2e/kafka-50.jsonl)、[Kafka 100](e2e/kafka-100.jsonl)。其中每条 sample/drain 都保存累计完成数与积压，final 保存业务计数和响应 P99。

测试入口：[scripts/perf-e2e/main.go](../../scripts/perf-e2e/main.go)；隔离依赖、测试配置、计数核对脚本及复跑步骤：[e2e/README.md](e2e/README.md)。本次实际使用等价的 `/tmp/msgmate-e2e/compose.yml` 配置运行，负载命令为 `docker exec msgmate-e2e-runtime-1 /work/bench -action load -rate <20|50|100> -seconds 30`。

验证与清理：`go test ./...`、`go vet ./scripts/perf-e2e`、测试入口静态构建及新增脚本语法检查通过；复跑 Compose 配置解析通过。测试专属容器、匿名数据卷与网络已删除，其他项目资源未调整。新增测试入口、配置、原始数据和本节留在工作区，未提交或推送。

## 消费模块一次性改造后的全链路对照

测试日期：2026-10-11（Asia/Shanghai），压测开始阶段约22:04。基于 `4137857` 上的消费改造（消费代码提交 `cce9497`，测试时尚未提交），消费参数及索引迁移见 [配置说明](../../config/README.md#消费并发与发送保护)。旧 JSONL 原样保留，新数据在 [e2e/optimized](e2e/optimized/)。

### 完成的修改与测试条件

- MySQL 普通队列忙时连续拉取、空闲50→500ms退避，低优先级批量10→30；普通优先级各4个worker，retry 1个并保留正常批间隔1000ms。处理前条件领取，退出时等待当前处理，未开始任务保持pending。
- Kafka 同分区小批次也可并发处理，每批最多4条、填充等待最多5ms；全部处理完成或可靠转入重试后提交整批offset。提交失败只重试提交；不扩分区、不重置offset，retry正常批间隔1000ms。
- 同一消息在进程内互斥；每个渠道跨优先级共用4个发送槽，发送速率可配置，本轮设0（不限速）。队列新增 `(status,create_time,id)` 索引，本轮使用包含该索引的新测试库。
- 其余仍为单实例 WSL2 / i9-13900HX / 32逻辑CPU、同机独立Docker MySQL 8.0 / Redis 7.2.14 / Kafka 7.6.1、10ms固定成功模拟渠道、低优先级立即发送、每秒20/50/100条目标速率、每组30秒、5秒采样。HTTP超时5秒，未做消费者预热，没有真实渠道调用。
- 缓存开启、数据库最大/空闲连接50/50、原有日志及事务、Kafka发布同步acks=-1且batch_timeout_ms=20均保留。构建与功能测试在正式压测前结束。
- WSL可见内存7783MiB；22:04阶段快照available 2287MiB、swap使用646MiB、load average 0.71/1.02/0.83。容器无独立CPU/内存限额，其他项目仍运行。与基线资源状态不同，因此只作历史条件对照，不作严格单变量归因。完整机器和构建信息见 [machine.json](e2e/optimized/machine.json)、[build.json](e2e/optimized/build.json)。

### 结果

| 模式 | 目标速率（条/s） | 发出/成功受理数 | 实际受理速度（条/s） | 提交结束时完成数 | 负载阶段平均完成速度（条/s） | 结束时积压 | 观测排空时间（s） | 提交P99（ms） |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| MYSQL | 20 | 600 | 19.99 | 598 | 19.92 | 2 | 5.0 | 15.71 |
| MYSQL | 50 | 1500 | 49.99 | 1498 | 49.92 | 2 | 5.0 | 9.43 |
| MYSQL | 100 | 3000 | 99.97 | 2996 | 99.84 | 4 | 5.0 | 9.98 |
| KAFKA | 20 | 600 | 19.98 | 599 | 19.94 | 1 | 5.0 | 36.69 |
| KAFKA | 50 | 1500 | 49.95 | 1499 | 49.92 | 1 | 5.0 | 32.55 |
| KAFKA | 100 | 2999 | 99.90 | 2996 | 99.80 | 3 | 5.0 | 29.68 |

六组共10199条，发出数、成功响应数、落库数、模拟渠道调用数和最终status=2数逐组相等；没有业务错误、传输错误、最终失败或遗留待处理。Kafka每秒100条组实际发出2999条而非3000条，固定速率计时器可能合并到期事件，不能把目标速率当成精确发出数量。

MySQL在100条/s组的5秒采样积压为3/3/4/2/3条，提交结束剩4条；Kafka在100条/s组为19/3/2/4/5条，初始积压消退、提交结束剩3条。两组均未观察到持续积压增长。与基线结束时MySQL积压2330、Kafka积压1299相比，改造后能追上本轮约100条/s的提交节奏。

排空时间仍按5秒轮询观测：表中5秒表示下一次采样已排空，不是尾部任务实际需要5秒。平均完成速度包含启动与末尾未完成任务；HTTP P99不是通知端到端延迟。低负载组的完成速度受提交速率限制，不能用表中约100条/s宣布消费容量上限。未进一步搜索更高负载或验证长时间稳定性。

### 验证、迁移与结论

- `go test ./...`、`go test -race ./...`、`go vet ./...`、主程序构建通过。
- 真实隔离MySQL/Redis/Kafka上的功能测试和race测试通过，包括失败计数阈值、定时补扫、坏参数、并发领取与终态重放、停止后未开始任务保持pending、恢复后完成。为了适配Alpine容器，race集成二进制使用 `-tags 'integration netgo osusergo' -ldflags '-linkmode external -extldflags -static'` 构建；不是生产二进制或性能负载二进制。
- Kafka乱序完成不得越过未完成消息提交、处理取消不提交、提交失败不重做渠道调用，由单元测试验证；集成使用真实Broker验证成功和重试，没有模拟Broker网络故障。证据：[functional-race.log](e2e/optimized/functional-race.log)、[concurrent-claim-race.log](e2e/optimized/concurrent-claim-race.log)。后者使用两个不共享消息互斥槽的消费者验证数据库条件领取。
- 新库四张队列表索引已核对；测试结束后在专属测试库删除新增索引，执行 [旧库迁移SQL](../../sql/migrations/20261011_consumer_indexes.sql)，再次核对四张表索引列均为status/create_time/id。现有业务库未迁移，旧库部署前需检查索引并按一次性脚本添加；不要重复执行已有索引的迁移。

**本轮结论：消费改造消除了原测试中50/100条每秒下的持续积压；两种模式在当前10ms模拟渠道、低优先级、30秒单次条件下均能追上约100条每秒。** 最终计数保持一致，功能和race检查通过。这不代表真实渠道容量、最高吞吐或生产SLA；所有改动一起对照，不能精确分离并发、轮询、批量和索引各自贡献。

复跑使用相同脚本：`python3 reports/performance/e2e/run.py --output-dir /tmp/msgmate-e2e/optimized`。不覆盖旧基线，不调整私有配置。

本轮收尾：专属测试容器、匿名卷、网络、临时配置、二进制和服务日志已清理，原始JSONL与验证证据保留在仓库；其他项目资源未调整。消费代码、测试工具与本报告按内容分批发布。
