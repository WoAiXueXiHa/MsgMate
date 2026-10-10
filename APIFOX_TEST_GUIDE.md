# Apifox 功能测试指南

本文整理 2026-10-10 的手动测试流程。服务在 WSL 中运行，Apifox 使用 Windows 桌面客户端；本轮使用 MySQL 队列、模板缓存和真实邮件渠道。

## 1. 测试目标与范围

业务链路：创建模板 → 查询与修改 → 管理员启用 → 提交通知 → 查询处理状态 → 核对实际收件。

本轮用户反馈和响应已验证：

- 创建模板、查询模板、部分更新正文。
- 即时邮件实际收件；未记录对应消息状态查询的最终响应。
- 定时邮件到期前状态为 1，到期后为 2，实际收件。
- 消息查询来源不匹配返回 8020，敏感结果字段为空。
- 非法优先级返回 8020，发送来源不匹配返回 8035。
- 两次并发提交分别返回 0 和 8038，入口限流生效。
- 删除后的模板查询返回 8023；聊天中未提供删除接口自身的成功响应，完整删除验收需补记。

本轮未验证 Kafka 模式、故障恢复、失败重试阈值、飞书、短信或性能容量。本文记录手动结果，不等同于自动化回归全部通过。

## 2. 启动与连接检查

所有 Linux 命令在仓库根目录执行：

```bash
cd ~/workspace/msg
docker compose up -d mysql redis
docker compose ps
make run
```

保留 Go 服务终端，不要关闭。已有数据库不要重新导入建表脚本。可检查表是否存在：

```bash
docker compose exec mysql mysql -u root -p msgcenter_db -e 'SHOW TABLES;'
```

本轮 HTTP 端口为 18109，`mysql_as_mq=true`，`open_cache=true`。实际地址以本地配置为准；不要复制或分享整份含凭据的配置。

在 WSL 另一个终端检查 HTTP 连接：

```bash
curl --noproxy '*' --max-time 5 \
  'http://127.0.0.1:18109/msg/get_template?templateID=connection-check'
```

在 Windows PowerShell 中检查同一路径：

```powershell
curl.exe --noproxy "*" --max-time 5 "http://127.0.0.1:18109/msg/get_template?templateID=connection-check"
```

收到 JSON 就证明 HTTP 可访问。不存在的模板当前返回 `code=8023`，这不是网络连接错误。

## 3. Apifox 公共设置

前置 URL：

```text
http://127.0.0.1:18109
```

公共 Headers：

| 名称 | 值 |
|---|---|
| Content-Type | application/json |
| Source-Id | apifox-test |

POST 请求的 Body 选择 JSON；GET 不填写 Body。路径必须以 `/` 开头，最终地址例如 `http://127.0.0.1:18109/msg/create_template`。

所有业务 Handler 使用 HTTP 200 返回结果，必须检查 JSON `code`。`code=0` 表示当前接口操作成功，不代表邮件已收到。`Source-Id` 是来源标识，不是认证凭证；模板管理接口没有按请求头隔离来源。

以下 `<templateID>`、`<msgID>` 等都是占位符，发送前替换。建议为每个请求保存独立名称，并记录本轮创建的 ID，不要混用历史 ID。

## 4. 正常流程

### 01：创建邮件模板

```text
POST /msg/create_template
```

```json
{
  "sourceID": "apifox-test",
  "name": "apifox-email-test",
  "subject": "MsgMate 功能测试",
  "channel": 1,
  "content": "{{.name}}，这是一条测试通知：{{.title}}"
}
```

预期 `code=0`，返回非空 `templateID`。保存 ID。创建模板不会发送邮件，新模板状态为待审核 1。

### 02：查询模板

```text
GET /msg/get_template?templateID=<templateID>
```

预期 `code=0`，`sourceID`、`name`、`subject`、`channel` 和 `content` 与创建时一致。此接口不返回模板状态，查询成功不代表可以发送。

### 03：部分更新模板

```text
POST /msg/update_template
```

```json
{
  "templateID": "<templateID>",
  "content": "{{.name}}，修改后的测试通知：{{.title}}"
}
```

预期 `code=0`。再次查询：正文更新，其他字段保持原值。没有在途消息时再修改模板，当前实现没有模板版本隔离。

### 04：管理员启用模板

没有启用 API，需要数据库操作。下面两个 ID 必须替换为同一个新模板 ID：

```bash
docker compose exec mysql mysql -u root -p msgcenter_db \
  -e "UPDATE t_msg_template SET status=2 WHERE template_id='<templateID>'; SELECT template_id,status,source_id FROM t_msg_template WHERE template_id='<templateID>';"
```

确认 `status=2`、`source_id=apifox-test`。开启缓存时，启用后等待 30 秒，避免读到已缓存的待审核状态。

### 05：即时发送

```text
POST /msg/send_msg
```

```json
{
  "to": "自己的测试邮箱",
  "templateID": "<templateID>",
  "priority": 2,
  "templateData": {
    "name": "测试同学",
    "title": "即时发送验证"
  }
}
```

邮件账号和 SMTP 授权码需在本地正确配置。替换为自己的邮箱，只发送一次。预期 `code=0`，返回非空 `msgID`，保存为即时消息 ID。

### 06：查询消息与核对收件

```text
GET /msg/get_msg_record?msgID=<即时消息ID>
```

必须携带 `Source-Id: apifox-test`。

| 字段 | 含义与预期 |
|---|---|
| code | 成功查询为 0 |
| status | 1 待处理，2 渠道接受，3 最终失败 |
| retryCount | 失败次数；正常情况下为 0 |
| templateID | 与本次发送一致 |
| templateData | 与发送参数一致 |

最终状态预期为 2。另行检查实际收到的邮件：主题为“MsgMate 功能测试”，正文为“测试同学，修改后的测试通知：即时发送验证”。渠道接受与实际收件需要分别记录。

### 07：定时发送

在 WSL 生成两分钟后的 Unix 秒时间戳：

```bash
date -d '+2 minutes' +%s
```

使用 `POST /msg/send_msg`，Body 为：

```json
{
  "to": "自己的测试邮箱",
  "templateID": "<templateID>",
  "priority": 2,
  "templateData": {
    "name": "测试同学",
    "title": "定时发送验证"
  },
  "sendTimestamp": 2000000000
}
```

`2000000000` 仅为占位数字，必须换成刚生成的未来时间戳。使用秒，不是毫秒，并在时间到期前提交。

保存新的定时消息 ID，通过记录查询接口检查：到期前状态为 1，到期后最终为 2，实际收到包含“定时发送验证”的邮件。允许调度和渠道传输延迟，不是准时送达保证。

## 5. 异常流程

| 请求场景 | 修改方式 | 预期 |
|---|---|---|
| 查询记录来源不匹配 | 保持有效 msgID，Header 改为 `Source-Id: other-source` | code=8020，收件人和模板数据为空 |
| 非法优先级 | 发送 Body 的 priority 改为 4，其他字段有效 | code=8020，msgID 为空 |
| 发送来源不匹配 | priority=2，Header 改为 `Source-Id: other-source` | code=8035，msgID 为空 |
| 模板尚未启用 | 使用状态为 1 的新模板发送 | code=8035 |

每次只改变一个条件，以便确定错误原因。测试后恢复 Header 和合法优先级。`8035` 的提示文字较笼统，来源不匹配也会触发它，不能仅根据提示认定模板未启用。

## 6. 入口限流测试

先检查实际配额：

```bash
docker compose exec mysql mysql -u root -p msgcenter_db \
  -e "SELECT channel,num,unit FROM t_global_quota WHERE channel=1; SELECT source_id,channel,num,unit FROM t_source_quota WHERE source_id='apifox-test' AND channel=1;"
```

本轮邮件默认配额 `num=1`、`unit=1000`，无该来源专属配额，即同一来源邮件渠道每个一秒固定窗口允许一次提交。来源配额若存在则覆盖默认值。

手动连续点击可能跨窗口。以下 WSL 并发请求用于辅助验证；需先准备已启用的测试模板，替换 ID。正常情况下会发送一封邮件，跨窗口时可能发送两封，不要反复执行：

```bash
read -r -p '请输入自己的测试邮箱：' TEST_EMAIL
read -r -p '请输入已启用的测试模板ID：' TEST_TEMPLATE_ID

for i in 1 2; do
  curl --noproxy '*' -sS --max-time 10 \
    'http://127.0.0.1:18109/msg/send_msg' \
    -H 'Content-Type: application/json' \
    -H 'Source-Id: apifox-test' \
    --data-binary "{\"to\":\"$TEST_EMAIL\",\"templateID\":\"$TEST_TEMPLATE_ID\",\"priority\":2,\"templateData\":{\"name\":\"限流测试\",\"title\":\"入口限流验证\"}}" \
    > "/tmp/msgmate-quota-$i.json" &
done
wait

cat /tmp/msgmate-quota-1.json
echo
cat /tmp/msgmate-quota-2.json
echo
```

本轮实际结果：一次 `code=0` 且 msgID 非空，另一次 `code=8038` 且 msgID 为空。若两次都成功，可能跨过固定窗口边界，不能仅凭这一组结果判断限流失效。

入口配额不等于实际渠道调用速率限制；定时到期和内部重试不受这个入口窗口控制。

## 7. 删除测试模板

完成所有发送测试，并确认没有待处理依赖后，最后删除专用测试模板。不要删除仍要使用的祝福模板。

```text
POST /msg/del_template
```

```json
{
  "templateID": "<templateID>"
}
```

预期删除响应 `code=0`。再查询同一个 ID，预期非零业务码，当前实现为 8023，原内容为空。需要同时保存删除和查询两份响应，才构成完整验收记录。

## 8. 本轮遇到的问题

| 现象 | 定位与处理 |
|---|---|
| 容器内能查表，Go 连数据库失败 | 宿主机 mysqld 占用 IPv4 3306；IPv6 TCP 检查能访问 Docker MySQL，本轮 MySQL URL 使用 `[::1]:3306`。这是本机环境处理，不是所有部署都需要这样设置 |
| IPv6 HTTP 可访问、IPv4 不通 | 当前源码改为 `net.Listen("tcp4", "0.0.0.0:<配置端口>")`，明确监听 IPv4，重启后验证 |
| Apifox 连接错误端口或示例域名 | 核对最终请求 URL，使用实际端口 18109；Windows curl 能访问后再检查 Apifox 地址与代理 |
| POST 没响应或地址错误 | 本轮曾写成 `18109msg/create_template`，缺少 `/`，且 Body 为空；改为正确地址并填写 JSON |
| 查询返回 HTTP 404 | 核对 GET 方法、完整路径和端口；消息不存在通常是 HTTP 200 加非零业务 code |
| 新模板提示未准备好 | 检查来源、数据库 status=2 和缓存；查询模板成功不代表已经启用 |
| 启用后仍显示 status=1 | UPDATE 与 SELECT 使用了不同 ID；两条语句应使用同一个模板 ID |

监听 `0.0.0.0` 允许其他可达设备访问，客户端应使用 `127.0.0.1` 或机器实际 IP。当前服务没有身份认证，应按项目约定在可信环境中使用。

## 9. 测试记录建议

每次重跑记录：提交号、队列模式、缓存开关、模板 ID、每次消息 ID、请求条件、预期业务码、实际响应、到期前后状态和实际收件结果。导出或分享记录前移除凭据、收件人和私人正文。

客户端超时后不要盲目重复提交，服务没有提交幂等保证。已公开的 SMTP 授权码应在邮箱后台撤销并重新配置，文档不记录凭据。

参数与业务边界参见 [API.md](API.md)，启动说明参见 [README.md](README.md)，配置说明参见 [config/README.md](config/README.md)。直接 wrk 测试命令、完整输出与结论见 [reports/performance/RESULTS.md](reports/performance/RESULTS.md)；压测使用独立依赖、关闭消费者，比较缓存开关下的发送受理与消息记录查询。
