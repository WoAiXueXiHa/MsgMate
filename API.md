# MsgMate API 接入指南

本文按当前源码整理，curl、SQL 和 Go 示例未做真实发送验证。沿用现有接口，不新增 API。默认示例地址 `http://localhost:8081`，以配置端口为准。

## 接入约定

业务通过 HTTP 调用独立服务。请求使用 `Content-Type: application/json`，用 `Source-Id` 标识业务来源；与模板 `sourceID` 保持一致，例如 `learnq`。该请求头不是鉴权凭证，接口没有校验调用方身份或模板所有权。

| 字段 | 约定 |
|---|---|
| `channel` | 邮件 `1`，飞书 `3`；短信 `2` 保留适配代码，不在本文演示 |
| `to` | 邮件填邮箱地址，飞书填应用可触达用户的 `open_id` |
| `priority` | 低 `1`、中 `2`、高 `3`；省略或 `0` 默认低；`4` 留给内部重试 |
| `templateData` | 字符串映射，正文使用 Go `text/template`，例如 `{{.title}}` |
| `sendTimestamp` | Unix 秒级时间戳；即时发送省略，定时发送填正数未来时间 |

模板渠道决定发送渠道；每个场景、每个渠道创建独立模板。模板主题为静态内容，只有正文进行参数渲染。发送请求中的 `subject` 不覆盖实际发送使用的模板主题。

所有 Handler 使用 HTTP 200 输出 JSON，必须检查 `code`。通用响应为 `{"code":0,"msg":"ok"}`。由于部分 Handler 未将内部错误映射为业务错误码，`code=0` 也不是全链路可靠性证明；尤其查询记录不应被解释为投递成功。

## 准备数据库和配额

按 [README](../README.md) 启动依赖并首次导入 `sql/msgcenter.sql`。建表脚本已包含邮件、短信、飞书各每秒 1 次的默认入口配额，不要重复导入。

确认配额：

```sql
SELECT channel, num, unit FROM t_global_quota;
SELECT source_id, channel, num, unit FROM t_source_quota WHERE source_id = 'learnq';
```

`unit` 为毫秒，必须为正数；示例采用 1000 毫秒。渠道全局配额必须存在，业务配额只在其基础上覆盖默认值，没有同时扣减两个层级。以下是本地开发准备示例，不自动执行；只在没有对应记录时插入，避免重复配置：

```sql
INSERT INTO t_global_quota (num, unit, channel)
SELECT 1, 1000, 1 WHERE NOT EXISTS (SELECT 1 FROM t_global_quota WHERE channel = 1);
INSERT INTO t_global_quota (num, unit, channel)
SELECT 1, 1000, 3 WHERE NOT EXISTS (SELECT 1 FROM t_global_quota WHERE channel = 3);
INSERT INTO t_source_quota (source_id, num, unit, channel)
SELECT 'learnq', 1, 1000, 1 WHERE NOT EXISTS
  (SELECT 1 FROM t_source_quota WHERE source_id = 'learnq' AND channel = 1);
```

即时提交与定时提交使用不同计数 key。开启缓存时，配额修改可能需等待缓存过期（30 秒）。入口配额不限制实际渠道发送速率。

## 六个接口

### 1. 创建模板：POST /msg/create_template

调用方应提供 `sourceID`、`name`、`subject`、`channel`、`content`。`signName` 是短信字段，邮件和飞书省略。

```bash
curl -X POST http://localhost:8081/msg/create_template \
  -H 'Content-Type: application/json' -H 'Source-Id: learnq' \
  -d '{"sourceID":"learnq","name":"learning-reminder-email","subject":"学习提醒","channel":1,"content":"{{.name}}，请复习：{{.title}}"}'
```

成功响应形状：

```json
{"code":0,"msg":"ok","templateID":"<templateID>"}
```

新模板的数据库状态为待审核 `1`，不能直接发送。当前无审核 API，需管理员在数据库启用：

```sql
UPDATE t_msg_template SET status = 2 WHERE template_id = '<templateID>';
SELECT template_id, status FROM t_msg_template WHERE template_id = '<templateID>';
```

如曾缓存待审核模板，启用后等待 30 秒或由管理员清理对应模板缓存。飞书模板使用 `channel=3`，其余流程一致；飞书应用由服务进程的 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 环境变量指定。

### 2. 查询模板：GET /msg/get_template

必填 query：`templateID`。

```bash
curl 'http://localhost:8081/msg/get_template?templateID=<templateID>' -H 'Source-Id: learnq'
```

响应包含 `code`、`msg`、`relTemplateID`、`sourceID`、`signName`、`name`、`subject`、`channel`、`content`，没有模板 `status`：

```json
{"code":0,"msg":"ok","relTemplateID":"","sourceID":"learnq","signName":"","name":"learning-reminder-email","subject":"学习提醒","channel":1,"content":"{{.name}}，请复习：{{.title}}"}
```

### 3. 更新模板：POST /msg/update_template

必须指定 `templateID`。虽然请求结构允许省略其他字段，当前实现会覆盖正文、主题和渠道；使用完整字段更新，不按 PATCH 使用：

```bash
curl -X POST http://localhost:8081/msg/update_template \
  -H 'Content-Type: application/json' -H 'Source-Id: learnq' \
  -d '{"templateID":"<templateID>","sourceID":"learnq","name":"learning-reminder-email","subject":"学习提醒","channel":1,"content":"{{.name}}，今天复习：{{.title}}"}'
```

成功响应为通用响应。接口不支持启用模板，也不提供版本隔离；修改模板可能影响尚未消费的消息。

### 4. 提交通知：POST /msg/send_msg

| 字段 | 类型 | 使用要求 |
|---|---|---|
| `to` | string | 必填，渠道接收人 |
| `templateID` | string | 必填，已启用模板 ID |
| `templateData` | object<string,string> | 必填，无变量时传 `{}` |
| `priority` | integer | 可选，业务使用 `0/1/2/3` |
| `subject` | string | 可选，不覆盖实际发送主题 |
| `sendTimestamp` | int64 | 可选，Unix 秒级时间戳 |

```bash
curl -X POST http://localhost:8081/msg/send_msg \
  -H 'Content-Type: application/json' -H 'Source-Id: learnq' \
  -d '{"to":"recipient@example.com","templateID":"<templateID>","priority":2,"templateData":{"name":"同学","title":"Go Channel"}}'
```

成功响应：

```json
{"code":0,"msg":"ok","msgID":"<msgID>"}
```

保存 `msgID` 关联业务事件。它由服务端生成，不是调用方提交幂等键。客户端超时可能发生在服务端已提交之后，不能盲目重复提交。

定时发送示例（Linux shell 中计算一分钟后的 Unix 秒）：

```bash
SEND_AT=$(( $(date +%s) + 60 ))
curl -X POST http://localhost:8081/msg/send_msg \
  -H 'Content-Type: application/json' -H 'Source-Id: learnq' \
  -d "{\"to\":\"recipient@example.com\",\"templateID\":\"<templateID>\",\"priority\":2,\"templateData\":{\"name\":\"同学\",\"title\":\"Go Channel\"},\"sendTimestamp\":$SEND_AT}"
```

定时接口成功表示 MySQL 任务和 Redis 时间项写入步骤返回成功，不是准时送达保证；没有取消、修改定时任务或周期性调度 API。

### 5. 查询记录：GET /msg/get_msg_record

必填 query：`msgID`。

```bash
curl 'http://localhost:8081/msg/get_msg_record?msgID=<msgID>' -H 'Source-Id: learnq'
```

当前响应结构包含以下字段：

```json
{"code":0,"msg":"ok","to":"","subject":"学习提醒","templateID":"<templateID>","templateData":{"name":"同学","title":"Go Channel"}}
```

当前 Handler 未填充 `to`，所以为空；没有返回 `status`、`retry_count`。不要轮询此接口判断投递成功。定时任务在转入普通投递链路前尚未创建普通消息记录，立即查询可能找不到；查不到记录也不能据此判断发送失败。

### 6. 删除模板：POST /msg/del_template

```bash
curl -X POST http://localhost:8081/msg/del_template \
  -H 'Content-Type: application/json' -H 'Source-Id: learnq' \
  -d '{"templateID":"<templateID>"}'
```

成功响应为通用响应。仅在没有待处理消息依赖时删除；接口不检查在途消息依赖，也不保证同步清除模板缓存。

## 业务侧 Go 调用示例

下面可保存为独立 `main.go`，用环境变量填写服务地址、模板和接收人；只提交一次，不自动重试。不需要导入 MsgMate 的模块。

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strings"
    "time"
)

func submit(ctx context.Context, baseURL, templateID, to string) (string, error) {
    payload := map[string]interface{}{
        "to": to, "templateID": templateID, "priority": 2,
        "templateData": map[string]string{"name": "同学", "title": "Go Channel"},
    }
    body, err := json.Marshal(payload)
    if err != nil { return "", err }
    req, err := http.NewRequestWithContext(ctx, http.MethodPost,
        strings.TrimRight(baseURL, "/")+"/msg/send_msg", bytes.NewReader(body))
    if err != nil { return "", err }
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Source-Id", "learnq")
    client := &http.Client{Timeout: 5 * time.Second}
    resp, err := client.Do(req)
    if err != nil { return "", err }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return "", fmt.Errorf("HTTP status: %d", resp.StatusCode)
    }
    // 指针用于区分缺失 code 与成功码 0。
    var result struct {
        Code *int `json:"code"`
        Msg string `json:"msg"`
        MsgID string `json:"msgID"`
    }
    if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
        return "", err
    }
    if result.Code == nil { return "", fmt.Errorf("missing code") }
    if *result.Code != 0 { return "", fmt.Errorf("code=%d: %s", *result.Code, result.Msg) }
    if result.MsgID == "" { return "", fmt.Errorf("missing msgID") }
    return result.MsgID, nil
}

func main() {
    baseURL := os.Getenv("MSGMATE_URL")
    templateID := os.Getenv("MSGMATE_TEMPLATE_ID")
    to := os.Getenv("MSGMATE_TO")
    if baseURL == "" || templateID == "" || to == "" {
        fmt.Fprintln(os.Stderr, "set MSGMATE_URL, MSGMATE_TEMPLATE_ID, MSGMATE_TO")
        os.Exit(1)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    msgID, err := submit(ctx, baseURL, templateID, to)
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    fmt.Println("submitted msgID:", msgID)
}
```

业务集成时在自身事务完成后调用，并保存 `msgID`；业务事件与通知提交没有跨服务事务保证。若需要事务级事件可靠性，应另行设计业务 Outbox，本项目未提供。

## 后续场景接入

| 场景 | 正文示例 | 业务提交参数 |
|---|---|---|
| 学习提醒 | `{{.name}}，请复习：{{.title}}` | `name`、`title`，可附 `sendTimestamp` |
| 任务完成 | `任务 {{.taskName}} 已完成：{{.result}}` | `taskName`、`result` |
| 报告生成 | `报告 {{.reportName}} 已生成：{{.url}}` | `reportName`、`url` |

以上是未来接入方式，不代表已与 LearnQ 集成。业务负责提醒规则、完成事件、报告生成和链接权限；本服务只消费通知请求。同一模板参数在邮件/飞书各自模板中渲染，切换渠道需使用对应模板 ID。

## 错误码与边界

| code | 含义 |
|---|---|
| 0 | Handler 返回成功；不等于用户收到 |
| 8020 | 输入无效 |
| 8021 | 请求绑定失败 |
| 8022 | JSON 序列化失败 |
| 8023 | 内部错误；当前可能返回 unknown error code 文案 |
| 8035 | 模板不存在、不可用或尚未启用 |
| 8036 | 投递或入口限流操作错误 |
| 8037 | 普通队列插入失败 |
| 8038 | 超过入口配额 |
| 8047 | 定时任务或 Redis 索引写入失败 |

部分输入检查设置错误码后返回 nil，公共 Handler 仍继续执行；部分查询/更新错误没有被正确映射到响应。调用方应按以上约定提交完整合法数据，不将该列表当作全面且严格的错误契约。

完整可靠性边界见 [README](../README.md#实现边界)。本次文档未修复这些代码问题；接口没有提供认证、发送幂等、可靠状态查询、批量发送或投递回调。

注意：当前短信业务个人无法注册，只有通过企业认证才可使用短信业务。
