# MsgMate API 接入指南

本文说明六个 HTTP 接口及业务接入规则。默认示例地址 `http://localhost:8081`，以配置端口为准。

## 接入约定

业务通过 HTTP 调用独立服务。请求使用 `Content-Type: application/json`，用 `Source-Id` 标识业务来源；与模板 `sourceID` 保持一致，例如 `learnq`。该请求头不是鉴权凭证；发送与记录查询检查来源一致性，不能替代调用方认证。

| 字段 | 约定 |
|---|---|
| `channel` | 邮件 `1`，飞书 `3`；短信 `2` 保留适配代码，不在本文演示 |
| `to` | 邮件填邮箱地址，飞书填应用可触达用户的 `open_id` |
| `priority` | 低 `1`、中 `2`、高 `3`；省略或 `0` 默认低；`4` 留给内部重试 |
| `templateData` | 字符串映射，正文使用 Go `text/template`，例如 `{{.title}}` |
| `sendTimestamp` | Unix 秒级时间戳；即时发送省略，定时发送填正数未来时间 |

模板渠道决定发送渠道；每个场景、每个渠道创建独立模板。模板主题为静态内容，只有正文进行参数渲染。发送请求中的 `subject` 不覆盖实际发送使用的模板主题。

所有 Handler 使用 HTTP 200 输出 JSON，必须检查 `code`。通用响应为 `{"code":0,"msg":"ok"}`。内部错误映射为非零业务码；`code=0` 表示接口操作成功，不表示用户已收到。

## 准备数据库和配额

按 [README](README.md) 启动依赖并首次导入 `sql/msgcenter.sql`。建表脚本已包含邮件、短信、飞书各每秒 1 次的默认入口配额，不要重复导入。

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

新模板的数据库状态为待审核 `1`，不能直接发送。没有审核 API，需管理员在数据库启用：

```sql
UPDATE t_msg_template SET status = 2 WHERE template_id = '<templateID>';
SELECT template_id, status FROM t_msg_template WHERE template_id = '<templateID>';
```

若已缓存待审核模板，启用后等待 30 秒或由管理员清理对应模板缓存。飞书模板使用 `channel=3`，其余流程一致；飞书应用由服务进程的 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 环境变量指定。

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

必须指定 `templateID`。省略或为空的字段保持原值，非空字段更新；不提供将正文清空的操作：

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

定时接口成功表示 MySQL 任务和待处理记录已提交；Redis 索引失败会由数据库补扫恢复，不是准时送达保证；没有取消、修改定时任务或周期性调度 API。

### 5. 查询记录：GET /msg/get_msg_record

必填 query：`msgID`。

```bash
curl 'http://localhost:8081/msg/get_msg_record?msgID=<msgID>' -H 'Source-Id: learnq'
```

响应结构包含以下字段：

```json
{"code":0,"msg":"ok","to":"<your-email>","subject":"学习提醒","templateID":"<templateID>","templateData":{"name":"同学","title":"Go Channel"},"status":1,"retryCount":0}
```

记录提交后即可查询。`status` 为待处理 `1`、渠道接受 `2`、最终失败 `3`；`retryCount` 为失败次数。接口直接读取 MySQL，并检查请求来源与记录来源一致。渠道接受不等于用户收件箱已收到。

### 6. 删除模板：POST /msg/del_template

```bash
curl -X POST http://localhost:8081/msg/del_template \
  -H 'Content-Type: application/json' -H 'Source-Id: learnq' \
  -d '{"templateID":"<templateID>"}'
```

成功响应为通用响应。仅在没有待处理消息依赖时删除；接口不检查在途消息依赖，更新和删除会清除模板缓存。

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

## 场景接入

| 场景 | 正文示例 | 业务提交参数 |
|---|---|---|
| 学习提醒 | `{{.name}}，请复习：{{.title}}` | `name`、`title`，可附 `sendTimestamp` |
| 任务完成 | `任务 {{.taskName}} 已完成：{{.result}}` | `taskName`、`result` |
| 报告生成 | `报告 {{.reportName}} 已生成：{{.url}}` | `reportName`、`url` |

业务负责提醒规则、完成事件、报告生成和链接权限；本服务只消费通知请求。同一模板参数在邮件/飞书各自模板中渲染，切换渠道需使用对应模板 ID。

## 错误码与边界

| code | 含义 |
|---|---|
| 0 | Handler 返回成功；不等于用户收到 |
| 8020 | 输入无效 |
| 8021 | 请求绑定失败 |
| 8022 | JSON 序列化失败 |
| 8023 | 内部错误，包括记录不存在或数据库操作失败 |
| 8035 | 模板不存在、不可用或尚未启用 |
| 8036 | 投递或入口限流操作错误 |
| 8037 | 普通队列插入失败 |
| 8038 | 超过入口配额 |
| 8047 | 定时任务或 Redis 索引写入失败 |

错误响应也可能包含空的结果字段。仅在 `code=0` 时使用结果；参数错误应修正输入，模板不可用应核对来源与启用状态，限流应等待窗口结束。内部错误请携带脱敏信息向维护者反馈，不能将失败响应中的 `msgID` 视为提交成功证明。

## 接入边界与反馈

发送和记录查询检查来源一致性；模板创建、查询、更新、删除不按请求头隔离来源。服务没有身份认证，应在可信网络内由可信调用方使用。

记录查询直接读取 MySQL，提供处理状态与失败次数。服务不提供提交幂等、批量发送、投递回调、模板审核 API 或定时任务取消/修改 API。可靠性与运行规则见 [README](README.md#接入与项目规则)，问题反馈与联系维护者见 [Issues 指引](README.md#联系与-issues)。

短信使用需要具备阿里云短信服务的账号、签名与模板权限，外部模板 ID 通过管理员配置 `t_msg_template.rel_template_id`。邮件和短信凭据从 TOML 加载，飞书凭据从服务进程环境变量加载；切勿在请求示例或 Issue 中公开凭据。
