# WebSupervisor-microservice-v2

基于 **Redis Streams** 的 Go 微服务框架与示例集合。所有服务之间不直接通信，而是通过 Redis Stream 传递消息实现"请求-响应"式 RPC；`MyTool/stream` 库封装了全部通信细节，让一个微服务只需要"写 handler + 注册"即可运行。

## 目录结构

```
WebSupervisor-microservice-v2/
├── MyTool/                        # 公共工具库（module: github.com/totooicu/go-mytool）
│   ├── stream/                    # ★ 微服务通信库（本文档主角）
│   ├── streamtool/                # 早期版本通信工具包（已由 stream/ 取代，保留参考）
│   ├── streams-manager/           # Redis Stream 命令行管理/调试工具（见其 README）
│   ├── http/                      # HTTP 客户端与 HTML/XPath/JSON 解析器
│   ├── redis/ config/ email/ file/ json/ string/ array/ set/ sync/ encryption/ response/
├── microservice/                  # 示例微服务
│   ├── calculator-service/        # 计算服务（最小示例）
│   ├── crawler-service/           # HTTP 抓取服务
│   ├── parser-service/            # HTML/XPath/JSON 解析服务
│   ├── redis_cache-service/       # 缓存与变更比较服务
│   ├── email-service/             # SMTP 邮件服务
│   ├── web_supervisor-manager/    # 网页监控编排器（jobs.json 驱动）
│   ├── microservice-manager/      # 调用各服务的示例客户端
│   ├── build_web_supervisor.bat   # 一键交叉编译 5 个服务
│   └── run_web_supervisor.bat     # 一键启动 5 个服务
├── go.work                        # Go workspace，聚合所有 module
└── go.mod
```

## 整体架构

```
        ┌─────────────────────────────┐
        │  web_supervisor-manager      │ 编排器：定时执行 jobs.json 监控任务
        │  microservice-manager        │ 示例客户端：演示如何调用各服务
        └──────────────┬──────────────┘
                       │ stream.Send(targetStream, service, payload, timeout)
                       ▼
              ┌─────────────────┐
              │  Redis Streams  │   所有消息（请求/响应）都经 Redis 中转
              └─────────────────┘
   ┌───────────┬───────────┬──────────────┬────────────┬─────────────┐
   ▼           ▼           ▼              ▼            ▼             ▼
crawler-    parser-     redis_cache-    email-      calculator-    （任意新增的
service     service      service        service      service        服务...）
```

一次调用的完整链路：

1. 客户端 `stream.Send(targetStream, serviceName, payload, timeoutMs)`：生成 `message_id`，把 `StreamMessage` 写入目标服务的 Stream，然后阻塞等待响应。
2. 目标服务的 `consumeLoop` 从自己的 Stream（`consumer_stream`）以消费者组（`consumer_group`）读取消息，按 `service_name` 分发到 `RegisterService` 注册的 handler。
3. handler 处理完调用 `stream.ResponseSucc / ResponseErr`，响应消息写回请求方的 `callback_stream`，`service_name` 固定为 `"response"`，`reply_id` 为请求的 `message_id`。
4. 客户端收到响应后从等待队列中取出并返回；消息处理完立即 `XAck + XDel`。

### 关键机制

| 机制 | 说明 |
|---|---|
| 请求-响应 RPC | 每条请求带唯一 `message_id` 和 `callback_stream`，响应通过 `reply_id` 关联 |
| 大消息自动分片 | 消息超过 `max_message_bytes` 自动按字节分片发送，接收端（`ShardManager`）重组，请求与响应均支持 |
| Deadline 超时丢弃 | 消息携带绝对时间戳（毫秒），消费端发现已过期直接 `Ack+Del` 丢弃，避免处理过期任务 |
| 并发控制 | `goroutine_num` 作为信号量限制同时执行的 handler 数量；`ping` 不占信号量 |
| 至少一次消费 | 消费者组 + 处理完 `XAck`/`XDel`；进程崩溃未确认的消息会留在 PEL 中 |
| 环境变量注入 | 配置文件中任意字符串支持 `${ENV_NAME}` 展开（如 `${QQ_MAIL_PASSWORD_1134}`） |
| 内置服务 | 每个服务自动注册 `ping`（返回协程状态）与 `response`（内部响应分发），可用于健康检查 |

## 如何用 stream 库构建一个微服务

stream 库位于 `MyTool/stream`（module `github.com/totooicu/go-mytool/stream`）。新建一个微服务只需三步：

### 第 1 步：建立 module 并引入库

在 `go.work` 中 `use` 你的新目录，然后在该目录 `go mod init` 并 `go mod tidy`（依赖 `github.com/totooicu/go-mytool`）。

### 第 2 步：编写 config.json

```json
{
  "redis": {
    "addr": "localhost:6379",
    "password": "",
    "db": 0
  },
  "stream": {
    "consumer_stream": "dev:my-stream",
    "consumer_group": "my-group",
    "goroutine_num": 5,
    "get_timeout_ms": 5000,
    "max_message_bytes": 204800,
    "cache_key_prefix": "my:"
  },
  "custom": {
    "any_business_field": "自由配置，代码里用 stream.Custom[\"any_business_field\"] 读取"
  }
}
```

#### 通用字段解释

**`redis`（通信使用的 Redis 实例）**

| 字段 | 说明 |
|---|---|
| `addr` | Redis 地址，如 `localhost:6379` |
| `password` | 密码，无密码留空 |
| `db` | 逻辑库编号 |

**`stream`（通信行为）**

| 字段 | 说明 |
|---|---|
| `consumer_stream` | 本服务监听的 Stream 名称，即服务身份。其它服务向该名称的 Stream 发消息即可调用本服务 |
| `consumer_group` | Redis 消费者组名称，同一 Stream 可有多个组各自独立消费 |
| `goroutine_num` | 业务 handler 的最大并发数（信号量） |
| `get_timeout_ms` | `stream.Send` 未显式指定超时时的默认等待时间（毫秒） |
| `max_message_bytes` | 单条消息最大字节数，超过自动分片；缺省 1MB（1048576） |
| `cache_key_prefix` | `stream.CacheSet/CacheGet/CacheDelete` 辅助缓存的 key 前缀，缺省 `stream:` |

**`custom`**：自由业务配置块（`map[string]interface{}`），库不做解析，由服务自己通过 `stream.Custom["字段名"]` 读取。各示例服务用它存放下游服务 Stream 名称、SMTP 账号、任务文件路径等。值中同样支持 `${ENV}` 展开。

### 第 3 步：编写 main.go（模板）

```go
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/totooicu/go-mytool/stream"
)

func initService() {
	// LoadConfig 默认读 "config.json"，也可用命令行第一个参数覆盖：my-service -config other.json
	if err := stream.LoadConfig("config.json"); err != nil {
		log.Fatal("load config error:", err)
	}
	if err := stream.Init(); err != nil { // 连接 Redis、建消费者组、启动消费循环
		log.Fatal("stream init error:", err)
	}
}

func main() {
	initService()

	// 注册服务：服务名 -> 处理函数
	stream.RegisterService("hello", handleHello)

	log.Println("My service started")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

// handler 签名固定：func(*stream.StreamMessage)
func handleHello(msg *stream.StreamMessage) {
	// 1. msg.Playload 是 map[string]interface{}（请求参数）
	name, _ := msg.Playload["name"].(string)
	if name == "" {
		stream.ResponseErr(msg, "name is required") // 失败响应：err_code=1
		return
	}
	// 2. 处理业务...
	// 3. 成功响应：playload 会作为调用方的返回值
	stream.ResponseSucc(msg, map[string]interface{}{"greeting": "hello " + name})
}
```

### 客户端调用

```go
// Send(目标Stream, 服务名, playload, 超时毫秒)
// 超时: 0=使用 get_timeout_ms；-1 设计为不限时(当前实现有立即超时问题，建议传足够大的正数)
resp, err := stream.Send("dev:my-stream", "hello",
	map[string]interface{}{"name": "world"}, 5000)
if err != nil {
	log.Println("failed:", err) // 超时或对端 ResponseErr
}
log.Println(resp.Playload["greeting"])

// 健康检查
stat, err := stream.SendPing("dev:my-stream")
log.Println(stat.Playload) // {"active_handlers":0,"capacity":5,"available":5,"total_goroutines":N}
```

### StreamMessage 消息结构

| 字段 | 说明 |
|---|---|
| `message_id` | 全局唯一消息 ID（时间戳 + Redis 自增） |
| `reply_id` | 响应消息中关联的请求 ID |
| `service_name` | 目标服务名；响应固定为 `response` |
| `callback_stream` | 响应回发的 Stream |
| `source_stream` / `source_service` | 请求来源 |
| `playload` | 业务数据，`map[string]interface{}`，以 JSON 字符串形式在 Stream 中传输（字段拼写即 `playload`） |
| `sharding_id` / `sharding_total` / `sharding_data` | 分片信息与分片数据 |
| `err_code` / `err_msg` | 0 成功；非 0 失败（`ResponseErr` 使用 1） |
| `deadline` | 绝对过期时间戳（毫秒），0 表示不限 |
| `trace_id` | 链路追踪 ID，透传 |

### 其它库函数

| 函数 | 说明 |
|---|---|
| `stream.CacheSet(key, value, ttlMs)` | 内容寻址简易缓存：实际存储 key = `前缀:key:MD5(value)` |
| `stream.CacheGet / CacheDelete` | 读取/删除上述缓存 |
| `stream.GetMyRedisClient()` | 获取封装后的 Redis 客户端（通信 Redis） |
| `stream.AckAndDelete(...)` | 确认并删除消息（消费循环内部使用） |

## 示例微服务一览

| 服务 | 监听 Stream（默认） | 提供的服务 | 用途 |
|---|---|---|---|
| [calculator-service](microservice/calculator-service/README.md) | `dev:calculator-stream` | `add`、`divide` | 最小示例：数组求和、除法 |
| [crawler-service](microservice/crawler-service/README.md) | `dev:crawler-stream` | `http_request` | 发送 HTTP 请求，返回页面内容与状态码 |
| [parser-service](microservice/parser-service/README.md) | `dev:parser-stream` | `parse_html_by_get_mid`、`parse_html_by_xpath`、`parse_json` | 从 HTML/JSON 文本中提取数据 |
| [redis_cache-service](microservice/redis_cache-service/README.md) | `dev:redis_cache-stream` | `compare_and_save`、`get`、`set`、`delete`、`get_and_set` | 缓存读写与新旧数据比较（变更检测） |
| [email-service](microservice/email-service/README.md) | `dev:email-stream` | `email_by_config`、`email_by_custom` | SMTP 发送邮件（支持 QQ 邮箱 465/587） |
| [web_supervisor-manager](microservice/web_supervisor-manager/README.md) | `dev:web_supervisor-stream` | （编排器，无对外业务服务） | 定时执行 jobs.json 中的网页监控任务：抓取→解析→变更检测→邮件通知 |
| [microservice-manager](microservice/microservice-manager/README.md) | `dev:client-stream` | （客户端示例） | 演示调用上述所有服务的方式 |

各服务 README 中包含：服务接口清单、请求/响应 playload 格式、`config.json` 中 `custom` 字段含义与调用示例。

各服务的详细数据流（以监控任务为例）：

```
web_supervisor-manager
   │ 1 http_request                2 parse_html_by_get_mid / parse_json
   ├──────────► crawler-service    ├──────────► parser-service
   │            返回 content,status │            返回 parsed_data
   │ 3 compare_and_save / set      4 有变化时 email_by_config
   ├──────────► redis_cache-service├──────────► email-service
   │            返回 changed        │            发送通知邮件
```

## Rust 轻量控制台

`microservice/web-ui-service` 默认编译为原生 Rust 桌面 GUI（`wry` + `tao`），复用现有 HTML/CSS 页面和 HTTP Gateway、Redis Streams 协议，不引入 Electron、Node 或独立数据库。Wry/Tao 比完整 Tauri 壳更轻量，但保留了真正的 WebView 桌面窗口；GUI 支持服务启动/停止/重启、批量操作、日志、HTTP 抓取、RPC 调试和最近结果查看。

```powershell
cd microservice\web-ui-service
cargo run --release -- -config_path .\config.json
```

需要浏览器控制台时使用 Web 模式：

```powershell
cargo run --release -- --web -config_path .\config.json
```

桌面模式会直接打开原生 WebView 窗口，页面采用纯白玻璃风格；需要浏览器控制台时使用 Web 模式。Windows 需要系统已安装 WebView2 Runtime（Windows 11 通常自带）。浏览器打开 `http://127.0.0.1:8090`。Gateway 根路径 `/` 返回 404 是正常的，实际接口是 `POST /rpc`；如果返回 HTTP 504，表示 Gateway 已连通但目标 Redis Stream 没有服务消费者响应。

## 构建与运行

前提：本机或远程运行 Redis；Go 1.25+（使用 `go.work`，无需手动替换依赖）。

```bat
cd microservice

:: 一键交叉编译 5 个服务（crawler/email/parser/redis_cache/web_supervisor）到 bin\ 目录
build_web_supervisor.bat

:: 一键启动 5 个服务（各开一个 cmd 窗口）
run_web_supervisor.bat
```

手动编译/运行单个服务（在服务目录内）：

```bash
go build -o my-service.exe .
./my-service.exe            # 使用默认 config.json
./my-service.exe -config my.json   # 用命令行参数指定配置文件
```

配置文件中密码、邮箱账号等敏感信息建议用环境变量注入，例如 config.json 写 `"password": "${QQ_MAIL_PASSWORD_1134}"`，启动前 `set QQ_MAIL_PASSWORD_1134=xxx`。

### 哪些配置必须修改

默认配置已经可以用于“本机 Redis（`localhost:6379`）+ 默认端口”的开发环境，不是每一项都必须改。启动前只需按实际环境确认下面几项：

| 配置 | 什么时候必须改 | 修改位置 |
|---|---|---|
| Redis 地址/密码/DB | Redis 不在本机、端口不是 `6379` 或启用了认证 | 各服务配置的 `redis`；至少要覆盖实际启动的服务 |
| Gateway 地址/端口 | `18080` 被占用，或 Gateway 不在本机 | `microservice/bin/http-gateway-service/config.json` 的 `custom.http_addr`，以及 `microservice/web-ui-service/config.json` 的 `gateway_addr`，两者必须匹配 |
| UI 端口 | `8090` 被占用 | `microservice/web-ui-service/config.json` 的 `listen_addr` |
| 服务可执行文件路径 | 换了操作系统、编译目标或 `bin` 目录 | `microservice/web-ui-service/config.json` 的 `services[].command/workdir` |
| 邮箱配置 | 只有要发送邮件或运行网页监控通知时 | `email-service` 的 SMTP 配置和 `web_supervisor-manager` 的收件人配置 |
| `jobs.json` | 只有要运行网页监控编排器时 | `microservice/bin/web_supervisor-manager/jobs.json` |

其中 Redis、Gateway 是控制台抓取链路的核心依赖；邮箱和 `web_supervisor-manager` 是可选功能。QQ 邮箱的 `password` 应填写 SMTP 授权码，不是 QQ 登录密码。

### 本地配置与 GitHub 默认配置

推荐把仓库中的 `config.json` 当作无个人信息的默认模板，本地个性化配置写入同目录的 `config.local.json`。Rust 控制台的“配置”弹窗会自动写入各服务的本地配置；控制台自身也会在 `config.json` 同目录自动优先读取 `config.local.json`。`config.local.json`、`.env` 和运行数据目录已加入 `.gitignore`，不会提交到 GitHub。

敏感值优先使用环境变量。仓库提供了安全模板 [`.env.example`](.env.example)，它不会包含真实密码；`.env` 由 Rust GUI 在启动时自动读取，并传递给它启动的 Go 子服务；如果直接启动 Go 服务，则仍需先把变量注入当前进程：

```powershell
$env:QQ_MAIL_ACCOUNT_1134 = "你的发件邮箱"
$env:QQ_MAIL_PASSWORD_1134 = "邮箱授权码"
$env:QQ_MAIL_ACCOUNT_2667 = "通知收件邮箱"
```

如果已经直接修改了 Git 已跟踪的 `config.json`，仅添加 `.gitignore` 不会隐藏这次修改。先把需要保留的内容复制到对应目录的 `config.local.json`（或用 GUI 配置弹窗保存），确认默认文件中的个人值已移除后，再执行：

```powershell
git restore -- microservice/bin/<service>/config.json
```

不要使用 `git add -f` 提交 `config.local.json`、`.env` 或真实密码。提交前可用 `git status --short` 和 `git diff --check` 检查。
## JSON 工作流轮询与增量文档

新版 `web_supervisor-manager` 提供独立 HTTP 管理器：支持 `${ENV}` 环境变量、`#{...}` 全局变量、`%{...}` 局部变量，以及 `get`、`set`、`func`、`execfunc`、`if`、`while`、`operators` 等服务。任务可在 Rust GUI 中创建、启用/暂停、立即执行和查看 JSON/Markdown 文档与 Diff。

- Manager 默认地址：`http://127.0.0.1:18081`
- GUI 通过 `/api/manager/...` 代理 manager API
- 状态保存到 SQLite，文档和快照保存在 `data/workflow-manager/documents/`
- 轮询关闭 GUI 后仍继续运行
- 详细任务格式和 API：[`microservice/web_supervisor-manager/WORKFLOW_GUIDE.md`](microservice/web_supervisor-manager/WORKFLOW_GUIDE.md)

## 调试工具

- `MyTool/streams-manager`：Redis Stream 命令行管理工具，可查看/添加/删除 Stream 与消息（详见其目录内 README），排障时可用 `streams-manager ls dev:crawler-stream` 直接查看队列内容。
