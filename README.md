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
│   ├── web_supervisor-manager/    # 网页监控编排器与 HTTP 工作流管理器
│   ├── http-gateway-service/      # Redis Streams 的 HTTP RPC 网关
│   ├── web-ui-service/            # Rust 桌面/浏览器控制台
│   ├── microservice-manager/      # 调用各服务的示例客户端
│   ├── build_web_supervisor.bat   # 一键交叉编译 6 个 Go 服务
│   └── run_web_supervisor.bat     # 旧版一键启动脚本（启动 5 个 Go 服务）
├── go.work                        # Go workspace，聚合所有 module
└── go.mod
```

## 快速开始

### 前置条件

- Windows 10/11（桌面模式需要 WebView2 Runtime；浏览器模式不需要桌面窗口）。
- Redis，默认地址为 `localhost:6379`。
- Go 1.25+，用于编译 Go 微服务。
- Rust stable toolchain，包含 Cargo，用于编译 Rust 控制台。

### 推荐启动流程（Windows）

```powershell
# 1. 编译 Go 服务，产物写入 microservice\bin\
cd microservice
.\build_web_supervisor.bat

# 2. 启动浏览器版控制台
cd web-ui-service
cargo run --release -- --web -config_path .\config.json
```

然后打开 `http://127.0.0.1:8090`，在控制台中启动需要的服务。默认配置会使用以下端口：

| 组件 | 地址 | 作用 |
|---|---|---|
| Redis | `localhost:6379` | Redis Streams 消息与缓存 |
| HTTP Gateway | `http://127.0.0.1:18080` | 将 HTTP RPC 转发到 Redis Streams |
| Workflow Manager | `http://127.0.0.1:18081` | 轮询执行 JSON workflow、保存文档和 Diff |
| Rust GUI | `http://127.0.0.1:8090` | 服务控制、抓取、工作流管理和调试 |

`build_web_supervisor.bat` 会编译 crawler、parser、redis_cache、email、http-gateway 和 web_supervisor-manager 的 Windows/Linux/ARM64 目标；`calculator-service` 是独立示例，需要在其目录单独编译；Rust 控制台也需要单独使用 Cargo 编译。旧版 `run_web_supervisor.bat` 依赖各服务目录下已有的可执行文件，优先推荐使用 GUI 的服务控制功能。

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

`microservice/web-ui-service` 是一个 Rust 控制台，支持原生 WebView 桌面模式和浏览器模式。它复用现有 HTTP Gateway、Redis Streams 和 Workflow Manager，不引入 Electron、Node 或额外数据库。

### 启动方式

```powershell
cd microservice\web-ui-service

# 开发版浏览器模式
cargo run -- --web -config_path .\config.json

# 发布版浏览器模式
cargo run --release -- --web -config_path .\config.json

# 发布版桌面模式（Windows 需要 WebView2 Runtime）
cargo run --release -- -config_path .\config.json
```

浏览器模式访问 `http://127.0.0.1:8090`。若 8090 已被占用，修改 `config.json`（推荐复制为 `config.local.json`）中的 `listen_addr`。

### 主要功能

- 查看所有微服务的在线状态、健康详情、PID 和进程管理状态。
- 启动、停止、重启单个服务，或批量启动/停止/重启服务。
- 接管已经在控制台外启动的唯一匹配进程。
- 查看服务日志、清理日志、编辑服务启动配置。
- 调用 crawler-service 抓取网页，并查看持久化的最近结果和历史记录。
- 通过 RPC 调试面板直接调用 Redis Streams 服务。
- 创建、启用/暂停、立即执行 Workflow Manager 任务，查看 JSON/Markdown 文档、快照和增量 Diff。

### 服务状态与“接管”规则

- crawler、parser、redis_cache、email、calculator、http-gateway 使用 Gateway RPC `ping` 判断在线。
- `workflow-manager` 是独立 HTTP 服务，使用 `GET http://127.0.0.1:18081/health` 判断在线；它不是 Gateway 的 Redis Stream RPC 服务，因此不应使用 `local:workflow-manager` 的 Gateway ping 作为接管条件。
- “启动”由 GUI 创建并管理新进程；“接管”会按 `services[].command` 的可执行文件名查找外部进程，只有唯一匹配的 PID 才会接管，避免误杀其它同名进程。
- `not_managed` 表示服务在线但未由 GUI 管理；`running` 表示由 GUI 启动；`adopted` 表示由 GUI 接管。
- GUI 关闭时会停止由 GUI 启动或接管的进程。若希望 Workflow Manager 在关闭 GUI 后继续轮询，请先独立启动 manager，再让 GUI 仅作为代理使用，不要点击“启动”或“接管”。

### 常用 HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/api/services` | 获取所有服务状态和健康详情 |
| `POST` | `/api/services/:name/start` | 启动配置中的服务 |
| `POST` | `/api/services/:name/stop` | 停止 GUI 管理的服务 |
| `POST` | `/api/services/:name/restart` | 重启 GUI 管理的服务 |
| `POST` | `/api/services/:name/adopt` | 接管外部运行中的唯一匹配进程 |
| `GET/PUT` | `/api/services/:name/config` | 读取或保存服务本地配置 |
| `GET` | `/api/services/:name/logs` | 查看服务日志 |
| `POST` | `/api/crawl` | 调用 crawler-service 抓取网页 |
| `POST` | `/api/rpc` | 调用任意 Redis Streams RPC |
| `GET/POST/PUT/DELETE` | `/api/manager/...` | 代理 Workflow Manager 的 `/api/v1/...` API |
| `GET/DELETE` | `/api/recent`、`/api/history` | 查看或清理抓取历史 |

### 接管失败排查

```powershell
# 1. 确认 Workflow Manager 自身在线
curl.exe -i http://127.0.0.1:18081/health

# 2. 确认 GUI 能读取到 manager 状态
curl.exe -i http://127.0.0.1:8090/api/services

# 3. 执行接管
curl.exe -i -X POST http://127.0.0.1:8090/api/services/workflow-manager/adopt
```

如果 `/health` 不是 HTTP 200，先启动或修正 `manager_addr`。如果提示“已经由控制台管理”，说明该服务已经处于 `running` 或 `adopted` 状态；如果提示无法定位唯一进程，请检查 `command` 的文件名和是否存在多个同名进程。

## 构建与运行

前提：本机或远程运行 Redis；Go 1.25+（使用 `go.work`，无需手动替换依赖）。

```bat
cd microservice

:: 一键交叉编译 6 个 Go 服务（crawler/email/parser/redis_cache/http-gateway/web_supervisor）到 bin\ 目录
build_web_supervisor.bat

:: 旧版一键启动 5 个 Go 服务（各开一个 cmd 窗口，不含 HTTP Gateway）
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

新版 `web_supervisor-manager` 提供独立 HTTP Workflow Manager：按照任务的 `schedule` 轮询执行 `workflow.jobs`，把结果保存为 JSON/Markdown 文档，并根据 `identity`、`compare` 生成增量 Diff。完整字段和接口请阅读 [`microservice/web_supervisor-manager/WORKFLOW_GUIDE.md`](microservice/web_supervisor-manager/WORKFLOW_GUIDE.md)。

### 最小任务结构

```json
{
  "name": "公告监控",
  "enabled": true,
  "schedule": { "interval_seconds": 120 },
  "workflow": {
    "jobs": [
      {
        "stream": "dev:crawler-stream",
        "service": "http_request",
        "payload": { "url": "https://example.com/data.json", "method": "GET" },
        "resultto": "response"
      },
      {
        "stream": "",
        "service": "get",
        "payload": { "path": "response.content", "default": "" },
        "resultto": "content"
      }
    ]
  },
  "document": {
    "source": "content",
    "directory": "documents/announcements",
    "identity": "id",
    "compare": ["id", "title", "updated_at"]
  }
}
```

### 变量、内置服务与 Diff

- `${NAME}`：读取系统环境变量；严格模式下变量不存在会报错。
- `#{path.to.value}`：读取 Workflow Manager 配置中的 `custom` 全局变量。
- `%{path.to.value}`：读取当前 workflow 局部变量。
- 内置服务包括 `get`、`set`、`func`、`execfunc`、`if`、`while` 和 `operators`。
- `operators` 支持算术、长度和比较/逻辑操作，例如 `+`、`-`、`*`、`/`、`%`、`len`、`==`、`!=`、`>`、`>=`、`<`、`<=`、`&&`、`||`、`!`。
- 第一次成功运行建立 baseline；后续运行根据 `identity` 和 `compare` 生成新增、修改、删除 Diff，并保留快照。
- `interval_seconds` 最小为 30 秒，也支持五段 cron 表达式；手动 `run` 会绕过轮询间隔限制。

### Manager 地址、数据和 API

- 默认地址：`http://127.0.0.1:18081`
- 健康检查：`GET /health`
- 任务 API：`/api/v1/tasks`、`/api/v1/tasks/:id/run`、`pause`、`resume`
- 文档 API：`/api/v1/tasks/:id/document`、`diff`、`snapshots`
- 工作流 API：`POST /api/v1/workflow/validate`、`dry-run`
- 事件流：`GET /api/v1/events`（SSE）
- 状态库：`manager.db`（SQLite）
- 当前文档、Markdown、baseline、snapshot：`data/workflow-manager/documents/`

GUI 通过 `/api/manager/...` 代理这些接口。Manager 是独立进程；如果由 GUI 启动或接管，GUI 关闭时会停止该进程。要让轮询在 GUI 关闭后继续，请独立启动 Manager，并在 GUI 中不要接管它。

## 调试工具

- `MyTool/streams-manager`：Redis Stream 命令行管理工具，可查看/添加/删除 Stream 与消息（详见其目录内 README），排障时可用 `streams-manager ls dev:crawler-stream` 直接查看队列内容。
