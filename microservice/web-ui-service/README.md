# Web UI Service

`web-ui-service` 是一个轻量 Rust 控制台，提供原生 WebView 桌面模式和浏览器模式。它统一管理本地 Go 微服务、HTTP Gateway 和独立的 Workflow Manager，并提供网页抓取、RPC 调试、任务轮询和增量文档查看功能。

> 项目整体架构、Go 微服务通信协议和默认配置请先阅读[仓库根目录 README](../../README.md)。

## 功能概览

- 服务状态：查看在线状态、健康详情、PID 和当前管理状态。
- 进程控制：启动、停止、重启单个服务，或批量操作多个服务。
- 外部进程接管：接管已经在控制台外启动的唯一匹配进程。
- 日志查看：查看服务 stdout/stderr，清理控制台保存的日志。
- HTTP 抓取：调用 crawler-service，保存最近结果和 JSONL 历史记录。
- RPC 调试：通过 Gateway 调用任意 Redis Streams 服务。
- Workflow Manager：创建、编辑、启用/暂停、立即执行任务，查看 JSON/Markdown 文档、Diff、快照和运行记录。

## 前置条件

- Windows 10/11。
- Redis，默认地址为 `localhost:6379`。
- Go 1.25+，用于编译被控制的 Go 服务。
- Rust stable toolchain 和 Cargo。
- 桌面模式需要 WebView2 Runtime；浏览器模式不需要桌面窗口。

## 启动

### 推荐：浏览器模式

```powershell
cd D:\code\WebSupervisor-microservice-v2\microservice
.\build_web_supervisor.bat

cd web-ui-service
cargo run --release -- --web -config_path .\config.json
```

启动后打开：`http://127.0.0.1:8090`。

### 桌面模式

```powershell
cd microservice\web-ui-service
cargo run --release -- -config_path .\config.json
```

### 直接运行已编译程序

```powershell
cd microservice\web-ui-service
cargo build --release
.\target\release\web-ui-service.exe -config_path .\config.json --web
```

### 默认服务地址

| 组件 | 默认地址 | 用途 |
|---|---|---|
| Redis | `localhost:6379` | Redis Streams 消息和缓存 |
| HTTP Gateway | `http://127.0.0.1:18080` | HTTP RPC 转发到 Redis Streams |
| Workflow Manager | `http://127.0.0.1:18081` | JSON workflow 轮询、文档和 Diff |
| Web UI | `http://127.0.0.1:8090` | 浏览器控制台/API |

推荐先用 `build_web_supervisor.bat` 编译 Go 服务，再从 GUI 的服务卡片启动需要的服务。`calculator-service` 是独立的最小示例，当前一键构建脚本未包含它，需要在 `microservice/calculator-service` 目录单独编译。

## 配置

默认配置文件：`config.json`。本地修改建议复制为同目录的 `config.local.json`，控制台会优先读取本地覆盖文件，避免把个人配置提交到 GitHub。

```json
{
  "listen_addr": "127.0.0.1:8090",
  "gateway_addr": "http://127.0.0.1:18080",
  "manager_addr": "http://127.0.0.1:18081",
  "crawl_timeout_ms": 120000,
  "services": [
    {
      "name": "crawler",
      "stream": "dev:crawler-stream",
      "command": "..\\bin\\crawler-service\\crawler-service_windows_amd64.exe",
      "args": ["-config_path", "config.json"],
      "workdir": "..\\bin\\crawler-service"
    }
  ]
}
```

`services[]` 字段：

| 字段 | 说明 |
|---|---|
| `name` | GUI 中显示的服务名；`workflow-manager` 会使用 HTTP 健康检查 |
| `stream` | Redis Stream 名称；普通 Go 服务用于 Gateway RPC ping |
| `command` | 可执行文件路径；相对路径以 UI 配置文件所在目录为基准 |
| `args` | 启动参数，例如 `-config_path config.json` |
| `workdir` | 进程工作目录；相对路径同样以配置文件所在目录为基准 |

若修改了监听地址或端口，同时更新 Gateway 的 `custom.http_addr`、UI 的 `gateway_addr`，以及浏览器访问地址。邮箱等敏感配置应放到仓库根目录 `.env` 或环境变量中，不要写入 Git 跟踪的默认配置。

## 服务状态与进程生命周期

### 在线状态

- crawler、parser、redis_cache、email、calculator、http-gateway：通过 Gateway 向对应 Redis Stream 发送 `ping`。
- `workflow-manager`：通过 `GET {manager_addr}/health` 判断在线。它是独立 HTTP 服务，不是 Gateway 的 Redis Stream RPC 服务。

因此，Workflow Manager 的 `local:workflow-manager` Gateway ping 超时并不代表 manager 离线；只要 `/health` 返回 HTTP 200，GUI 就会将其显示为在线。

### 启动、接管和关闭

- `启动`：GUI 创建进程并保存句柄，状态通常显示为 `running`。
- `接管`：GUI 根据 `services[].command` 的文件名查找外部进程；只有唯一匹配的 PID 才允许接管，状态显示为 `adopted`。
- `not_managed`：服务在线，但进程不是由 GUI 启动或接管的。
- GUI 关闭时会停止由 GUI 启动或接管的进程。
- 如果希望 Workflow Manager 在 GUI 关闭后继续轮询，请先独立启动 manager，再让 GUI 只做代理，不要点击“启动”或“接管”。

## 常用 HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/` | 内置控制台页面 |
| `GET` | `/api/services` | 获取服务状态和健康详情 |
| `POST` | `/api/services/start-all` | 启动所有已配置服务 |
| `POST` | `/api/services/stop-all` | 停止 GUI 管理的所有服务 |
| `POST` | `/api/services/restart-all` | 重启 GUI 管理的所有服务 |
| `POST` | `/api/services/:name/start` | 启动单个服务 |
| `POST` | `/api/services/:name/stop` | 停止单个 GUI 管理的服务 |
| `POST` | `/api/services/:name/restart` | 重启单个 GUI 管理的服务 |
| `POST` | `/api/services/:name/adopt` | 接管外部运行中的唯一匹配进程 |
| `GET/PUT` | `/api/services/:name/config` | 读取或保存服务本地配置 |
| `GET` | `/api/services/:name/logs` | 读取服务日志 |
| `POST` | `/api/services/:name/logs/clear` | 清理服务日志 |
| `POST` | `/api/crawl` | 调用 crawler-service 抓取网页 |
| `POST` | `/api/rpc` | 调用任意 Redis Streams RPC |
| `GET/POST/PUT/DELETE` | `/api/manager/...` | 代理 Workflow Manager 的 `/api/v1/...` API |
| `GET/DELETE` | `/api/recent`、`/api/history` | 查看或清理抓取历史 |

## 接管 Workflow Manager

Workflow Manager 已经在外部运行时，可以在 GUI 中点击“接管”。也可以用命令行验证：

```powershell
# 检查 manager 是否在线
curl.exe -i http://127.0.0.1:18081/health

# 查看 GUI 识别到的服务状态
curl.exe -i http://127.0.0.1:8090/api/services

# 执行接管
curl.exe -i -X POST http://127.0.0.1:8090/api/services/workflow-manager/adopt
```

成功响应示例：

```json
{
  "action": "adopt",
  "name": "workflow-manager",
  "pid": 40856,
  "state": "adopted"
}
```

常见错误：

| 错误 | 原因 | 处理方式 |
|---|---|---|
| `服务当前不在线，无法接管` | manager `/health` 不可访问，或 UI 仍在运行旧二进制 | 检查 `manager_addr`，重新 `cargo build` 并重启 UI |
| `该服务已经由控制台管理` | 已处于 `running` 或 `adopted` | 直接使用停止/重启，或先停止后再操作 |
| `无法定位唯一外部进程` | 找不到进程或有多个同名进程 | 检查 `command` 文件名并关闭重复进程 |
| Gateway `504 request timeout` | 对 `local:workflow-manager` 使用了错误的 Redis RPC ping | 使用 `/health` 检查；GUI 已对 manager 做 HTTP 特殊处理 |

## Workflow Manager 与增量文档

Workflow Manager 的完整任务格式和 API 见 [`../web_supervisor-manager/WORKFLOW_GUIDE.md`](../web_supervisor-manager/WORKFLOW_GUIDE.md)。核心能力包括：

- `${ENV}`：系统环境变量。
- `#{path.to.value}`：配置中的 `custom` 全局变量。
- `%{path.to.value}`：当前 workflow 的局部变量。
- `get`、`set`、`func`、`execfunc`、`if`、`while`、`operators` 内置服务。
- `+ - * / % len` 算术/长度操作，以及 `== != > >= < <= && || !` 比较和逻辑操作。
- 轮询执行、baseline、对象 identity、字段 compare、增量 Diff 和 snapshots。

GUI 通过 `/api/manager/...` 代理 manager API。manager 独立运行时，即使关闭 GUI，轮询仍会继续；如果 manager 是由 GUI 启动或接管的，GUI 关闭时会按进程管理规则停止它。

## 抓取历史

- 位置：`data/crawl-history.jsonl`。
- 每条记录一行 JSON，保留最近记录；损坏的单行会被跳过。
- `GET /api/recent` 和 `GET /api/history` 读取记录。
- `DELETE /api/recent` 和 `DELETE /api/history` 清空记录。

## 开发与测试

```powershell
cd microservice/web-ui-service
cargo fmt -- --check
cargo check
cargo test
cargo build
```

当前服务没有内置 Rust 单元测试时，`cargo test` 仍应成功完成；建议同时通过 `/api/services`、`/health` 和接管接口执行一次手工冒烟测试。

## 数据与安全注意事项

- `config.local.json`、`.env`、`data/` 和运行日志不应提交到 GitHub。
- 不要在默认 `config.json`、jobs 文件或 README 中写入真实密码、SMTP 授权码或收件人隐私信息。
- GUI 只会停止自己启动或明确接管的进程；未管理的外部进程不会因为 GUI 关闭而被停止。
