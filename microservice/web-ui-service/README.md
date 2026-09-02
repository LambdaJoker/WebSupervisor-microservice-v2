# Web UI Service

轻量 Rust 控制台：通过现有 HTTP Gateway 调用 Redis Streams 微服务，提供服务状态、进程控制、页面抓取和最近结果查看。

## 启动

先确保 Redis 可用，然后在本目录执行：

```powershell
cargo run -- -config_path .\config.json
```

浏览器打开 `http://127.0.0.1:8090`。

当前配置使用 `http://127.0.0.1:18080` 作为 HTTP Gateway，避免与本机常见的 Java 服务占用 8080 端口冲突。

## 接口

- `GET /`：内置控制台页面
- `GET /api/services`：并行检查各 Stream 的 `ping`，并返回进程状态
- `POST /api/services/:name/start`：启动配置中的本地服务进程（包括 HTTP Gateway）
- `POST /api/services/:name/stop`：停止由控制台启动的本地服务进程
- `POST /api/services/:name/restart`：重启由控制台管理的本地服务
- `POST /api/crawl`：调用 `dev:crawler-stream/http_request`
- `GET /api/recent`：读取本进程内最近 20 条抓取记录，不引入数据库

## 服务启动配置

`config.json` 的 `services` 项支持以下字段：

- `name`：服务名称
- `stream`：Redis Stream 名称
- `command`：可执行文件路径，相对路径以配置文件所在目录为基准
- `args`：启动参数
- `workdir`：进程工作目录，相对路径以配置文件所在目录为基准

控制台只会停止自己启动的进程。已经在外部窗口或脚本中启动的进程会显示为“外部进程”，避免误杀其他程序。

服务状态仍以 Redis Streams 的 `ping` 响应为准；进程启动后可能需要短暂时间才能变为 `online`。

