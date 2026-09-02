# Web UI Service

轻量 Rust 控制台：通过现有 HTTP Gateway 调用 Redis Streams 微服务，提供服务状态、页面抓取和最近结果查看。

## 启动

先启动 Redis、HTTP Gateway 和需要查看的微服务，然后在本目录执行：

```powershell
cargo run -- -config_path .\config.json
```

浏览器打开 `http://127.0.0.1:8090`。

## 接口

- `GET /`：内置控制台页面
- `GET /api/services`：并行调用各 Stream 的 `ping`
- `POST /api/crawl`：调用 `dev:crawler-stream/http_request`
- `GET /api/recent`：读取本进程内最近 20 条抓取记录，不引入数据库

## 配置

`config.json` 中的 `gateway_addr` 指向 HTTP Gateway；`services` 只决定状态面板检查哪些 Stream。服务状态是实时检查结果，不会写入 Redis。
