# Web UI Service

轻量 Rust 控制台：通过现有 HTTP Gateway 调用 Redis Streams 微服务，提供服务状态、进程控制、页面抓取和可持久化历史查看。

## 启动

先确保 Redis 可用，然后在本目录执行：

```powershell
cargo run -- -config_path .\config.json
```

默认启动为原生 Rust WebView 桌面应用，页面使用纯白玻璃风格；Windows 需要 WebView2 Runtime。若只需要浏览器页面，可使用 `--web` 参数，浏览器打开 `http://127.0.0.1:8090`。

当前配置使用 `http://127.0.0.1:18080` 作为 HTTP Gateway，避免与本机常见的 Java 服务占用 8080 端口冲突。

## 接口

- `GET /`：内置控制台页面
- `GET /api/services`：并行检查各 Stream 的 `ping`，并返回进程状态
- `POST /api/services/:name/start`：启动配置中的本地服务进程（包括 HTTP Gateway）
- `POST /api/services/:name/stop`：停止由控制台启动的本地服务进程
- `POST /api/services/:name/restart`：重启由控制台管理的本地服务
- `POST /api/crawl`：调用 `dev:crawler-stream/http_request`
- `GET /api/recent` / `GET /api/history`：读取持久化的最近 20 条抓取记录
- `DELETE /api/recent` / `DELETE /api/history`：清空 JSONL 历史记录
- `POST /api/services/:name/adopt`：按配置中的可执行文件名接管唯一匹配的外部进程

## 服务启动配置


### 必须确认的配置

- **Redis**：默认是 `localhost:6379`、无密码。Redis 在其它主机或端口时，修改每个实际启动服务配置中的 `redis.addr/password/db`。
- **HTTP Gateway**：UI 的 `gateway_addr` 必须与 Gateway 的 `custom.http_addr` 对应；默认分别是 `http://127.0.0.1:18080` 和 `127.0.0.1:18080`。
- **UI 监听地址**：只有 `8090` 被占用时才改 `listen_addr`。
- **邮箱**：只有发送邮件或监控通知时才需要配置。复制仓库根目录 `.env.example` 为 `.env`，填写发件邮箱、SMTP 授权码和通知收件邮箱；Rust GUI 启动时会自动加载 `.env`。配置弹窗只显示邮箱账号和密码这两个必要字段。
`config.json` 的 `services` 项支持以下字段：

- `name`：服务名称
- `stream`：Redis Stream 名称
- `command`：可执行文件路径，相对路径以配置文件所在目录为基准
- `args`：启动参数
- `workdir`：进程工作目录，相对路径以配置文件所在目录为基准

配置文件分为默认配置和本地覆盖配置：仓库中的 `config.json` 是默认模板；控制台保存服务配置时会写入对应服务目录下的 `config.local.json`，该文件已被 `.gitignore` 忽略，不会污染默认配置。启动由控制台管理的服务时，如果存在 `config.local.json`，会自动使用它。控制台自身也支持同样规则：把 `web-ui-service/config.json` 复制为 `web-ui-service/config.local.json` 后，本来的启动命令无需修改，会自动优先读取本地文件。

抓取历史保存于 `data/crawl-history.jsonl`，使用 JSONL 追加写入，不引入数据库；损坏的单行会被跳过。

控制台只会停止自己启动的进程，或用户明确点击“接管”的外部进程。接管需要按可执行文件名找到唯一 PID，避免误杀；接管后可以停止、重启，并会在关闭桌面 GUI 时自动终止。未接管的外部服务不会因 GUI 关闭而被关闭。

服务状态仍以 Redis Streams 的 `ping` 响应为准；进程启动后可能需要短暂时间才能变为 `online`。



### `.env` 与配置弹窗

推荐流程：

1. 复制 `D:\code\WebSupervisor-microservice-v2\.env.example` 为同目录的 `.env`。
2. 填写 `QQ_MAIL_ACCOUNT_1134`（发件邮箱）、`QQ_MAIL_PASSWORD_1134`（QQ SMTP 授权码）和 `QQ_MAIL_ACCOUNT_2667`（通知收件邮箱）。
3. 在 `microservice/web-ui-service` 目录启动 GUI；GUI 会从配置文件目录向父目录查找 `.env`，并把变量传给它启动的 Go 服务。
4. 打开服务卡片的“配置”时，只会显示真正需要修改的字段；Redis、Stream、SMTP 主机/端口等默认值不会出现在表单里。

`.env`、`config.local.json` 和运行数据不会提交到 GitHub；仓库里的 `config.json` 始终作为干净默认模板。
