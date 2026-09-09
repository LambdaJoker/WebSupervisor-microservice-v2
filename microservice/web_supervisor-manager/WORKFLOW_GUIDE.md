# Workflow Manager 使用指南

`web_supervisor-manager` 是一个 HTTP 工作流执行器。它按任务的 `schedule` 轮询执行 JSON workflow，把结果保存为 JSON/Markdown 文档，并根据 identity/compare 配置生成增量 Diff。

## 启动

```powershell
web_supervisor-manager.exe -config_path config.json
```

默认 API：`http://127.0.0.1:18081`。`api.gateway_timeout_ms` 默认为 `0`，表示使用 HTTP 网关自身的默认超时；只有在需要覆盖网关配置时才设置为正数。数据目录下会生成 `manager.db`（SQLite 状态库）和 `documents/`（当前文档、Markdown、baseline、快照）。旧版本的 `state.json` 会在首次启动时自动导入并重命名为 `state.json.migrated`。

## 任务 JSON

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
        "payload": {
          "url": "https://example.com/data.json",
          "method": "GET",
          "headers": {}
        },
        "resultto": "response"
      },
      {
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
    "compare": ["id", "title", "updated_at"],
    "template": "# {{.Task.Name}}\n\n{{range .Data}}- {{.title}}\n{{end}}",
    "keep_snapshots": 100
  },
  "retry": { "max_attempts": 3, "backoff_seconds": 2 },
  "notify": {
    "on_change": false,
    "on_error": false,
    "stream": "dev:email-stream",
    "service": "email_by_config",
    "to": ["${NOTIFY_EMAIL}"]
  }
}
```

- `interval_seconds` 最小为 30 秒；也可以使用五段 cron：`minute hour day-of-month month day-of-week`，例如 `*/5 * * * *`。
- `run` 接口是手动执行，会绕过轮询间隔限制；同一任务同时只会运行一次。
- 第一次成功执行建立 baseline，不会视为后续更新；此后 current 文档与上一次 current 比较。
- 指定 `identity` 时，对象数组按 identity 比较，顺序变化不会导致全部更新。`compare` 非空时只比较指定字段。
- 删除的对象不再出现在 current 文档，但会在 Diff 中列出，并保留历史 snapshot。
- `keep_snapshots` 为 0 时使用默认 100。
- `document.directory` 必须位于 `data_dir` 内，防止路径穿越。

## 变量语法

工作流严格模式默认开启：

- `${NAME}`：读取进程环境变量；不存在时报错。
- `#{path.to.value}`：读取配置中的 `custom` 全局变量。
- `%{path.to.value}`：读取当前 workflow local 变量。
- 占位符单独占据整个值时保留原始 JSON 类型；嵌入普通字符串时转为文本。

## 内置服务

### `set`

```json
{"service":"set", "payload":{"value":42}, "resultto":"count"}
```

### `get`

```json
{"service":"get", "payload":{"path":"items[0].title", "default":"无标题"}, "resultto":"title"}
```

`path` 也可以是 `["items", 0, "title"]` 形式的 JSON 数组。

### `operators`

支持：`+ - * / % len == = eq != ne > >= < <= && and || or ! not`。

```json
{"service":"operators", "payload":{"operator":">", "parameter1":"%{count}", "parameter2":0}, "resultto":"has_items"}
```

### `if` 与 `while`

```json
{
  "service":"if",
  "payload":{
    "value":"%{has_items}",
    "truejobs":[{"service":"set","payload":"yes","resultto":"status"}],
    "falsejobs":[{"service":"set","payload":"no","resultto":"status"}]
  }
}
```

`while` 每轮重新解析条件，并有最大循环次数保护。

### `func` 与 `execfunc`

函数执行会深复制上一层 local，函数内部修改不会回写父级：

```json
{
  "service":"func",
  "payload":{
    "name":"add",
    "parameters":["amount"],
    "returnto":"result",
    "jobs":[
      {"service":"operators","payload":{"operator":"+","parameter1":"%{base}","parameter2":"%{amount}"},"resultto":"result"}
    ]
  }
}
```

调用：

```json
{"service":"execfunc","payload":{"func":"add","parameters":[2]},"resultto":"answer"}
```

## HTTP API

- `GET /health`
- `GET /api/v1/tasks`
- `POST /api/v1/tasks`
- `GET /api/v1/tasks/:id`
- `PUT /api/v1/tasks/:id`
- `DELETE /api/v1/tasks/:id`
- `POST /api/v1/tasks/:id/run`
- `POST /api/v1/tasks/:id/pause`
- `POST /api/v1/tasks/:id/resume`
- `GET /api/v1/tasks/:id/runs`
- `GET /api/v1/tasks/:id/document`（加 `?format=markdown` 返回 Markdown）
- `GET /api/v1/tasks/:id/diff`
- `GET /api/v1/tasks/:id/snapshots`
- `POST /api/v1/workflow/validate`
- `POST /api/v1/workflow/dry-run`
- `GET /api/v1/events`（SSE，任务和运行状态变化）

GUI 通过 `/api/manager/...` 代理这些接口。Manager 独立运行时，即使关闭 GUI，后台轮询仍会继续；如果 manager 是由 GUI 启动或接管的，GUI 关闭时会按进程管理规则停止它。
