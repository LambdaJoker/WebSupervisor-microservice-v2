# HTTP Gateway Service

把 HTTP 请求转换为项目现有的 Redis Streams RPC 调用。

## 启动

```powershell
go run . -config_path .\config.json
```

默认监听 `127.0.0.1:8080`，需要 Redis 和目标微服务已启动。

## 调用

```powershell
curl.exe -X POST http://127.0.0.1:8080/rpc `
  -H "Content-Type: application/json" `
  -d '{"stream":"dev:calculator-stream","service":"add","payload":{"nums":[1,2,3]},"timeout_ms":5000}'
```

成功时直接返回目标服务的 `playload`。请求参数错误返回 `400`，目标服务超时返回 `504`，目标服务或 Redis 调用失败返回 `502`。

### 请求字段

- `stream`：目标服务监听的 Stream 名称。
- `service`：目标服务注册的服务名。
- `payload`：业务参数对象。
- `timeout_ms`：可选；`0` 使用配置中的 `default_timeout_ms`，不能超过 `max_timeout_ms`。

`custom.http_addr`、`custom.default_timeout_ms` 和 `custom.max_timeout_ms` 可在 `config.json` 中调整。
