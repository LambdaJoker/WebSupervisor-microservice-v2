package main

type rpcRequest struct {
	Stream    string                 `json:"stream"`
	Service   string                 `json:"service"`
	Payload   map[string]interface{} `json:"payload"`
	TimeoutMs int64                  `json:"timeout_ms"`
}
