package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/totooicu/go-mytool/stream"
)

type gateway struct {
	send func(targetStream, targetService string, payload map[string]interface{}, timeoutMs int64) (*stream.StreamMessage, error)
}

func (g *gateway) handleRPC(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	maxBody := int64(stream.MaxMsgSize)
	if maxBody <= 0 {
		maxBody = 1024 * 1024
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Stream == "" || req.Service == "" || req.Payload == nil {
		writeError(w, http.StatusBadRequest, "stream, service and payload(object) are required")
		return
	}
	if req.TimeoutMs < 0 {
		writeError(w, http.StatusBadRequest, "timeout_ms must be non-negative")
		return
	}

	timeout := req.TimeoutMs
	if timeout == 0 {
		timeout = customInt64("default_timeout_ms", 0)
	}
	if timeout < 0 {
		writeError(w, http.StatusBadRequest, "default_timeout_ms must be non-negative")
		return
	}
	if timeout > customInt64("max_timeout_ms", 300000) {
		writeError(w, http.StatusBadRequest, "timeout_ms exceeds limit")
		return
	}

	resp, err := g.send(req.Stream, req.Service, req.Payload, timeout)
	if err != nil {
		if strings.EqualFold(err.Error(), "request timeout") {
			writeError(w, http.StatusGatewayTimeout, err.Error())
			return
		}
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if resp == nil {
		writeError(w, http.StatusBadGateway, "empty response")
		return
	}
	writeJSON(w, http.StatusOK, resp.Playload)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}

func customInt64(key string, fallback int64) int64 {
	switch value := stream.Custom[key].(type) {
	case float64:
		return int64(value)
	case int:
		return int64(value)
	case int64:
		return value
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func customString(key, fallback string) string {
	if value, ok := stream.Custom[key].(string); ok && value != "" {
		return value
	}
	return fallback
}
