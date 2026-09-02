package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/totooicu/go-mytool/stream"
)

func TestHandleRPCReturnsPayload(t *testing.T) {
	oldCustom := stream.Custom
	oldMax := stream.MaxMsgSize
	defer func() {
		stream.Custom = oldCustom
		stream.MaxMsgSize = oldMax
	}()
	stream.Custom = map[string]interface{}{"default_timeout_ms": float64(3000), "max_timeout_ms": float64(10000)}
	stream.MaxMsgSize = 204800

	var gotStream, gotService string
	var gotTimeout int64
	gw := &gateway{send: func(targetStream, targetService string, payload map[string]interface{}, timeoutMs int64) (*stream.StreamMessage, error) {
		gotStream, gotService, gotTimeout = targetStream, targetService, timeoutMs
		return &stream.StreamMessage{Playload: map[string]interface{}{"result": 6}}, nil
	}}

	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"stream":"dev:calculator-stream","service":"add","payload":{"nums":[1,2,3]},"timeout_ms":5000}`))
	rec := httptest.NewRecorder()
	gw.handleRPC(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if gotStream != "dev:calculator-stream" || gotService != "add" || gotTimeout != 5000 {
		t.Fatalf("send args = %q, %q, %d", gotStream, gotService, gotTimeout)
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"result":6}` {
		t.Fatalf("body = %s, want %s", body, `{"result":6}`)
	}
}

func TestHandleRPCUsesDefaultTimeout(t *testing.T) {
	oldCustom := stream.Custom
	defer func() { stream.Custom = oldCustom }()
	stream.Custom = map[string]interface{}{"default_timeout_ms": float64(3000), "max_timeout_ms": float64(10000)}

	var gotTimeout int64
	gw := &gateway{send: func(_, _ string, _ map[string]interface{}, timeoutMs int64) (*stream.StreamMessage, error) {
		gotTimeout = timeoutMs
		return &stream.StreamMessage{Playload: map[string]interface{}{}}, nil
	}}

	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"stream":"s","service":"x","payload":{}}`))
	rec := httptest.NewRecorder()
	gw.handleRPC(rec, req)

	if rec.Code != http.StatusOK || gotTimeout != 3000 {
		t.Fatalf("status/timeout = %d/%d, want %d/3000", rec.Code, gotTimeout, http.StatusOK)
	}
}

func TestHandleRPCValidation(t *testing.T) {
	oldCustom := stream.Custom
	defer func() { stream.Custom = oldCustom }()
	stream.Custom = map[string]interface{}{"max_timeout_ms": float64(1000)}
	gw := &gateway{send: func(string, string, map[string]interface{}, int64) (*stream.StreamMessage, error) {
		t.Fatal("send must not be called")
		return nil, nil
	}}

	cases := []struct {
		name string
		body string
	}{
		{"invalid json", `{`},
		{"missing fields", `{"stream":"s","service":"x"}`},
		{"negative timeout", `{"stream":"s","service":"x","payload":{},"timeout_ms":-1}`},
		{"timeout too large", `{"stream":"s","service":"x","payload":{},"timeout_ms":1001}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			gw.handleRPC(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestHandleRPCMapsUpstreamErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"timeout", errors.New("request timeout"), http.StatusGatewayTimeout},
		{"upstream", errors.New("service unavailable"), http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gw := &gateway{send: func(string, string, map[string]interface{}, int64) (*stream.StreamMessage, error) {
				return nil, tc.err
			}}
			req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"stream":"s","service":"x","payload":{}}`))
			rec := httptest.NewRecorder()
			gw.handleRPC(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}
