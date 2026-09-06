package workflow

import (
	"context"
	"os"
	"reflect"
	"testing"
)

type fakeCaller struct{ calls []string }

func (f *fakeCaller) Call(_ context.Context, stream, service string, payload map[string]any) (any, error) {
	f.calls = append(f.calls, stream+":"+service)
	return map[string]any{"echo": payload["value"]}, nil
}

func TestExecuteResolvesVariablesAndBuiltins(t *testing.T) {
	t.Setenv("WORKFLOW_ENV", "from-env")
	wf, err := Parse([]byte(`{
      "jobs": [
        {"service":"set", "payload":{"value":"${WORKFLOW_ENV}"}, "resultto":"env"},
        {"service":"set", "payload":{"value":42}, "resultto":"counter"},
        {"service":"get", "payload":{"path":"counter"}, "resultto":"readback"},
        {"service":"operators", "payload":{"operator":"+", "parameter1":"%{counter}", "parameter2":8}, "resultto":"sum"},
        {"service":"operators", "payload":{"operator":">", "parameter1":"%{sum}", "parameter2":40}, "resultto":"is_large"}
      ]
    }`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(Options{Strict: true, Custom: map[string]any{"label": "custom"}}).Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"env": "from-env", "counter": float64(42), "readback": float64(42), "sum": float64(50), "is_large": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestFunctionUsesIsolatedLocalAndReturnPath(t *testing.T) {
	wf, err := Parse([]byte(`{
      "jobs": [
        {"service":"func", "payload":{"name":"increment", "parameters":["amount"], "returnto":"result", "jobs":[
          {"service":"operators", "payload":{"operator":"+", "parameter1":"%{counter}", "parameter2":"%{amount}"}, "resultto":"result"},
          {"service":"set", "payload":999, "resultto":"counter"}
        ]}},
        {"service":"execfunc", "payload":{"func":"increment", "parameters":[2]}, "resultto":"answer"}
      ]
    }`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(Options{Strict: true}).Execute(context.Background(), wf, map[string]any{"counter": 3})
	if err != nil {
		t.Fatal(err)
	}
	if got["counter"] != float64(3) {
		t.Fatalf("child mutated parent local: %#v", got)
	}
	if got["answer"] != float64(5) {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestIfAndWhileReevaluateLocalCondition(t *testing.T) {
	wf, err := Parse([]byte(`{
      "jobs": [
        {"service":"set", "payload":0, "resultto":"i"},
        {"service":"while", "payload":{"value":{"operator":"<","parameter1":"%{i}","parameter2":3},"jobs":[
          {"service":"operators", "payload":{"operator":"+","parameter1":"%{i}","parameter2":1},"resultto":"i"}
        ]}},
        {"service":"if", "payload":{"value":"%{i}","truejobs":[{"service":"set","payload":"yes","resultto":"status"}],"falsejobs":[{"service":"set","payload":"no","resultto":"status"}]}}
      ]
    }`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(Options{Strict: true}).Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got["i"] != float64(3) || got["status"] != "yes" {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestExternalCallerAndCustomPath(t *testing.T) {
	caller := &fakeCaller{}
	wf, err := Parse([]byte(`{"jobs":[{"stream":"s","service":"echo","payload":{"value":"#{input}"},"resultto":"out"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(Options{Strict: true, Custom: map[string]any{"input": "hello"}, Caller: caller}).Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got["out"].(map[string]any)["echo"] != "hello" || len(caller.calls) != 1 {
		t.Fatalf("unexpected external call: %#v calls=%v", got, caller.calls)
	}
	_ = os.Getenv("WORKFLOW_ENV")
}

func TestGetArrayPathAndDefaults(t *testing.T) {
	wf, err := Parse([]byte(`{"jobs":[
		{"service":"set","payload":{"items":[{"title":"first"}]},"resultto":"data"},
		{"service":"get","payload":{"path":["data","items",0,"title"]},"resultto":"title"},
		{"service":"get","payload":{"path":"data.missing","default":"fallback"},"resultto":"fallback"}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(Options{Strict: true}).Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got["title"] != "first" || got["fallback"] != "fallback" {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestStrictMissingVariableAndArithmeticErrors(t *testing.T) {
	wf, err := Parse([]byte(`{"jobs":[{"service":"set","payload":"%{missing}","resultto":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Strict: true}).Execute(context.Background(), wf, nil); err == nil {
		t.Fatal("expected missing variable error")
	}
	wf, err = Parse([]byte(`{"jobs":[{"service":"operators","payload":{"operator":"/","parameter1":1,"parameter2":0}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Strict: true}).Execute(context.Background(), wf, nil); err == nil {
		t.Fatal("expected division by zero error")
	}
}

func TestWhileIterationLimit(t *testing.T) {
	wf, err := Parse([]byte(`{"jobs":[{"service":"while","payload":{"value":true,"jobs":[]}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Strict: true, MaxLoop: 2}).Execute(context.Background(), wf, nil); err == nil {
		t.Fatal("expected loop limit error")
	}
}

func TestGetSupportsDottedExternalResultKeys(t *testing.T) {
	wf, err := Parse([]byte(`{"jobs":[
		{"service":"set","payload":{"parsed_data":{"data.list":[[{"id":1,"title":"first"}]]}},"resultto":"parsed"},
		{"service":"get","payload":{"path":"parsed.parsed_data.data.list[0]"},"resultto":"document"}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := New(Options{Strict: true}).Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	document, ok := got["document"].([]any)
	if !ok || len(document) != 1 || document[0].(map[string]any)["title"] != "first" {
		t.Fatalf("unexpected document: %#v", got["document"])
	}
}

func TestValidateNestedJobs(t *testing.T) {
	_, err := Parse([]byte(`{"jobs":[{"service":"if","payload":{"value":true,"truejobs":[{}]}}]}`))
	if err == nil {
		t.Fatal("expected nested job validation error")
	}
}
