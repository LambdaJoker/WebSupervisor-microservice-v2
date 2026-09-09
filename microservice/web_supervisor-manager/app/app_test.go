package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/totooicu/web_supervisor-manager/workflow"
)

type sequenceCaller struct {
	values []any
	calls  int
}

func (c *sequenceCaller) Call(context.Context, string, string, map[string]any) (any, error) {
	value := c.values[len(c.values)-1]
	if c.calls < len(c.values) {
		value = c.values[c.calls]
	}
	c.calls++
	return value, nil
}

func testTask(dir string) Task {
	return Task{
		ID: "task-1", Name: "test", Enabled: false,
		Workflow: workflow.Workflow{Jobs: []workflow.Job{{Stream: "s", Service: "fetch", Payload: map[string]any{"url": "x"}, ResultTo: "result"}}},
		Document: DocumentConfig{Directory: dir, Identity: "id", Compare: []string{"id", "title"}, KeepSnapshots: 2},
	}
}

func TestPersistDocumentUsesPreviousCurrentAndPrunesSnapshots(t *testing.T) {
	s, err := NewServer(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	task := testTask("docs/task-1")
	for i := 0; i < 4; i++ {
		diff, err := s.persistDocument(task, []any{map[string]any{"id": float64(1), "title": string(rune('a' + i))}})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && !diff.Baseline {
			t.Fatal("first document should establish baseline")
		}
		if i > 0 && diff.Baseline {
			t.Fatal("later documents must not be baseline")
		}
		if i == 1 && diff.Updated != 1 {
			t.Fatalf("expected one incremental update, got %#v", diff)
		}
	}
	files, err := os.ReadDir(filepath.Join(s.cfg.DataDir, "docs", "task-1", "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("expected two retained snapshots, got %d", len(files))
	}
	current, err := os.ReadFile(filepath.Join(s.cfg.DataDir, "docs", "task-1", "current.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(current) {
		t.Fatal("current document is not valid JSON")
	}
}

func TestDocumentDirectoryCannotEscapeDataDir(t *testing.T) {
	s, err := NewServer(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.documentDirectory(Task{ID: "x", Document: DocumentConfig{Directory: "../../outside"}}); err == nil {
		t.Fatal("expected path traversal to be rejected")
	}
}

func TestTaskAPIAndDryRun(t *testing.T) {
	s, err := NewServer(Config{DataDir: t.TempDir(), Custom: map[string]any{"name": "custom"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler()
	task := Task{Name: "api-task", Workflow: workflow.Workflow{Jobs: []workflow.Job{{Service: "set", Payload: "#{name}", ResultTo: "value"}}}}
	body, _ := json.Marshal(task)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytesReader(body))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", res.Code, res.Body.String())
	}
	var created Task
	if err := json.Unmarshal(res.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("server did not assign task id")
	}

	res = httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+created.ID, nil))
	if res.Code != http.StatusOK {
		t.Fatalf("get status=%d", res.Code)
	}

	dry, _ := json.Marshal(map[string]any{"workflow": task.Workflow})
	res = httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v1/workflow/dry-run", bytesReader(dry)))
	if res.Code != http.StatusOK {
		t.Fatalf("dry-run status=%d body=%s", res.Code, res.Body.String())
	}
	var output map[string]any
	if err := json.Unmarshal(res.Body.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["local"].(map[string]any)["value"] != "custom" {
		t.Fatalf("unexpected dry-run result: %#v", output)
	}
}

func TestSQLiteStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := NewServer(Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(Task{Name: "persisted", Workflow: workflow.Workflow{Jobs: []workflow.Job{}}})
	res := httptest.NewRecorder()
	s.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytesReader(body)))
	if res.Code != http.StatusCreated {
		t.Fatalf("create status=%d", res.Code)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := NewServer(Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res = httptest.NewRecorder()
	s2.Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("list status=%d", res.Code)
	}
	var tasks []Task
	if err := json.Unmarshal(res.Body.Bytes(), &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Name != "persisted" {
		t.Fatalf("state was not persisted: %#v", tasks)
	}
}

func TestCronAndScheduleValidation(t *testing.T) {
	if !cronMatches("*/5 * * * *", time.Date(2026, 9, 6, 12, 10, 0, 0, time.Local)) {
		t.Fatal("cron should match")
	}
	if cronMatches("*/5 * * * *", time.Date(2026, 9, 6, 12, 11, 0, 0, time.Local)) {
		t.Fatal("cron should not match")
	}
	if err := validateTask(Task{Name: "bad", Schedule: Schedule{IntervalSeconds: 10}}); err == nil {
		t.Fatal("short interval should be rejected")
	}
	if err := validateTask(Task{Name: "good", Schedule: Schedule{Cron: "0 * * * *"}, Workflow: workflow.Workflow{Jobs: []workflow.Job{}}}); err != nil {
		t.Fatal(err)
	}
}

// bytesReader keeps the tests independent of an io helper's concrete type.
type byteReader struct {
	data []byte
	pos  int
}

func bytesReader(data []byte) *byteReader { return &byteReader{data: data} }
func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, os.ErrClosed
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestGatewayCallerUsesConfiguredTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var request map[string]any
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		if request["timeout_ms"] != float64(0) {
			t.Fatalf("timeout_ms = %#v, want 0 so gateway default is used", request["timeout_ms"])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	caller := &GatewayCaller{BaseURL: server.URL, Client: server.Client()}
	if _, err := caller.Call(context.Background(), "stream", "service", map[string]any{}); err != nil {
		t.Fatal(err)
	}
}
