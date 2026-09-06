package app

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/totooicu/web_supervisor-manager/workflow"
	_ "modernc.org/sqlite"
)

type Task struct {
	ID        string             `json:"id"`
	Name      string             `json:"name"`
	Enabled   bool               `json:"enabled"`
	Schedule  Schedule           `json:"schedule"`
	Workflow  workflow.Workflow  `json:"workflow"`
	Local     map[string]any     `json:"local,omitempty"`
	Document  DocumentConfig     `json:"document"`
	Retry     RetryConfig        `json:"retry,omitempty"`
	Notify    NotificationConfig `json:"notify,omitempty"`
	CreatedAt time.Time          `json:"created_at"`
	UpdatedAt time.Time          `json:"updated_at"`
	Status    TaskStatus         `json:"status"`
	LastRunAt *time.Time         `json:"last_run_at,omitempty"`
	LastError string             `json:"last_error,omitempty"`
}
type Schedule struct {
	IntervalSeconds int    `json:"interval_seconds,omitempty"`
	Cron            string `json:"cron,omitempty"`
}
type RetryConfig struct {
	MaxAttempts    int `json:"max_attempts,omitempty"`
	BackoffSeconds int `json:"backoff_seconds,omitempty"`
}

type NotificationConfig struct {
	OnChange bool     `json:"on_change,omitempty"`
	OnError  bool     `json:"on_error,omitempty"`
	Stream   string   `json:"stream,omitempty"`
	Service  string   `json:"service,omitempty"`
	To       []string `json:"to,omitempty"`
}
type DocumentConfig struct {
	Directory     string   `json:"directory,omitempty"`
	Source        string   `json:"source,omitempty"`
	Identity      string   `json:"identity,omitempty"`
	Compare       []string `json:"compare,omitempty"`
	Template      string   `json:"template,omitempty"`
	KeepSnapshots int      `json:"keep_snapshots,omitempty"`
}
type TaskStatus struct {
	State     string       `json:"state"`
	Runs      int          `json:"runs"`
	Successes int          `json:"successes"`
	Failures  int          `json:"failures"`
	LastDiff  *DiffSummary `json:"last_diff,omitempty"`
}
type Run struct {
	ID         string       `json:"id"`
	TaskID     string       `json:"task_id"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt time.Time    `json:"finished_at"`
	State      string       `json:"state"`
	Error      string       `json:"error,omitempty"`
	Diff       *DiffSummary `json:"diff,omitempty"`
}
type DiffSummary struct {
	Added     int           `json:"added"`
	Updated   int           `json:"updated"`
	Deleted   int           `json:"deleted"`
	Unchanged int           `json:"unchanged"`
	Changes   []FieldChange `json:"changes,omitempty"`
	Baseline  bool          `json:"baseline,omitempty"`
}
type FieldChange struct {
	Identity string `json:"identity,omitempty"`
	Kind     string `json:"kind"`
	Path     string `json:"path,omitempty"`
	Before   any    `json:"before,omitempty"`
	After    any    `json:"after,omitempty"`
}
type Snapshot struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	CreatedAt time.Time `json:"created_at"`
	Data      any       `json:"data"`
}
type State struct {
	Tasks []Task `json:"tasks"`
	Runs  []Run  `json:"runs,omitempty"`
}

type Config struct {
	ListenAddr       string
	DataDir          string
	GatewayURL       string
	GatewayTimeoutMs int64
	Custom           map[string]any
}
type Server struct {
	cfg     Config
	mu      sync.RWMutex
	state   State
	running map[string]context.CancelFunc
	caller  workflow.ServiceCaller
	events  map[chan []byte]struct{}
	db      *sql.DB
}
type GatewayCaller struct {
	BaseURL   string
	Client    *http.Client
	TimeoutMs int64
}

func (g *GatewayCaller) Call(ctx context.Context, stream, service string, payload map[string]any) (any, error) {
	b, _ := json.Marshal(map[string]any{"stream": stream, "service": service, "payload": payload, "timeout_ms": g.TimeoutMs})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(g.BaseURL, "/")+"/rpc", strings.NewReader(string(b)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("gateway status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func NewServer(cfg Config) (*Server, error) {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:18081"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data/workflow-manager"
	}
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "http://127.0.0.1:18080"
	}
	dataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	cfg.DataDir = dataDir
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "manager.db"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	clientTimeout := 130 * time.Second
	if cfg.GatewayTimeoutMs > 0 {
		clientTimeout = time.Duration(cfg.GatewayTimeoutMs+10000) * time.Millisecond
	}
	s := &Server{cfg: cfg, running: map[string]context.CancelFunc{}, events: map[chan []byte]struct{}{}, caller: &GatewayCaller{BaseURL: cfg.GatewayURL, Client: &http.Client{Timeout: clientTimeout}, TimeoutMs: cfg.GatewayTimeoutMs}, db: db}
	if err := s.initStore(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.load(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Server) initStore() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_task_started ON runs(task_id, started_at);
`)
	return err
}
func (s *Server) documentDirectory(task Task) (string, error) {
	base, err := filepath.Abs(s.cfg.DataDir)
	if err != nil {
		return "", err
	}
	dir := task.Document.Directory
	if dir == "" {
		dir = filepath.Join("documents", safeName(task.ID))
	}
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(base, dir)
	}
	dir, err = filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(base, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("document directory must be inside data directory")
	}
	return dir, nil
}

func (s *Server) publishEvent(event map[string]any) {
	event["time"] = time.Now().UTC()
	b, err := json.Marshal(event)
	if err != nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for ch := range s.events {
		select {
		case ch <- b:
		default:
			// A slow client must not block task execution.
		}
	}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming is not supported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch := make(chan []byte, 16)
	s.mu.Lock()
	s.events[ch] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.events, ch)
		close(ch)
		s.mu.Unlock()
	}()
	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) load() error {
	s.state = State{}
	rows, err := s.db.Query(`SELECT data FROM tasks ORDER BY rowid`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var task Task
		if err := json.Unmarshal([]byte(raw), &task); err != nil {
			rows.Close()
			return fmt.Errorf("decode task: %w", err)
		}
		s.state.Tasks = append(s.state.Tasks, task)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	rows, err = s.db.Query(`SELECT data FROM runs ORDER BY started_at`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var run Run
		if err := json.Unmarshal([]byte(raw), &run); err != nil {
			rows.Close()
			return fmt.Errorf("decode run: %w", err)
		}
		s.state.Runs = append(s.state.Runs, run)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(s.state.Tasks) == 0 && len(s.state.Runs) == 0 {
		return s.importLegacyState()
	}
	return nil
}

func (s *Server) importLegacyState() error {
	path := filepath.Join(s.cfg.DataDir, "state.json")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var legacy State
	if err := json.Unmarshal(b, &legacy); err != nil {
		return fmt.Errorf("decode legacy state: %w", err)
	}
	s.state = legacy
	if err := s.saveLocked(); err != nil {
		return err
	}
	return os.Rename(path, path+".migrated")
}

func (s *Server) saveLocked() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	rollback := func(e error) error { _ = tx.Rollback(); return e }
	if _, err := tx.Exec(`DELETE FROM tasks`); err != nil {
		return rollback(err)
	}
	for _, task := range s.state.Tasks {
		b, err := json.Marshal(task)
		if err != nil {
			return rollback(err)
		}
		if _, err := tx.Exec(`INSERT INTO tasks(id, data) VALUES(?, ?)`, task.ID, string(b)); err != nil {
			return rollback(err)
		}
	}
	if _, err := tx.Exec(`DELETE FROM runs`); err != nil {
		return rollback(err)
	}
	for _, run := range s.state.Runs {
		b, err := json.Marshal(run)
		if err != nil {
			return rollback(err)
		}
		if _, err := tx.Exec(`INSERT INTO runs(id, task_id, started_at, data) VALUES(?, ?, ?, ?)`, run.ID, run.TaskID, run.StartedAt.UTC().Format(time.RFC3339Nano), string(b)); err != nil {
			return rollback(err)
		}
	}
	return tx.Commit()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true, "time": time.Now()})
	})
	mux.HandleFunc("/api/v1/tasks", s.handleTasks)
	mux.HandleFunc("/api/v1/tasks/", s.handleTask)
	mux.HandleFunc("/api/v1/workflow/validate", s.handleValidate)
	mux.HandleFunc("/api/v1/workflow/dry-run", s.handleDryRun)
	mux.HandleFunc("/api/v1/events", s.handleEvents)
	return mux
}
func (s *Server) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Server) Serve(ctx context.Context) error {
	defer s.db.Close()
	srv := &http.Server{Addr: s.cfg.ListenAddr, Handler: s.Handler()}
	go s.scheduler(ctx)
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	log.Printf("workflow manager listening on %s", s.cfg.ListenAddr)
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.RLock()
		defer s.mu.RUnlock()
		writeJSON(w, 200, s.state.Tasks)
	case http.MethodPost:
		var t Task
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			writeError(w, 400, err)
			return
		}
		if t.ID == "" {
			t.ID = strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		if t.Name == "" {
			t.Name = t.ID
		}
		if t.CreatedAt.IsZero() {
			t.CreatedAt = time.Now()
		}
		t.UpdatedAt = time.Now()
		if err := validateTask(t); err != nil {
			writeError(w, 400, err)
			return
		}
		if _, err := s.documentDirectory(t); err != nil {
			writeError(w, 400, err)
			return
		}
		s.mu.Lock()
		if existing, _ := s.findTask(t.ID); existing != nil {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, fmt.Errorf("task %q already exists", t.ID))
			return
		}
		s.state.Tasks = append(s.state.Tasks, t)
		err := s.saveLocked()
		if err != nil {
			// Keep in-memory state consistent if persistence fails.
			s.state.Tasks = s.state.Tasks[:len(s.state.Tasks)-1]
		}
		s.mu.Unlock()
		if err != nil {
			writeError(w, 500, err)
			return
		}
		s.publishEvent(map[string]any{"type": "task.created", "task_id": t.ID})
		writeJSON(w, 201, t)
	default:
		writeError(w, 405, errors.New("method not allowed"))
	}
}
func (s *Server) findTask(id string) (*Task, int) {
	for i := range s.state.Tasks {
		if s.state.Tasks[i].ID == id {
			return &s.state.Tasks[i], i
		}
	}
	return nil, -1
}
func (s *Server) handleTask(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 {
		writeError(w, 404, errors.New("not found"))
		return
	}
	id := parts[3]
	s.mu.RLock()
	t, _ := s.findTask(id)
	if t == nil {
		s.mu.RUnlock()
		writeError(w, 404, errors.New("task not found"))
		return
	}
	task := *t
	s.mu.RUnlock()
	if len(parts) == 4 {
		if r.Method == http.MethodGet {
			writeJSON(w, 200, task)
			return
		}
		if r.Method == http.MethodPut {
			var next Task
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				writeError(w, 400, err)
				return
			}
			if next.Name == "" {
				next.Name = task.Name
			}
			if err := validateTask(next); err != nil {
				writeError(w, 400, err)
				return
			}
			next.ID = id
			if _, err := s.documentDirectory(next); err != nil {
				writeError(w, 400, err)
				return
			}
			next.CreatedAt = task.CreatedAt
			next.Status = task.Status
			next.LastRunAt = task.LastRunAt
			next.LastError = task.LastError
			next.UpdatedAt = time.Now()
			s.mu.Lock()
			_, idx := s.findTask(id)
			if idx < 0 {
				s.mu.Unlock()
				writeError(w, 404, errors.New("task not found"))
				return
			}
			s.state.Tasks[idx] = next
			err := s.saveLocked()
			s.mu.Unlock()
			if err != nil {
				writeError(w, 500, err)
				return
			}
			s.publishEvent(map[string]any{"type": "task.updated", "task_id": id})
			writeJSON(w, 200, next)
			return
		}
		if r.Method == http.MethodDelete {
			s.mu.Lock()
			_, idx := s.findTask(id)
			if idx < 0 {
				s.mu.Unlock()
				writeError(w, http.StatusNotFound, errors.New("task not found"))
				return
			}
			s.state.Tasks = append(s.state.Tasks[:idx], s.state.Tasks[idx+1:]...)
			if cancel := s.running[id]; cancel != nil {
				cancel()
				delete(s.running, id)
			}
			err := s.saveLocked()
			s.mu.Unlock()
			if err != nil {
				writeError(w, 500, err)
				return
			}
			s.publishEvent(map[string]any{"type": "task.deleted", "task_id": id})
			writeJSON(w, 200, map[string]bool{"deleted": true})
			return
		}
	}
	if r.Method == http.MethodGet && len(parts) == 5 && parts[4] == "runs" {
		s.mu.RLock()
		runs := []Run{}
		for _, run := range s.state.Runs {
			if run.TaskID == id {
				runs = append(runs, run)
			}
		}
		s.mu.RUnlock()
		writeJSON(w, 200, runs)
		return
	}
	if r.Method == http.MethodGet && len(parts) == 5 && (parts[4] == "diff" || parts[4] == "document") {
		dir, err := s.documentDirectory(task)
		if err != nil {
			writeError(w, 400, err)
			return
		}
		name := "current.json"
		if parts[4] == "document" && strings.EqualFold(r.URL.Query().Get("format"), "markdown") {
			name = "current.md"
		}
		if parts[4] == "diff" {
			if task.Status.LastDiff == nil {
				writeJSON(w, 200, DiffSummary{})
				return
			}
			writeJSON(w, 200, task.Status.LastDiff)
			return
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			writeError(w, 404, err)
			return
		}
		if strings.HasSuffix(name, ".md") {
			w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		} else {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
		}
		w.WriteHeader(200)
		_, _ = w.Write(data)
		return
	}
	if r.Method == http.MethodGet && len(parts) == 5 && parts[4] == "snapshots" {
		dir, err := s.documentDirectory(task)
		if err != nil {
			writeError(w, 400, err)
			return
		}
		entries, err := os.ReadDir(filepath.Join(dir, "snapshots"))
		if err != nil {
			if os.IsNotExist(err) {
				writeJSON(w, 200, []any{})
				return
			}
			writeError(w, 500, err)
			return
		}
		items := make([]string, 0, len(entries))
		for _, entry := range entries {
			if !entry.IsDir() {
				items = append(items, entry.Name())
			}
		}
		writeJSON(w, 200, items)
		return
	}
	if r.Method == http.MethodPost && len(parts) == 5 {
		switch parts[4] {
		case "run":
			go s.runTask(context.Background(), id, true)
			writeJSON(w, 202, map[string]any{"accepted": true})
			return
		case "pause":
			s.setEnabled(id, false)
			writeJSON(w, 200, map[string]bool{"enabled": false})
			return
		case "resume":
			s.setEnabled(id, true)
			writeJSON(w, 200, map[string]bool{"enabled": true})
			return
		}
	}
	writeError(w, 404, errors.New("not found"))
}
func (s *Server) setEnabled(id string, enabled bool) {
	s.mu.Lock()
	changed := false
	if t, _ := s.findTask(id); t != nil {
		t.Enabled = enabled
		t.UpdatedAt = time.Now()
		_ = s.saveLocked()
		changed = true
	}
	s.mu.Unlock()
	if changed {
		s.publishEvent(map[string]any{"type": "task.status", "task_id": id, "enabled": enabled})
	}
}

func validateTask(t Task) error {
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("task.name is required")
	}
	if t.Schedule.IntervalSeconds < 0 {
		return errors.New("schedule.interval_seconds cannot be negative")
	}
	if t.Schedule.IntervalSeconds > 0 && strings.TrimSpace(t.Schedule.Cron) != "" {
		return errors.New("schedule.interval_seconds and schedule.cron are mutually exclusive")
	}
	if t.Schedule.IntervalSeconds > 0 && t.Schedule.IntervalSeconds < 30 {
		return errors.New("schedule.interval_seconds must be at least 30 seconds")
	}
	if strings.TrimSpace(t.Schedule.Cron) != "" {
		if _, err := parseCron(t.Schedule.Cron); err != nil {
			return fmt.Errorf("schedule.cron: %w", err)
		}
	}
	if t.Retry.MaxAttempts < 0 || t.Retry.BackoffSeconds < 0 {
		return errors.New("retry values cannot be negative")
	}
	if t.Document.KeepSnapshots < 0 {
		return errors.New("document.keep_snapshots cannot be negative")
	}
	return workflow.Validate(t.Workflow)
}

type cronSchedule struct{ fields [5]map[int]bool }

func parseCron(expr string) (cronSchedule, error) {
	var out cronSchedule
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return out, errors.New("cron must contain five fields: minute hour day-of-month month day-of-week")
	}
	limits := [5][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 6}}
	for i, part := range parts {
		values, err := parseCronField(part, limits[i][0], limits[i][1])
		if err != nil {
			return out, fmt.Errorf("field %d: %w", i+1, err)
		}
		out.fields[i] = values
	}
	return out, nil
}

func parseCronField(field string, min, max int) (map[int]bool, error) {
	values := make(map[int]bool)
	for _, item := range strings.Split(field, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, errors.New("empty item")
		}
		step := 1
		base := item
		if strings.Contains(item, "/") {
			parts := strings.Split(item, "/")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid step %q", item)
			}
			base = parts[0]
			n, err := strconv.Atoi(parts[1])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid step %q", item)
			}
			step = n
		}
		start, end := min, max
		if base != "" && base != "*" {
			if strings.Contains(base, "-") {
				rangeParts := strings.Split(base, "-")
				if len(rangeParts) != 2 {
					return nil, fmt.Errorf("invalid range %q", base)
				}
				var err error
				start, err = strconv.Atoi(rangeParts[0])
				if err != nil {
					return nil, fmt.Errorf("invalid range %q", base)
				}
				end, err = strconv.Atoi(rangeParts[1])
				if err != nil {
					return nil, fmt.Errorf("invalid range %q", base)
				}
			} else {
				n, err := strconv.Atoi(base)
				if err != nil {
					return nil, fmt.Errorf("invalid value %q", base)
				}
				start, end = n, n
			}
		}
		if start < min || end > max || start > end {
			return nil, fmt.Errorf("value out of range %q", item)
		}
		for n := start; n <= end; n += step {
			values[n] = true
		}
	}
	return values, nil
}

func cronMatches(expr string, now time.Time) bool {
	c, err := parseCron(expr)
	if err != nil {
		return false
	}
	return c.fields[0][now.Minute()] && c.fields[1][now.Hour()] && c.fields[2][now.Day()] && c.fields[3][int(now.Month())] && c.fields[4][int(now.Weekday())]
}

func (s *Server) handleValidate(w http.ResponseWriter, r *http.Request) {
	var wf workflow.Workflow
	if err := json.NewDecoder(r.Body).Decode(&wf); err != nil {
		writeError(w, 400, err)
		return
	}
	if err := workflow.Validate(wf); err != nil {
		writeJSON(w, 422, map[string]any{"valid": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]bool{"valid": true})
}
func (s *Server) handleDryRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Workflow workflow.Workflow `json:"workflow"`
		Local    map[string]any    `json:"local"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, 400, err)
		return
	}
	out, err := workflow.New(workflow.Options{Custom: s.cfg.Custom, Caller: s.caller, Strict: true}).Execute(r.Context(), req.Workflow, req.Local)
	if err != nil {
		writeError(w, 422, err)
		return
	}
	writeJSON(w, 200, map[string]any{"local": out})
}

func (s *Server) scheduler(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.mu.RLock()
			tasks := append([]Task(nil), s.state.Tasks...)
			s.mu.RUnlock()
			for _, task := range tasks {
				if task.Enabled && taskDue(task, now) {
					go s.runTask(ctx, task.ID, false)
				}
			}
		}
	}
}

func taskDue(task Task, now time.Time) bool {
	if task.Schedule.IntervalSeconds > 0 {
		if task.Schedule.IntervalSeconds < 30 || task.LastRunAt == nil {
			return task.LastRunAt == nil
		}
		return now.Sub(*task.LastRunAt) >= time.Duration(task.Schedule.IntervalSeconds)*time.Second
	}
	if strings.TrimSpace(task.Schedule.Cron) == "" || !cronMatches(task.Schedule.Cron, now) {
		return false
	}
	if task.LastRunAt == nil {
		return true
	}
	last := task.LastRunAt.In(now.Location())
	return last.Year() != now.Year() || last.YearDay() != now.YearDay() || last.Hour() != now.Hour() || last.Minute() != now.Minute()
}

func (s *Server) runTask(parent context.Context, id string, manual bool) {
	s.mu.Lock()
	if _, exists := s.running[id]; exists {
		s.mu.Unlock()
		return
	}
	task, _ := s.findTask(id)
	if task == nil {
		s.mu.Unlock()
		return
	}
	taskCopy := *task
	ctx, cancel := context.WithCancel(parent)
	s.running[id] = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.running, id)
		s.mu.Unlock()
	}()

	started := time.Now()
	run := Run{ID: strconv.FormatInt(started.UnixNano(), 10), TaskID: id, StartedAt: started, State: "running"}
	s.publishEvent(map[string]any{"type": "run.started", "task_id": id, "run_id": run.ID})
	s.updateTaskState(id, func(t *Task) {
		t.Status.State = "running"
		// Manual runs must also advance LastRunAt; otherwise a task with no
		// previous scheduled run is immediately picked up again by the scheduler.
		t.LastRunAt = &started
		t.Status.Runs++
	})

	attempts := taskCopy.Retry.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	var local map[string]any
	var result any
	var diff DiffSummary
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = ctx.Err(); err != nil {
			break
		}
		engine := workflow.New(workflow.Options{Custom: s.cfg.Custom, Caller: s.caller, Strict: true})
		local, err = engine.Execute(ctx, taskCopy.Workflow, taskCopy.Local)
		if err == nil {
			result = any(local)
			if taskCopy.Document.Source != "" {
				if value, found := mapPath(local, taskCopy.Document.Source); found {
					result = value
				}
			}
			diff, err = s.persistDocument(taskCopy, result)
		}
		if err == nil {
			break
		}
		if attempt < attempts {
			backoff := taskCopy.Retry.BackoffSeconds
			if backoff <= 0 {
				backoff = 1
			}
			shift := attempt - 1
			if shift > 6 {
				shift = 6
			}
			delay := time.Duration(backoff*(1<<shift)) * time.Second
			if delay > 5*time.Minute {
				delay = 5 * time.Minute
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				err = ctx.Err()
			case <-timer.C:
			}
		}
	}
	run.FinishedAt = time.Now()
	if err != nil {
		run.State, run.Error = "failed", err.Error()
		s.updateTaskState(id, func(t *Task) { t.Status.State = "failed"; t.Status.Failures++; t.LastError = err.Error() })
		s.publishEvent(map[string]any{"type": "run.failed", "task_id": id, "run_id": run.ID, "error": err.Error()})
		if taskCopy.Notify.OnError {
			s.sendNotification(ctx, taskCopy, "error", nil, err)
		}
	} else {
		run.State, run.Diff = "success", &diff
		s.updateTaskState(id, func(t *Task) {
			t.Status.State = "success"
			t.Status.Successes++
			t.LastError = ""
			t.Status.LastDiff = &diff
		})
		s.publishEvent(map[string]any{"type": "run.completed", "task_id": id, "run_id": run.ID, "diff": diff})
		if taskCopy.Notify.OnChange && !diff.Baseline && (diff.Added > 0 || diff.Updated > 0 || diff.Deleted > 0) {
			s.sendNotification(ctx, taskCopy, "change", &diff, nil)
		}
	}
	s.mu.Lock()
	s.state.Runs = append(s.state.Runs, run)
	if len(s.state.Runs) > 500 {
		s.state.Runs = s.state.Runs[len(s.state.Runs)-500:]
	}
	_ = s.saveLocked()
	s.mu.Unlock()
}

func (s *Server) sendNotification(ctx context.Context, task Task, kind string, diff *DiffSummary, runErr error) {
	stream := task.Notify.Stream
	service := task.Notify.Service
	if stream == "" || service == "" {
		log.Printf("task %s notification %s skipped: notify.stream and notify.service are required", task.ID, kind)
		return
	}
	payload := map[string]any{"task_id": task.ID, "task_name": task.Name, "kind": kind, "to": task.Notify.To}
	if diff != nil {
		payload["diff"] = diff
	}
	if runErr != nil {
		payload["error"] = runErr.Error()
	}
	if _, err := s.caller.Call(ctx, stream, service, payload); err != nil {
		log.Printf("task %s notification failed: %v", task.ID, err)
	}
}

func (s *Server) updateTaskState(id string, update func(*Task)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if task, _ := s.findTask(id); task != nil {
		update(task)
		task.UpdatedAt = time.Now()
		_ = s.saveLocked()
	}
}

func (s *Server) persistDocument(task Task, current any) (DiffSummary, error) {
	dir, err := s.documentDirectory(task)
	if err != nil {
		return DiffSummary{}, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0755); err != nil {
		return DiffSummary{}, err
	}
	baselinePath := filepath.Join(dir, "baseline.json")
	currentPath := filepath.Join(dir, "current.json")
	var previous any
	baseline := false
	if data, readErr := os.ReadFile(currentPath); readErr == nil {
		if err := json.Unmarshal(data, &previous); err != nil {
			return DiffSummary{}, fmt.Errorf("parse current document: %w", err)
		}
	} else if os.IsNotExist(readErr) {
		baseline = true
	} else {
		return DiffSummary{}, readErr
	}
	diff := compareValues(previous, current, task.Document)
	diff.Baseline = baseline

	// Render and validate every artifact before changing the persisted state. A
	// malformed template must not advance current/baseline or create a snapshot.
	markdown := "```json\n" + mustJSONIndent(current) + "\n```\n"
	if task.Document.Template != "" {
		tpl, err := template.New("document").Parse(task.Document.Template)
		if err != nil {
			return diff, fmt.Errorf("parse markdown template: %w", err)
		}
		var rendered bytes.Buffer
		if err := tpl.Execute(&rendered, map[string]any{"Data": current, "Diff": diff, "Task": task}); err != nil {
			return diff, fmt.Errorf("render markdown template: %w", err)
		}
		markdown = rendered.String() + "\n"
	}

	if baseline {
		if err := atomicJSONWrite(baselinePath, current); err != nil {
			return diff, err
		}
	}
	if err := atomicJSONWrite(currentPath, current); err != nil {
		return diff, err
	}
	if err := atomicTextWrite(filepath.Join(dir, "current.md"), markdown); err != nil {
		return diff, err
	}
	snapshot := filepath.Join(dir, "snapshots", time.Now().Format("20060102-150405.000000000")+".json")
	if err := atomicJSONWrite(snapshot, current); err != nil {
		return diff, err
	}
	if err := pruneSnapshots(filepath.Join(dir, "snapshots"), task.Document.KeepSnapshots); err != nil {
		return diff, err
	}
	return diff, nil
}

func pruneSnapshots(dir string, keep int) error {
	if keep <= 0 {
		keep = 100
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	files := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for len(files) > keep {
		if err := os.Remove(filepath.Join(dir, files[0].Name())); err != nil {
			return err
		}
		files = files[1:]
	}
	return nil
}

func compareValues(before, after any, cfg DocumentConfig) DiffSummary {
	if before == nil {
		return DiffSummary{Added: 1}
	}
	if reflectJSONEqual(before, after) {
		return DiffSummary{Unchanged: 1}
	}
	if cfg.Identity != "" {
		if b, bok := recordsByIdentity(before, cfg.Identity); bok {
			if a, aok := recordsByIdentity(after, cfg.Identity); aok {
				out := DiffSummary{}
				for id, value := range a {
					old, exists := b[id]
					if !exists {
						out.Added++
					} else if !reflectJSONEqual(projectFields(old, cfg.Compare), projectFields(value, cfg.Compare)) {
						out.Updated++
						out.Changes = append(out.Changes, FieldChange{Identity: id, Kind: "updated", Before: old, After: value})
					} else {
						out.Unchanged++
					}
				}
				for id, value := range b {
					if _, exists := a[id]; !exists {
						out.Deleted++
						out.Changes = append(out.Changes, FieldChange{Identity: id, Kind: "deleted", Before: value})
					}
				}
				return out
			}
		}
	}
	return DiffSummary{Updated: 1, Changes: []FieldChange{{Kind: "updated", Before: before, After: after}}}
}

func recordsByIdentity(value any, identity string) (map[string]any, bool) {
	arr, ok := value.([]any)
	if !ok {
		arr = []any{value}
	}
	out := make(map[string]any, len(arr))
	for _, item := range arr {
		id, found := mapPath(item, identity)
		if !found {
			return nil, false
		}
		key := fmt.Sprint(id)
		if _, dup := out[key]; dup {
			return nil, false
		}
		out[key] = item
	}
	return out, true
}
func projectFields(v any, fields []string) any {
	if len(fields) == 0 {
		return v
	}
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	out := map[string]any{}
	for _, field := range fields {
		if value, exists := m[field]; exists {
			out[field] = value
		}
	}
	return out
}
func reflectJSONEqual(a, b any) bool           { return mustJSON(a) == mustJSON(b) }
func mustJSON(v any) string                    { b, _ := json.Marshal(v); return string(b) }
func atomicJSONWrite(path string, v any) error { return atomicTextWrite(path, mustJSONIndent(v)) }
func mustJSONIndent(v any) string              { b, _ := json.MarshalIndent(v, "", "  "); return string(b) }
func atomicTextWrite(path, text string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(text), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func safeName(value string) string {
	value = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
	if value == "" {
		return "task"
	}
	return value
}
func mapPath(root any, path string) (any, bool) {
	parts := strings.FieldsFunc(strings.ReplaceAll(path, "[", "."), func(r rune) bool { return r == '.' || r == ']' })
	cur := root
	for _, part := range parts {
		if part == "" {
			continue
		}
		if m, ok := cur.(map[string]any); ok {
			var exists bool
			cur, exists = m[part]
			if !exists {
				return nil, false
			}
			continue
		}
		if a, ok := cur.([]any); ok {
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(a) {
				return nil, false
			}
			cur = a[idx]
			continue
		}
		return nil, false
	}
	return cur, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
