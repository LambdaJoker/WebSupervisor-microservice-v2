package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Workflow is the executable JSON workflow document.
type Workflow struct {
	Jobs      []Job               `json:"jobs"`
	Functions map[string]Function `json:"functions,omitempty"`
}

// Job describes one workflow operation. Payload accepts the historical "playload" spelling too.
type Job struct {
	Stream   string `json:"stream,omitempty"`
	Service  string `json:"service"`
	Payload  any    `json:"payload,omitempty"`
	Playload any    `json:"playload,omitempty"`
	ResultTo string `json:"resultto,omitempty"`
}

// Function is a named, isolated workflow subprogram.
type Function struct {
	Name       string `json:"name,omitempty"`
	Parameters any    `json:"parameters,omitempty"`
	Jobs       []Job  `json:"jobs"`
	ReturnTo   string `json:"returnto,omitempty"`
}

// ServiceCaller is the seam for external stream services.
type ServiceCaller interface {
	Call(ctx context.Context, stream, service string, payload map[string]any) (any, error)
}

type Options struct {
	Custom       map[string]any
	Caller       ServiceCaller
	MaxLoop      int
	MaxCallDepth int
	Strict       bool
}

type Engine struct {
	custom       map[string]any
	caller       ServiceCaller
	maxLoop      int
	maxCallDepth int
	strict       bool
}

func New(options Options) *Engine {
	maxLoop := options.MaxLoop
	if maxLoop <= 0 {
		maxLoop = 10000
	}
	maxDepth := options.MaxCallDepth
	if maxDepth <= 0 {
		maxDepth = 100
	}
	return &Engine{custom: options.Custom, caller: options.Caller, maxLoop: maxLoop, maxCallDepth: maxDepth, strict: options.Strict}
}

func Parse(data []byte) (Workflow, error) {
	var wf Workflow
	if err := json.Unmarshal(data, &wf); err != nil {
		return wf, fmt.Errorf("parse workflow: %w", err)
	}
	if err := Validate(wf); err != nil {
		return wf, err
	}
	return wf, nil
}

func Validate(wf Workflow) error {
	if wf.Jobs == nil {
		return errors.New("workflow.jobs is required")
	}
	for i, job := range wf.Jobs {
		if strings.TrimSpace(job.Service) == "" {
			return fmt.Errorf("jobs[%d].service is required", i)
		}
		if job.Payload != nil && job.Playload != nil {
			return fmt.Errorf("jobs[%d] cannot contain both payload and playload", i)
		}
		if _, err := validateJob(job, fmt.Sprintf("jobs[%d]", i)); err != nil {
			return err
		}
	}
	for name, fn := range wf.Functions {
		if strings.TrimSpace(name) == "" {
			return errors.New("function name cannot be empty")
		}
		if _, err := validateJobs(fn.Jobs, "function "+name); err != nil {
			return err
		}
	}
	return nil
}

func validateNestedJobs(value any, path string) error {
	jobs, err := decodeJobs(value)
	if err != nil {
		return fmt.Errorf("%s must be an array: %w", path, err)
	}
	for i, job := range jobs {
		if strings.TrimSpace(job.Service) == "" {
			return fmt.Errorf("%s[%d].service is required", path, i)
		}
		if _, err := validateJob(job, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func validateJobs(jobs []Job, prefix string) (bool, error) {
	for i, job := range jobs {
		if strings.TrimSpace(job.Service) == "" {
			return false, fmt.Errorf("%s.jobs[%d].service is required", prefix, i)
		}
		if _, err := validateJob(job, fmt.Sprintf("%s.jobs[%d]", prefix, i)); err != nil {
			return false, err
		}
	}
	return true, nil
}

func validateJob(job Job, path string) (bool, error) {
	payload := job.Payload
	if payload == nil {
		payload = job.Playload
	}
	switch strings.ToLower(job.Service) {
	case "if":
		m, ok := payload.(map[string]any)
		if !ok {
			return false, fmt.Errorf("%s.payload must be an object", path)
		}
		if _, ok := m["value"]; !ok {
			return false, fmt.Errorf("%s.payload.value is required", path)
		}
		for _, key := range []string{"truejobs", "falsejobs"} {
			if v, exists := m[key]; exists {
				if err := validateNestedJobs(v, path+".payload."+key); err != nil {
					return false, err
				}
			}
		}
	case "while":
		m, ok := payload.(map[string]any)
		if !ok {
			return false, fmt.Errorf("%s.payload must be an object", path)
		}
		if _, ok := m["value"]; !ok {
			return false, fmt.Errorf("%s.payload.value is required", path)
		}
		if v, exists := m["jobs"]; exists {
			if err := validateNestedJobs(v, path+".payload.jobs"); err != nil {
				return false, err
			}
		}
	case "func":
		m, ok := payload.(map[string]any)
		if !ok {
			return false, fmt.Errorf("%s.payload must be an object", path)
		}
		if v, exists := m["jobs"]; exists {
			if err := validateNestedJobs(v, path+".payload.jobs"); err != nil {
				return false, err
			}
		}
	case "operators":
		m, ok := payload.(map[string]any)
		if !ok {
			return false, fmt.Errorf("%s.payload must be an object", path)
		}
		if s, ok := m["operator"].(string); !ok || strings.TrimSpace(s) == "" {
			return false, fmt.Errorf("%s.payload.operator is required", path)
		}
	}
	return true, nil
}

func (e *Engine) Execute(ctx context.Context, wf Workflow, initial map[string]any) (map[string]any, error) {
	if err := Validate(wf); err != nil {
		return nil, err
	}
	local := cloneMap(initial)
	if local == nil {
		local = map[string]any{}
	}
	if wf.Functions == nil {
		wf.Functions = make(map[string]Function)
	}
	if err := e.executeJobs(ctx, wf, wf.Jobs, local, 0); err != nil {
		return nil, err
	}
	return local, nil
}

func (e *Engine) executeJobs(ctx context.Context, wf Workflow, jobs []Job, local map[string]any, depth int) error {
	if depth > e.maxCallDepth {
		return fmt.Errorf("function call depth exceeds %d", e.maxCallDepth)
	}
	for i, job := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.executeJob(ctx, wf, job, local, depth); err != nil {
			return fmt.Errorf("job[%d] %s: %w", i, job.Service, err)
		}
	}
	return nil
}

func (e *Engine) executeJob(ctx context.Context, wf Workflow, job Job, local map[string]any, depth int) error {
	raw := job.Payload
	if raw == nil {
		raw = job.Playload
	}
	service := strings.ToLower(strings.TrimSpace(job.Service))
	// Control-flow and function definitions contain deferred jobs. Resolve only
	// their scalar control fields; nested jobs must see local state at execution time.
	if service == "func" {
		result, err := e.defineFunction(wf, raw, job, local)
		if err != nil {
			return err
		}
		if strings.TrimSpace(job.ResultTo) != "" {
			return setPath(local, job.ResultTo, result)
		}
		return nil
	}
	if service == "if" || service == "while" {
		result, err := e.executeControl(ctx, wf, service, raw, local, depth)
		if err != nil {
			return err
		}
		if strings.TrimSpace(job.ResultTo) != "" {
			return setPath(local, job.ResultTo, result)
		}
		return nil
	}
	payload, err := e.resolve(raw, local)
	if err != nil {
		return err
	}
	var result any
	switch service {
	case "get":
		result, err = e.serviceGet(payload, local)
	case "set":
		if m, ok := payload.(map[string]any); ok {
			if value, exists := m["value"]; exists {
				result = value
			} else {
				result = payload
			}
		} else {
			result = payload
		}
	case "execfunc":
		result, err = e.executeFunction(ctx, wf, payload, local, depth)
	case "operators", "operator", "compare":
		result, err = e.serviceOperator(payload)
	default:
		if e.caller == nil {
			return fmt.Errorf("external service %q requires a ServiceCaller", job.Service)
		}
		object, ok := payload.(map[string]any)
		if !ok {
			return fmt.Errorf("external service payload must be an object")
		}
		result, err = e.caller.Call(ctx, job.Stream, job.Service, object)
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(job.ResultTo) != "" {
		return setPath(local, job.ResultTo, result)
	}
	return nil
}

func (e *Engine) executeControl(ctx context.Context, wf Workflow, service string, raw any, local map[string]any, depth int) (any, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s payload must be an object", service)
	}
	if service == "if" {
		condition, err := e.evaluate(m["value"], local)
		if err != nil {
			return nil, err
		}
		truth, err := isTruthy(condition)
		if err != nil {
			return nil, err
		}
		key := "falsejobs"
		if truth {
			key = "truejobs"
		}
		jobs, err := decodeJobs(m[key])
		if err != nil {
			return nil, err
		}
		if err := e.executeJobs(ctx, wf, jobs, local, depth); err != nil {
			return nil, err
		}
		return truth, nil
	}

	jobs, err := decodeJobs(m["jobs"])
	if err != nil {
		return nil, err
	}
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		condition, err := e.evaluate(m["value"], local)
		if err != nil {
			return nil, err
		}
		truth, err := isTruthy(condition)
		if err != nil {
			return nil, err
		}
		if !truth {
			return count, nil
		}
		if count >= e.maxLoop {
			return nil, fmt.Errorf("while loop exceeds %d iterations", e.maxLoop)
		}
		if err := e.executeJobs(ctx, wf, jobs, local, depth); err != nil {
			return nil, err
		}
		count++
	}
}

func (e *Engine) serviceGet(payload any, local map[string]any) (any, error) {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("get payload must be an object")
	}
	pathValue, exists := m["path"]
	if !exists {
		return nil, errors.New("get.path is required")
	}
	path, pathOK := pathValue.(string)
	if pathOK && strings.TrimSpace(path) != "" {
		value, found := getPath(local, path)
		if found {
			return value, nil
		}
	} else if parts, ok := pathValue.([]any); ok {
		value, found := getPathParts(local, parts)
		if found {
			return value, nil
		}
	} else {
		return nil, errors.New("get.path must be a string or array")
	}
	if pathOK && strings.TrimSpace(path) == "" {
		return nil, errors.New("get.path is required")
	}
	if value, exists := m["default"]; exists {
		return value, nil
	}
	return nil, fmt.Errorf("local path %q not found", path)
}

func (e *Engine) defineFunction(wf Workflow, payload any, job Job, local map[string]any) (any, error) {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("func payload must be an object")
	}
	rm := make(map[string]any, len(m))
	for _, key := range []string{"name", "parameters", "returnto"} {
		if value, exists := m[key]; exists {
			resolved, err := e.resolve(value, local)
			if err != nil {
				return nil, err
			}
			rm[key] = resolved
		}
	}
	name, _ := rm["name"].(string)
	if name == "" {
		name = job.ResultTo
	}
	if name == "" {
		return nil, errors.New("func.name or resultto is required")
	}
	jobs, err := decodeJobs(m["jobs"])
	if err != nil {
		return nil, err
	}
	fn := Function{Name: name, Parameters: rm["parameters"], Jobs: jobs}
	if returnTo, ok := rm["returnto"].(string); ok {
		fn.ReturnTo = returnTo
	}
	wf.Functions[name] = fn
	return name, nil
}

func (e *Engine) executeFunction(ctx context.Context, wf Workflow, payload any, parent map[string]any, depth int) (any, error) {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("execfunc payload must be an object")
	}
	name, ok := m["func"].(string)
	if !ok || strings.TrimSpace(name) == "" {
		return nil, errors.New("execfunc.func is required")
	}
	fn, exists := wf.Functions[name]
	if !exists {
		return nil, fmt.Errorf("function %q not found", name)
	}
	child := cloneMap(parent)
	if child == nil {
		child = map[string]any{}
	}
	if params, exists := m["parameters"]; exists {
		if err := bindParameters(child, fn.Parameters, params, e, parent); err != nil {
			return nil, err
		}
	}
	if err := e.executeJobs(ctx, wf, fn.Jobs, child, depth+1); err != nil {
		return nil, err
	}
	if fn.ReturnTo == "" {
		return child, nil
	}
	value, found := getPath(child, fn.ReturnTo)
	if !found {
		return nil, fmt.Errorf("function %q returnto path %q not found", name, fn.ReturnTo)
	}
	return value, nil
}

func bindParameters(child map[string]any, definitions any, values any, e *Engine, parent map[string]any) error {
	resolved, err := e.resolve(values, parent)
	if err != nil {
		return err
	}
	switch defs := definitions.(type) {
	case []any:
		vals, ok := resolved.([]any)
		if !ok {
			return errors.New("function parameters must be an array")
		}
		for i, def := range defs {
			name, defaultValue, ok := parameterDefinition(def)
			if !ok {
				return fmt.Errorf("invalid function parameter definition at %d", i)
			}
			if i < len(vals) {
				child[name] = vals[i]
			} else if defaultValue != nil {
				child[name] = defaultValue
			} else {
				return fmt.Errorf("missing function parameter %q", name)
			}
		}
	case map[string]any:
		vals, ok := resolved.(map[string]any)
		if !ok {
			return errors.New("function parameters must be an object")
		}
		for name, def := range defs {
			defaultValue, hasDefault := defValue(def)
			if value, exists := vals[name]; exists {
				child[name] = value
			} else if hasDefault {
				child[name] = defaultValue
			} else {
				return fmt.Errorf("missing function parameter %q", name)
			}
		}
	case nil:
		if resolved != nil {
			if vals, ok := resolved.(map[string]any); ok {
				for k, v := range vals {
					child[k] = v
				}
			}
		}
	default:
		return errors.New("function parameters definition must be an array or object")
	}
	return nil
}

func parameterDefinition(v any) (string, any, bool) {
	switch x := v.(type) {
	case string:
		return x, nil, x != ""
	case map[string]any:
		name, _ := x["name"].(string)
		if name == "" {
			return "", nil, false
		}
		d, _ := x["default"]
		return name, d, true
	default:
		return "", nil, false
	}
}
func defValue(v any) (any, bool) {
	if m, ok := v.(map[string]any); ok {
		d, exists := m["default"]
		return d, exists
	}
	return v, true
}

func (e *Engine) evaluate(value any, local map[string]any) (any, error) {
	resolved, err := e.resolve(value, local)
	if err != nil {
		return nil, err
	}
	if m, ok := resolved.(map[string]any); ok {
		if _, exists := m["operator"]; exists {
			return e.serviceOperator(m)
		}
	}
	return resolved, nil
}

func isTruthy(value any) (bool, error) {
	switch v := value.(type) {
	case nil:
		return false, nil
	case bool:
		return v, nil
	case string:
		return v != "" && !strings.EqualFold(v, "false") && v != "0", nil
	case float64:
		return v != 0 && !math.IsNaN(v), nil
	case int:
		return v != 0, nil
	case []any:
		return len(v) > 0, nil
	case map[string]any:
		return len(v) > 0, nil
	default:
		return !reflect.ValueOf(v).IsZero(), nil
	}
}

func (e *Engine) serviceIf(ctx context.Context, wf Workflow, payload any, local map[string]any, depth int) (any, error) {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("if payload must be an object")
	}
	condition, err := e.truthy(m["value"], local)
	if err != nil {
		return nil, err
	}
	key := "falsejobs"
	if condition {
		key = "truejobs"
	}
	jobs, err := decodeJobs(m[key])
	if err != nil {
		return nil, err
	}
	if err := e.executeJobs(ctx, wf, jobs, local, depth); err != nil {
		return nil, err
	}
	return condition, nil
}

func (e *Engine) serviceWhile(ctx context.Context, wf Workflow, payload any, local map[string]any, depth int) (any, error) {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("while payload must be an object")
	}
	jobs, err := decodeJobs(m["jobs"])
	if err != nil {
		return nil, err
	}
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		condition, err := e.truthy(m["value"], local)
		if err != nil {
			return nil, err
		}
		if !condition {
			return count, nil
		}
		if count >= e.maxLoop {
			return nil, fmt.Errorf("while loop exceeds %d iterations", e.maxLoop)
		}
		if err := e.executeJobs(ctx, wf, jobs, local, depth); err != nil {
			return nil, err
		}
		count++
	}
}

func (e *Engine) truthy(value any, local map[string]any) (bool, error) {
	resolved, err := e.resolve(value, local)
	if err != nil {
		return false, err
	}
	switch v := resolved.(type) {
	case nil:
		return false, nil
	case bool:
		return v, nil
	case string:
		return v != "" && !strings.EqualFold(v, "false") && v != "0", nil
	case float64:
		return v != 0 && !math.IsNaN(v), nil
	case int:
		return v != 0, nil
	case []any:
		return len(v) > 0, nil
	case map[string]any:
		return len(v) > 0, nil
	default:
		return !reflect.ValueOf(v).IsZero(), nil
	}
}

func (e *Engine) serviceOperator(payload any) (any, error) {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("operators payload must be an object")
	}
	op, _ := m["operator"].(string)
	op = strings.ToLower(strings.TrimSpace(op))
	left, lok := m["parameter1"]
	if !lok {
		left, lok = m["left"]
	}
	right, rok := m["parameter2"]
	if !rok {
		right, rok = m["parameters2"]
		if !rok {
			right, rok = m["right"]
		}
	}
	if op == "len" {
		if !lok {
			return nil, errors.New("len requires parameter1")
		}
		return lengthOf(left), nil
	}
	if !lok {
		return nil, errors.New("operator parameter1 is required")
	}
	if op == "!" || op == "not" {
		return !isTruthyValue(left), nil
	}
	if !rok {
		return nil, errors.New("operator parameter2 is required")
	}
	switch op {
	case "+", "-", "*", "/", "%":
		return arithmetic(op, left, right)
	case "==", "=", "eq":
		return reflect.DeepEqual(normalizeNumber(left), normalizeNumber(right)), nil
	case "!=", "ne":
		return !reflect.DeepEqual(normalizeNumber(left), normalizeNumber(right)), nil
	case ">", ">=", "<", "<=":
		return compare(op, left, right)
	case "&&", "and":
		return isTruthyValue(left) && isTruthyValue(right), nil
	case "||", "or":
		return isTruthyValue(left) || isTruthyValue(right), nil
	}
	return nil, fmt.Errorf("unsupported operator %q", op)
}

func arithmetic(op string, a, b any) (any, error) {
	x, ok := number(a)
	if !ok {
		return nil, fmt.Errorf("parameter1 %v is not numeric", a)
	}
	y, ok := number(b)
	if !ok {
		return nil, fmt.Errorf("parameter2 %v is not numeric", b)
	}
	switch op {
	case "+":
		return x + y, nil
	case "-":
		return x - y, nil
	case "*":
		return x * y, nil
	case "/":
		if y == 0 {
			return nil, errors.New("division by zero")
		}
		return x / y, nil
	case "%":
		if y == 0 {
			return nil, errors.New("modulo by zero")
		}
		return math.Mod(x, y), nil
	}
	return nil, fmt.Errorf("unsupported arithmetic operator %q", op)
}
func compare(op string, a, b any) (bool, error) {
	x, xok := number(a)
	y, yok := number(b)
	if xok && yok {
		switch op {
		case ">":
			return x > y, nil
		case ">=":
			return x >= y, nil
		case "<":
			return x < y, nil
		case "<=":
			return x <= y, nil
		}
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		switch op {
		case ">":
			return as > bs, nil
		case ">=":
			return as >= bs, nil
		case "<":
			return as < bs, nil
		case "<=":
			return as <= bs, nil
		}
	}
	return false, errors.New("comparison parameters must both be numbers or strings")
}
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case json.Number:
		f, e := n.Float64()
		return f, e == nil
	case string:
		f, e := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, e == nil
	}
	return 0, false
}
func normalizeNumber(v any) any {
	if n, ok := number(v); ok {
		return n
	}
	return v
}
func lengthOf(v any) int {
	switch x := v.(type) {
	case string:
		return len([]rune(x))
	case []any:
		return len(x)
	case map[string]any:
		return len(x)
	default:
		return 0
	}
}
func isTruthyValue(v any) bool { b, _ := New(Options{}).truthy(v, map[string]any{}); return b }

var tokenPattern = regexp.MustCompile(`^(\$|#|%)\{([^{}]+)\}$`)
var embeddedPattern = regexp.MustCompile(`(\$|#|%)\{([^{}]+)\}`)

func (e *Engine) resolve(value any, local map[string]any) (any, error) {
	switch v := value.(type) {
	case string:
		return e.resolveString(v, local)
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			r, err := e.resolve(x, local)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, x := range v {
			r, err := e.resolve(x, local)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	default:
		return value, nil
	}
}
func (e *Engine) resolveString(s string, local map[string]any) (any, error) {
	if m := tokenPattern.FindStringSubmatch(s); m != nil {
		return e.lookupToken(m[1], m[2], local)
	}
	var firstErr error
	out := embeddedPattern.ReplaceAllStringFunc(s, func(token string) string {
		m := embeddedPattern.FindStringSubmatch(token)
		v, err := e.lookupToken(m[1], m[2], local)
		if err != nil {
			firstErr = err
			return token
		}
		return fmt.Sprint(v)
	})
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}
func (e *Engine) lookupToken(prefix, path string, local map[string]any) (any, error) {
	var (
		root any
		ok   bool
	)
	switch prefix {
	case "$":
		varName := path
		root, ok = os.LookupEnv(varName)
		if !ok {
			if e.strict {
				return nil, fmt.Errorf("environment variable %q is not set", varName)
			}
			return "", nil
		}
		return root, nil
	case "#":
		root, ok = getPath(e.custom, path)
	case "%":
		root, ok = getPath(local, path)
	}
	if !ok {
		if e.strict {
			return nil, fmt.Errorf("variable %s{%s} is not defined", prefix, path)
		}
		return "", nil
	}
	return root, nil
}

func decodeJobs(value any) ([]Job, error) {
	if value == nil {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var jobs []Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		return nil, fmt.Errorf("jobs must be an array: %w", err)
	}
	return jobs, nil
}
func cloneMap(v map[string]any) map[string]any {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var out map[string]any
	if json.Unmarshal(b, &out) != nil {
		return nil
	}
	return out
}

func getPathParts(root any, parts []any) (any, bool) {
	cur := root
	for _, part := range parts {
		switch p := part.(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			cur, ok = m[p]
			if !ok {
				return nil, false
			}
		case float64:
			a, ok := cur.([]any)
			idx := int(p)
			if !ok || p != float64(idx) || idx < 0 || idx >= len(a) {
				return nil, false
			}
			cur = a[idx]
		case int:
			a, ok := cur.([]any)
			if !ok || p < 0 || p >= len(a) {
				return nil, false
			}
			cur = a[p]
		default:
			return nil, false
		}
	}
	return cur, true
}

func getPath(root any, path string) (any, bool) {
	parts, err := parsePath(path)
	if err != nil {
		return nil, false
	}
	cur := root
	for i := 0; i < len(parts); {
		switch p := parts[i].(type) {
		case string:
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			if value, exists := m[p]; exists {
				cur = value
				i++
				continue
			}
			// External parsers may use dotted keys (for example, "data.list")
			// inside a result map. Prefer regular nested lookup above, then fall
			// back to the longest dotted key that matches the remaining path.
			end := i
			for end < len(parts) {
				if _, isString := parts[end].(string); !isString {
					break
				}
				end++
			}
			matched := false
			for candidateEnd := end; candidateEnd > i+1; candidateEnd-- {
				keyParts := make([]string, candidateEnd-i)
				for j := i; j < candidateEnd; j++ {
					keyParts[j-i] = parts[j].(string)
				}
				if value, exists := m[strings.Join(keyParts, ".")]; exists {
					cur = value
					i = candidateEnd
					matched = true
					break
				}
			}
			if !matched {
				return nil, false
			}
		case int:
			a, ok := cur.([]any)
			if !ok || p < 0 || p >= len(a) {
				return nil, false
			}
			cur = a[p]
			i++
		default:
			return nil, false
		}
	}
	return cur, true
}
func setPath(root map[string]any, path string, value any) error {
	parts, err := parsePath(path)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return errors.New("path cannot be empty")
	}
	for i, part := range parts {
		key, ok := part.(string)
		if !ok {
			return errors.New("resultto must use object path, not array index")
		}
		if i == len(parts)-1 {
			root[key] = value
			return nil
		}
		next, exists := root[key]
		if !exists || next == nil {
			next = map[string]any{}
			root[key] = next
		}
		m, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("path %q crosses non-object at %q", path, key)
		}
		root = m
	}
	return nil
}
func parsePath(path string) ([]any, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("path is empty")
	}
	path = strings.ReplaceAll(path, "[", ".[")
	path = strings.ReplaceAll(path, "..", ".")
	raw := strings.Split(path, ".")
	parts := make([]any, 0, len(raw))
	for _, p := range raw {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.HasPrefix(p, "[") && strings.HasSuffix(p, "]") {
			n, e := strconv.Atoi(p[1 : len(p)-1])
			if e != nil {
				return nil, fmt.Errorf("invalid array index %q", p)
			}
			parts = append(parts, n)
		} else {
			parts = append(parts, p)
		}
	}
	return parts, nil
}
