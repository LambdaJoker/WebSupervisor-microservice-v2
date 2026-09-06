use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

mod desktop;

const INDEX_HTML: &str = include_str!("index.html");
const ICON_SVG: &str = include_str!("icon.svg");
const RECENT_LIMIT: usize = 20;
const LOG_LINE_LIMIT: usize = 200;
const LOG_BYTE_LIMIT: usize = 32 * 1024;

#[derive(Clone, Deserialize, Serialize)]
struct Config {
    listen_addr: String,
    gateway_addr: String,
    #[serde(default = "default_manager_addr")]
    manager_addr: String,
    crawl_timeout_ms: i64,
    #[serde(default)]
    services: Vec<ServiceConfig>,
    #[serde(skip)]
    base_dir: PathBuf,
}

#[derive(Clone, Deserialize, Serialize)]
struct ServiceConfig {
    name: String,
    stream: String,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    config_path: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct CrawlRecord {
    url: String,
    method: String,
    status: i64,
    content: String,
    created_at: u64,
}

enum ProcessHandle {
    Spawned(Child),
    Adopted(u32),
}

struct ManagedProcess {
    handle: ProcessHandle,
    pid: u32,
}

struct LogBuffer {
    lines: VecDeque<String>,
    bytes: usize,
}

impl LogBuffer {
    fn new() -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
        }
    }

    fn push(&mut self, line: String) {
        self.bytes += line.len();
        self.lines.push_back(line);
        while self.lines.len() > LOG_LINE_LIMIT || self.bytes > LOG_BYTE_LIMIT {
            if let Some(old) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(old.len());
            } else {
                break;
            }
        }
    }
}

#[derive(Clone)]
struct AppState {
    config: Config,
    recent: Arc<Mutex<VecDeque<CrawlRecord>>>,
    processes: Arc<Mutex<HashMap<String, ManagedProcess>>>,
    logs: Arc<Mutex<HashMap<String, LogBuffer>>>,
    history_path: PathBuf,
    history_lock: Arc<Mutex<()>>,
}

#[derive(Deserialize)]
struct CrawlRequest {
    url: String,
    #[serde(default = "default_method")]
    method: String,
    #[serde(default = "default_object")]
    headers: Value,
    #[serde(default = "default_object")]
    body: Value,
    #[serde(default)]
    str_payload: String,
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_manager_addr() -> String {
    "http://127.0.0.1:18081".to_string()
}

fn default_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn config_path() -> PathBuf {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let requested = ["-config_path", "-config"]
        .iter()
        .find_map(|flag| {
            args.iter()
                .position(|arg| arg == flag)
                .and_then(|index| args.get(index + 1))
        })
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.json"));
    prefer_local_config(&requested)
}

/// Loads the nearest `.env` file before the UI config and child services start.
/// Explicit environment variables always win over values from `.env`.
fn load_dotenv(config_path: &Path) {
    let mut directories = Vec::new();
    let mut directory = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    loop {
        directories.push(directory.clone());
        let Some(parent) = directory.parent() else {
            break;
        };
        if parent == directory {
            break;
        }
        directory = parent.to_path_buf();
    }

    let Some(env_path) = directories
        .into_iter()
        .map(|directory| directory.join(".env"))
        .find(|path| path.is_file())
    else {
        return;
    };

    let Ok(content) = fs::read_to_string(&env_path) else {
        eprintln!("读取 .env 失败: {}", env_path.display());
        return;
    };

    let mut loaded = 0;
    for line in content.lines() {
        let line = line.trim().trim_start_matches('\u{feff}');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, raw_value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty()
            || !key
                .chars()
                .all(|character| character == '_' || character.is_ascii_alphanumeric())
        {
            continue;
        }
        if std::env::var_os(key).is_some() {
            continue;
        }
        let value = raw_value.trim();
        let value = if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            &value[1..value.len() - 1]
        } else {
            value
        };
        std::env::set_var(key, value);
        loaded += 1;
    }
    println!("已加载 .env: {}（{} 项）", env_path.display(), loaded);
}

fn prefer_local_config(requested: &Path) -> PathBuf {
    if requested.file_name().and_then(|name| name.to_str()) != Some("config.json") {
        return requested.to_path_buf();
    }
    let local = requested.with_file_name("config.local.json");
    if local.is_file() {
        local
    } else {
        requested.to_path_buf()
    }
}

fn has_flag(flag: &str) -> bool {
    std::env::args().skip(1).any(|arg| arg == flag)
}

fn main() {
    let config_path = config_path();
    load_dotenv(&config_path);
    let mut config: Config = read_json(&config_path).unwrap_or_else(|err| {
        eprintln!("读取配置失败: {err}");
        std::process::exit(1);
    });
    config.base_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));

    let history_path = config.base_dir.join("data").join("crawl-history.jsonl");
    let state = AppState {
        config,
        recent: Arc::new(Mutex::new(VecDeque::with_capacity(RECENT_LIMIT))),
        processes: Arc::new(Mutex::new(HashMap::new())),
        logs: Arc::new(Mutex::new(HashMap::new())),
        history_path,
        history_lock: Arc::new(Mutex::new(())),
    };
    load_recent_history(&state);

    let http_state = state.clone();
    let (http_ready_tx, http_ready_rx) = mpsc::channel();
    thread::spawn(move || run_http_server(http_state, http_ready_tx));
    match http_ready_rx.recv_timeout(Duration::from_secs(3)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("启动 UI HTTP API 失败: {error}");
            return;
        }
        Err(_) => {
            eprintln!("启动 UI HTTP API 超时");
            return;
        }
    }

    if has_flag("--web") {
        println!("web UI listening on http://{}", state.config.listen_addr);
        loop {
            thread::sleep(Duration::from_secs(3600));
        }
    } else if let Err(error) = desktop::run(state) {
        eprintln!("启动桌面 UI 失败: {error}");
    }
}

fn run_http_server(state: AppState, ready: mpsc::Sender<Result<(), String>>) {
    let server = match Server::http(&state.config.listen_addr) {
        Ok(server) => server,
        Err(error) => {
            let _ = ready.send(Err(error.to_string()));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    println!("UI API listening on http://{}", state.config.listen_addr);
    for request in server.incoming_requests() {
        let state = state.clone();
        thread::spawn(move || handle_request(request, state));
    }
}

fn handle_request(mut request: Request, state: AppState) {
    let path = request.url().split('?').next().unwrap_or("/").to_string();
    let method = request.method().clone();
    match (method, path.as_str()) {
        (Method::Get, "/") => respond_text(request, 200, INDEX_HTML, "text/html; charset=utf-8"),
        (Method::Get, "/icon.svg") => respond_text(request, 200, ICON_SVG, "image/svg+xml"),
        (Method::Get, "/api/recent") | (Method::Get, "/api/history") => {
            respond_json(request, 200, &read_history(&state));
        }
        (Method::Delete, "/api/recent") | (Method::Delete, "/api/history") => {
            respond_json(request, 200, &clear_history(&state));
        }
        (Method::Get, "/api/services") => respond_json(request, 200, &service_statuses(&state)),
        (Method::Get, _) if path.starts_with("/api/manager/") => {
            let manager_path = request.url().to_string();
            match manager_proxy(&state, &manager_path, "GET", None) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Post, _) if path.starts_with("/api/manager/") => {
            let manager_path = path.clone();
            let body = match read_body(&mut request) {
                Ok(body) => body,
                Err(error) => {
                    respond_json(request, 400, &json!({"error": error}));
                    return;
                }
            };
            match manager_proxy(&state, &manager_path, "POST", Some(&body)) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Put, _) if path.starts_with("/api/manager/") => {
            let manager_path = path.clone();
            let body = match read_body(&mut request) {
                Ok(body) => body,
                Err(error) => {
                    respond_json(request, 400, &json!({"error": error}));
                    return;
                }
            };
            match manager_proxy(&state, &manager_path, "PUT", Some(&body)) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Delete, _) if path.starts_with("/api/manager/") => {
            match manager_proxy(&state, &path, "DELETE", None) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Post, "/api/crawl") => {
            let body = match read_body(&mut request) {
                Ok(body) => body,
                Err(error) => {
                    respond_json(request, 400, &json!({"error": error}));
                    return;
                }
            };
            let input: CrawlRequest = match serde_json::from_str(&body) {
                Ok(input) => input,
                Err(err) => {
                    respond_json(
                        request,
                        400,
                        &json!({"error": format!("请求 JSON 无效: {err}")}),
                    );
                    return;
                }
            };
            match crawl(&state, input) {
                Ok(record) => respond_json(request, 200, &record),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Post, "/api/rpc") => {
            let body = match read_body(&mut request) {
                Ok(body) => body,
                Err(error) => {
                    respond_json(request, 400, &json!({"error": error}));
                    return;
                }
            };
            let input: Value = match serde_json::from_str(&body) {
                Ok(input) => input,
                Err(err) => {
                    respond_json(
                        request,
                        400,
                        &json!({"error": format!("请求 JSON 无效: {err}")}),
                    );
                    return;
                }
            };
            if input
                .get("stream")
                .and_then(Value::as_str)
                .unwrap_or("")
                .is_empty()
                || input
                    .get("service")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .is_empty()
            {
                respond_json(
                    request,
                    400,
                    &json!({"error": "stream 和 service 不能为空"}),
                );
                return;
            }
            match call_gateway(&state.config.gateway_addr, &input) {
                Ok(result) => respond_json(request, 200, &result),
                Err(error) => respond_json(
                    request,
                    gateway_error_code(&error),
                    &json!({"error": error}),
                ),
            }
        }
        (Method::Get, _) if path.starts_with("/api/services/") && path.ends_with("/config") => {
            let name = path
                .trim_start_matches("/api/services/")
                .trim_end_matches("/config")
                .trim_end_matches('/');
            match load_service_config(&state, name) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Put, _) if path.starts_with("/api/services/") && path.ends_with("/config") => {
            let name = path
                .trim_start_matches("/api/services/")
                .trim_end_matches("/config")
                .trim_end_matches('/');
            let body = match read_body(&mut request) {
                Ok(body) => body,
                Err(error) => {
                    respond_json(request, 400, &json!({"error": error}));
                    return;
                }
            };
            match save_service_config(&state, name, &body) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Get, _) if path.starts_with("/api/services/") && path.ends_with("/logs") => {
            let name = path
                .trim_start_matches("/api/services/")
                .trim_end_matches("/logs")
                .trim_end_matches('/');
            match service_logs(&state, name) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Post, _)
            if path.starts_with("/api/services/") && path.ends_with("/logs/clear") =>
        {
            let name = path
                .trim_start_matches("/api/services/")
                .trim_end_matches("/logs/clear")
                .trim_end_matches('/');
            match clear_logs(&state, name) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Post, "/api/services/start-offline") => {
            respond_json(request, 200, &batch_service_action(&state, "start-offline"))
        }
        (Method::Post, "/api/services/start-all") => {
            respond_json(request, 200, &batch_service_action(&state, "start-all"))
        }
        (Method::Post, "/api/services/stop-all") => {
            respond_json(request, 200, &batch_service_action(&state, "stop-all"))
        }
        (Method::Post, "/api/services/restart-all") => {
            respond_json(request, 200, &batch_service_action(&state, "restart-all"))
        }
        (Method::Post, _) if path.starts_with("/api/services/") && path.ends_with("/adopt") => {
            let name = path
                .trim_start_matches("/api/services/")
                .trim_end_matches("/adopt")
                .trim_end_matches('/');
            match adopt_service(&state, name) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        (Method::Post, _) if path.starts_with("/api/services/") => {
            let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
            if parts.len() != 4 || parts[0] != "api" || parts[1] != "services" {
                respond_json(request, 404, &json!({"error": "not found"}));
                return;
            }
            match service_action(&state, parts[2], parts[3]) {
                Ok(result) => respond_json(request, 200, &result),
                Err((code, message)) => respond_json(request, code, &json!({"error": message})),
            }
        }
        _ => respond_json(request, 404, &json!({"error": "not found"})),
    }
}

fn read_body(request: &mut Request) -> Result<String, String> {
    let mut body = String::new();
    request
        .as_reader()
        .read_to_string(&mut body)
        .map_err(|err| format!("无法读取请求体: {err}"))?;
    Ok(body)
}

fn configured_service<'a>(
    state: &'a AppState,
    name: &str,
) -> Result<&'a ServiceConfig, (u16, String)> {
    state
        .config
        .services
        .iter()
        .find(|service| service.name == name)
        .ok_or_else(|| (404, format!("服务不存在: {name}")))
}

fn service_config_base(state: &AppState, service: &ServiceConfig) -> PathBuf {
    service
        .workdir
        .as_deref()
        .map(|workdir| resolve_path(&state.config.base_dir, workdir))
        .unwrap_or_else(|| state.config.base_dir.clone())
}

fn service_default_config_path(state: &AppState, service: &ServiceConfig) -> PathBuf {
    let base = service_config_base(state, service);
    service
        .config_path
        .as_deref()
        .map(|path| resolve_path(&base, path))
        .unwrap_or_else(|| base.join("config.json"))
}

fn service_local_config_path(state: &AppState, service: &ServiceConfig) -> PathBuf {
    service_config_base(state, service).join("config.local.json")
}

fn service_config_path(state: &AppState, service: &ServiceConfig) -> PathBuf {
    let local = service_local_config_path(state, service);
    if local.is_file() {
        local
    } else {
        service_default_config_path(state, service)
    }
}

fn service_command_args(state: &AppState, service: &ServiceConfig) -> Vec<String> {
    let local = service_local_config_path(state, service);
    if !local.is_file() {
        return service.args.clone();
    }

    let mut args = service.args.clone();
    for index in 0..args.len() {
        if matches!(args[index].as_str(), "-config_path" | "-config") {
            if let Some(path) = args.get_mut(index + 1) {
                *path = "config.local.json".to_string();
            }
            return args;
        }
    }
    args.extend(["-config_path".to_string(), "config.local.json".to_string()]);
    args
}

fn load_service_config(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    let service = configured_service(state, name)?;
    let path = service_config_path(state, service);
    let content = fs::read_to_string(&path)
        .map_err(|error| (404, format!("读取 {name} 配置失败: {error}")))?;
    let config = serde_json::from_str::<Value>(&content)
        .map_err(|error| (500, format!("{name} 配置不是有效 JSON: {error}")))?;
    Ok(json!({"name": name, "path": path.display().to_string(), "config": config}))
}

fn save_service_config(state: &AppState, name: &str, body: &str) -> Result<Value, (u16, String)> {
    let service = configured_service(state, name)?;
    let path = service_local_config_path(state, service);
    let input = serde_json::from_str::<Value>(body)
        .map_err(|error| (400, format!("配置 JSON 无效: {error}")))?;
    let config = input.get("config").cloned().unwrap_or(input);
    if !config.is_object() {
        return Err((400, "服务配置必须是 JSON 对象".to_string()));
    }
    let formatted = serde_json::to_string_pretty(&config)
        .map_err(|error| (500, format!("配置序列化失败: {error}")))?;
    fs::write(&path, format!("{formatted}\n"))
        .map_err(|error| (500, format!("写入 {name} 配置失败: {error}")))?;
    Ok(json!({
        "name": name,
        "path": path.display().to_string(),
        "uses_local_override": true,
        "default_path": service_default_config_path(state, service).display().to_string(),
        "saved": true,
        "requires_restart": true,
    }))
}

fn service_action(state: &AppState, name: &str, action: &str) -> Result<Value, (u16, String)> {
    configured_service(state, name)?;
    match action {
        "start" => start_service(state, name),
        "stop" => stop_service(state, name),
        "restart" => {
            let _ = stop_service(state, name)?;
            start_service(state, name)
        }
        _ => Err((404, "支持的操作只有 start、stop、restart".to_string())),
    }
}

fn batch_service_action(state: &AppState, action: &str) -> Value {
    if action == "restart-all" {
        let stopped = batch_service_action(state, "stop-all");
        let started = batch_service_action(state, "start-all");
        return json!({
            "action": action,
            "stop": stopped["results"],
            "start": started["results"],
        });
    }

    let mut services = state.config.services.clone();
    services.sort_by_key(|service| match action {
        "stop-all" => service.name == "http-gateway",
        _ => service.name != "http-gateway",
    });

    let mut results = Vec::with_capacity(services.len());
    for service in services {
        if action == "start-offline"
            && (managed_state(&state.processes, &service.name) == "running"
                || service_online(state, &service))
        {
            results.push(json!({
                "name": service.name,
                "ok": true,
                "skipped": true,
                "reason": "服务已运行或已在线",
            }));
            continue;
        }
        let result = match action {
            "start-all" | "start-offline" => start_service(state, &service.name),
            "stop-all" => stop_service(state, &service.name),
            _ => Err((500, "未知批量操作".to_string())),
        };
        results.push(match result {
            Ok(value) => json!({"name": service.name, "ok": true, "result": value}),
            Err((code, error)) => {
                json!({"name": service.name, "ok": false, "status": code, "error": error})
            }
        });
    }
    json!({"action": action, "results": results})
}

fn start_service(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    let service = configured_service(state, name)?.clone();
    let command = service
        .command
        .as_deref()
        .filter(|command| !command.trim().is_empty())
        .ok_or_else(|| (400, "该服务没有配置启动命令".to_string()))?;

    let command_path = resolve_path(&state.config.base_dir, command);
    if !command_path.exists() {
        return Err((500, format!("启动文件不存在: {}", command_path.display())));
    }
    let workdir = service
        .workdir
        .as_deref()
        .map(|value| resolve_path(&state.config.base_dir, value));
    if let Some(path) = &workdir {
        if !path.is_dir() {
            return Err((500, format!("工作目录不存在: {}", path.display())));
        }
    }

    let mut processes = state.processes.lock().unwrap();
    if let Some(process) = processes.get_mut(name) {
        match process_running(process) {
            Ok(true) => return Err((409, "服务已经在运行".to_string())),
            Ok(false) => {
                append_log(&state.logs, name, "[ui] 进程已退出".to_string());
                processes.remove(name);
            }
            Err(_) => {
                processes.remove(name);
            }
        }
    }
    drop(processes);

    if service_online(state, &service) {
        return Err((
            409,
            "服务已在线，可能由外部进程管理；为避免重复启动，本次未执行".to_string(),
        ));
    }

    let mut process = Command::new(&command_path);
    process.args(service_command_args(state, &service));
    if let Some(path) = workdir {
        process.current_dir(path);
    }
    process
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = process.spawn().map_err(|err| {
        (
            500,
            format!("启动 {name} 失败: {err} ({})", command_path.display()),
        )
    })?;
    let pid = child.id();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    state
        .logs
        .lock()
        .unwrap()
        .insert(name.to_string(), LogBuffer::new());
    if let Some(stdout) = stdout {
        spawn_log_reader(name.to_string(), stdout, state.logs.clone(), "stdout");
    }
    if let Some(stderr) = stderr {
        spawn_log_reader(name.to_string(), stderr, state.logs.clone(), "stderr");
    }
    state.processes.lock().unwrap().insert(
        name.to_string(),
        ManagedProcess {
            pid,
            handle: ProcessHandle::Spawned(child),
        },
    );
    Ok(json!({"name": name, "action": "start", "state": "starting", "pid": pid}))
}

fn stop_service(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    let mut processes = state.processes.lock().unwrap();
    let Some(process) = processes.remove(name) else {
        return Ok(json!({"name": name, "action": "stop", "state": "stopped", "managed": false}));
    };
    drop(processes);
    terminate_process(process).map_err(|err| (500, format!("停止 {name} 失败: {err}")))?;
    append_log(&state.logs, name, "[ui] 进程已停止".to_string());
    Ok(json!({"name": name, "action": "stop", "state": "stopped", "managed": true}))
}

fn service_online(state: &AppState, service: &ServiceConfig) -> bool {
    let rpc =
        json!({"stream": service.stream, "service": "ping", "payload": {}, "timeout_ms": 1500});
    call_gateway(&state.config.gateway_addr, &rpc).is_ok()
}

fn spawn_log_reader<R: Read + Send + 'static>(
    name: String,
    reader: R,
    logs: Arc<Mutex<HashMap<String, LogBuffer>>>,
    channel: &'static str,
) {
    thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            match line {
                Ok(line) => append_log(&logs, &name, format!("[{channel}] {line}")),
                Err(error) => {
                    append_log(&logs, &name, format!("[{channel}] 读取日志失败: {error}"));
                    break;
                }
            }
        }
    });
}

fn append_log(logs: &Arc<Mutex<HashMap<String, LogBuffer>>>, name: &str, line: String) {
    logs.lock()
        .unwrap()
        .entry(name.to_string())
        .or_insert_with(LogBuffer::new)
        .push(line);
}

fn service_logs(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    configured_service(state, name)?;
    let lines = state
        .logs
        .lock()
        .unwrap()
        .get(name)
        .map(|buffer| buffer.lines.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let process = managed_state(&state.processes, name);
    Ok(json!({"name": name, "lines": lines, "process": process, "limit": LOG_LINE_LIMIT}))
}

fn clear_logs(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    configured_service(state, name)?;
    state
        .logs
        .lock()
        .unwrap()
        .insert(name.to_string(), LogBuffer::new());
    Ok(json!({"name": name, "cleared": true}))
}

fn resolve_path(base_dir: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn managed_state(processes: &Arc<Mutex<HashMap<String, ManagedProcess>>>, name: &str) -> String {
    let mut processes = processes.lock().unwrap();
    let Some(process) = processes.get_mut(name) else {
        return "not_managed".to_string();
    };
    match process_running(process) {
        Ok(true) => match process.handle {
            ProcessHandle::Adopted(_) => "adopted".to_string(),
            ProcessHandle::Spawned(_) => "running".to_string(),
        },
        Ok(false) | Err(_) => {
            processes.remove(name);
            "stopped".to_string()
        }
    }
}

fn process_pid(processes: &Arc<Mutex<HashMap<String, ManagedProcess>>>, name: &str) -> Option<u32> {
    processes
        .lock()
        .unwrap()
        .get(name)
        .map(|process| process.pid)
}

fn recent_records(state: &AppState) -> Vec<CrawlRecord> {
    state.recent.lock().unwrap().iter().cloned().collect()
}

fn read_history(state: &AppState) -> Vec<CrawlRecord> {
    let _guard = state.history_lock.lock().unwrap();
    let Ok(file) = fs::File::open(&state.history_path) else {
        return recent_records(state);
    };
    let mut records = VecDeque::with_capacity(RECENT_LIMIT);
    for line in BufReader::new(file).lines().flatten() {
        if let Ok(record) = serde_json::from_str::<CrawlRecord>(&line) {
            records.push_front(record);
        }
    }
    records.into_iter().take(RECENT_LIMIT).collect()
}

fn load_recent_history(state: &AppState) {
    let records = read_history(state);
    let mut recent = state.recent.lock().unwrap();
    recent.extend(records);
    recent.truncate(RECENT_LIMIT);
}

fn clear_history(state: &AppState) -> Value {
    let _guard = state.history_lock.lock().unwrap();
    let cleared = fs::remove_file(&state.history_path).is_ok() || !state.history_path.exists();
    state.recent.lock().unwrap().clear();
    json!({"cleared": cleared, "path": state.history_path.display().to_string()})
}

fn process_running(process: &mut ManagedProcess) -> Result<bool, String> {
    match &mut process.handle {
        ProcessHandle::Spawned(child) => child
            .try_wait()
            .map(|status| status.is_none())
            .map_err(|error| error.to_string()),
        ProcessHandle::Adopted(pid) => Ok(process_exists(*pid)),
    }
}

fn process_exists(pid: u32) -> bool {
    #[cfg(windows)]
    {
        Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\"")))
            .unwrap_or(false)
    }
    #[cfg(not(windows))]
    {
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
}

fn terminate_process(mut process: ManagedProcess) -> Result<(), String> {
    match &mut process.handle {
        ProcessHandle::Spawned(child) => {
            if child
                .try_wait()
                .map_err(|error| error.to_string())?
                .is_none()
            {
                #[cfg(windows)]
                {
                    let _ = Command::new("taskkill")
                        .args(["/PID", &process.pid.to_string(), "/T", "/F"])
                        .status();
                }
                #[cfg(not(windows))]
                {
                    child.kill().map_err(|error| error.to_string())?;
                }
                let _ = child.wait();
            }
        }
        ProcessHandle::Adopted(pid) => {
            #[cfg(windows)]
            let status = Command::new("taskkill")
                .args(["/PID", &pid.to_string(), "/T", "/F"])
                .status()
                .map_err(|error| error.to_string())?;
            #[cfg(not(windows))]
            let status = Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .map_err(|error| error.to_string())?;
            if !status.success() && process_exists(*pid) {
                return Err(format!("终止 PID {pid} 失败"));
            }
        }
    }
    Ok(())
}

fn adopt_service(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    let service = configured_service(state, name)?.clone();
    let command = service
        .command
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| (400, "该服务没有配置启动命令，无法定位外部进程".to_string()))?;
    if managed_state(&state.processes, name) != "not_managed" {
        return Err((409, "该服务已经由控制台管理".to_string()));
    }
    if !service_online(state, &service) {
        return Err((409, "服务当前不在线，无法接管".to_string()));
    }
    let executable = Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(command)
        .trim_matches('"')
        .to_string();
    #[cfg(windows)]
    let output = Command::new("tasklist")
        .args([
            "/FI",
            &format!("IMAGENAME eq {executable}"),
            "/FO",
            "CSV",
            "/NH",
        ])
        .output()
        .map_err(|error| (500, format!("查询外部进程失败: {error}")))?;
    #[cfg(windows)]
    let pids = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('"');
            fields.nth(3).and_then(|value| value.parse::<u32>().ok())
        })
        .collect::<Vec<_>>();
    #[cfg(not(windows))]
    let pids: Vec<u32> = Vec::new();
    if pids.len() != 1 {
        return Err((
            409,
            if pids.is_empty() {
                format!("没有找到匹配的外部进程: {executable}")
            } else {
                format!("找到 {} 个匹配进程，请只保留一个后再接管", pids.len())
            },
        ));
    }
    let pid = pids[0];
    state.processes.lock().unwrap().insert(
        name.to_string(),
        ManagedProcess {
            handle: ProcessHandle::Adopted(pid),
            pid,
        },
    );
    append_log(&state.logs, name, format!("[ui] 已接管外部进程 PID {pid}"));
    Ok(json!({"name": name, "action": "adopt", "state": "adopted", "pid": pid}))
}

pub(crate) fn shutdown_services(state: &AppState) {
    let names = state
        .processes
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for name in names {
        if let Err(error) = stop_service(state, &name) {
            eprintln!("关闭 {name} 失败: {error:?}");
        }
    }
}

fn crawl(state: &AppState, input: CrawlRequest) -> Result<CrawlRecord, (u16, String)> {
    let url = input.url.trim().to_string();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err((400, "url 必须以 http:// 或 https:// 开头".to_string()));
    }
    let method = input.method.trim().to_uppercase();
    if method != "GET" && method != "POST" {
        return Err((400, "当前爬虫只支持 GET 或 POST".to_string()));
    }
    if !input.headers.is_object() {
        return Err((400, "headers 必须是 JSON 对象".to_string()));
    }
    if !input.body.is_object() {
        return Err((400, "body 必须是 JSON 对象".to_string()));
    }
    let payload = json!({"url": url, "method": method, "headers": input.headers, "body": input.body, "str_payload": input.str_payload});
    let rpc = json!({"stream": "dev:crawler-stream", "service": "http_request", "payload": payload, "timeout_ms": state.config.crawl_timeout_ms});
    let response = call_gateway(&state.config.gateway_addr, &rpc)
        .map_err(|message| (gateway_error_code(&message), message))?;
    let record = CrawlRecord {
        url: rpc["payload"]["url"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        method: rpc["payload"]["method"]
            .as_str()
            .unwrap_or("GET")
            .to_string(),
        status: response.get("status").and_then(Value::as_i64).unwrap_or(0),
        content: response
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        created_at: now_seconds(),
    };
    let serialized = serde_json::to_string(&record)
        .map_err(|error| (500, format!("序列化抓取记录失败: {error}")))?;
    {
        let _guard = state.history_lock.lock().unwrap();
        if let Some(parent) = state.history_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| (500, format!("创建历史记录目录失败: {error}")))?;
        }
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&state.history_path)
            .map_err(|error| (500, format!("打开历史记录文件失败: {error}")))?;
        writeln!(file, "{serialized}")
            .map_err(|error| (500, format!("保存抓取记录失败: {error}")))?;
    }
    let mut recent = state.recent.lock().unwrap();
    recent.push_front(record.clone());
    recent.truncate(RECENT_LIMIT);
    Ok(record)
}

fn service_statuses(state: &AppState) -> Vec<Value> {
    let (tx, rx) = mpsc::channel();
    for (index, service) in state.config.services.clone().into_iter().enumerate() {
        let tx = tx.clone();
        let gateway = state.config.gateway_addr.clone();
        let manager_addr = state.config.manager_addr.clone();
        let processes = state.processes.clone();
        thread::spawn(move || {
            let process = managed_state(&processes, &service.name);
            if process == "stopped" {
                let _ = tx.send((index, json!({"name": service.name, "stream": service.stream, "online": false, "managed": service.command.is_some() || process == "adopted", "process": process, "pid": process_pid(&processes, &service.name), "health": "stopped", "error": "进程未运行"})));
                return;
            }
            let rpc = json!({"stream": service.stream, "service": "ping", "payload": {}, "timeout_ms": 3000});
            let result = if service.name == "workflow-manager" {
                match manager_health(&manager_addr) {
                    Ok(details) => {
                        json!({"name": service.name, "stream": service.stream, "online": true, "managed": service.command.is_some() || process == "adopted", "process": process, "pid": process_pid(&processes, &service.name), "health": "online", "details": details})
                    }
                    Err(error) => {
                        json!({"name": service.name, "stream": service.stream, "online": false, "managed": service.command.is_some() || process == "adopted", "process": process, "pid": process_pid(&processes, &service.name), "health": "service_unavailable", "error": error})
                    }
                }
            } else {
                match call_gateway(&gateway, &rpc) {
                    Ok(details) => {
                        json!({"name": service.name, "stream": service.stream, "online": true, "managed": service.command.is_some() || process == "adopted", "process": process, "pid": process_pid(&processes, &service.name), "health": if process == "not_managed" { "external" } else { "online" }, "details": details})
                    }
                    Err(error) => {
                        json!({"name": service.name, "stream": service.stream, "online": false, "managed": service.command.is_some() || process == "adopted", "process": process, "pid": process_pid(&processes, &service.name), "health": gateway_error_health(&error), "error": error})
                    }
                }
            };
            let _ = tx.send((index, result));
        });
    }
    drop(tx);
    let mut statuses = vec![Value::Null; state.config.services.len()];
    for (index, result) in rx {
        statuses[index] = result;
    }
    statuses
}

fn manager_health(manager_addr: &str) -> Result<Value, String> {
    let endpoint = format!("{}/health", manager_addr.trim_end_matches('/'));
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(3))
        .build();
    match agent.get(&endpoint).call() {
        Ok(response) => response
            .into_string()
            .map_err(|e| e.to_string())
            .and_then(|body| serde_json::from_str(&body).map_err(|e| e.to_string())),
        Err(error) => Err(error.to_string()),
    }
}

fn manager_proxy(
    state: &AppState,
    path: &str,
    method: &str,
    body: Option<&str>,
) -> Result<Value, (u16, String)> {
    let suffix = path.strip_prefix("/api/manager").unwrap_or("/");
    let endpoint = format!(
        "{}/api/v1{}",
        state.config.manager_addr.trim_end_matches('/'),
        suffix
    );
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(130))
        .build();
    let request = agent
        .request(method, &endpoint)
        .set("Content-Type", "application/json");
    let response = match body {
        Some(content) => request.send_string(content),
        None => request.call(),
    };
    match response {
        Ok(response) => {
            let content_type = response.header("Content-Type").unwrap_or("").to_string();
            let raw = response.into_string().map_err(|e| (502, e.to_string()))?;
            if content_type.starts_with("text/markdown") || content_type.starts_with("text/plain") {
                return Ok(Value::String(raw));
            }
            serde_json::from_str(&raw)
                .map_err(|e| (502, format!("manager response JSON invalid: {e}")))
        }
        Err(ureq::Error::Status(code, response)) => {
            let raw = response.into_string().unwrap_or_default();
            let message = serde_json::from_str::<Value>(&raw)
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
                .unwrap_or(raw);
            Err((code as u16, message))
        }
        Err(error) => Err((502, format!("无法连接 workflow manager: {error}"))),
    }
}

fn gateway_error_code(error: &str) -> u16 {
    if error.starts_with("无法连接 HTTP 网关") {
        502
    } else if error.contains("网关 HTTP 504") {
        504
    } else {
        502
    }
}

fn gateway_error_health(error: &str) -> &'static str {
    match gateway_error_code(error) {
        504 => "service_unavailable",
        _ if error.starts_with("无法连接 HTTP 网关") => "gateway_unavailable",
        _ => "service_error",
    }
}

fn call_gateway(gateway_addr: &str, request: &Value) -> Result<Value, String> {
    let endpoint = format!("{}/rpc", gateway_addr.trim_end_matches('/'));
    let body = serde_json::to_string(request).map_err(|err| err.to_string())?;
    let timeout_ms = request
        .get("timeout_ms")
        .and_then(Value::as_i64)
        .unwrap_or(5000)
        .max(1000) as u64;
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_millis(timeout_ms.saturating_add(5000)))
        .build();
    match agent
        .post(&endpoint)
        .set("Content-Type", "application/json")
        .send_string(&body)
    {
        Ok(response) => {
            let body = response.into_string().map_err(|err| err.to_string())?;
            serde_json::from_str(&body).map_err(|err| format!("网关响应 JSON 无效: {err}"))
        }
        Err(ureq::Error::Status(code, response)) => {
            let body = response.into_string().unwrap_or_default();
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|value| {
                    value
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or(body);
            Err(format!("网关 HTTP {code}: {message}"))
        }
        Err(err) => Err(format!("无法连接 HTTP 网关: {err}")),
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let content = fs::read_to_string(path).map_err(|err| err.to_string())?;
    serde_json::from_str(&content).map_err(|err| err.to_string())
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn respond_json<T: Serialize>(request: Request, code: u16, value: &T) {
    let body = serde_json::to_string(value)
        .unwrap_or_else(|_| "{\"error\":\"serialization failed\"}".to_string());
    respond_text(request, code, &body, "application/json; charset=utf-8");
}

fn respond_text(request: Request, code: u16, body: &str, content_type: &str) {
    let header = Header::from_bytes(b"Content-Type" as &[u8], content_type.as_bytes()).unwrap();
    let response = Response::from_string(body)
        .with_status_code(StatusCode(code))
        .with_header(header);
    let _ = request.respond(response);
}
