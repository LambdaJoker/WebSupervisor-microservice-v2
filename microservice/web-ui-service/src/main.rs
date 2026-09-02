use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

const INDEX_HTML: &str = include_str!("index.html");
const RECENT_LIMIT: usize = 20;

#[derive(Clone, Deserialize, Serialize)]
struct Config {
    listen_addr: String,
    gateway_addr: String,
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
}

#[derive(Clone, Serialize)]
struct CrawlRecord {
    url: String,
    method: String,
    status: i64,
    content: String,
    created_at: u64,
}

struct ManagedProcess {
    child: Child,
}

#[derive(Clone)]
struct AppState {
    config: Config,
    recent: Arc<Mutex<VecDeque<CrawlRecord>>>,
    processes: Arc<Mutex<HashMap<String, ManagedProcess>>>,
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

fn default_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn config_path() -> String {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    for flag in ["-config_path", "-config"] {
        if let Some(index) = args.iter().position(|arg| arg == flag) {
            if let Some(path) = args.get(index + 1) {
                return path.clone();
            }
        }
    }
    "config.json".to_string()
}

fn main() {
    let config_path = config_path();
    let mut config: Config = read_json(&config_path).unwrap_or_else(|err| {
        eprintln!("读取配置失败: {err}");
        std::process::exit(1);
    });
    config.base_dir = Path::new(&config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));

    let server = Server::http(&config.listen_addr).unwrap_or_else(|err| {
        eprintln!("启动 UI 服务失败: {err}");
        std::process::exit(1);
    });
    println!("web UI listening on http://{}", config.listen_addr);

    let state = AppState {
        config,
        recent: Arc::new(Mutex::new(VecDeque::with_capacity(RECENT_LIMIT))),
        processes: Arc::new(Mutex::new(HashMap::new())),
    };
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
        (Method::Get, "/api/recent") => {
            let records = state
                .recent
                .lock()
                .unwrap()
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            respond_json(request, 200, &records);
        }
        (Method::Get, "/api/services") => respond_json(request, 200, &service_statuses(&state)),
        (Method::Post, "/api/crawl") => {
            let mut body = String::new();
            if request.as_reader().read_to_string(&mut body).is_err() {
                respond_json(request, 400, &json!({"error": "无法读取请求体"}));
                return;
            }
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

fn service_action(state: &AppState, name: &str, action: &str) -> Result<Value, (u16, String)> {
    if state
        .config
        .services
        .iter()
        .all(|service| service.name != name)
    {
        return Err((404, format!("服务不存在: {name}")));
    }
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

fn start_service(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    let service = state
        .config
        .services
        .iter()
        .find(|service| service.name == name)
        .cloned()
        .ok_or_else(|| (404, format!("服务不存在: {name}")))?;
    let command = service
        .command
        .as_deref()
        .filter(|command| !command.trim().is_empty())
        .ok_or_else(|| (400, "该服务没有配置启动命令".to_string()))?;

    let mut processes = state.processes.lock().unwrap();
    if let Some(process) = processes.get_mut(name) {
        match process.child.try_wait() {
            Ok(None) => return Err((409, "服务已经在运行".to_string())),
            Ok(Some(_)) | Err(_) => {
                processes.remove(name);
            }
        }
    }

    let command_path = resolve_path(&state.config.base_dir, command);
    let mut process = Command::new(&command_path);
    process.args(&service.args);
    if let Some(workdir) = service.workdir.as_deref() {
        process.current_dir(resolve_path(&state.config.base_dir, workdir));
    }
    process
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = process.spawn().map_err(|err| {
        (
            500,
            format!("启动 {name} 失败: {err} ({})", command_path.display()),
        )
    })?;
    let pid = child.id();
    processes.insert(name.to_string(), ManagedProcess { child });
    Ok(json!({"name": name, "action": "start", "state": "starting", "pid": pid}))
}

fn stop_service(state: &AppState, name: &str) -> Result<Value, (u16, String)> {
    let mut processes = state.processes.lock().unwrap();
    let Some(mut process) = processes.remove(name) else {
        return Ok(json!({"name": name, "action": "stop", "state": "stopped"}));
    };
    if let Ok(None) = process.child.try_wait() {
        process
            .child
            .kill()
            .map_err(|err| (500, format!("停止 {name} 失败: {err}")))?;
        let _ = process.child.wait();
    }
    Ok(json!({"name": name, "action": "stop", "state": "stopped"}))
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
    match process.child.try_wait() {
        Ok(None) => "running".to_string(),
        Ok(Some(_)) | Err(_) => {
            processes.remove(name);
            "stopped".to_string()
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

    let payload = json!({
        "url": url,
        "method": method,
        "headers": input.headers,
        "body": input.body,
        "str_payload": input.str_payload,
    });
    let rpc = json!({
        "stream": "dev:crawler-stream",
        "service": "http_request",
        "payload": payload,
        "timeout_ms": state.config.crawl_timeout_ms,
    });
    let response =
        call_gateway(&state.config.gateway_addr, &rpc).map_err(|message| (502, message))?;
    let status = response.get("status").and_then(Value::as_i64).unwrap_or(0);
    let content = response
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let record = CrawlRecord {
        url: rpc["payload"]["url"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        method: rpc["payload"]["method"]
            .as_str()
            .unwrap_or("GET")
            .to_string(),
        status,
        content,
        created_at: now_seconds(),
    };
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
        let processes = state.processes.clone();
        thread::spawn(move || {
            let process = managed_state(&processes, &service.name);
            if process == "stopped" {
                let _ = tx.send((
                    index,
                    json!({
                        "name": service.name,
                        "stream": service.stream,
                        "online": false,
                        "managed": service.command.is_some(),
                        "process": process,
                        "error": "进程未运行",
                    }),
                ));
                return;
            }
            let rpc = json!({
                "stream": service.stream,
                "service": "ping",
                "payload": {},
                "timeout_ms": 3000,
            });
            let result = match call_gateway(&gateway, &rpc) {
                Ok(details) => json!({
                    "name": service.name,
                    "stream": service.stream,
                    "online": true,
                    "managed": service.command.is_some(),
                    "process": process,
                    "details": details,
                }),
                Err(error) => json!({
                    "name": service.name,
                    "stream": service.stream,
                    "online": false,
                    "managed": service.command.is_some(),
                    "process": process,
                    "error": error,
                }),
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
    let response = agent
        .post(&endpoint)
        .set("Content-Type", "application/json")
        .send_string(&body);
    match response {
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

fn read_json<T: for<'de> Deserialize<'de>>(path: &str) -> Result<T, String> {
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
