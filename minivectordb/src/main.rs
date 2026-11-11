//! MiniVectorDB: single-process embedded SQL + vector engine speaking the
//! competition JSON Lines protocol over stdin/stdout.

mod ann;
mod engine;
mod fastscan;
mod exec;
mod json;
mod sql;
mod storage;
mod types;
mod vector;

use std::io::{BufRead, Write};
use std::path::Path;

use engine::Engine;
use json::Json;
use types::{ExecResult, SearchMode, Value};

fn main() {
    let data_dir = resolve_data_dir();
    let mut engine = match Engine::open(Path::new(&data_dir)) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("minivectordb: cannot open data directory {}: {}", data_dir, err);
            std::process::exit(2);
        }
    };

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();

    if write_line(&mut output, &Json::Obj(vec![("type".to_string(), Json::Str("ready".to_string()))])).is_err() {
        std::process::exit(2);
    }

    let mut line = String::new();
    loop {
        line.clear();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        let text = line.trim();
        if text.is_empty() {
            continue;
        }
        let request = match Json::parse(text) {
            Ok(v) => v,
            Err(err) => {
                let response = error_response(Json::Null, "invalid_request", format!("invalid JSON request: {}", err));
                let _ = write_line(&mut output, &response);
                continue;
            }
        };
        let id = request.get("id").cloned().unwrap_or(Json::Null);
        let op = request.get("op").and_then(|v| v.as_str()).unwrap_or("");
        if op == "shutdown" {
            engine.shutdown().ok();
            let response = Json::Obj(vec![
                ("id".to_string(), id),
                ("ok".to_string(), Json::Bool(true)),
            ]);
            let _ = write_line(&mut output, &response);
            let _ = output.flush();
            std::process::exit(0);
        }
        if op != "execute" {
            let response = error_response(id, "unsupported_op", "only execute and shutdown are supported");
            let _ = write_line(&mut output, &response);
            continue;
        }
        let sql = request.get("sql").and_then(|v| v.as_str()).unwrap_or("");
        let mode = SearchMode::from_wire(request.get("search_mode").and_then(|v| v.as_str()));
        let response = match engine.execute(sql, mode) {
            Ok(result) => success_response(id, result),
            Err(err) => error_response(id, &err.code, err.message),
        };
        if write_line(&mut output, &response).is_err() {
            break;
        }
    }

    engine.shutdown().ok();
}

fn resolve_data_dir() -> String {
    let mut dir = "/data".to_string();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        let arg = &args[i];
        if let Some(rest) = arg.strip_prefix("--data-dir=") {
            dir = rest.to_string();
            i += 1;
        } else if arg == "--data-dir" && i + 1 < args.len() {
            dir = args[i + 1].clone();
            i += 2;
        } else {
            i += 1;
        }
    }
    if dir == "/data" {
        if let Ok(env_dir) = std::env::var("VECTORDB_DATA_DIR") {
            if !env_dir.is_empty() {
                dir = env_dir;
            }
        }
    }
    dir
}

fn write_line<W: Write>(out: &mut W, value: &Json) -> std::io::Result<()> {
    let mut buffer = String::with_capacity(256);
    value.write_to(&mut buffer);
    buffer.push('\n');
    out.write_all(buffer.as_bytes())?;
    out.flush()
}

fn success_response(id: Json, result: ExecResult) -> Json {
    let mut items = vec![("id".to_string(), id), ("ok".to_string(), Json::Bool(true))];
    match result {
        ExecResult::Ack => {}
        ExecResult::Affected(count) => {
            items.push(("affected_rows".to_string(), Json::Int(count as i64)));
        }
        ExecResult::Query { columns, rows } => {
            items.push((
                "columns".to_string(),
                Json::Arr(columns.into_iter().map(Json::Str).collect()),
            ));
            let mut json_rows = Vec::with_capacity(rows.len());
            for row in rows {
                json_rows.push(Json::Arr(row.into_iter().map(value_to_json).collect()));
            }
            items.push(("rows".to_string(), Json::Arr(json_rows)));
        }
    }
    Json::Obj(items)
}

fn error_response(id: Json, code: &str, message: impl Into<String>) -> Json {
    Json::Obj(vec![
        ("id".to_string(), id),
        ("ok".to_string(), Json::Bool(false)),
        (
            "error".to_string(),
            Json::Obj(vec![
                ("code".to_string(), Json::Str(code.to_string())),
                ("message".to_string(), Json::Str(message.into())),
            ]),
        ),
    ])
}

fn value_to_json(value: Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Int(v) => Json::Int(v),
        Value::Float(v) => Json::Float(v),
        Value::Str(s) => Json::Str(s),
        Value::Vec(components) => Json::Arr(components.into_iter().map(|c| Json::Float(c as f64)).collect()),
    }
}