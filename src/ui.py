from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json

import pyodbc
import requests
import yaml
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from collector import fetch_rows, get_last_query, get_last_sample
from config import (
    DEFAULT_CONFIG_PATH,
    build_connection_string,
    load_config,
    load_config_from_dict,
)
from runner import start as start_agent, stop as stop_agent, status as agent_status
from sender import get_last_send
from state import load_state


app = FastAPI()


def _load_yaml_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _save_yaml_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _parse_config_from_body(payload: dict):
    raw = payload.get("yaml", "")
    if not raw:
        return load_config(DEFAULT_CONFIG_PATH)
    data = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        raise ValueError("YAML inválido")
    return load_config_from_dict(data)


def _redact_conn_str(conn_str: str) -> str:
    parts = conn_str.split(";")
    redacted = []
    for part in parts:
        if part.upper().startswith("PWD="):
            redacted.append("PWD=***")
        else:
            redacted.append(part)
    return ";".join(redacted)


def _odbc_hint(message: str) -> str:
    msg = message.lower()
    if "im002" in msg or "data source name not found" in msg:
        return "Driver ODBC não encontrado. Verifique o nome exato do driver instalado."
    if "08001" in msg or "server not found" in msg:
        return "Servidor/instância não encontrado. Verifique SERVER, instância e porta."
    if "28000" in msg or "login failed" in msg:
        return "Falha de autenticação. Verifique auth, user e password."
    if "hyt00" in msg or "timeout" in msg:
        return "Timeout de conexão. Verifique rede, SQL Browser e porta."
    return "Verifique driver, servidor, instância e credenciais."


@app.get("/", response_class=HTMLResponse)
def index():
    html = """
<!DOCTYPE html>
<html lang="pt-br">
  <head>
    <meta charset="utf-8"/>
    <title>AgenteColetorSQL - Setup</title>
    <style>
      body{font-family:Segoe UI,Tahoma,Arial,sans-serif;margin:20px;background:#f5f5f5;color:#222}
      .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
      textarea{width:100%;height:420px;font-family:Consolas,monospace;font-size:12px}
      button{padding:8px 12px;border:0;background:#2b4a6f;color:#fff;cursor:pointer;border-radius:4px}
      button.secondary{background:#666}
      .panel{background:#fff;padding:12px;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,0.08)}
      .logs{white-space:pre-wrap;background:#111;color:#ddd;padding:10px;border-radius:4px;height:200px;overflow:auto}
      select{padding:6px}
    </style>
  </head>
  <body>
    <h2>AgenteColetorSQL - Configuração</h2>
    <div class="panel">
      <div class="row">
        <button onclick="loadConfig()">Carregar</button>
        <button onclick="saveConfig()">Salvar</button>
        <button class="secondary" onclick="testSql()">Testar SQL</button>
        <button class="secondary" onclick="listTables()">Listar Tabelas</button>
        <button class="secondary" onclick="listViews()">Listar Views</button>
        <button class="secondary" onclick="listColumns()">Listar Colunas</button>
        <button class="secondary" onclick="refreshSources()">Atualizar fontes</button>
        <label>Fonte:
          <select id="sourceSelect"></select>
        </label>
        <button class="secondary" onclick="debugQuery()">Query usada</button>
        <button class="secondary" onclick="debugSample()">Amostra SQL</button>
        <button class="secondary" onclick="debugLastSend()">Último envio</button>
        <button class="secondary" onclick="preview()">Preview TOP 20</button>
        <button class="secondary" onclick="validateIncremental()">Validar incremental</button>
        <button class="secondary" onclick="testEndpoint()">Testar Endpoint</button>
        <button onclick="startAgent()">Iniciar</button>
        <button onclick="stopAgent()">Parar</button>
        <button class="secondary" onclick="loadStatus()">Status</button>
      </div>
      <p></p>
      <textarea id="yaml"></textarea>
    </div>
    <p></p>
    <div class="panel">
      <h3>Status / Logs</h3>
      <div class="logs" id="logs">...</div>
    </div>
    <script>
      async function api(url, data){
        const res = await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data||{})});
        const text = await res.text();
        if(!res.ok){
          try{
            const obj = JSON.parse(text);
            throw new Error(obj.detail || text);
          }catch(e){
            throw new Error(text);
          }
        }
        return text;
      }
      async function loadConfig(){
        const res = await fetch('/api/config');
        document.getElementById('yaml').value = await res.text();
      }
      async function saveConfig(){
        await api('/api/config',{yaml:document.getElementById('yaml').value});
        alert('Config salva');
      }
      async function testSql(){
        try{
          const text = await api('/api/test-sql',{yaml:document.getElementById('yaml').value});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function listTables(){
        try{
          const text = await api('/api/list-tables',{yaml:document.getElementById('yaml').value});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function listViews(){
        try{
          const text = await api('/api/list-views',{yaml:document.getElementById('yaml').value});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function listColumns(){
        const source = document.getElementById('sourceSelect').value;
        try{
          const text = await api('/api/list-columns',{yaml:document.getElementById('yaml').value, source: source});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function preview(){
        const source = document.getElementById('sourceSelect').value;
        try{
          const text = await api('/api/preview',{yaml:document.getElementById('yaml').value, source: source});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function validateIncremental(){
        const source = document.getElementById('sourceSelect').value;
        try{
          const text = await api('/api/validate-incremental',{yaml:document.getElementById('yaml').value, source: source});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function testEndpoint(){
        try{
          const text = await api('/api/test-endpoint',{yaml:document.getElementById('yaml').value});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function startAgent(){
        try{
          const text = await api('/api/start',{yaml:document.getElementById('yaml').value});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function stopAgent(){
        try{
          const text = await api('/api/stop');
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function loadStatus(){
        const res = await fetch('/api/status');
        document.getElementById('logs').textContent = await res.text();
      }
      async function refreshSources(){
        const res = await fetch('/api/sources',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({yaml:document.getElementById('yaml').value})});
        const list = await res.json();
        const select = document.getElementById('sourceSelect');
        select.innerHTML = '';
        list.forEach(name=>{
          const opt = document.createElement('option');
          opt.value = name;
          opt.textContent = name;
          select.appendChild(opt);
        });
      }
      loadConfig().then(refreshSources);
      async function debugQuery(){
        const source = document.getElementById('sourceSelect').value;
        try{
          const text = await api('/api/debug-query',{source: source});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function debugSample(){
        const source = document.getElementById('sourceSelect').value;
        try{
          const text = await api('/api/debug-sample',{source: source});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
      async function debugLastSend(){
        try{
          const text = await api('/api/debug-send',{});
          document.getElementById('logs').textContent = text;
        }catch(err){
          document.getElementById('logs').textContent = err.message;
        }
      }
    </script>
  </body>
</html>
"""
    return HTMLResponse(html)


@app.get("/api/config")
def get_config():
    return HTMLResponse(_load_yaml_text(DEFAULT_CONFIG_PATH))


@app.post("/api/config")
async def save_config(request: Request):
    payload = await request.json()
    yaml_text = payload.get("yaml", "")
    _save_yaml_text(DEFAULT_CONFIG_PATH, yaml_text)
    return {"ok": True}


@app.post("/api/test-sql")
async def test_sql(request: Request):
    payload = await request.json()
    stage = "parse"
    try:
        stage = "load_config"
        config = _parse_config_from_body(payload)
        stage = "build_connection"
        conn_str = build_connection_string(config.sql)
        stage = "connect"
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            stage = "execute"
            cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
            server_name, db_name = cursor.fetchone()
        response = {
            "stage": "ok",
            "driver": config.sql.driver,
            "server": config.sql.server,
            "database": config.sql.database,
            "auth": config.sql.auth,
            "server_name": server_name,
            "db_name": db_name,
        }
        return json.dumps(response, ensure_ascii=False, indent=2)
    except Exception as exc:
        detail = {
            "stage": stage,
            "error": str(exc),
        }
        try:
            config = _parse_config_from_body(payload)
            conn_str = build_connection_string(config.sql)
            detail.update(
                {
                    "driver": config.sql.driver,
                    "server": config.sql.server,
                    "database": config.sql.database,
                    "auth": config.sql.auth,
                    "conn": _redact_conn_str(conn_str),
                    "hint": _odbc_hint(str(exc)),
                }
            )
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=detail)


@app.post("/api/list-tables")
async def list_tables(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        conn_str = build_connection_string(config.sql)
        rows = []
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT s.name, t.name FROM sys.tables t "
                "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                "ORDER BY s.name, t.name"
            )
            for schema, table in cursor.fetchall():
                rows.append(f"{schema}.{table}")
        return "\n".join(rows) if rows else "Nenhuma tabela encontrada"
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/list-views")
async def list_views(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        conn_str = build_connection_string(config.sql)
        rows = []
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT s.name, v.name FROM sys.views v "
                "JOIN sys.schemas s ON v.schema_id = s.schema_id "
                "ORDER BY s.name, v.name"
            )
            for schema, view in cursor.fetchall():
                rows.append(f"{schema}.{view}")
        return "\n".join(rows) if rows else "Nenhuma view encontrada"
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/list-columns")
async def list_columns(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        source_name = payload.get("source") or config.sources[0].name
        source = next((s for s in config.sources if s.name == source_name), None)
        if not source:
            raise ValueError("Fonte nao encontrada")
        conn_str = build_connection_string(config.sql)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            if source.kind == "table":
                schema, table = (
                    source.table.split(".")
                    if "." in source.table
                    else ("dbo", source.table)
                )
                query = (
                    "SELECT c.name, ty.name FROM sys.columns c "
                    "JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
                    "JOIN sys.tables t ON c.object_id = t.object_id "
                    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                    "WHERE s.name = ? AND t.name = ? "
                    "ORDER BY c.column_id"
                )
                cursor.execute(query, (schema, table))
                rows = cursor.fetchall()
                if not rows:
                    query = (
                        "SELECT c.name, ty.name FROM sys.columns c "
                        "JOIN sys.types ty ON c.user_type_id = ty.user_type_id "
                        "JOIN sys.views v ON c.object_id = v.object_id "
                        "JOIN sys.schemas s ON v.schema_id = s.schema_id "
                        "WHERE s.name = ? AND v.name = ? "
                        "ORDER BY c.column_id"
                    )
                    cursor.execute(query, (schema, table))
                    rows = cursor.fetchall()
                cols = [f"{row[0]} ({row[1]})" for row in rows]
            else:
                cursor.execute(f"SELECT TOP 0 * FROM ({source.query}) AS q")
                cols = [
                    f"{col[0]} ({col[1].__name__})"
                    for col in cursor.description
                ]
        return "\n".join(cols) if cols else "Nenhuma coluna encontrada"
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/sources")
async def sources(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        return [source.name for source in config.sources]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/debug-query")
async def debug_query(request: Request):
    payload = await request.json()
    source_name = payload.get("source", "")
    data = get_last_query(source_name)
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


@app.post("/api/debug-sample")
async def debug_sample(request: Request):
    payload = await request.json()
    source_name = payload.get("source", "")
    data = get_last_sample(source_name)
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


@app.post("/api/debug-send")
async def debug_send(request: Request):
    data = get_last_send()
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


@app.post("/api/preview")
async def preview(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        source_name = payload.get("source") or config.sources[0].name
        source = next((s for s in config.sources if s.name == source_name), None)
        if not source:
            raise ValueError("Fonte nao encontrada")
        conn_str = build_connection_string(config.sql)
        rows = fetch_rows(
            conn_str,
            source,
            last_id=0,
            last_ts=datetime(1900, 1, 1),
            last_tie=0,
            batch_size=20,
        )
        return json.dumps(rows, default=str, ensure_ascii=False, indent=2)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/validate-incremental")
async def validate_incremental(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        source_name = payload.get("source") or config.sources[0].name
        source = next((s for s in config.sources if s.name == source_name), None)
        if not source:
            raise ValueError("Fonte nao encontrada")
        conn_str = build_connection_string(config.sql)
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            if source.kind == "table":
                schema, table = (
                    source.table.split(".")
                    if "." in source.table
                    else ("dbo", source.table)
                )
                cursor.execute(
                    "SELECT c.name FROM sys.columns c "
                    "JOIN sys.tables t ON c.object_id = t.object_id "
                    "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                    "WHERE s.name = ? AND t.name = ?",
                    (schema, table),
                )
                cols = {row[0] for row in cursor.fetchall()}
                if not cols:
                    cursor.execute(
                        "SELECT c.name FROM sys.columns c "
                        "JOIN sys.views v ON c.object_id = v.object_id "
                        "JOIN sys.schemas s ON v.schema_id = s.schema_id "
                        "WHERE s.name = ? AND v.name = ?",
                        (schema, table),
                    )
                    cols = {row[0] for row in cursor.fetchall()}
            else:
                cursor.execute(f"SELECT TOP 0 * FROM ({source.query}) AS q")
                cols = {col[0] for col in cursor.description}
        inc = source.incremental
        missing = []
        if inc.id_column and inc.id_column not in cols:
            missing.append(inc.id_column)
        if inc.mode == "ts" and inc.ts_column not in cols:
            missing.append(inc.ts_column)
        if inc.tie_breaker and inc.tie_breaker not in cols:
            missing.append(inc.tie_breaker)
        if missing:
            return "Colunas faltando: " + ", ".join(missing)
        return "Incremental OK"
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/test-endpoint")
async def test_endpoint(request: Request):
    payload = await request.json()
    try:
        config = _parse_config_from_body(payload)
        response = requests.post(
            config.sink.api_url,
            json=[],
            headers={"Authorization": f"Bearer {config.sink.token}"},
            timeout=config.sink.timeout,
            verify=config.sink.verify_ssl,
        )
        return f"HTTP {response.status_code}"
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/start")
async def start(request: Request):
    payload = await request.json()
    yaml_text = payload.get("yaml", "")
    if yaml_text:
        _save_yaml_text(DEFAULT_CONFIG_PATH, yaml_text)
    ok = start_agent(DEFAULT_CONFIG_PATH)
    return "Agente iniciado" if ok else "Agente já está em execução"


@app.post("/api/stop")
async def stop():
    ok = stop_agent()
    return "Agente parado" if ok else "Agente já está parado"


@app.get("/api/status")
def status():
    status_data = agent_status()
    try:
        config = load_config(DEFAULT_CONFIG_PATH)
        queue_path = config.paths.queue
        state_path = config.paths.state
        status_data["queue_mb"] = (
            round(queue_path.stat().st_size / (1024 * 1024), 2) if queue_path.exists() else 0
        )
        status_data["state"] = load_state(state_path)
    except Exception as exc:
        status_data["config_error"] = str(exc)
    return json.dumps(status_data, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8765)
