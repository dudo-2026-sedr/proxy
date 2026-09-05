import os
import re
import json
import sqlite3
from typing import List, Tuple, Dict
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.security import APIKeyQuery
import httpx

app = FastAPI(title="Transparent LLM Proxy & Admin Dashboard")

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://syntro.up.railway.app").rstrip("/")
ADMIN_KEY = os.getenv("ADMIN_KEY")
DB_FILE = "/data/models.db" if os.path.exists("/data") else "models.db"

MODEL_MAPPINGS_LIST: List[Tuple[int, str, str]] = []
ERROR_RULES_LIST: List[Tuple[int, str, str]] = []
SETTINGS: Dict[str, str] = {}

DEFAULT_MODELS = [
    ("qwen3.8-flash", "qwen-3.8-max-0902"),
    ("myt/MiniMax-M3-free", "qwen-3.8-max-0902"),
    (r"myt\/MiniMax-M3-free", "qwen-3.8-max-0902"),
    ("MiniMax AI", "Qwen AI"),
    ("MiniMax-response-v1", "qwen-response-v1"),
    ("MiniMax", "Qwen"),
    ("minimax/minimax-m3:free", "claude-sonnet-5"),
    ("orcarouter/free", "claude-fable-5.1"),
    ("deepseek-v4-flash", "claude-fable-5.1"),
    ("deepseek-v4-pro", "claude-fable-5.1"),
    ("deepseek/deepseek-v4-flash", "claude-fable-5.1"),
    ("deepseek/deepseek-v4-pro", "claude-fable-5.1"),
    ("z-ai/glm-5.3-free", "claude-fable-5.1"),
    ("glm-5.3", "claude-fable-5.1"),
    ("qwen/qwen3.8-max:free", "gpt-6-astra")
]

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # 1. Таблица моделей
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='mappings'")
        row = cursor.fetchone()
        if row and "PRIMARY KEY" in row[0] and "id" not in row[0]:
            conn.execute("ALTER TABLE mappings RENAME TO mappings_old")
            conn.execute("""
                CREATE TABLE mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    real_model TEXT NOT NULL,
                    fake_model TEXT NOT NULL,
                    UNIQUE(real_model, fake_model)
                )
            """)
            conn.execute("INSERT OR IGNORE INTO mappings (real_model, fake_model) SELECT real_model, fake_model FROM mappings_old")
            conn.execute("DROP TABLE mappings_old")
        else:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    real_model TEXT NOT NULL,
                    fake_model TEXT NOT NULL,
                    UNIQUE(real_model, fake_model)
                )
            """)

        conn.executemany("INSERT OR IGNORE INTO mappings (real_model, fake_model) VALUES (?, ?)", DEFAULT_MODELS)

        # 2. Таблица настроек (без жестко зашитых текстов)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # 3. Таблица правил ошибок (чистая, без дефолтных шаблонов)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS error_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger TEXT NOT NULL UNIQUE,
                message TEXT NOT NULL
            )
        """)
        conn.commit()

def load_data():
    global MODEL_MAPPINGS_LIST, ERROR_RULES_LIST, SETTINGS
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, real_model, fake_model FROM mappings ORDER BY id ASC")
        MODEL_MAPPINGS_LIST = cursor.fetchall()

        cursor.execute("SELECT id, trigger, message FROM error_rules ORDER BY id ASC")
        ERROR_RULES_LIST = cursor.fetchall()

        cursor.execute("SELECT key, value FROM settings")
        SETTINGS = dict(cursor.fetchall())

@app.on_event("startup")
def startup():
    init_db()
    load_data()

def replace_models(text: str) -> str:
    for _, real, fake in MODEL_MAPPINGS_LIST:
        text = text.replace(f'"{real}"', f'"{fake}"')
        escaped_real = real.replace("/", r"\/")
        if escaped_real != real:
            text = text.replace(f'"{escaped_real}"', f'"{fake}"')
    return text

def extract_original_error_message(raw_text: str) -> str:
    try:
        data = json.loads(raw_text)
        if isinstance(data, dict):
            if "error" in data and isinstance(data["error"], dict):
                return data["error"].get("message", raw_text)
            if "message" in data:
                return data["message"]
    except Exception:
        pass
    return raw_text

def resolve_custom_error(raw_text: str, status_code: int = 400) -> str:
    lowered = raw_text.lower()
    for _, trigger, msg in ERROR_RULES_LIST:
        if trigger.lower() in lowered or str(status_code) == trigger.strip():
            return msg
    
    default_err = SETTINGS.get("default_error", "").strip()
    if default_err:
        return default_err
        
    return extract_original_error_message(raw_text)

def make_error_response(message: str, status_code: int = 400) -> Response:
    return Response(
        content=json.dumps({
            "error": {
                "message": message,
                "type": "api_error",
                "param": None,
                "code": "service_error"
            }
        }, ensure_ascii=False),
        status_code=status_code,
        media_type="application/json"
    )

# --- Админ-панель (Монохромная) ---

api_key_query = APIKeyQuery(name="key", auto_error=False)

def verify_admin(key: str = Depends(api_key_query)):
    if not ADMIN_KEY or key != ADMIN_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin Key is not configured or invalid"
        )
    return key

@app.get("/admin", response_class=HTMLResponse)
def admin_page(key: str = Depends(verify_admin)):
    default_err = SETTINGS.get("default_error", "")
    
    model_rows = "".join(
        f"<tr><td><code>{r}</code></td><td><span class='badge'>{f}</span></td>"
        f"<td style='text-align: right;'><form method='post' action='/admin/models/delete?key={key}' style='margin:0;'>"
        f"<input type='hidden' name='row_id' value='{mid}'>"
        f"<button type='submit' class='btn-danger'>Удалить</button></form></td></tr>"
        for mid, r, f in MODEL_MAPPINGS_LIST
    )

    error_rows = "".join(
        f"<tr><td><code>{t}</code></td><td>{m}</td>"
        f"<td style='text-align: right;'><form method='post' action='/admin/errors/delete?key={key}' style='margin:0;'>"
        f"<input type='hidden' name='rule_id' value='{rid}'>"
        f"<button type='submit' class='btn-danger'>Удалить</button></form></td></tr>"
        for rid, t, m in ERROR_RULES_LIST
    ) if ERROR_RULES_LIST else "<tr><td colspan='3' style='text-align: center; color: var(--muted); padding: 18px;'>Точечные правила не настроены</td></tr>"

    return f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Control Center — Transparent Proxy</title>
        <style>
            :root {{
                --bg: #09090b;
                --card-bg: #121214;
                --border: #27272a;
                --text: #f4f4f5;
                --muted: #a1a1aa;
                --input-bg: #000000;
            }}
            * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
            body {{ background: var(--bg); color: var(--text); padding: 40px 20px; line-height: 1.5; }}
            .container {{ max-width: 960px; margin: 0 auto; display: flex; flex-direction: column; gap: 32px; }}
            
            .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 20px; }}
            .title {{ font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }}
            .status-pill {{ background: #18181b; border: 1px solid #3f3f46; color: #fff; font-size: 12px; padding: 4px 10px; border-radius: 999px; }}
            
            .card {{ background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 24px; display: flex; flex-direction: column; gap: 16px; }}
            .card-title {{ font-size: 15px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }}
            
            input, textarea {{ background: var(--input-bg); border: 1px solid var(--border); color: var(--text); padding: 10px 14px; border-radius: 8px; font-size: 14px; outline: none; transition: border-color 0.2s; }}
            input:focus, textarea:focus {{ border-color: #ffffff; }}
            
            button {{ background: #ffffff; color: #000000; font-weight: 600; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-size: 14px; transition: opacity 0.15s; }}
            button:hover {{ opacity: 0.85; }}
            .btn-danger {{ background: transparent; color: #ef4444; border: 1px solid #3f1d1d; padding: 6px 12px; font-size: 12px; }}
            .btn-danger:hover {{ background: #ef4444; color: #ffffff; }}
            
            table {{ width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 14px; }}
            th {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 500; font-size: 12px; text-transform: uppercase; }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }}
            tr:last-child td {{ border-bottom: none; }}
            
            code {{ background: #000; border: 1px solid #27272a; padding: 3px 6px; border-radius: 4px; font-family: monospace; font-size: 13px; }}
            .badge {{ background: #27272a; color: #fff; padding: 3px 8px; border-radius: 4px; font-size: 12px; }}
            .form-grid {{ display: flex; gap: 10px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div>
                    <h1 class="title">Proxy Gateway Console</h1>
                    <p style="color: var(--muted); font-size: 13px; margin-top: 4px;">Управление роутингом, фильтрами и ошибками</p>
                </div>
                <div class="status-pill">Active</div>
            </div>

            <!-- Глобальный текст ошибки -->
            <div class="card">
                <div class="card-title">1. Глобальный текст ошибки</div>
                <p style="font-size: 13px; color: var(--muted);">Заменяет все непредвиденные ошибки и сбои вышестоящего провайдера. Если оставить пустым, отдается оригинальная ошибка.</p>
                <form method="post" action="/admin/settings/default-error?key={key}" style="display: flex; gap: 10px;">
                    <input name="default_error" value="{default_err}" placeholder="Введите общий текст ошибки..." style="flex: 1;">
                    <button type="submit">Сохранить</button>
                </form>
            </div>

            <!-- Точечные правила ошибок -->
            <div class="card">
                <div class="card-title">2. Точечные правила замены ошибок</div>
                <p style="font-size: 13px; color: var(--muted);">Если исходный ответ содержит триггер (слово или HTTP-код), он будет подменен на указанный текст.</p>
                <form method="post" action="/admin/errors/add?key={key}" class="form-grid">
                    <input name="trigger" placeholder="Триггер (напр: balance, quota, 429)" required style="width: 30%;">
                    <input name="message" placeholder="Сообщение для пользователя" required style="flex: 1;">
                    <button type="submit">Добавить</button>
                </form>
                <table>
                    <thead><tr><th>Триггер</th><th>Отображаемый текст</th><th style="text-align: right;">Действие</th></tr></thead>
                    <tbody>{error_rows}</tbody>
                </table>
            </div>

            <!-- Модели -->
            <div class="card">
                <div class="card-title">3. Алиасы моделей</div>
                <form method="post" action="/admin/models/add?key={key}" class="form-grid">
                    <input name="real_model" placeholder="Реальная модель / строка" required style="flex: 1;">
                    <input name="fake_model" placeholder="Фейковое имя" required style="flex: 1;">
                    <button type="submit">Привязать</button>
                </form>
                <table>
                    <thead><tr><th>Реальная модель / строка</th><th>Фейковое имя для клиента</th><th style="text-align: right;">Действие</th></tr></thead>
                    <tbody>{model_rows}</tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

# --- Роуты админки ---

@app.post("/admin/settings/default-error")
def update_default_error(default_error: str = Form(""), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('default_error', ?)", (default_error.strip(),))
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

@app.post("/admin/errors/add")
def add_error_rule(trigger: str = Form(...), message: str = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR REPLACE INTO error_rules (trigger, message) VALUES (?, ?)", (trigger.strip(), message.strip()))
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

@app.post("/admin/errors/delete")
def delete_error_rule(rule_id: int = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM error_rules WHERE id = ?", (rule_id,))
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

@app.post("/admin/models/add")
def add_model(real_model: str = Form(...), fake_model: str = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR REPLACE INTO mappings (real_model, fake_model) VALUES (?, ?)", (real_model.strip(), fake_model.strip()))
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

@app.post("/admin/models/delete")
def delete_model(row_id: int = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM mappings WHERE id = ?", (row_id,))
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

# --- Потоковый генератор с фильтрами ---

async def stream_filter_generator(upstream_response: httpx.Response, requested_model: str = None):
    line_buffer = ""
    in_think = False
    think_acc = ""
    event_skipped = False

    async for raw_chunk in upstream_response.aiter_bytes():
        line_buffer += raw_chunk.decode("utf-8", errors="ignore")
        while "\n" in line_buffer:
            line, line_buffer = line_buffer.split("\n", 1)
            line = line.strip("\r")

            if not line:
                if not event_skipped:
                    yield b"\n"
                event_skipped = False
                continue

            line = replace_models(line)

            if line.startswith("data: ") and line != "data: [DONE]":
                payload = line[6:].strip()
                try:
                    data = json.loads(payload)

                    if "error" in data:
                        err_str = json.dumps(data["error"])
                        custom_msg = resolve_custom_error(err_str, 400)
                        data["error"]["message"] = custom_msg
                        data["error"]["code"] = "service_error"
                        data["error"].pop("param", None)
                        yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")
                        continue

                    if requested_model and "model" in data:
                        data["model"] = requested_model

                    if "choices" in data and len(data["choices"]) > 0:
                        choice = data["choices"][0]
                        delta = choice.get("delta", {})

                        delta.pop("reasoning_content", None)
                        delta.pop("reasoning_details", None)
                        if delta.get("name") in ["MiniMax AI", "Qwen AI"]:
                            delta.pop("name", None)

                        content = delta.get("content", "")

                        if content:
                            if not in_think:
                                if "<think>" in content:
                                    in_think = True
                                    parts = content.split("<think>", 1)
                                    before_think = parts[0]
                                    think_acc = parts[1]
                                    clean_acc = think_acc.replace(r"<\/think>", "</think>")
                                    if "</think>" in clean_acc:
                                        after_think = clean_acc.split("</think>", 1)[1]
                                        in_think = False
                                        think_acc = ""
                                        delta["content"] = before_think + after_think.lstrip("\n")
                                    else:
                                        if before_think:
                                            delta["content"] = before_think
                                        else:
                                            event_skipped = True
                                            continue
                            else:
                                think_acc += content
                                clean_acc = think_acc.replace(r"<\/think>", "</think>")
                                if "</think>" in clean_acc:
                                    after_think = clean_acc.split("</think>", 1)[1]
                                    in_think = False
                                    think_acc = ""
                                    delta["content"] = after_think.lstrip("\n")
                                else:
                                    event_skipped = True
                                    continue

                        if not delta.get("content") and not delta.get("role") and not choice.get("finish_reason"):
                            event_skipped = True
                            continue

                    line = "data: " + json.dumps(data, ensure_ascii=False)
                except Exception:
                    pass

            yield (line + "\n").encode("utf-8")

    if line_buffer:
        yield replace_models(line_buffer).encode("utf-8")

# --- Основной прокси ---

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    target_url = f"{UPSTREAM_URL}/{path}"
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    body = await request.body()
    requested_model = None
    try:
        if body:
            parsed_req = json.loads(body)
            if isinstance(parsed_req, dict):
                requested_model = parsed_req.get("model")
    except Exception:
        pass

    client = httpx.AsyncClient(timeout=180.0)

    # 1. Сетевые таймауты
    try:
        req = client.build_request(
            method=request.method,
            url=target_url,
            headers=headers,
            params=request.query_params,
            content=body
        )
        upstream_resp = await client.send(req, stream=True)
    except Exception:
        await client.aclose()
        msg = resolve_custom_error("connection_error timeout 502", 502)
        return make_error_response(msg, status_code=502)

    # 2. HTTP ошибки (4xx, 5xx)
    if upstream_resp.status_code >= 400:
        try:
            err_bytes = await upstream_resp.aread()
            raw_err_text = err_bytes.decode("utf-8", errors="ignore")
        finally:
            await upstream_resp.aclose()
            await client.aclose()
        
        msg = resolve_custom_error(raw_err_text, upstream_resp.status_code)
        return make_error_response(msg, status_code=upstream_resp.status_code)

    content_type = upstream_resp.headers.get("content-type", "")

    # 3. Стриминг (SSE)
    if "text/event-stream" in content_type:
        async def close_wrapper():
            try:
                async for chunk in stream_filter_generator(upstream_resp, requested_model=requested_model):
                    yield chunk
            finally:
                await upstream_resp.aclose()
                await client.aclose()

        resp_headers = dict(upstream_resp.headers)
        resp_headers.pop("content-length", None)
        return StreamingResponse(close_wrapper(), status_code=upstream_resp.status_code, headers=resp_headers)

    # 4. JSON-ответ
    try:
        raw_body = await upstream_resp.aread()
        text = raw_body.decode("utf-8", errors="ignore")

        try:
            data = json.loads(text)
            
            if data.get("error") or (data.get("base_resp", {}).get("status_code", 0) != 0):
                msg = resolve_custom_error(text, 400)
                return make_error_response(msg, status_code=400)

            text = replace_models(text)
            data = json.loads(text)

            if requested_model and "model" in data:
                data["model"] = requested_model

            if "choices" in data and isinstance(data["choices"], list):
                for ch in data["choices"]:
                    msg = ch.get("message", {})
                    msg.pop("reasoning_content", None)
                    msg.pop("reasoning_details", None)
                    if msg.get("name") in ["MiniMax AI", "Qwen AI"]:
                        msg.pop("name", None)
                    if "content" in msg and msg["content"]:
                        msg["content"] = re.sub(r"<think>.*?</think>", "", msg["content"], flags=re.DOTALL).lstrip("\n")
            text = json.dumps(data, ensure_ascii=False)
        except Exception:
            pass

        resp_headers = dict(upstream_resp.headers)
        resp_headers.pop("content-length", None)
        return Response(content=text, status_code=upstream_resp.status_code, headers=resp_headers)
    finally:
        await upstream_resp.aclose()
        await client.aclose()
