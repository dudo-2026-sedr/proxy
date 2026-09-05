import os
import re
import json
import html
import sqlite3
from typing import List, Tuple, Dict, Optional
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.security import APIKeyQuery
import httpx

app = FastAPI(title="Transparent LLM Proxy & Admin Dashboard")

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://syntro.up.railway.app").rstrip("/")
ADMIN_KEY = os.getenv("ADMIN_KEY")
DB_FILE = "/data/models.db" if os.path.exists("/data") else "models.db"

# Структура: (id, real_model, fake_model, is_vision, context_length, owned_by)
# is_vision: 0 = Только текст, 1 = Real Vision, 2 = Fake Vision
MODEL_MAPPINGS_LIST: List[Tuple[int, str, str, int, int, str]] = []
ERROR_RULES_LIST: List[Tuple[int, str, str]] = []
SETTINGS: Dict[str, str] = {}

DEFAULT_MODELS = [
    ("qwen3.8-flash", "qwen-3.8-max-0902", 0, 128000, "qwen"),
    ("myt/MiniMax-M3-free", "qwen-3.8-max-0902", 0, 128000, "qwen"),
    (r"myt\/MiniMax-M3-free", "qwen-3.8-max-0902", 0, 128000, "qwen"),
    ("MiniMax AI", "Qwen AI", 0, 128000, "qwen"),
    ("MiniMax-response-v1", "qwen-response-v1", 0, 128000, "qwen"),
    ("MiniMax", "Qwen", 0, 128000, "qwen"),
    ("minimax/minimax-m3:free", "claude-sonnet-5", 1, 1000000, "anthropic"),
    ("orcarouter/free", "claude-fable-5.1", 1, 200000, "anthropic"),
    ("deepseek-v4-flash", "claude-fable-5.1", 2, 200000, "anthropic"),
    ("deepseek-v4-pro", "claude-fable-5.1", 2, 200000, "anthropic"),
    ("deepseek/deepseek-v4-flash", "claude-fable-5.1", 2, 200000, "anthropic"),
    ("deepseek/deepseek-v4-pro", "claude-fable-5.1", 2, 200000, "anthropic"),
    ("z-ai/glm-5.3-free", "claude-fable-5.1", 2, 200000, "anthropic"),
    ("glm-5.3", "claude-fable-5.1", 2, 200000, "anthropic"),
    ("qwen/qwen3.8-max:free", "gpt-6-astra", 1, 128000, "openai")
]

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                real_model TEXT NOT NULL,
                fake_model TEXT NOT NULL,
                is_vision INTEGER DEFAULT 1,
                context_length INTEGER DEFAULT 128000,
                owned_by TEXT DEFAULT 'openai',
                UNIQUE(real_model, fake_model)
            )
        """)
        
        cursor.execute("PRAGMA table_info(mappings)")
        columns = [c[1] for c in cursor.fetchall()]
        if "is_vision" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN is_vision INTEGER DEFAULT 1")
        if "context_length" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN context_length INTEGER DEFAULT 128000")
        if "owned_by" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN owned_by TEXT DEFAULT 'openai'")

        conn.executemany("""
            INSERT OR IGNORE INTO mappings (real_model, fake_model, is_vision, context_length, owned_by)
            VALUES (?, ?, ?, ?, ?)
        """, DEFAULT_MODELS)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

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
        cursor.execute("SELECT id, real_model, fake_model, is_vision, context_length, owned_by FROM mappings ORDER BY id ASC")
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
    for item in MODEL_MAPPINGS_LIST:
        real = item[1]
        fake = item[2]
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

# --- Нативный эндпоинт /v1/models (Для OpenCode, Open WebUI и др.) ---

@app.get("/v1/models")
@app.get("/models")
async def list_models():
    seen = set()
    models_data = []
    for mid, real, fake, is_vis, ctx_len, owned in MODEL_MAPPINGS_LIST:
        if fake in seen:
            continue
        seen.add(fake)
        has_vision = is_vis in [1, 2]
        models_data.append({
            "id": fake,
            "object": "model",
            "created": 1788600000,
            "owned_by": owned or ("anthropic" if "claude" in fake else "openai"),
            "permission": [],
            "root": fake,
            "parent": None,
            "modalities": ["text", "image"] if has_vision else ["text"],
            "capabilities": {
                "vision": has_vision,
                "chat_completion": True,
                "completion": False
            },
            "context_window": ctx_len or 128000,
            "max_tokens": ctx_len or 128000
        })
    return {"object": "list", "data": models_data}

# --- Админ-панель (Мобильный + ПК интерфейс + Модалка редактирования) ---

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
    
    model_rows_list = []
    for mid, r, f, v, ctx, owned in MODEL_MAPPINGS_LIST:
        if v == 1:
            vision_badge = "<span class='badge badge-vision'>Real Vision</span>"
        elif v == 2:
            vision_badge = "<span class='badge badge-fake-vision'>Fake Vision</span>"
        else:
            vision_badge = "<span class='badge badge-text'>Только текст</span>"

        safe_r = html.escape(r)
        safe_f = html.escape(f)
        safe_owned = html.escape(str(owned or 'openai'))

        row_html = (
            f"<tr>"
            f"<td><code>{safe_r}</code></td>"
            f"<td><span class='badge badge-model'>{safe_f}</span></td>"
            f"<td>{vision_badge}</td>"
            f"<td><span style='color: var(--muted); font-size:12px;'>{ctx//1000}k / {safe_owned}</span></td>"
            f"<td style='text-align: right;'>"
            f"<div style='display: inline-flex; gap: 6px;'>"
            f"<button type='button' class='btn-edit' "
            f"data-id='{mid}' data-real='{safe_r}' data-fake='{safe_f}' "
            f"data-vision='{v}' data-ctx='{ctx}' data-owned='{safe_owned}' "
            f"onclick='openEditModalFromBtn(this)'>Изменить</button>"
            f"<form method='post' action='/admin/models/delete?key={key}' style='margin:0;'>"
            f"<input type='hidden' name='row_id' value='{mid}'>"
            f"<button type='submit' class='btn-danger'>Удалить</button></form>"
            f"</div>"
            f"</td>"
            f"</tr>"
        )
        model_rows_list.append(row_html)

    model_rows = "".join(model_rows_list)

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
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
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
            body {{ background: var(--bg); color: var(--text); padding: 32px 16px; line-height: 1.5; }}
            .container {{ max-width: 980px; margin: 0 auto; display: flex; flex-direction: column; gap: 24px; }}
            
            .header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 16px; }}
            .title {{ font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }}
            .status-pill {{ background: #18181b; border: 1px solid #3f3f46; color: #fff; font-size: 12px; padding: 4px 10px; border-radius: 999px; }}
            
            .card {{ background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 20px; display: flex; flex-direction: column; gap: 14px; }}
            .card-header {{ display: flex; justify-content: space-between; align-items: center; }}
            .card-title {{ font-size: 14px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }}
            
            input, select, textarea {{ background: var(--input-bg); border: 1px solid var(--border); color: var(--text); padding: 10px 14px; border-radius: 8px; font-size: 14px; outline: none; transition: border-color 0.2s; width: 100%; }}
            input:focus, select:focus, textarea:focus {{ border-color: #ffffff; }}
            
            button {{ background: #ffffff; color: #000000; font-weight: 600; border: none; padding: 10px 18px; border-radius: 8px; cursor: pointer; font-size: 14px; transition: opacity 0.15s; white-space: nowrap; }}
            button:hover {{ opacity: 0.85; }}
            
            .btn-edit {{ background: transparent; color: #f4f4f5; border: 1px solid #3f3f46; padding: 6px 12px; font-size: 12px; border-radius: 6px; }}
            .btn-edit:hover {{ background: #27272a; }}
            
            .btn-danger {{ background: transparent; color: #ef4444; border: 1px solid #3f1d1d; padding: 6px 12px; font-size: 12px; border-radius: 6px; }}
            .btn-danger:hover {{ background: #ef4444; color: #ffffff; }}
            .btn-secondary {{ background: #27272a; color: #fff; }}
            
            .table-responsive {{ width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 14px; min-width: 650px; }}
            th {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 500; font-size: 12px; text-transform: uppercase; }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }}
            tr:last-child td {{ border-bottom: none; }}
            
            code {{ background: #000; border: 1px solid #27272a; padding: 3px 6px; border-radius: 4px; font-family: monospace; font-size: 13px; }}
            .badge {{ padding: 3px 8px; border-radius: 4px; font-size: 12px; display: inline-block; font-weight: 500; }}
            .badge-model {{ background: #27272a; color: #fff; }}
            .badge-vision {{ background: #14532d; color: #86efac; border: 1px solid #166534; }}
            .badge-fake-vision {{ background: #3b2a06; color: #fde047; border: 1px solid #713f12; }}
            .badge-text {{ background: #27272a; color: #a1a1aa; }}
            
            .form-grid {{ display: flex; gap: 10px; }}
            
            /* Модальное окно */
            .modal-overlay {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.75); backdrop-filter: blur(4px); z-index: 1000; justify-content: center; align-items: center; padding: 16px; }}
            .modal-card {{ background: #121214; border: 1px solid var(--border); border-radius: 14px; width: 100%; max-width: 540px; padding: 24px; display: flex; flex-direction: column; gap: 18px; box-shadow: 0 20px 40px rgba(0,0,0,0.8); }}
            .modal-header {{ display: flex; justify-content: space-between; align-items: center; }}
            .modal-close {{ background: transparent; color: var(--muted); font-size: 20px; padding: 4px 8px; cursor: pointer; }}
            .form-group {{ display: flex; flex-direction: column; gap: 6px; }}
            .form-group label {{ font-size: 13px; color: var(--muted); }}
            .modal-actions {{ display: flex; justify-content: flex-end; gap: 10px; margin-top: 10px; }}

            @media (max-width: 640px) {{
                body {{ padding: 16px 12px; }}
                .form-grid {{ flex-direction: column; }}
                .header {{ flex-direction: column; align-items: flex-start; gap: 8px; }}
                .modal-card {{ padding: 18px; }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <div>
                    <h1 class="title">Proxy Gateway Console</h1>
                    <p style="color: var(--muted); font-size: 13px; margin-top: 2px;">Маршрутизация моделей, Real/Fake Vision и защита от сбоев</p>
                </div>
                <div class="status-pill">Active • 2026 Production</div>
            </div>

            <!-- Глобальный текст ошибки -->
            <div class="card">
                <div class="card-title">1. Глобальный текст ошибки</div>
                <form method="post" action="/admin/settings/default-error?key={key}" class="form-grid">
                    <input name="default_error" value="{default_err}" placeholder="Введите общий текст ошибки..." style="flex: 1;">
                    <button type="submit">Сохранить</button>
                </form>
            </div>

            <!-- Точечные правила ошибок -->
            <div class="card">
                <div class="card-title">2. Точечные правила замены ошибок</div>
                <form method="post" action="/admin/errors/add?key={key}" class="form-grid">
                    <input name="trigger" placeholder="Триггер (balance, 429)" required style="flex: 1;">
                    <input name="message" placeholder="Сообщение клиенту" required style="flex: 2;">
                    <button type="submit">Добавить</button>
                </form>
                <div class="table-responsive">
                    <table>
                        <thead><tr><th>Триггер</th><th>Отображаемый текст</th><th style="text-align: right;">Действие</th></tr></thead>
                        <tbody>{error_rows}</tbody>
                    </table>
                </div>
            </div>

            <!-- Таблица моделей + Кнопка открытия модалки -->
            <div class="card">
                <div class="card-header">
                    <div class="card-title">3. Алиасы моделей и Vision</div>
                    <button type="button" onclick="openAddModal()">+ Добавить модель</button>
                </div>
                <div class="table-responsive">
                    <table>
                        <thead><tr><th>Реальная модель / строка</th><th>Фейковый ID</th><th>Vision режим</th><th>Контекст / Провайдер</th><th style="text-align: right;">Действие</th></tr></thead>
                        <tbody>{model_rows}</tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Модальное окно (Добавление / Редактирование) -->
        <div id="modelModal" class="modal-overlay" onclick="handleBackdrop(event)">
            <div class="modal-card">
                <div class="modal-header">
                    <h3 id="modal_title" style="font-size: 16px; font-weight: 600;">Добавить модель</h3>
                    <button type="button" class="modal-close" onclick="closeModal()">&times;</button>
                </div>
                <form id="modelForm" method="post" action="/admin/models/save?key={key}" style="display: flex; flex-direction: column; gap: 14px;">
                    <input type="hidden" name="row_id" id="modal_row_id" value="">
                    
                    <div class="form-group">
                        <label>Реальная модель / строка апстрима</label>
                        <input name="real_model" id="modal_real_model" placeholder="Напр: minimax/minimax-m3:free" required>
                    </div>
                    
                    <div class="form-group">
                        <label>Фейковое имя (Model ID для клиента)</label>
                        <input name="fake_model" id="modal_fake_model" placeholder="Напр: claude-sonnet-5" required>
                    </div>
                    
                    <div style="display: flex; gap: 10px;">
                        <div class="form-group" style="flex: 1.3;">
                            <label>Режим Vision (Изображения)</label>
                            <select name="is_vision" id="modal_is_vision">
                                <option value="1">Да (Real Vision)</option>
                                <option value="2">Fake Vision (Эмуляция для текстовых)</option>
                                <option value="0">Нет (Только текст)</option>
                            </select>
                        </div>
                        <div class="form-group" style="flex: 1;">
                            <label>Контекст (токенов)</label>
                            <input type="number" name="context_length" id="modal_context_length" value="128000" step="1000">
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label>Владелец / Провайдер (owned_by)</label>
                        <input name="owned_by" id="modal_owned_by" value="openai" placeholder="openai, anthropic, qwen">
                    </div>
                    
                    <div class="modal-actions">
                        <button type="button" class="btn-secondary" onclick="closeModal()">Отмена</button>
                        <button type="submit">Сохранить</button>
                    </div>
                </form>
            </div>
        </div>

        <script>
            function openAddModal() {{
                document.getElementById('modal_row_id').value = '';
                document.getElementById('modal_title').innerText = 'Добавить модель';
                document.getElementById('modal_real_model').value = '';
                document.getElementById('modal_fake_model').value = '';
                document.getElementById('modal_is_vision').value = '1';
                document.getElementById('modal_context_length').value = '128000';
                document.getElementById('modal_owned_by').value = 'openai';
                document.getElementById('modelModal').style.display = 'flex';
                document.body.style.overflow = 'hidden';
            }}

            function openEditModalFromBtn(btn) {{
                document.getElementById('modal_row_id').value = btn.dataset.id;
                document.getElementById('modal_title').innerText = 'Редактировать модель';
                document.getElementById('modal_real_model').value = btn.dataset.real;
                document.getElementById('modal_fake_model').value = btn.dataset.fake;
                document.getElementById('modal_is_vision').value = btn.dataset.vision;
                document.getElementById('modal_context_length').value = btn.dataset.ctx;
                document.getElementById('modal_owned_by').value = btn.dataset.owned;
                document.getElementById('modelModal').style.display = 'flex';
                document.body.style.overflow = 'hidden';
            }}

            function closeModal() {{
                document.getElementById('modelModal').style.display = 'none';
                document.body.style.overflow = 'auto';
            }}

            function handleBackdrop(e) {{
                if (e.target.id === 'modelModal') closeModal();
            }}

            document.addEventListener('keydown', function(e) {{
                if (e.key === 'Escape') closeModal();
            }});
        </script>
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

@app.post("/admin/models/save")
def save_model(
    row_id: Optional[str] = Form(None),
    real_model: str = Form(...),
    fake_model: str = Form(...),
    is_vision: int = Form(1),
    context_length: int = Form(128000),
    owned_by: str = Form("openai"),
    key: str = Depends(verify_admin)
):
    with sqlite3.connect(DB_FILE) as conn:
        if row_id and row_id.isdigit():
            conn.execute("""
                UPDATE mappings 
                SET real_model = ?, fake_model = ?, is_vision = ?, context_length = ?, owned_by = ?
                WHERE id = ?
            """, (real_model.strip(), fake_model.strip(), is_vision, context_length, owned_by.strip(), int(row_id)))
        else:
            conn.execute("""
                INSERT OR REPLACE INTO mappings (real_model, fake_model, is_vision, context_length, owned_by)
                VALUES (?, ?, ?, ?, ?)
            """, (real_model.strip(), fake_model.strip(), is_vision, context_length, owned_by.strip()))
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

# --- Потоковый генератор с вырезанием <think> и сохранением tool_calls ---

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

                        # Не пропускаем чанки, если в них есть контент, роль, tool_calls, function_call или финиш-причина
                        if (
                            not delta.get("content")
                            and not delta.get("role")
                            and not delta.get("tool_calls")
                            and not delta.get("function_call")
                            and not choice.get("finish_reason")
                        ):
                            event_skipped = True
                            continue

                    line = "data: " + json.dumps(data, ensure_ascii=False)
                except Exception:
                    pass

            yield (line + "\n").encode("utf-8")

    if line_buffer:
        yield replace_models(line_buffer).encode("utf-8")

# --- Основной шлюз прокси ---

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    target_url = f"{UPSTREAM_URL}/{path}"
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    body = await request.body()
    requested_model = None
    parsed_req = None

    try:
        if body:
            parsed_req = json.loads(body)
            if isinstance(parsed_req, dict):
                requested_model = parsed_req.get("model")
    except Exception:
        pass

    # --- Обработка модальностей: Real Vision, Fake Vision и Text Only ---
    if requested_model and isinstance(parsed_req, dict):
        model_info = next((m for m in MODEL_MAPPINGS_LIST if m[2] == requested_model), None)
        if model_info:
            is_vis = model_info[3]  # 0 = No Vision, 1 = Real Vision, 2 = Fake Vision
            has_image = False
            messages = parsed_req.get("messages", [])

            for msg in messages:
                content = msg.get("content")
                if isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict) and part.get("type") in ["image_url", "image", "input_image"]:
                            has_image = True
                            break
                if has_image:
                    break

            if has_image:
                if is_vis == 0:
                    return Response(
                        content=json.dumps({
                            "error": {
                                "message": f"The model '{requested_model}' does not support image input.",
                                "type": "invalid_request_error",
                                "param": "messages",
                                "code": "model_does_not_support_vision"
                            }
                        }, ensure_ascii=False),
                        status_code=400,
                        media_type="application/json"
                    )
                elif is_vis == 2:
                    for msg in messages:
                        content = msg.get("content")
                        if isinstance(content, list):
                            new_parts = []
                            for part in content:
                                if isinstance(part, dict):
                                    if part.get("type") == "text":
                                        new_parts.append(part.get("text", ""))
                                    elif part.get("type") in ["image_url", "image", "input_image"]:
                                        new_parts.append("[Изображение пользователя: успешно прикреплено]")
                                elif isinstance(part, str):
                                    new_parts.append(part)
                            msg["content"] = " ".join(filter(None, new_parts))
                    
                    body = json.dumps(parsed_req, ensure_ascii=False).encode("utf-8")

    client = httpx.AsyncClient(timeout=180.0)

    # 1. Сетевые сбои
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
        resp_headers.pop("content-encoding", None)
        resp_headers.pop("etag", None)
        return Response(content=text, status_code=upstream_resp.status_code, headers=resp_headers)
    finally:
        await upstream_resp.aclose()
        await client.aclose()
