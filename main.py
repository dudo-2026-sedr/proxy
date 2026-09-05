import os
import re
import json
import sqlite3
from typing import List, Tuple
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.security import APIKeyQuery
import httpx

app = FastAPI(title="Transparent LLM Proxy with Dynamic Model Routing")

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://syntro.up.railway.app").rstrip("/")
ADMIN_KEY = os.getenv("ADMIN_KEY", "")
DB_FILE = "/data/models.db" if os.path.exists("/data") else "models.db"

MODEL_MAPPINGS_LIST: List[Tuple[int, str, str]] = []

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
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='mappings'")
        row = cursor.fetchone()
        
        # Миграция старой схемы (где real_model был PRIMARY KEY) на гибкую структуру
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
        conn.commit()

def load_mappings():
    global MODEL_MAPPINGS_LIST
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, real_model, fake_model FROM mappings ORDER BY id ASC")
        MODEL_MAPPINGS_LIST = cursor.fetchall()

@app.on_event("startup")
def startup():
    init_db()
    load_mappings()

def replace_models(text: str) -> str:
    for _, real, fake in MODEL_MAPPINGS_LIST:
        text = text.replace(f'"{real}"', f'"{fake}"')
        escaped_real = real.replace("/", r"\/")
        if escaped_real != real:
            text = text.replace(f'"{escaped_real}"', f'"{fake}"')
    return text

# --- Админ-панель ---

api_key_query = APIKeyQuery(name="key", auto_error=False)

def verify_admin(key: str = Depends(api_key_query)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Admin Key")
    return key

@app.get("/admin", response_class=HTMLResponse)
def admin_page(key: str = Depends(verify_admin)):
    rows = "".join(
        f"<tr><td><code>{r}</code></td><td><code>{f}</code></td>"
        f"<td><form method='post' action='/admin/delete?key={key}' style='margin:0;'>"
        f"<input type='hidden' name='row_id' value='{mid}'>"
        f"<button type='submit' style='color:red;'>Удалить</button></form></td></tr>"
        for mid, r, f in MODEL_MAPPINGS_LIST
    )
    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"><title>Proxy Admin</title></head>
    <body style="font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px;">
        <h2>Управление подменой моделей</h2>
        <form method="post" action="/admin/add?key={key}" style="display:flex; gap:10px; margin-bottom: 20px;">
            <input name="real_model" placeholder="Реальная модель / строка" required style="flex:1; padding:8px;">
            <input name="fake_model" placeholder="Фейковое имя" required style="flex:1; padding:8px;">
            <button type="submit" style="padding:8px 16px;">Сохранить</button>
        </form>
        <table border="1" cellpadding="8" style="width:100%; border-collapse: collapse;">
            <thead><tr><th>Реальная модель / строка</th><th>Фейковое имя</th><th>Действие</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </body>
    </html>
    """

@app.post("/admin/add")
def admin_add(real_model: str = Form(...), fake_model: str = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR REPLACE INTO mappings (real_model, fake_model) VALUES (?, ?)", (real_model.strip(), fake_model.strip()))
        conn.commit()
    load_mappings()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

@app.post("/admin/delete")
def admin_delete(row_id: int = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM mappings WHERE id = ?", (row_id,))
        conn.commit()
    load_mappings()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

# --- Потоковый генератор с вырезанием <think> и динамической моделью ---

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

                    # Динамически возвращаем ту модель, которую запросил клиент
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

# --- Основной прозрачный Proxy ---

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

    try:
        req = client.build_request(
            method=request.method,
            url=target_url,
            headers=headers,
            params=request.query_params,
            content=body
        )
        upstream_resp = await client.send(req, stream=True)
    except Exception as e:
        await client.aclose()
        return Response(content=f"Proxy error: {str(e)}", status_code=502)

    content_type = upstream_resp.headers.get("content-type", "")

    # Стриминг (SSE)
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

    # Обычный JSON-ответ
    try:
        raw_body = await upstream_resp.aread()
        text = raw_body.decode("utf-8", errors="ignore")
        text = replace_models(text)

        # Перехват региональных ошибок лимитов с индонезийским текстом
        if "Model gratis" in text or "PAYG" in text:
            try:
                err_data = {
                    "error": {
                        "message": "The request exceeded the maximum context limit (35,000 input / 20,000 output tokens).",
                        "type": "invalid_request_error",
                        "param": None,
                        "code": "context_length_exceeded"
                    }
                }
                text = json.dumps(err_data, ensure_ascii=False)
            except Exception:
                pass

        try:
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
