import os
import sqlite3
import httpx
from fastapi import FastAPI, Request, Response, Form
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse

app = FastAPI()

UPSTREAM_URL = "https://syntro.up.railway.app"
ADMIN_KEY = os.getenv("ADMIN_KEY", "")  # Смените на свой пароль
DB_FILE = "/data/models.db" if os.path.exists("/data") else "models.db"

# --- Инициализация базы данных и кэша ---
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mappings (
                real_model TEXT PRIMARY KEY,
                fake_model TEXT NOT NULL
            )
        """)
        # Начальные данные, если база пустая
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM mappings")
        if cursor.fetchone()[0] == 0:
            conn.execute("INSERT INTO mappings VALUES (?, ?)", ("qwen3.8-flash", "qwen-3.8-max-0902"))
            conn.commit()

init_db()

# Локальный кэш, чтобы не дергать диск на каждый входящий чанк
MODEL_MAPPING = {}

def reload_cache():
    global MODEL_MAPPING
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("SELECT real_model, fake_model FROM mappings").fetchall()
        MODEL_MAPPING = {real: fake for real, fake in rows}

reload_cache()

def replace_models(text: str) -> str:
    for real, fake in MODEL_MAPPING.items():
        text = text.replace(f'"{real}"', f'"{fake}"')
    return text

client = httpx.AsyncClient(timeout=300.0)

# --- Веб-админка для управления ---
@app.get("/admin", response_class=HTMLResponse)
async def admin_page(key: str = ""):
    if key != ADMIN_KEY:
        return HTMLResponse("""
            <form method="get" style="margin: 50px auto; width: 300px; font-family: sans-serif;">
                <h3>Вход в управление моделями</h3>
                <input type="password" name="key" placeholder="Секретный ключ" style="width:100%; padding:8px; margin-bottom:10px;">
                <button type="submit" style="width:100%; padding:8px;">Войти</button>
            </form>
        """, status_code=401)

    rows_html = ""
    for real, fake in MODEL_MAPPING.items():
        rows_html += f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ccc;">{real}</td>
            <td style="padding: 8px; border: 1px solid #ccc;">{fake}</td>
            <td style="padding: 8px; border: 1px solid #ccc; text-align: center;">
                <form method="post" action="/admin/delete?key={key}" style="display:inline;">
                    <input type="hidden" name="real_model" value="{real}">
                    <button type="submit" style="color:red; cursor:pointer;">Удалить</button>
                </form>
            </td>
        </tr>
        """

    return HTMLResponse(f"""
        <div style="max-width: 650px; margin: 40px auto; font-family: sans-serif;">
            <h2>Управление моделями New API Proxy</h2>
            
            <form method="post" action="/admin/add?key={key}" style="background: #f4f4f4; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
                <h4>Добавить / Изменить маппинг</h4>
                <input type="text" name="real_model" placeholder="Реальная модель (напр. qwen3.8-flash)" required style="width: 45%; padding: 8px;">
                <input type="text" name="fake_model" placeholder="Фейк модель (напр. qwen-3.8-max-0902)" required style="width: 45%; padding: 8px;">
                <button type="submit" style="padding: 8px 15px; margin-top: 10px; cursor:pointer;">Сохранить</button>
            </form>

            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #eee;">
                        <th style="padding: 8px; border: 1px solid #ccc;">Реальная (от провайдера)</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Фейковая (видит клиент)</th>
                        <th style="padding: 8px; border: 1px solid #ccc;">Действие</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    """)

@app.post("/admin/add")
async def add_model(real_model: str = Form(...), fake_model: str = Form(...), key: str = ""):
    if key != ADMIN_KEY:
        return Response("Forbidden", status_code=403)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR REPLACE INTO mappings VALUES (?, ?)", (real_model.strip(), fake_model.strip()))
        conn.commit()
    reload_cache()
    return RedirectResponse(f"/admin?key={key}", status_code=303)

@app.post("/admin/delete")
async def delete_model(real_model: str = Form(...), key: str = ""):
    if key != ADMIN_KEY:
        return Response("Forbidden", status_code=403)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM mappings WHERE real_model = ?", (real_model.strip(),))
        conn.commit()
    reload_cache()
    return RedirectResponse(f"/admin?key={key}", status_code=303)

# --- Проксирование API ---
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    target_url = f"{UPSTREAM_URL}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    body = await request.body()

    req = client.build_request(
        method=request.method,
        url=target_url,
        headers=headers,
        content=body
    )
    upstream_res = await client.send(req, stream=True)

    content_type = upstream_res.headers.get("content-type", "")
    res_headers = {
        k: v for k, v in upstream_res.headers.items()
        if k.lower() not in ["content-length", "content-encoding", "transfer-encoding"]
    }

    if "text/event-stream" in content_type:
        async def event_generator():
            async for chunk in upstream_res.aiter_text():
                yield replace_models(chunk).encode("utf-8")
        return StreamingResponse(event_generator(), status_code=upstream_res.status_code, headers=res_headers, media_type="text/event-stream")

    if "application/json" in content_type:
        raw_body = await upstream_res.aread()
        text = raw_body.decode("utf-8", errors="replace")
        return Response(content=replace_models(text).encode("utf-8"), status_code=upstream_res.status_code, headers=res_headers, media_type="application/json")

    raw_body = await upstream_res.aread()
    return Response(content=raw_body, status_code=upstream_res.status_code, headers=res_headers)
