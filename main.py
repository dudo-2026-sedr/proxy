import os
import re
import io
import json
import html
import sqlite3
import asyncio
import string
import secrets
import time
import zlib
import base64
import hmac
import hashlib
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional
from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, StreamingResponse, Response, RedirectResponse
from fastapi.security import APIKeyQuery
import httpx

app = FastAPI(
    title="API Gateway",
    docs_url=None,
    redoc_url=None,
    openapi_url=None
)

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://new-api-production-44f4.up.railway.app").rstrip("/")
ADMIN_KEY = os.getenv("ADMIN_KEY")
DB_FILE = "/data/models.db" if os.path.exists("/data") else "models.db"

MODEL_MAPPINGS_LIST: List[Tuple[int, str, str, int, int, str, float, int, int]] = []
ERROR_RULES_LIST: List[Tuple[int, str, str]] = []
SETTINGS: Dict[str, str] = {}
API_KEYS_LIST: List[Tuple[int, str, str, str, str]] = []  # (id, anthropic_key, real_key, note, created_at)
KEY_USAGE: Dict[int, Dict[str, int]] = {}  # key_id -> {requests, input_tokens, output_tokens}
KEY_ID_BY_ANTHROPIC: Dict[str, int] = {}  # anthropic_key -> key_id


def bump_key_usage(key_id: Optional[int], input_tokens: int = 0, output_tokens: int = 0):
    """Инкрементирует счётчики per-key. Сначала в память, потом в БД."""
    if not key_id:
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if key_id not in KEY_USAGE:
        KEY_USAGE[key_id] = {"requests": 0, "input_tokens": 0, "output_tokens": 0}
    KEY_USAGE[key_id]["requests"] += 1
    KEY_USAGE[key_id]["input_tokens"] += max(0, int(input_tokens))
    KEY_USAGE[key_id]["output_tokens"] += max(0, int(output_tokens))
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("""
                INSERT INTO key_usage (key_id, requests, input_tokens, output_tokens, last_used)
                VALUES (?, 1, ?, ?, ?)
                ON CONFLICT(key_id) DO UPDATE SET
                    requests = requests + 1,
                    input_tokens = input_tokens + ?,
                    output_tokens = output_tokens + ?,
                    last_used = ?
            """, (key_id, int(input_tokens), int(output_tokens), now,
                  int(input_tokens), int(output_tokens), now))
            conn.commit()
    except Exception:
        pass

PRICING_INJECTION = """<style>
  a[href*="pricing"], a[href*="price"],
  [href*="/pricing"], [to*="/pricing"],
  [data-nav*="pricing"], .semi-navigation-item[href*="pricing"] {
    display: none !important;
  }
</style>
<script>
  (function() {
    function removePricingElements() {
      if (window.location.pathname.includes('/pricing')) {
        window.location.replace('/');
        return;
      }
      const elements = document.querySelectorAll('a, button, div, span, li');
      elements.forEach(el => {
        const href = (el.getAttribute('href') || el.getAttribute('to') || '').toLowerCase();
        const text = (el.innerText || el.textContent || '').trim().toLowerCase();
        if (href.includes('pricing') || href.includes('price')) {
          el.style.setProperty('display', 'none', 'important');
          el.remove();
        } else if (text === 'посмотреть цены' || text === 'цены' || text === 'pricing' || text === 'view pricing') {
          const target = el.closest('a') || el.closest('button') || el.closest('li') || el;
          target.style.setProperty('display', 'none', 'important');
          target.remove();
        }
      });
    }
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', removePricingElements);
    } else {
      removePricingElements();
    }
    new MutationObserver(removePricingElements).observe(document.documentElement, { childList: true, subtree: true });
  })();
</script>"""

BASE62 = string.ascii_letters + string.digits

SIGNATURE_SECRET = os.getenv("SIGNATURE_SECRET", "").encode("utf-8")
if not SIGNATURE_SECRET:
    SIGNATURE_SECRET = secrets.token_bytes(32)
    print("[WARN] SIGNATURE_SECRET not set — using ephemeral key. "
          "Set it in env to keep signatures valid across restarts.", flush=True)

_PREFIX_CACHE: "OrderedDict[str, None]" = OrderedDict()
_PREFIX_CACHE_MAX = 4096

# --- Реальный скользящий ratelimit-трекер (per-process, окно 60 сек) ---
import threading as _threading
_RL_LOCK = _threading.Lock()
_RL_REQ_TS = []
_RL_IN_TS = []
_RL_OUT_TS = []
_RL_WINDOW = 60
_RL_REQ_LIMIT = 50
_RL_IN_LIMIT = 40000
_RL_OUT_LIMIT = 8000
# Консистентные ID (фейковые, но стабильные в рамках процесса)
_ORG_ID = "7c4b0f1a-2e5d-4f8b-9a3c-6e1d8b2f5a9e"
_WORKSPACE_ID = "wrkspc_01JwQvzr7rXLA5AGx3HKfFUJ"

def _rl_prune():
    global _RL_REQ_TS, _RL_IN_TS, _RL_OUT_TS
    cutoff = time.time() - _RL_WINDOW
    _RL_REQ_TS = [t for t in _RL_REQ_TS if t > cutoff]
    _RL_IN_TS = [(t, n) for t, n in _RL_IN_TS if t > cutoff]
    _RL_OUT_TS = [(t, n) for t, n in _RL_OUT_TS if t > cutoff]

def rl_register(input_tokens: int = 0, output_tokens: int = 0):
    now = time.time()
    with _RL_LOCK:
        _RL_REQ_TS.append(now)
        if input_tokens:
            _RL_IN_TS.append((now, int(input_tokens)))
        if output_tokens:
            _RL_OUT_TS.append((now, int(output_tokens)))
        _rl_prune()

def rl_snapshot():
    with _RL_LOCK:
        _rl_prune()
        used_req = len(_RL_REQ_TS)
        used_in = sum(n for _, n in _RL_IN_TS)
        used_out = sum(n for _, n in _RL_OUT_TS)
        all_ts = list(_RL_REQ_TS) + [t for t, _ in _RL_IN_TS] + [t for t, _ in _RL_OUT_TS]
        if all_ts:
            reset_at = min(all_ts) + _RL_WINDOW
        else:
            reset_at = time.time() + _RL_WINDOW
        return used_req, used_in, used_out, reset_at

def gen_base62(length: int) -> str:
    return "".join(secrets.choice(BASE62) for _ in range(length))

def _encode_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)

def gen_anthropic_style_key() -> str:
    """Генерирует ключ в формате Anthropic: sk-ant-api03-<90 base62>."""
    body = "".join(secrets.choice(BASE62) for _ in range(90))
    return f"sk-ant-api03-{body}"


def gen_thinking_signature() -> str:
    version = b"\x18\x02"
    nonce = secrets.token_bytes(16)
    body = secrets.token_bytes(400 + secrets.randbelow(400))

    mac_input = version + nonce + body
    tag = hmac.new(SIGNATURE_SECRET, mac_input, hashlib.sha256).digest()

    inner = (
        b"\x0a" + _encode_varint(len(version)) + version +
        b"\x12" + _encode_varint(len(nonce)) + nonce +
        b"\x1a" + _encode_varint(len(body)) + body +
        b"\x22" + _encode_varint(len(tag)) + tag
    )

    outer = b"\x12" + _encode_varint(len(inner)) + inner
    return base64.b64encode(outer).decode("ascii")

def _parse_nested_signature(buf: bytes):
    """Возвращает (version, nonce, body, tag) или (None,)*4."""
    def read_varint(b, p):
        end = len(b)
        val = 0
        shift = 0
        while True:
            if p >= end:
                return None, p
            byte = b[p]
            p += 1
            val |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return val, p
            shift += 7
            if shift > 63:
                return None, p

    # Внешний уровень — из buf
    tag, pos = read_varint(buf, 0)
    if tag is None or (tag >> 3) != 2 or (tag & 0x07) != 2:
        return None, None, None, None
    outer_len, pos = read_varint(buf, pos)
    if outer_len is None or pos + outer_len != len(buf):
        return None, None, None, None
    inner = buf[pos:pos + outer_len]

    # Внутренний уровень — ИЗ INNER, а не из buf
    version = nonce = body = tag_val = None
    ipos = 0
    iend = len(inner)
    while ipos < iend:
        t, ipos = read_varint(inner, ipos)
        if t is None:
            return None, None, None, None
        fnum = t >> 3
        wtype = t & 0x07
        if wtype != 2:
            return None, None, None, None
        flen, ipos = read_varint(inner, ipos)
        if flen is None or ipos + flen > iend:
            return None, None, None, None
        payload = inner[ipos:ipos + flen]
        ipos += flen
        if fnum == 1:
            version = payload
        elif fnum == 2:
            nonce = payload
        elif fnum == 3:
            body = payload
        elif fnum == 4:
            tag_val = payload

    return version, nonce, body, tag_val

def validate_signature_format(sig) -> bool:
    if not isinstance(sig, str) or not sig:
        return False
    if len(sig) < 50 or len(sig) > 15400:
        return False
    if len(sig) % 4 != 0:
        return False
    if not re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", sig):
        return False

    try:
        decoded = base64.b64decode(sig, validate=True)
    except Exception:
        return False

    if len(decoded) < 32:
        return False

    version, nonce, body, tag_val = _parse_nested_signature(decoded)
    if version is None or nonce is None or body is None or tag_val is None:
        return False
    if version != b"\x18\x02":
        return False
    if len(tag_val) != 32:
        return False

    expected = hmac.new(SIGNATURE_SECRET, version + nonce + body,
                        hashlib.sha256).digest()
    return hmac.compare_digest(expected, tag_val)

def generate_provider_ids(owned_by: Optional[str]) -> Tuple[str, str]:
    ob = (owned_by or "").strip().lower()
    if "anthropic" in ob or "claude" in ob:
        res_id = f"msg_01{gen_base62(22)}"
    elif "openai" in ob or "gpt" in ob:
        res_id = f"chatcmpl-{gen_base62(29)}"
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        res_id = f"{ts}{secrets.token_hex(9)}"
    req_id = f"req_01{gen_base62(22)}"
    return res_id, req_id

def build_gateway_headers(owned_by: Optional[str], req_id: str, processing_ms: int = 280, is_stream: bool = False) -> dict:
    ob = (owned_by or "").strip().lower()
    now = datetime.now(timezone.utc)
    reset_time = (now + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
        "strict-transport-security": "max-age=31536000; includeSubDomains",
    }
    if is_stream:
        headers["content-type"] = "text/event-stream; charset=utf-8"
        headers["cache-control"] = "no-cache"
        headers["connection"] = "keep-alive"
    else:
        headers["content-type"] = "application/json"

    if "anthropic" in ob or "claude" in ob:
        used_req, used_in, used_out, reset_ts = rl_snapshot()
        reset_iso = datetime.fromtimestamp(reset_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rem_req = max(0, _RL_REQ_LIMIT - used_req)
        rem_in = max(0, _RL_IN_LIMIT - used_in)
        rem_out = max(0, _RL_OUT_LIMIT - used_out)
        headers.update({
            # Эти три заголовка у настоящего Anthropic ЕСТЬ — не убирать
            "server": "cloudflare",
            "via": "1.1 google",
            "x-cloud-trace-context": secrets.token_hex(16),
            "cf-ray": f"{secrets.token_hex(8)}-{secrets.choice(['FRA', 'AMS', 'LHR', 'CDG', 'IAD', 'SJC', 'NRT', 'SIN'])}",
            "cf-cache-status": "DYNAMIC",
            # Реальные идентификаторы
            "request-id": req_id,
            "anthropic-organization-id": _ORG_ID,
            "anthropic-workspace-id": _WORKSPACE_ID,
            # Реальные счётчики
            "anthropic-ratelimit-requests-limit": str(_RL_REQ_LIMIT),
            "anthropic-ratelimit-requests-remaining": str(rem_req),
            "anthropic-ratelimit-requests-reset": reset_iso,
            "anthropic-ratelimit-tokens-limit": str(_RL_IN_LIMIT),
            "anthropic-ratelimit-tokens-remaining": str(rem_in),
            "anthropic-ratelimit-tokens-reset": reset_iso,
            "anthropic-ratelimit-input-tokens-limit": str(_RL_IN_LIMIT),
            "anthropic-ratelimit-input-tokens-remaining": str(rem_in),
            "anthropic-ratelimit-input-tokens-reset": reset_iso,
            "anthropic-ratelimit-output-tokens-limit": str(_RL_OUT_LIMIT),
            "anthropic-ratelimit-output-tokens-remaining": str(rem_out),
            "anthropic-ratelimit-output-tokens-reset": reset_iso,
        })
    elif "openai" in ob or "gpt" in ob:
        headers.update({
            "server": "cloudflare",
            "x-request-id": req_id,
            "openai-organization": "user-default",
            "openai-processing-ms": str(max(10, processing_ms)),
            "openai-version": "2020-10-01",
            "access-control-expose-headers": "X-Request-ID",
            "x-ratelimit-limit-requests": "10000",
            "x-ratelimit-remaining-requests": "9999",
            "x-ratelimit-reset-requests": "6ms",
            "x-ratelimit-limit-tokens": "2000000",
            "x-ratelimit-remaining-tokens": "1999950",
            "x-ratelimit-reset-tokens": "1ms",
            "cf-cache-status": "DYNAMIC",
        })
    else:
        headers.update({
            "server": "openresty",
            "x-request-id": req_id,
            "connection": "keep-alive"
        })

    return headers

def estimate_tokens_text(text: str) -> int:
    if not text:
        return 0
    return max(1, (len(text.encode("utf-8")) + 2) // 3)

def estimate_messages_tokens(messages: list) -> int:
    if not messages or not isinstance(messages, list):
        return 8
    total = 0
    for m in messages:
        if not isinstance(m, dict):
            continue
        total += 6
        c = m.get("content", "")
        if isinstance(c, str):
            total += estimate_tokens_text(c)
        elif isinstance(c, list):
            for part in c:
                if isinstance(part, dict):
                    if part.get("type") == "text":
                        total += estimate_tokens_text(part.get("text", ""))
                    elif part.get("type") in ["image_url", "image", "input_image"]:
                        total += 85
                elif isinstance(part, str):
                    total += estimate_tokens_text(part)
    return max(total, 6)

def _prefix_cache_key(system_text: str, messages: list) -> str:
    h = hashlib.sha256()
    h.update((system_text or "").encode("utf-8", errors="ignore"))
    if isinstance(messages, list):
        for m in messages[:3]:
            if isinstance(m, dict):
                c = m.get("content", "")
                if isinstance(c, str):
                    h.update(c.encode("utf-8", errors="ignore"))
                elif isinstance(c, list):
                    for p in c:
                        if isinstance(p, dict) and p.get("type") == "text":
                            h.update(p.get("text", "").encode("utf-8", errors="ignore"))
    return h.hexdigest()

def _prefix_cache_seen(key: str) -> bool:
    if key in _PREFIX_CACHE:
        _PREFIX_CACHE.move_to_end(key)
        return True
    _PREFIX_CACHE[key] = None
    if len(_PREFIX_CACHE) > _PREFIX_CACHE_MAX:
        _PREFIX_CACHE.popitem(last=False)
    return False

def estimate_cache_from_request(anthropic_req: dict) -> Tuple[int, int]:
    try:
        cached_bytes = 0
        has_marker = False

        def scan_block(b):
            nonlocal cached_bytes, has_marker
            if isinstance(b, dict) and b.get("cache_control"):
                has_marker = True
                if b.get("type") == "text":
                    text_val = b.get("text", "")
                    if isinstance(text_val, str):
                        cached_bytes += len(text_val.encode("utf-8"))
                elif b.get("type") == "tool_result":
                    c = b.get("content", "")
                    if isinstance(c, str):
                        cached_bytes += len(c.encode("utf-8"))

        sys_field = anthropic_req.get("system")
        sys_text = ""
        if isinstance(sys_field, list):
            for b in sys_field:
                scan_block(b)
                if isinstance(b, dict) and b.get("type") == "text":
                    sys_text += b.get("text", "")
        elif isinstance(sys_field, str):
            sys_text = sys_field
            if anthropic_req.get("cache_control"):
                has_marker = True
                cached_bytes += len(sys_field.encode("utf-8"))

        messages = anthropic_req.get("messages")
        if isinstance(messages, list):
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                content = msg.get("content")
                if isinstance(content, list):
                    for b in content:
                        scan_block(b)

        if not has_marker:
            return 0, 0

        token_estimate = max(0, cached_bytes // 3)

        key = _prefix_cache_key(sys_text, messages or [])
        if _prefix_cache_seen(key):
            return 0, token_estimate
        else:
            return token_estimate, 0
    except Exception:
        return 0, 0

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    text_parts = []
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        for idx, page in enumerate(reader.pages):
            txt = page.extract_text() or ""
            if txt.strip():
                text_parts.append(f"--- Page {idx + 1} ---\n{txt.strip()}")
        if text_parts:
            return "\n\n".join(text_parts).strip()
    except Exception:
        pass

    stream_regex = re.compile(b"stream[\r\n]+(.*?)[\r\n]+endstream", re.DOTALL)
    streams = stream_regex.findall(pdf_bytes)
    for s in streams:
        data = None
        try:
            data = zlib.decompress(s)
        except Exception:
            try:
                data = zlib.decompress(s, -15)
            except Exception:
                data = s
        if data:
            tj_strings = re.findall(rb"\((.*?)\)\s*T[jJ]", data)
            for t in tj_strings:
                try:
                    unescaped = re.sub(rb"\\([0-7]{1,3})", lambda m: bytes([int(m.group(1), 8)]), t)
                    unescaped = unescaped.replace(b"\\n", b"\n").replace(b"\\r", b"\r").replace(b"\\t", b"\t").replace(b"\\(", b"(").replace(b"\\)", b")").replace(b"\\\\", b"\\")
                    decoded = unescaped.decode("utf-8", errors="ignore").strip()
                    if decoded:
                        text_parts.append(decoded)
                except Exception:
                    pass
    if not text_parts:
        for s in streams:
            try:
                data = zlib.decompress(s)
            except Exception:
                data = s
            parenthesized = re.findall(rb"\(([^(]{2,})\)", data)
            for p in parenthesized:
                try:
                    decoded = p.decode("utf-8", errors="ignore").strip()
                    if decoded and not any(k in decoded for k in ["Filter", "FlateDecode", "Length"]):
                        text_parts.append(decoded)
                except Exception:
                    pass
    return " ".join(text_parts).strip()

def process_pdf_content(pdf_bytes: bytes, allow_vision: bool = True) -> List[dict]:
    parts = []
    text_content = extract_text_from_pdf(pdf_bytes)
    rendered_images = []

    if allow_vision:
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            max_pages = min(len(doc), 6)
            for p_no in range(max_pages):
                page = doc[p_no]
                pix = page.get_pixmap(dpi=150)
                img_data = pix.tobytes("jpeg")
                b64 = base64.b64encode(img_data).decode("ascii")
                rendered_images.append(b64)
            doc.close()
        except Exception:
            pass

    if rendered_images:
        for b64 in rendered_images:
            parts.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}"}
            })
        if text_content:
            parts.append({
                "type": "text",
                "text": f"\n[Document Extracted Text]:\n{text_content}\n"
            })
    elif text_content:
        parts.append({
            "type": "text",
            "text": f"\n[Extracted Document Content]:\n{text_content}\n"
        })
    else:
        parts.append({
            "type": "text",
            "text": "\n[Attached PDF Document successfully loaded]\n"
        })

    return parts

def get_reasoning_prompt(level: int) -> str:
    if level <= 0:
        return ""
    base_rules = (
        "Reasoning Protocol:\n"
        "Deliberate step-by-step strictly inside <think>...</think> tags before writing your final response.\n\n"
        "Guidelines for internal thought:\n"
        "- Dive immediately into solving the user's inquiry, formulas, logic, and edge cases.\n"
        "- CRITICAL: Always deliberate in the EXACT SAME LANGUAGE as the user query (no Chinese thoughts unless the user prompt is in Chinese).\n"
        "- Maintain an objective, calm, and analytical first-person tone without unnecessary conversational filler.\n"
    )
    if level == 1:
        return base_rules + (
            "REASONING DEPTH [1x - Direct Execution]:\n"
            "- Immediately analyze key parameters.\n"
            "- Plan solution steps and proceed directly to completion.\n"
        )
    elif level == 2:
        return base_rules + (
            "REASONING DEPTH [2x - Verification Pass]:\n"
            "- Step 1: Break down requirements and formulate resolution.\n"
            "- Step 2: Verify logic, calculations, and consistency.\n"
        )
    elif level == 3:
        return base_rules + (
            "REASONING DEPTH [3x - Double Verification]:\n"
            "- Step 1: Systematic problem analysis.\n"
            "- Step 2: Thorough validation of logic and potential edge cases.\n"
            "- Step 3: Final sanity check before formulating answer.\n"
        )
    elif level == 4:
        return base_rules + (
            "REASONING DEPTH [4x - Multi-Pass Audit]:\n"
            "- Step 1: Decomposition of constraints and explicit goals.\n"
            "- Step 2: Methodical derivation.\n"
            "- Step 3: Edge-case scrutiny.\n"
            "- Step 4: Fact-checking and consistency audit.\n"
        )
    elif level == 5:
        return base_rules + (
            "REASONING DEPTH [5x - Maximum Exhaustive Audit]:\n"
            "- Step 1: Deep deconstruction of nuance and hidden requirements.\n"
            "- Step 2: Comprehensive step-by-step solution derivation.\n"
            "- Step 3: Boundary value and assumption check.\n"
            "- Step 4: Adversarial review for subtle logical traps.\n"
            "- Step 5: Final review before closing the thinking block.\n"
        )
    else:
        return base_rules + (
            "REASONING DEPTH [AUTO - Adaptive]:\n"
            "- Match deliberation depth proportionally to problem complexity.\n"
            "- Always close </think> before beginning the final answer.\n"
        )

CHINESE_THOUGHT_LEAK_REGEX = re.compile(
    r"^[\s\r\n]*(?:首先[，,]?|我们(?:需要|来看)|用户(?:要求|想要|的意图)[^.\n]*[。\n]|好的[，,]?|分析一下[，,]?)+",
    re.IGNORECASE
)

DEFAULT_ERROR_MESSAGE = (
    "Нейросеть слишком глубоко задумалась о смысле бытия и временно вышла в астрал. "
    "Дайте кремниевому мозгу 30 секунд на перекур и отправьте снова."
)

DEFAULT_MODELS = []

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
                delay_sec REAL DEFAULT 0.0,
                stream_throttle INTEGER DEFAULT 0,
                is_reasoning INTEGER DEFAULT 0,
                endpoint_mode INTEGER DEFAULT 3,
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
        if "delay_sec" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN delay_sec REAL DEFAULT 0.0")
        if "stream_throttle" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN stream_throttle INTEGER DEFAULT 0")
        if "is_reasoning" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN is_reasoning INTEGER DEFAULT 0")
        if "endpoint_mode" not in columns:
            conn.execute("ALTER TABLE mappings ADD COLUMN endpoint_mode INTEGER DEFAULT 3")

        conn.executemany("""
            INSERT OR IGNORE INTO mappings (real_model, fake_model, is_vision, context_length, owned_by, delay_sec, stream_throttle, is_reasoning)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, DEFAULT_MODELS)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        conn.execute("""
            INSERT OR IGNORE INTO settings (key, value) VALUES ('default_error', ?)
        """, (DEFAULT_ERROR_MESSAGE,))

        conn.execute("""
            CREATE TABLE IF NOT EXISTS error_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger TEXT NOT NULL UNIQUE,
                message TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                anthropic_key TEXT NOT NULL UNIQUE,
                real_key TEXT NOT NULL,
                note TEXT DEFAULT '',
                created_at TEXT DEFAULT ''
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS key_usage (
                key_id INTEGER PRIMARY KEY,
                requests INTEGER DEFAULT 0,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                last_used TEXT DEFAULT ''
            )
        """)
        conn.commit()

def load_data():
    global MODEL_MAPPINGS_LIST, ERROR_RULES_LIST, SETTINGS, API_KEYS_LIST, KEY_USAGE, KEY_ID_BY_ANTHROPIC
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, real_model, fake_model, is_vision, context_length, owned_by, delay_sec, stream_throttle, is_reasoning, endpoint_mode FROM mappings ORDER BY id ASC")
        MODEL_MAPPINGS_LIST = cursor.fetchall()

        cursor.execute("SELECT id, trigger, message FROM error_rules ORDER BY id ASC")
        ERROR_RULES_LIST = cursor.fetchall()

        cursor.execute("SELECT key, value FROM settings")
        SETTINGS = dict(cursor.fetchall())

        cursor.execute("SELECT id, anthropic_key, real_key, note, created_at FROM api_keys ORDER BY id ASC")
        API_KEYS_LIST = cursor.fetchall()

        KEY_ID_BY_ANTHROPIC.clear()
        for _r in API_KEYS_LIST:
            KEY_ID_BY_ANTHROPIC[_r[1]] = _r[0]

        cursor.execute("SELECT key_id, requests, input_tokens, output_tokens FROM key_usage")
        KEY_USAGE.clear()
        for _u in cursor.fetchall():
            KEY_USAGE[_u[0]] = {
                "requests": _u[1] or 0,
                "input_tokens": _u[2] or 0,
                "output_tokens": _u[3] or 0,
            }

@app.on_event("startup")
def startup():
    init_db()
    load_data()
    # Диагностика PDF-библиотек — чтобы в логах HF было видно, установлены ли они
    try:
        import pypdf
        print(f"[diag] pypdf {getattr(pypdf, '__version__', '?')} OK", flush=True)
    except Exception as e:
        print(f"[diag] pypdf FAILED: {e}", flush=True)
    try:
        import fitz
        print(f"[diag] PyMuPDF OK", flush=True)
    except Exception as e:
        print(f"[diag] PyMuPDF FAILED: {e}", flush=True)

def replace_models(
    text: str,
    specific_real: Optional[str] = None,
    specific_fake: Optional[str] = None
) -> str:
    if specific_real and specific_fake:
        pairs = [(specific_real, specific_fake)]
    else:
        pairs = [(item[1], item[2]) for item in MODEL_MAPPINGS_LIST]
    for real, fake in pairs:
        text = text.replace(f'"{real}"', f'"{fake}"')
        escaped_real = real.replace("/", r"\/")
        if escaped_real != real:
            text = text.replace(f'"{escaped_real}"', f'"{fake}"')
    return text

def sanitize_metadata(meta: dict, target_model: Optional[str] = None):
    if not isinstance(meta, dict):
        return

    def process_val(val: str) -> str:
        if not isinstance(val, str):
            return val
        for item in MODEL_MAPPINGS_LIST:
            real = item[1]
            fake = item[2]
            if val.lower() == real.lower():
                return fake
            if real in val:
                val = val.replace(real, fake)
            elif real.lower() in val.lower():
                val = re.sub(re.escape(real), fake, val, flags=re.IGNORECASE)
        return val

    def walk(obj):
        if isinstance(obj, dict):
            for k, v in list(obj.items()):
                if isinstance(v, str):
                    if k in ["model", "requested_model", "used_model", "underlying_used_model"] and target_model:
                        obj[k] = target_model
                    else:
                        obj[k] = process_val(v)
                elif isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(meta)
    if target_model:
        for field in ["requested_model", "used_model", "underlying_used_model"]:
            if field in meta:
                meta[field] = target_model

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

def make_error_response(
    message: str,
    status_code: int = 400,
    owned_by: str = "openai",
    req_id: Optional[str] = None,
    is_anthropic: bool = False,
    retry_after: Optional[str] = None
) -> Response:
    if not req_id:
        _, req_id = generate_provider_ids(owned_by)
    clean_headers = build_gateway_headers(owned_by, req_id, processing_ms=15, is_stream=False)
    if status_code == 429:
        clean_headers["retry-after"] = retry_after if retry_after else "60"

    if is_anthropic:
        if status_code == 400:
            err_type = "invalid_request_error"
        elif status_code == 401:
            err_type = "authentication_error"
        elif status_code == 403:
            err_type = "permission_error"
        elif status_code == 404:
            err_type = "not_found_error"
        elif status_code == 413:
            err_type = "request_too_large"
        elif status_code == 429:
            err_type = "rate_limit_error"
        elif status_code == 529:
            err_type = "overloaded_error"
        elif status_code == 402:
            err_type = "billing_error"
        elif status_code == 409:
            err_type = "conflict_error"
        elif status_code == 413:
            err_type = "request_too_large"
        elif status_code == 504:
            err_type = "timeout_error"
        else:
            err_type = "api_error"
        content = {
            "type": "error",
            "error": {
                "type": err_type,
                "message": message
            }
        }
    else:
        content = {
            "error": {
                "message": message,
                "type": "rate_limit_error" if status_code == 429 else "invalid_request_error" if status_code == 400 else "api_error",
                "param": None,
                "code": "rate_limit_exceeded" if status_code == 429 else "service_error"
            }
        }

    return Response(
        content=json.dumps(content, ensure_ascii=False),
        status_code=status_code,
        headers=clean_headers,
        media_type="application/json"
    )

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
    for mid, r, f, v, ctx, owned, delay, throttle, is_reas, ep_mode in MODEL_MAPPINGS_LIST:
        if v == 1:
            vision_badge = "<span class='badge badge-vision'>Real Vision</span>"
        elif v == 2:
            vision_badge = "<span class='badge badge-fake-vision'>Fake Vision</span>"
        else:
            vision_badge = "<span class='badge badge-text'>Только текст</span>"

        if is_reas == 0:
            reasoning_badge = "<span style='color:var(--muted); font-size:12px;'>Нет (0x)</span>"
        elif is_reas == 1:
            reasoning_badge = "<span class='badge badge-reasoning'>1x (Скрыто)</span>"
        elif is_reas == 2:
            reasoning_badge = "<span class='badge badge-reasoning'>2x (1 пров.)</span>"
        elif is_reas == 3:
            reasoning_badge = "<span class='badge badge-reasoning'>3x (2 пров.)</span>"
        elif is_reas == 4:
            reasoning_badge = "<span class='badge badge-reasoning'>4x (3 пров.)</span>"
        elif is_reas == 5:
            reasoning_badge = "<span class='badge badge-reasoning'>5x (Макс)</span>"
        else:
            reasoning_badge = "<span class='badge badge-reasoning' style='background:#1e3a5f; color:#93c5fd; border-color:#1e40af;'>Auto (0-5x)</span>"

        if ep_mode == 1:
            endpoint_badge = "<span class='badge' style='background:#3b0764;color:#d8b4fe;border:1px solid #581c87;'>Only Anthropic</span>"
        elif ep_mode == 2:
            endpoint_badge = "<span class='badge' style='background:#064e3b;color:#6ee7b7;border:1px solid #065f46;'>Only OpenAI</span>"
        else:
            endpoint_badge = "<span class='badge badge-text'>Full</span>"

        safe_r = html.escape(r)
        safe_f = html.escape(f)
        safe_owned = html.escape(str(owned or 'openai'))
        delay_badge = f"<span class='badge badge-delay'>+{delay}с" + (" / плывёт" if throttle else "") + "</span>" if delay > 0 or throttle else "<span style='color:var(--muted); font-size:12px;'>0с</span>"

        row_html = (
            f"<tr>"
            f"<td><code>{safe_r}</code></td>"
            f"<td><span class='badge badge-model'>{safe_f}</span></td>"
            f"<td>{vision_badge}</td>"
            f"<td>{endpoint_badge}</td>"
            f"<td>{reasoning_badge}</td>"
            f"<td>{delay_badge}</td>"
            f"<td><span style='color: var(--muted); font-size: 12px;'>{ctx//1000}k / {safe_owned}</span></td>"
            f"<td style='text-align: right;'>"
            f"<div style='display: inline-flex; gap: 6px;'>"
            f"<button type='button' class='btn-edit' "
            f"data-id='{mid}' data-real='{safe_r}' data-fake='{safe_f}' "
            f"data-vision='{v}' data-ctx='{ctx}' data-owned='{safe_owned}' "
            f"data-delay='{delay}' data-throttle='{throttle}' "
            f"data-reasoning='{is_reas}' "
            f"data-endpoint='{ep_mode}' "
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

    keys_rows = "".join(
        f"<tr>"
        f"<td>"
        f"<code style='font-size:11px;word-break:break-all;'>{html.escape(k[1])}</code><br>"
        f"<button type='button' class='btn-edit' style='margin-top:4px;' onclick=\"copyKey('{html.escape(k[1])}', this)\">Копировать</button>"
        f"</td>"
        f"<td>{html.escape(k[3] or '')}</td>"
        f"<td style='font-size:11px;color:var(--muted);'>{html.escape(k[4] or '')}</td>"
        f"<td style='text-align:right;'>"
        f"<form method='post' action='/admin/keys/delete?key={key}' style='margin:0;'>"
        f"<input type='hidden' name='key_id' value='{k[0]}'>"
        f"<button type='submit' class='btn-danger'>Удалить</button></form>"
        f"</td>"
        f"</tr>"
        for k in API_KEYS_LIST
    ) if API_KEYS_LIST else "<tr><td colspan='4' style='text-align:center;color:var(--muted);padding:18px;'>Ключей ещё нет</td></tr>"

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
            .container {{ max-width: 1040px; margin: 0 auto; display: flex; flex-direction: column; gap: 24px; }}
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
            table {{ width: 100%; border-collapse: collapse; font-size: 14px; min-width: 740px; }}
            th {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 500; font-size: 12px; text-transform: uppercase; }}
            td {{ padding: 12px; border-bottom: 1px solid var(--border); vertical-align: middle; }}
            tr:last-child td {{ border-bottom: none; }}
            code {{ background: #000; border: 1px solid #27272a; padding: 3px 6px; border-radius: 4px; font-family: monospace; font-size: 13px; }}
            .badge {{ padding: 3px 8px; border-radius: 4px; font-size: 12px; display: inline-block; font-weight: 500; }}
            .badge-model {{ background: #27272a; color: #fff; }}
            .badge-vision {{ background: #14532d; color: #86efac; border: 1px solid #166534; }}
            .badge-fake-vision {{ background: #3b2a06; color: #fde047; border: 1px solid #713f12; }}
            .badge-delay {{ background: #1e1b4b; color: #c7d2fe; border: 1px solid #3730a3; }}
            .badge-reasoning {{ background: #31135e; color: #d8b4fe; border: 1px solid #581c87; }}
            .badge-text {{ background: #27272a; color: #a1a1aa; }}
            .form-grid {{ display: flex; gap: 10px; }}
            .modal-overlay {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.75); backdrop-filter: blur(4px); z-index: 1000; justify-content: center; align-items: center; padding: 16px; }}
            .modal-card {{ background: #121214; border: 1px solid var(--border); border-radius: 14px; width: 100%; max-width: 540px; padding: 24px; display: flex; flex-direction: column; gap: 16px; box-shadow: 0 20px 40px rgba(0,0,0,0.8); max-height: 90vh; overflow-y: auto; }}
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
                    <p style="color: var(--muted); font-size: 13px; margin-top: 2px;">Маршрутизация, скрытое мышление (0x-5x), OpenAI и Anthropic эндпоинты</p>
                </div>
                <div class="status-pill">Active • 2026 Production</div>
            </div>

            <div class="card">
                <div class="card-title">1. Глобальный текст ошибки</div>
                <form method="post" action="/admin/settings/default-error?key={key}" class="form-grid">
                    <input name="default_error" value="{default_err}" placeholder="Введите общий текст ошибки..." style="flex: 1;">
                    <button type="submit">Сохранить</button>
                </form>
            </div>

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

            <div class="card">
                <div class="card-header">
                    <div class="card-title">3. Модели: Vision, Скрытое рассуждение (0x - 5x) и Задержка</div>
                    <button type="button" onclick="openAddModal()">+ Добавить модель</button>
                </div>
                <div class="table-responsive">
                    <table>
                        <thead><tr><th>Реальная модель / строка</th><th>Фейковый ID</th><th>Vision</th><th>Эндпоинт</th><th>Рассуждения</th><th>Задержка</th><th>Контекст / Владелец</th><th style="text-align: right;">Действие</th></tr></thead>
                        <tbody>{model_rows}</tbody>
                    </table>
                </div>
            </div>

            <div class="card">
                <div class="card-header">
                    <div class="card-title">4. API Ключи (Anthropic-стиль)</div>
                </div>
                <form method="post" action="/admin/keys/generate?key={key}" style="display: flex; gap: 10px; flex-wrap: wrap;">
                    <input name="real_key" placeholder="Реальный ключ из newapi (sk-...)" required style="flex: 2; min-width: 240px;">
                    <input name="note" placeholder="Заметка (кому выдан)" style="flex: 1; min-width: 150px;">
                    <button type="submit">Сгенерировать ключ</button>
                </form>
                <div class="table-responsive">
                    <table>
                        <thead><tr><th>Anthropic-ключ</th><th>Заметка</th><th>Создан</th><th style="text-align: right;">Действие</th></tr></thead>
                        <tbody>{keys_rows}</tbody>
                    </table>
                </div>
            </div>
        </div>

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
                        <div class="form-group" style="flex: 1.2;">
                            <label>Режим Vision</label>
                            <select name="is_vision" id="modal_is_vision">
                                <option value="1">Да (Real Vision)</option>
                                <option value="2">Fake Vision (Эмуляция)</option>
                                <option value="0">Нет (Только текст)</option>
                            </select>
                        </div>
                        <div class="form-group" style="flex: 1;">
                            <label>Мышление (Всегда скрыто от клиента)</label>
                            <select name="is_reasoning" id="modal_is_reasoning">
                                <option value="0">0x: Без мышления</option>
                                <option value="1">1x: Мышление (1 проход)</option>
                                <option value="2">2x: Мышление (Перепроверка 1 раз)</option>
                                <option value="3">3x: Мышление (Перепроверка 2 раза)</option>
                                <option value="4">4x: Мышление (Глубокий аудит)</option>
                                <option value="5">5x: Мышление (Максимальная глубина)</option>
                                <option value="6">Auto: Авто-мышление (0-5x, модель решает)</option>
                            </select>
                        </div>
                    </div>

                    <div style="display: flex; gap: 10px;">
                        <div class="form-group" style="flex: 1;">
                            <label>Контекст (токенов)</label>
                            <input type="number" name="context_length" id="modal_context_length" value="128000" step="1000">
                        </div>
                        <div class="form-group" style="flex: 1;">
                            <label>Задержка ПОСЛЕ генерации (сек)</label>
                            <input type="number" step="0.1" name="delay_sec" id="modal_delay_sec" value="0.0" placeholder="Напр: 2.0">
                        </div>
                    </div>
                    
                    <div style="display: flex; gap: 10px;">
                        <div class="form-group" style="flex: 1;">
                            <label>Плавный стриминг</label>
                            <select name="stream_throttle" id="modal_stream_throttle">
                                <option value="0">Мгновенный сброс</option>
                                <option value="1">Плавная печать</option>
                            </select>
                        </div>
                        <div class="form-group" style="flex: 1;">
                            <label>Владелец (owned_by)</label>
                            <input name="owned_by" id="modal_owned_by" value="openai" placeholder="openai, anthropic, siliconflow, qwen">
                        </div>
                    </div>

                    <div class="form-group">
                        <label>Доступность модели (какие эндпоинты её видят)</label>
                        <select name="endpoint_mode" id="modal_endpoint_mode">
                            <option value="3">Полная доступность (Anthropic + OpenAI)</option>
                            <option value="1">Только Anthropic-эндпоинт (/v1/messages)</option>
                            <option value="2">Только OpenAI-эндпоинт (/v1/chat/completions)</option>
                        </select>
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
                document.getElementById('modal_is_reasoning').value = '1';
                document.getElementById('modal_context_length').value = '128000';
                document.getElementById('modal_owned_by').value = 'openai';
                document.getElementById('modal_endpoint_mode').value = '3';
                document.getElementById('modal_delay_sec').value = '0.0';
                document.getElementById('modal_stream_throttle').value = '0';
                document.getElementById('modelModal').style.display = 'flex';
                document.body.style.overflow = 'hidden';
            }}

            function openEditModalFromBtn(btn) {{
                document.getElementById('modal_row_id').value = btn.dataset.id;
                document.getElementById('modal_title').innerText = 'Редактировать модель';
                document.getElementById('modal_real_model').value = btn.dataset.real;
                document.getElementById('modal_fake_model').value = btn.dataset.fake;
                document.getElementById('modal_is_vision').value = btn.dataset.vision;
                document.getElementById('modal_is_reasoning').value = btn.dataset.reasoning !== undefined ? btn.dataset.reasoning : '0';
                document.getElementById('modal_context_length').value = btn.dataset.ctx;
                document.getElementById('modal_owned_by').value = btn.dataset.owned;
                document.getElementById('modal_endpoint_mode').value = btn.dataset.endpoint || '3';
                document.getElementById('modal_delay_sec').value = btn.dataset.delay || '0.0';
                document.getElementById('modal_stream_throttle').value = btn.dataset.throttle || '0';
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

            function copyKey(text, btn) {{
                if (navigator.clipboard && navigator.clipboard.writeText) {{
                    navigator.clipboard.writeText(text).then(function() {{
                        var orig = btn.innerText;
                        btn.innerText = 'Скопировано!';
                        setTimeout(function() {{ btn.innerText = orig; }}, 1200);
                    }}).catch(function() {{
                        alert('Не удалось скопировать. Выделите текст вручную.');
                    }});
                }} else {{
                    var ta = document.createElement('textarea');
                    ta.value = text;
                    document.body.appendChild(ta);
                    ta.select();
                    try {{ document.execCommand('copy'); btn.innerText = 'Скопировано!'; setTimeout(function() {{ btn.innerText = 'Копировать'; }}, 1200); }} catch(e) {{ alert('Не удалось скопировать'); }}
                    document.body.removeChild(ta);
                }}
            }}

            document.addEventListener('keydown', function(e) {{
                if (e.key === 'Escape') closeModal();
            }});
        </script>
    </body>
    </html>
    """

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
    delay_sec: float = Form(0.0),
    stream_throttle: int = Form(0),
    is_reasoning: int = Form(0),
    endpoint_mode: int = Form(3),
    key: str = Depends(verify_admin)
):
    with sqlite3.connect(DB_FILE) as conn:
        if row_id and row_id.isdigit():
            conn.execute("""
                UPDATE mappings 
                SET real_model = ?, fake_model = ?, is_vision = ?, context_length = ?, owned_by = ?, delay_sec = ?, stream_throttle = ?, is_reasoning = ?, endpoint_mode = ?
                WHERE id = ?
            """, (real_model.strip(), fake_model.strip(), is_vision, context_length, owned_by.strip(), float(delay_sec), int(stream_throttle), int(is_reasoning), int(endpoint_mode), int(row_id)))
        else:
            conn.execute("""
                INSERT OR REPLACE INTO mappings (real_model, fake_model, is_vision, context_length, owned_by, delay_sec, stream_throttle, is_reasoning, endpoint_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (real_model.strip(), fake_model.strip(), is_vision, context_length, owned_by.strip(), float(delay_sec), int(stream_throttle), int(is_reasoning), int(endpoint_mode)))
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


@app.post("/admin/keys/generate")
def admin_generate_key(
    real_key: str = Form(...),
    note: str = Form(""),
    key: str = Depends(verify_admin)
):
    real_key = real_key.strip()
    if not real_key:
        return HTMLResponse(f"<script>alert('Ключ не может быть пустым'); location.href='/admin?key={key}';</script>")
    new_anthropic_key = gen_anthropic_style_key()
    created = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO api_keys (anthropic_key, real_key, note, created_at) VALUES (?, ?, ?, ?)",
            (new_anthropic_key, real_key, note.strip(), created)
        )
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")


@app.post("/admin/keys/delete")
def admin_delete_key(key_id: int = Form(...), key: str = Depends(verify_admin)):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM api_keys WHERE id = ?", (key_id,))
        conn.commit()
    load_data()
    return HTMLResponse(f"<script>location.href='/admin?key={key}';</script>")

async def stream_filter_generator(
    upstream_response: httpx.Response,
    requested_model: str = None,
    real_model: Optional[str] = None,
    is_reasoning: int = 0,
    orig_prompt_tokens: int = 0,
    fixed_id: Optional[str] = None
):
    line_buffer = ""
    event_skipped = False
    accum_emitted_text = ""

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

            line = replace_models(line, real_model, requested_model)

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
                        return

                    if fixed_id and "id" in data:
                        data["id"] = fixed_id

                    if requested_model and "model" in data:
                        data["model"] = requested_model

                    if "metadata" in data and isinstance(data["metadata"], dict):
                        sanitize_metadata(data["metadata"], requested_model)

                    if "choices" in data and len(data["choices"]) > 0:
                        choice = data["choices"][0]
                        delta = choice.get("delta", {})

                        if delta.get("name") in ["MiniMax AI", "Qwen AI"]:
                            delta.pop("name", None)

                        content = delta.get("content", "")
                        if content:
                            accum_emitted_text += content

                        if (
                            not delta.get("content")
                            and not delta.get("role")
                            and not delta.get("tool_calls")
                            and not delta.get("function_call")
                            and not delta.get("reasoning_content")
                            and not delta.get("reasoning")
                            and not choice.get("finish_reason")
                        ):
                            event_skipped = True
                            continue

                    if "usage" in data and isinstance(data["usage"], dict):
                        comp_tok = estimate_tokens_text(accum_emitted_text)
                        data["usage"] = {
                            "prompt_tokens": orig_prompt_tokens,
                            "completion_tokens": comp_tok,
                            "total_tokens": orig_prompt_tokens + comp_tok
                        }

                    line = "data: " + json.dumps(data, ensure_ascii=False)
                except Exception:
                    pass

            yield (line + "\n").encode("utf-8")

    if line_buffer:
        yield replace_models(line_buffer, real_model, requested_model).encode("utf-8")

async def anthropic_stream_generator(
    upstream_resp: httpx.Response,
    requested_model: str,
    real_model: Optional[str] = None,
    is_reasoning: int = 0,
    prompt_tokens: int = 0,
    fixed_id: str = "",
    delay_sec: float = 0.0,
    stream_throttle: bool = False,
    thinking_requested: bool = False,
    thinking_omitted: bool = False,
    cache_creation_tokens: int = 0,
    cache_read_tokens: int = 0
):
    if delay_sec > 0:
        await asyncio.sleep(delay_sec)

    non_cached_input = max(0, prompt_tokens - cache_read_tokens - cache_creation_tokens)

    start_payload = {
        "type": "message_start",
        "message": {
            "id": fixed_id,
            "type": "message",
            "role": "assistant",
            "content": [],
            "model": requested_model,
            "stop_reason": None,
            "stop_sequence": None,
            "usage": {
                "input_tokens": non_cached_input,
                "cache_creation_input_tokens": cache_creation_tokens,
                "cache_read_input_tokens": cache_read_tokens,
                "output_tokens": 1
            }
        }
    }
    yield f"event: message_start\ndata: {json.dumps(start_payload, ensure_ascii=False)}\n\n".encode("utf-8")

    current_block_index = 0

    # Anthropic-style omitted thinking: пустой блок + только signature_delta.
    # Реальный текст размышлений никогда не уходит клиенту.
    if thinking_requested:
        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': current_block_index, 'content_block': {'type': 'thinking', 'thinking': ''}}, ensure_ascii=False)}\n\n".encode("utf-8")
        yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': current_block_index, 'delta': {'type': 'signature_delta', 'signature': gen_thinking_signature()}}, ensure_ascii=False)}\n\n".encode("utf-8")
        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': current_block_index}, ensure_ascii=False)}\n\n".encode("utf-8")
        current_block_index += 1

    text_block_opened = False
    in_think = False
    tag_buf = ""
    accum_text = ""
    tool_blocks_map = {}
    final_stop_reason = "end_turn"

    line_buffer = ""

    _aiter = upstream_resp.aiter_bytes().__aiter__()
    while True:
        try:
            raw_chunk = await asyncio.wait_for(_aiter.__anext__(), timeout=15.0)
        except asyncio.TimeoutError:
            # Anthropic-style keepalive: event: ping
            yield b"event: ping\ndata: {\"type\": \"ping\"}\n\n"
            continue
        except StopAsyncIteration:
            break

        line_buffer += raw_chunk.decode("utf-8", errors="ignore")
        while "\n" in line_buffer:
            line, line_buffer = line_buffer.split("\n", 1)
            line = line.strip("\r").strip()

            if not line:
                continue
            if line == "data: [DONE]":
                break
            if not line.startswith("data: "):
                continue

            payload = line[6:].strip()
            try:
                c_data = json.loads(payload)
            except Exception:
                continue

            if "error" in c_data:
                err_body = c_data["error"]
                if not isinstance(err_body, dict):
                    err_body = {"message": str(err_body)}
                err_type = err_body.get("type", "api_error")
                if err_type not in ["invalid_request_error", "authentication_error", "permission_error", "not_found_error", "rate_limit_error", "api_error", "overloaded_error"]:
                    err_type = "api_error"
                err_event = {
                    "type": "error",
                    "error": {
                        "type": err_type,
                        "message": err_body.get("message", "Upstream error")
                    }
                }
                yield f"event: error\ndata: {json.dumps(err_event, ensure_ascii=False)}\n\n".encode("utf-8")
                return

            if "choices" in c_data and len(c_data["choices"]) > 0:
                ch = c_data["choices"][0]
                delta = ch.get("delta", {})

                fr = ch.get("finish_reason")
                if fr == "length":
                    final_stop_reason = "max_tokens"
                elif fr in ["tool_calls", "function_call"]:
                    final_stop_reason = "tool_use"

                content_chunk = delta.get("content", "")
                if content_chunk:
                    curr = tag_buf + content_chunk
                    tag_buf = ""

                    while curr:
                        if in_think:
                            m = re.search(r"</think>|<\\/think>", curr)
                            if m:
                                curr = curr[m.end():].lstrip("\n")
                                in_think = False
                            else:
                                matched_prefix = False
                                for tag in ["</think>", r"<\/think>"]:
                                    for i in range(min(len(curr), len(tag) - 1), 0, -1):
                                        tail = curr[-i:]
                                        if tag.startswith(tail):
                                            tag_buf = tail
                                            curr = ""
                                            matched_prefix = True
                                            break
                                    if matched_prefix:
                                        break
                                if not matched_prefix:
                                    curr = ""
                        else:
                            if "<think>" in curr:
                                before, rest = curr.split("<think>", 1)
                                curr = rest
                                in_think = True
                                if before:
                                    if not text_block_opened:
                                        text_block_opened = True
                                        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': current_block_index, 'content_block': {'type': 'text', 'text': ''}}, ensure_ascii=False)}\n\n".encode("utf-8")
                                    accum_text += before
                                    yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': current_block_index, 'delta': {'type': 'text_delta', 'text': before}}, ensure_ascii=False)}\n\n".encode("utf-8")
                            else:
                                matched_prefix = False
                                for i in range(min(len(curr), 6), 0, -1):
                                    tail = curr[-i:]
                                    if "<think>".startswith(tail):
                                        text_part = curr[:-i]
                                        tag_buf = tail
                                        curr = ""
                                        matched_prefix = True
                                        if text_part:
                                            if not text_block_opened:
                                                text_block_opened = True
                                                yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': current_block_index, 'content_block': {'type': 'text', 'text': ''}}, ensure_ascii=False)}\n\n".encode("utf-8")
                                            accum_text += text_part
                                            yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': current_block_index, 'delta': {'type': 'text_delta', 'text': text_part}}, ensure_ascii=False)}\n\n".encode("utf-8")
                                        break
                                if not matched_prefix:
                                    if not text_block_opened:
                                        text_block_opened = True
                                        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': current_block_index, 'content_block': {'type': 'text', 'text': ''}}, ensure_ascii=False)}\n\n".encode("utf-8")
                                    accum_text += curr
                                    yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': current_block_index, 'delta': {'type': 'text_delta', 'text': curr}}, ensure_ascii=False)}\n\n".encode("utf-8")
                                    curr = ""
                                    if stream_throttle:
                                        await asyncio.sleep(0.015)

                tool_calls = delta.get("tool_calls", [])
                if tool_calls:
                    final_stop_reason = "tool_use"
                    if text_block_opened:
                        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': current_block_index}, ensure_ascii=False)}\n\n".encode("utf-8")
                        current_block_index += 1
                        text_block_opened = False

                    for tc in tool_calls:
                        tc_idx = tc.get("index", 0)
                        if tc_idx not in tool_blocks_map:
                            t_id = tc.get("id") or f"toolu_{gen_base62(22)}"
                            if not t_id.startswith("toolu_"):
                                t_id = f"toolu_{gen_base62(22)}"
                            fn_info = tc.get("function", {})
                            t_name = fn_info.get("name", "function_tool")

                            tool_block_index = current_block_index
                            current_block_index += 1
                            tool_blocks_map[tc_idx] = tool_block_index

                            t_start_event = {
                                "type": "content_block_start",
                                "index": tool_block_index,
                                "content_block": {
                                    "type": "tool_use",
                                    "id": t_id,
                                    "name": t_name,
                                    "input": {}
                                }
                            }
                            yield f"event: content_block_start\ndata: {json.dumps(t_start_event, ensure_ascii=False)}\n\n".encode("utf-8")

                        fn_delta = tc.get("function", {})
                        args_chunk = fn_delta.get("arguments", "")
                        if args_chunk:
                            target_idx = tool_blocks_map[tc_idx]
                            t_delta_event = {
                                "type": "content_block_delta",
                                "index": target_idx,
                                "delta": {
                                    "type": "input_json_delta",
                                    "partial_json": args_chunk
                                }
                            }
                            yield f"event: content_block_delta\ndata: {json.dumps(t_delta_event, ensure_ascii=False)}\n\n".encode("utf-8")

    if tag_buf:
        if not in_think:
            if not text_block_opened:
                text_block_opened = True
                yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': current_block_index, 'content_block': {'type': 'text', 'text': ''}}, ensure_ascii=False)}\n\n".encode("utf-8")
            accum_text += tag_buf
            yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': current_block_index, 'delta': {'type': 'text_delta', 'text': tag_buf}}, ensure_ascii=False)}\n\n".encode("utf-8")
        tag_buf = ""

    if text_block_opened:
        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': current_block_index}, ensure_ascii=False)}\n\n".encode("utf-8")
        current_block_index += 1

    # Tool_use блоки закрываем ПЕРЕД message_stop (фикс обрыва Claude Code / OpenCode)
    for t_idx in list(tool_blocks_map.values()):
        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': t_idx}, ensure_ascii=False)}\n\n".encode("utf-8")

    # КРИТИЧНО: если stop_reason=tool_use, обязан быть хотя бы один tool_use блок.
    # Если блоков нет — понижаем до end_turn, иначе Claude Code упадёт с "tool call could not be parsed".
    if final_stop_reason == "tool_use" and not tool_blocks_map:
        final_stop_reason = "end_turn"

    # Обратное: если есть tool_blocks_map — принудительно tool_use.
    if tool_blocks_map and final_stop_reason != "tool_use":
        final_stop_reason = "tool_use"

    if not text_block_opened and not tool_blocks_map and not thinking_requested:
        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}, ensure_ascii=False)}\n\n".encode("utf-8")
        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0}, ensure_ascii=False)}\n\n".encode("utf-8")

    out_tokens = max(1, estimate_tokens_text(accum_text))

    msg_delta = {
        "type": "message_delta",
        "delta": {
            "stop_reason": final_stop_reason,
            "stop_sequence": None
        },
        "usage": {
            "output_tokens": out_tokens
        }
    }
    if final_stop_reason == "refusal":
        msg_delta["delta"]["stop_details"] = {"type": "refusal", "reason": "Model declined to respond"}
    yield f"event: message_delta\ndata: {json.dumps(msg_delta, ensure_ascii=False)}\n\n".encode("utf-8")
    yield b"event: message_stop\ndata: {\"type\": \"message_stop\"}\n\n"

class _AnthropicRequestError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code

@app.api_route("/v1/messages/count_tokens", methods=["POST", "OPTIONS"])
@app.api_route("/messages/count_tokens", methods=["POST", "OPTIONS"])
async def anthropic_count_tokens(request: Request):
    if request.method == "OPTIONS":
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "*"
            }
        )

    body_bytes = await request.body()
    try:
        req = json.loads(body_bytes)
    except Exception:
        return make_error_response("Invalid JSON", status_code=400, owned_by="anthropic", is_anthropic=True)

    if not isinstance(req, dict):
        return make_error_response("Request body must be a JSON object", status_code=400, owned_by="anthropic", is_anthropic=True)

    requested_model = req.get("model")
    if not requested_model or not isinstance(requested_model, str):
        return make_error_response("model: field required", status_code=400, owned_by="anthropic", is_anthropic=True)

    to_count = []

    system_field = req.get("system")
    if isinstance(system_field, str):
        to_count.append({"role": "system", "content": system_field})
    elif isinstance(system_field, list):
        sys_text = "".join(b.get("text", "") for b in system_field if isinstance(b, dict) and b.get("type") == "text")
        if sys_text:
            to_count.append({"role": "system", "content": sys_text})

    messages = req.get("messages", [])
    if isinstance(messages, list):
        for m in messages:
            if not isinstance(m, dict):
                continue
            role = m.get("role", "user")
            content = m.get("content", "")
            if isinstance(content, str):
                to_count.append({"role": role, "content": content})
            elif isinstance(content, list):
                parts = []
                for b in content:
                    if not isinstance(b, dict):
                        continue
                    b_type = b.get("type")
                    if b_type == "text":
                        parts.append({"type": "text", "text": b.get("text", "")})
                    elif b_type in ("image", "image_url", "input_image"):
                        parts.append({"type": "image_url", "image_url": {}})
                    elif b_type == "tool_result":
                        c = b.get("content", "")
                        if isinstance(c, list):
                            c = " ".join(x.get("text", "") for x in c if isinstance(x, dict) and x.get("type") == "text")
                        if isinstance(c, str):
                            parts.append({"type": "text", "text": c})
                    elif b_type == "thinking":
                        tt = b.get("thinking", "")
                        if isinstance(tt, str) and tt:
                            parts.append({"type": "text", "text": tt})
                to_count.append({"role": role, "content": parts})

    tools = req.get("tools")
    if isinstance(tools, list) and tools:
        tools_text = json.dumps(tools, ensure_ascii=False)
        to_count.append({"role": "system", "content": tools_text})

    estimated = estimate_messages_tokens(to_count)

    _, req_id = generate_provider_ids("anthropic")
    clean_headers = build_gateway_headers("anthropic", req_id, processing_ms=5, is_stream=False)

    return Response(
        content=json.dumps({"input_tokens": estimated}, ensure_ascii=False),
        status_code=200,
        headers=clean_headers,
        media_type="application/json"
    )

@app.api_route("/v1/messages", methods=["POST", "OPTIONS"])
@app.api_route("/messages", methods=["POST", "OPTIONS"])
async def anthropic_messages_endpoint(request: Request):
    if request.method == "OPTIONS":
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "*"
            }
        )

    t_start = time.perf_counter()
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)
    headers.pop("accept-encoding", None)

    # --- API key resolution ---
    incoming_key = headers.get("x-api-key") or headers.get("anthropic-api-key")
    if not incoming_key:
        _auth = headers.get("authorization", "")
        if _auth.lower().startswith("bearer "):
            incoming_key = _auth[7:].strip()

    _resolved_key_id = None
    if incoming_key:
        _resolved_real = None
        for _row in API_KEYS_LIST:
            if _row[1] == incoming_key:
                _resolved_real = _row[2]
                _resolved_key_id = _row[0]
                break

        if _resolved_real:
            headers["x-api-key"] = _resolved_real
            headers["authorization"] = f"Bearer {_resolved_real}"
        elif incoming_key.startswith("sk-ant-"):
            return make_error_response(
                "Invalid API key",
                status_code=401,
                owned_by="anthropic",
                is_anthropic=True
            )
        else:
            if "authorization" not in headers:
                headers["authorization"] = f"Bearer {incoming_key}"

    body_bytes = await request.body()
    try:
        anthropic_req = json.loads(body_bytes)
    except Exception:
        return make_error_response("Invalid JSON", status_code=400, owned_by="anthropic", is_anthropic=True)

    if not isinstance(anthropic_req, dict):
        return make_error_response("Request body must be a JSON object", status_code=400, owned_by="anthropic", is_anthropic=True)

    requested_model = anthropic_req.get("model", "")
    stream = bool(anthropic_req.get("stream", False))

    if not requested_model or not isinstance(requested_model, str):
        return make_error_response("model: field required", status_code=400, owned_by="anthropic", is_anthropic=True)

    model_info = next((m for m in MODEL_MAPPINGS_LIST if m[2] == requested_model), None)

    # Anthropic-style 404 для неизвестной модели
    if model_info is None:
        return make_error_response(
            f"model: {requested_model}",
            status_code=404,
            owned_by="anthropic",
            is_anthropic=True
        )

    # Проверка endpoint_mode — доступна ли модель на Anthropic-эндпоинте
    _ep_mode = model_info[9] if len(model_info) > 9 else 3
    if _ep_mode == 2:
        return make_error_response(
            f"model: {requested_model} is not available on this endpoint",
            status_code=404,
            owned_by="anthropic",
            is_anthropic=True
        )

    is_vis_model = (model_info[3] != 0)

    thinking_cfg = anthropic_req.get("thinking")
    thinking_requested = bool(thinking_cfg)
    thinking_omitted = isinstance(thinking_cfg, dict) and thinking_cfg.get("display") == "omitted"

    cache_creation_tokens, cache_read_tokens = estimate_cache_from_request(anthropic_req)

    openai_messages = []
    system_field = anthropic_req.get("system")
    if system_field:
        if isinstance(system_field, str):
            openai_messages.append({"role": "system", "content": system_field})
        elif isinstance(system_field, list):
            sys_text = "".join(b.get("text", "") for b in system_field if isinstance(b, dict) and b.get("type") == "text")
            if sys_text:
                openai_messages.append({"role": "system", "content": sys_text})

    # Anthropic-style валидация чередования ролей
    _msgs_check = anthropic_req.get("messages", [])
    if isinstance(_msgs_check, list) and len(_msgs_check) > 0:
        _prev_role = None
        for _mi, _mm in enumerate(_msgs_check):
            if not isinstance(_mm, dict):
                continue
            _r = _mm.get("role", "")
            if _r == "system":
                return make_error_response(
                    "messages: use the top-level `system` parameter instead of the `system` role",
                    status_code=400,
                    owned_by="anthropic",
                    is_anthropic=True
                )
            if _prev_role is not None and _r == _prev_role:
                return make_error_response(
                    "messages: roles must alternate between \"user\" and \"assistant\"",
                    status_code=400,
                    owned_by="anthropic",
                    is_anthropic=True
                )
            if _r in ("user", "assistant"):
                _prev_role = _r

    try:
        messages_raw = anthropic_req.get("messages", [])
        if not isinstance(messages_raw, list):
            messages_raw = []
        for msg_idx, msg in enumerate(messages_raw):
            if not isinstance(msg, dict):
                continue
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if isinstance(content, str):
                openai_messages.append({"role": role, "content": content})
            elif isinstance(content, list):
                new_parts = []
                for blk_idx, b in enumerate(content):
                    if isinstance(b, dict):
                        b_type = b.get("type")
                        if b_type == "text":
                            new_parts.append({"type": "text", "text": b.get("text", "")})
                        elif b_type == "image":
                            src = b.get("source", {})
                            if src.get("type") == "base64":
                                med = src.get("media_type", "image/jpeg")
                                data = src.get("data", "")
                                new_parts.append({
                                    "type": "image_url",
                                    "image_url": {"url": f"data:{med};base64,{data}"}
                                })
                        elif b_type == "document":
                            src = b.get("source", {})
                            if src.get("type") == "base64":
                                data = src.get("data", "")
                                try:
                                    pdf_bytes = base64.b64decode(data)
                                    doc_parts = process_pdf_content(pdf_bytes, allow_vision=is_vis_model)
                                    new_parts.extend(doc_parts)
                                except Exception:
                                    new_parts.append({"type": "text", "text": "\n[Attached PDF Document]\n"})
                        elif b_type == "tool_result":
                            t_id = b.get("tool_use_id", "")
                            t_content = b.get("content", "")
                            if isinstance(t_content, list):
                                t_content = " ".join(x.get("text", "") for x in t_content if isinstance(x, dict) and x.get("type") == "text")
                            new_parts.append({
                                "type": "text",
                                "text": f"\n[Tool Result {t_id}]: {t_content}\n"
                            })
                        elif b_type == "thinking":
                            sig = b.get("signature", "")
                            thinking_text = b.get("thinking", "")
                            # Anthropic отклоняет пустые thinking-блоки в assistant-сообщении
                            if not sig or (isinstance(thinking_text, str) and thinking_text == "" and not sig):
                                raise _AnthropicRequestError(
                                    f"messages.{msg_idx}.content.{blk_idx}.thinking.signature: Thinking block must have a signature",
                                    status_code=400
                                )
                            if not validate_signature_format(sig):
                                raise _AnthropicRequestError(
                                    f"messages.{msg_idx}.content.{blk_idx}.thinking.signature: Invalid `signature` in `thinking` block",
                                    status_code=400
                                )
                            if isinstance(thinking_text, str) and thinking_text:
                                new_parts.append({"type": "text", "text": thinking_text})
                        elif b_type == "redacted_thinking":
                            data_field = b.get("data", "")
                            if not validate_signature_format(data_field):
                                raise _AnthropicRequestError(
                                    f"messages.{msg_idx}.content.{blk_idx}.redacted_thinking.data: Invalid `data` in `redacted_thinking` block",
                                    status_code=400
                                )
                    elif isinstance(b, str):
                        new_parts.append({"type": "text", "text": b})

                has_img = any(p.get("type") == "image_url" for p in new_parts if isinstance(p, dict))
                if not has_img:
                    merged_txt = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in new_parts)
                    openai_messages.append({"role": role, "content": merged_txt})
                else:
                    openai_messages.append({"role": role, "content": new_parts})
    except _AnthropicRequestError as e:
        return make_error_response(e.message, status_code=e.status_code, owned_by="anthropic", is_anthropic=True)

    if model_info:
        real_target_model = model_info[1]
        owned_by = model_info[5] if len(model_info) > 5 and model_info[5] else "anthropic"
    else:
        real_target_model = requested_model
        owned_by = "anthropic"

    res_id, req_id = generate_provider_ids(owned_by)
    orig_prompt_tokens = estimate_messages_tokens(openai_messages)

    openai_req = {
        "model": real_target_model,
        "messages": openai_messages,
        "stream": stream
    }
    if "max_tokens" in anthropic_req:
        openai_req["max_tokens"] = anthropic_req["max_tokens"]
    # Claude Opus 5 / Fable 5 / Opus 4.8/4.7 — удалены temperature, top_p, top_k, thinking.enabled
    _is_new_gen_model = any(_tag in requested_model.lower() for _tag in ["opus-5", "opus-4.7", "opus-4.8", "fable", "sonnet-5"])
    if "temperature" in anthropic_req and not _is_new_gen_model:
        openai_req["temperature"] = anthropic_req["temperature"]
    if "top_p" in anthropic_req and not _is_new_gen_model:
        openai_req["top_p"] = anthropic_req["top_p"]
    if "top_k" in anthropic_req and isinstance(anthropic_req["top_k"], int) and not _is_new_gen_model:
        openai_req["top_k"] = anthropic_req["top_k"]

    if "stop_sequences" in anthropic_req and isinstance(anthropic_req["stop_sequences"], list) and anthropic_req["stop_sequences"]:
        openai_req["stop"] = anthropic_req["stop_sequences"]

    if "tools" in anthropic_req and isinstance(anthropic_req["tools"], list):
        openai_tools = []
        for t in anthropic_req["tools"]:
            if isinstance(t, dict):
                openai_tools.append({
                    "type": "function",
                    "function": {
                        "name": t.get("name"),
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", {})
                    }
                })
        if openai_tools:
            openai_req["tools"] = openai_tools

    if "tool_choice" in anthropic_req:
        tc = anthropic_req["tool_choice"]
        if isinstance(tc, dict):
            tc_type = tc.get("type")
            if tc_type == "auto":
                openai_req["tool_choice"] = "auto"
            elif tc_type == "any":
                openai_req["tool_choice"] = "required"
            elif tc_type == "none":
                openai_req["tool_choice"] = "none"
            elif tc_type == "tool":
                openai_req["tool_choice"] = {
                    "type": "function",
                    "function": {"name": tc.get("name")}
                }
        elif isinstance(tc, str):
            openai_req["tool_choice"] = tc

    delay_sec = float(model_info[6]) if model_info and len(model_info) > 6 else 0.0
    stream_throttle = bool(model_info[7]) if model_info and len(model_info) > 7 else False
    is_reasoning = int(model_info[8]) if model_info and len(model_info) > 8 else (1 if "opus" in requested_model.lower() else 0)

    _max_tok = anthropic_req.get("max_tokens")
    _skip_reasoning = (isinstance(_max_tok, int) and _max_tok < 256 and is_reasoning != 6)
    if is_reasoning >= 1 and not _skip_reasoning:
        reasoning_instruction = get_reasoning_prompt(is_reasoning)
        sys_msg = next((m for m in openai_req["messages"] if m.get("role") == "system"), None)
        if sys_msg:
            sys_msg["content"] = str(sys_msg.get("content", "")) + "\n\n" + reasoning_instruction
        else:
            openai_req["messages"].insert(0, {"role": "system", "content": reasoning_instruction})

    if model_info:
        is_vis = model_info[3]
        has_image = any(
            isinstance(m.get("content"), list) and any(
                isinstance(p, dict) and p.get("type") == "image_url" for p in m.get("content")
            )
            for m in openai_req["messages"]
        )
        if has_image:
            if is_vis == 0:
                return make_error_response(f"The model '{requested_model}' does not support images.", status_code=400, owned_by=owned_by, req_id=req_id, is_anthropic=True)
            elif is_vis == 2:
                for m in openai_req["messages"]:
                    if isinstance(m.get("content"), list):
                        new_parts = []
                        for p in m["content"]:
                            if isinstance(p, dict):
                                if p.get("type") == "text":
                                    new_parts.append(p.get("text", ""))
                                elif p.get("type") == "image_url":
                                    new_parts.append("[Изображение пользователя: успешно прикреплено]")
                            elif isinstance(p, str):
                                new_parts.append(p)
                        m["content"] = " ".join(filter(None, new_parts))

    client = httpx.AsyncClient(timeout=180.0)
    target_url = f"{UPSTREAM_URL}/v1/chat/completions"

    try:
        req = client.build_request(
            method="POST",
            url=target_url,
            headers=headers,
            json=openai_req
        )
    except Exception:
        await client.aclose()
        return make_error_response("Upstream request could not be built", status_code=502, owned_by=owned_by, req_id=req_id, is_anthropic=True)

    try:
        upstream_resp = await client.send(req, stream=True)
    except Exception:
        await client.aclose()
        msg = resolve_custom_error("connection_error timeout 502", 502)
        return make_error_response(msg, status_code=502, owned_by=owned_by, req_id=req_id, is_anthropic=True)

    if upstream_resp.status_code >= 400:
        retry_after_val = upstream_resp.headers.get("retry-after")
        try:
            err_bytes = await upstream_resp.aread()
            raw_err_text = err_bytes.decode("utf-8", errors="ignore")
        finally:
            await upstream_resp.aclose()
            await client.aclose()
        msg = resolve_custom_error(raw_err_text, upstream_resp.status_code)
        return make_error_response(msg, status_code=upstream_resp.status_code, owned_by=owned_by, req_id=req_id, is_anthropic=True, retry_after=retry_after_val)

    if stream:
        async def anthropic_stream_wrapper():
            try:
                async for chunk in anthropic_stream_generator(
                    upstream_resp,
                    requested_model=requested_model,
                    real_model=real_target_model,
                    is_reasoning=is_reasoning,
                    prompt_tokens=orig_prompt_tokens,
                    fixed_id=res_id,
                    delay_sec=delay_sec,
                    stream_throttle=stream_throttle,
                    thinking_requested=thinking_requested,
                    thinking_omitted=thinking_omitted,
                    cache_creation_tokens=cache_creation_tokens,
                    cache_read_tokens=cache_read_tokens
                ):
                    yield chunk
            finally:
                await upstream_resp.aclose()
                await client.aclose()

        rl_register(input_tokens=orig_prompt_tokens, output_tokens=0)

        bump_key_usage(_resolved_key_id,
                       input_tokens=orig_prompt_tokens,
                       output_tokens=0)

        processing_ms = int((time.perf_counter() - t_start) * 1000)
        clean_headers = build_gateway_headers(owned_by, req_id, processing_ms=processing_ms, is_stream=True)
        return StreamingResponse(anthropic_stream_wrapper(), status_code=200, headers=clean_headers)

    try:
        raw_body = await upstream_resp.aread()
        text = raw_body.decode("utf-8", errors="ignore")
        clean_content = ""
        tool_calls_found = []
        upstream_finish_reason = None

        try:
            data = json.loads(text)
            if "choices" in data and len(data["choices"]) > 0:
                ch = data["choices"][0]
                upstream_finish_reason = ch.get("finish_reason")
                msg = ch.get("message", {})

                raw_c = msg.get("content", "")
                if raw_c:
                    if "<think>" in raw_c:
                        clean_content = re.sub(r"<think>[\s\S]*?(?:</think>|<\\/think>|$)", "", raw_c).strip()
                    else:
                        clean_content = raw_c.strip()

                if "tool_calls" in msg and isinstance(msg["tool_calls"], list):
                    tool_calls_found = msg["tool_calls"]
        except Exception:
            clean_content = text

        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        anthropic_content_blocks = []

        # Anthropic-style omitted thinking: пустой thinking + signature.
        # Реальный текст размышлений наружу не отдаём — это часть маскировки.
        if thinking_requested:
            anthropic_content_blocks.append({
                "type": "thinking",
                "thinking": "",
                "signature": gen_thinking_signature()
            })

        if clean_content:
            anthropic_content_blocks.append({
                "type": "text",
                "text": clean_content
            })

        stop_reason = "end_turn"
        if tool_calls_found:
            stop_reason = "tool_use"
        elif upstream_finish_reason == "length":
            stop_reason = "max_tokens"

        if tool_calls_found:
            for tc in tool_calls_found:
                fn = tc.get("function", {})
                f_name = fn.get("name", "tool")
                raw_args = fn.get("arguments", "{}")
                try:
                    p_args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except Exception:
                    p_args = {}
                tc_id = tc.get("id") or f"toolu_{gen_base62(22)}"
                if not tc_id.startswith("toolu_"):
                    tc_id = f"toolu_{gen_base62(22)}"
                anthropic_content_blocks.append({
                    "type": "tool_use",
                    "id": tc_id,
                    "name": f_name,
                    "input": p_args
                })

        if not anthropic_content_blocks:
            anthropic_content_blocks.append({"type": "text", "text": ""})

        # output_tokens = только текст ответа, без thinking (как у Anthropic)
        comp_tokens = estimate_tokens_text(clean_content)

        final_cache_read = min(cache_read_tokens, orig_prompt_tokens)
        final_cache_creation = min(cache_creation_tokens, orig_prompt_tokens)
        non_cached_input = max(0, orig_prompt_tokens - final_cache_read - final_cache_creation)

        anthropic_response_data = {
            "id": res_id,
            "type": "message",
            "role": "assistant",
            "model": requested_model,
            "content": anthropic_content_blocks,
            "stop_reason": stop_reason,
            "stop_sequence": None,
            "stop_details": stop_details if 'stop_details' in dir() else None,
            "usage": {
                "input_tokens": non_cached_input,
                "cache_creation_input_tokens": final_cache_creation,
                "cache_read_input_tokens": final_cache_read,
                "output_tokens": max(1, comp_tokens)
            }
        }

        rl_register(input_tokens=non_cached_input, output_tokens=max(1, comp_tokens))

        bump_key_usage(_resolved_key_id,
                       input_tokens=non_cached_input,
                       output_tokens=max(1, comp_tokens))

        processing_ms = int((time.perf_counter() - t_start) * 1000)
        clean_headers = build_gateway_headers(owned_by, req_id, processing_ms=processing_ms, is_stream=False)

        return Response(
            content=json.dumps(anthropic_response_data, ensure_ascii=False),
            status_code=200,
            headers=clean_headers,
            media_type="application/json"
        )
    finally:
        await upstream_resp.aclose()
        await client.aclose()

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def proxy(request: Request, path: str):
    clean_path = path.lstrip("/").lower()

    if clean_path in ["pricing", "pricing/"]:
        return RedirectResponse(url="/", status_code=302)

    if any(clean_path.startswith(p) for p in ["api/pricing", "api/prices", "api/ratio", "api/model/pricing"]):
        return make_error_response("Not Found", status_code=404, owned_by="openai")

    t_start = time.perf_counter()
    target_url = f"{UPSTREAM_URL}/{path}"
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)
    headers.pop("accept-encoding", None)

    # --- API key resolution ---
    incoming_key = headers.get("x-api-key") or headers.get("anthropic-api-key")
    if not incoming_key:
        _auth = headers.get("authorization", "")
        if _auth.lower().startswith("bearer "):
            incoming_key = _auth[7:].strip()

    _resolved_key_id = None
    if incoming_key:
        _resolved_real = None
        for _row in API_KEYS_LIST:
            if _row[1] == incoming_key:
                _resolved_real = _row[2]
                _resolved_key_id = _row[0]
                break

        if _resolved_real:
            headers["x-api-key"] = _resolved_real
            headers["authorization"] = f"Bearer {_resolved_real}"
        elif incoming_key.startswith("sk-ant-"):
            return make_error_response(
                "Invalid API key",
                status_code=401,
                owned_by="openai"
            )
        else:
            if "authorization" not in headers:
                headers["authorization"] = f"Bearer {incoming_key}"

    body = await request.body()
    requested_model = None
    parsed_req = None
    orig_prompt_tokens = 8

    try:
        if body:
            parsed_req = json.loads(body)
            if isinstance(parsed_req, dict):
                requested_model = parsed_req.get("model")
                orig_prompt_tokens = estimate_messages_tokens(parsed_req.get("messages", []))
    except Exception:
        pass

    model_info = None
    delay_sec = 0.0
    stream_throttle = False
    is_reasoning = 0
    owned_by = "openai"
    real_model_name = None

    if requested_model:
        model_info = next((m for m in MODEL_MAPPINGS_LIST if m[2] == requested_model), None)
        if model_info:
            # Проверка endpoint_mode — доступна ли модель на OpenAI-эндпоинте
            _ep_mode = model_info[9] if len(model_info) > 9 else 3
            if _ep_mode == 1:
                return make_error_response(
                    f"The model '{requested_model}' is not available on this endpoint.",
                    status_code=404,
                    owned_by="openai"
                )
            real_model_name = model_info[1]
            owned_by = model_info[5] if len(model_info) > 5 and model_info[5] else "openai"
            delay_sec = float(model_info[6]) if len(model_info) > 6 else 0.0
            stream_throttle = bool(model_info[7]) if len(model_info) > 7 else False
            is_reasoning = int(model_info[8]) if len(model_info) > 8 else 0
            if isinstance(parsed_req, dict):
                parsed_req["model"] = model_info[1]
                body = json.dumps(parsed_req, ensure_ascii=False).encode("utf-8")

    res_id, req_id = generate_provider_ids(owned_by)

    _max_tok = parsed_req.get("max_tokens") if isinstance(parsed_req, dict) else None
    _skip_reasoning = (isinstance(_max_tok, int) and _max_tok < 256 and is_reasoning != 6)
    if requested_model and isinstance(parsed_req, dict) and is_reasoning >= 1 and not _skip_reasoning:
        reasoning_instruction = get_reasoning_prompt(is_reasoning)
        messages = parsed_req.setdefault("messages", [])
        system_msg = next((m for m in messages if m.get("role") == "system"), None)
        if system_msg:
            system_msg["content"] = str(system_msg.get("content", "")) + "\n\n" + reasoning_instruction
        else:
            messages.insert(0, {"role": "system", "content": reasoning_instruction})
        body = json.dumps(parsed_req, ensure_ascii=False).encode("utf-8")

    if requested_model and isinstance(parsed_req, dict) and model_info:
        is_vis = model_info[3]
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
                return make_error_response(f"The model '{requested_model}' does not support image input.", status_code=400, owned_by=owned_by, req_id=req_id)
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
        return make_error_response(msg, status_code=502, owned_by=owned_by, req_id=req_id)

    if upstream_resp.status_code >= 400:
        retry_after_val = upstream_resp.headers.get("retry-after")
        try:
            err_bytes = await upstream_resp.aread()
            raw_err_text = err_bytes.decode("utf-8", errors="ignore")
        finally:
            await upstream_resp.aclose()
            await client.aclose()
        
        msg = resolve_custom_error(raw_err_text, upstream_resp.status_code)
        return make_error_response(msg, status_code=upstream_resp.status_code, owned_by=owned_by, req_id=req_id, retry_after=retry_after_val)

    content_type = upstream_resp.headers.get("content-type", "").lower()

    if "text/event-stream" in content_type:
        processing_ms = int((time.perf_counter() - t_start) * 1000)
        clean_headers = build_gateway_headers(owned_by, req_id, processing_ms=processing_ms, is_stream=True)

        if delay_sec > 0 or stream_throttle:
            async def buffered_stream_wrapper():
                try:
                    collected_chunks = []
                    async for chunk in stream_filter_generator(
                        upstream_resp,
                        requested_model=requested_model,
                        real_model=real_model_name,
                        is_reasoning=is_reasoning,
                        orig_prompt_tokens=orig_prompt_tokens,
                        fixed_id=res_id
                    ):
                        collected_chunks.append(chunk)

                    if delay_sec > 0:
                        await asyncio.sleep(delay_sec)

                    for chunk in collected_chunks:
                        yield chunk
                        if stream_throttle:
                            await asyncio.sleep(0.015)
                finally:
                    await upstream_resp.aclose()
                    await client.aclose()

            return StreamingResponse(buffered_stream_wrapper(), status_code=upstream_resp.status_code, headers=clean_headers)
        else:
            async def live_stream_wrapper():
                try:
                    async for chunk in stream_filter_generator(
                        upstream_resp,
                        requested_model=requested_model,
                        real_model=real_model_name,
                        is_reasoning=is_reasoning,
                        orig_prompt_tokens=orig_prompt_tokens,
                        fixed_id=res_id
                    ):
                        yield chunk
                finally:
                    await upstream_resp.aclose()
                    await client.aclose()

            return StreamingResponse(live_stream_wrapper(), status_code=upstream_resp.status_code, headers=clean_headers)

    try:
        raw_body = await upstream_resp.aread()

        if not any(t in content_type for t in ["text/", "application/json", "application/javascript"]):
            return Response(
                content=raw_body,
                status_code=upstream_resp.status_code,
                media_type=content_type or "application/octet-stream"
            )

        text = raw_body.decode("utf-8", errors="ignore")

        if "text/html" in content_type:
            if "</head>" in text:
                text = text.replace("</head>", f"{PRICING_INJECTION}</head>", 1)
            elif "</body>" in text:
                text = text.replace("</body>", f"{PRICING_INJECTION}</body>", 1)
            else:
                text = PRICING_INJECTION + text

            return HTMLResponse(
                content=text,
                status_code=upstream_resp.status_code,
                headers={"cache-control": "no-cache"}
            )

        if "application/json" not in content_type:
            return Response(
                content=text,
                status_code=upstream_resp.status_code,
                media_type=content_type
            )

        try:
            data = json.loads(text)
            
            if data.get("error") or (data.get("base_resp", {}).get("status_code", 0) != 0):
                msg = resolve_custom_error(text, 400)
                return make_error_response(msg, status_code=400, owned_by=owned_by, req_id=req_id)

            text = replace_models(text, real_model_name, requested_model)
            data = json.loads(text)

            data["id"] = res_id

            if requested_model and "model" in data:
                data["model"] = requested_model

            if "metadata" in data and isinstance(data["metadata"], dict):
                sanitize_metadata(data["metadata"], requested_model)

            clean_content = ""
            if "choices" in data and isinstance(data["choices"], list):
                for ch in data["choices"]:
                    msg = ch.get("message", {})
                    if msg.get("name") in ["MiniMax AI", "Qwen AI"]:
                        msg.pop("name", None)

                    raw_content = msg.get("content", "")
                    if raw_content and isinstance(raw_content, str):
                        clean_content += raw_content

            comp_tokens = estimate_tokens_text(clean_content)
            data["usage"] = {
                "prompt_tokens": orig_prompt_tokens,
                "completion_tokens": comp_tokens,
                "total_tokens": orig_prompt_tokens + comp_tokens
            }

            text = json.dumps(data, ensure_ascii=False)
        except Exception:
            pass

        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        if _resolved_key_id:
            bump_key_usage(_resolved_key_id,
                           input_tokens=orig_prompt_tokens,
                           output_tokens=comp_tokens)

        processing_ms = int((time.perf_counter() - t_start) * 1000)
        clean_headers = build_gateway_headers(owned_by, req_id, processing_ms=processing_ms, is_stream=False)

        return Response(content=text, status_code=upstream_resp.status_code, headers=clean_headers)
    finally:
        await upstream_resp.aclose()
        await client.aclose()
