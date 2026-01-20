"""
Telegram Bot for Yandex Tracker integration.
Optimized for low-resource servers (Render free tier).
"""
import os
import sys
import asyncio
import re
import time
import logging
import signal
from datetime import datetime
from functools import wraps
from typing import Optional, Tuple, Dict, List, Any
from collections import OrderedDict

import httpx
from fastapi import FastAPI
import uvicorn

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, CallbackQuery, BotCommand
from aiogram.utils.keyboard import InlineKeyboardBuilder

# =============================================================================
# Configuration
# =============================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
DEBUG = os.getenv("DEBUG", "").lower() == "true"

# HTTP settings optimized for low-resource server
HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)
HTTP_TIMEOUT_LONG = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
HTTP_LIMITS = httpx.Limits(max_keepalive_connections=3, max_connections=5)

# Cache settings
CACHE_MAX_SIZE = 50
CACHE_TTL_SECONDS = 3600  # 1 hour
KEEP_ALIVE_INTERVAL = 600  # 10 minutes

# =============================================================================
# Logging Configuration (simplified)
# =============================================================================
logging.basicConfig(
    level=logging.INFO if DEBUG else logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress aiogram conflict errors
for name in ["aiogram", "aiogram.event", "aiogram.dispatcher", "aiogram.polling"]:
    logging.getLogger(name).setLevel(logging.ERROR)

# =============================================================================
# Simple LRU Cache with TTL
# =============================================================================
class TTLCache:
    """Simple LRU cache with TTL, bounded size."""
    
    def __init__(self, maxsize: int = CACHE_MAX_SIZE, ttl: int = CACHE_TTL_SECONDS):
        self._cache: OrderedDict = OrderedDict()
        self._maxsize = maxsize
        self._ttl = ttl
    
    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None
        value, timestamp = self._cache[key]
        if time.time() - timestamp > self._ttl:
            del self._cache[key]
            return None
        # Move to end (LRU)
        self._cache.move_to_end(key)
        return value
    
    def set(self, key: str, value: Any) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = (value, time.time())
        # Evict oldest if over size
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)
    
    def clear(self) -> None:
        self._cache.clear()

# =============================================================================
# Global State (minimal)
# =============================================================================
class AppState:
    """Application state container."""
    
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dispatcher: Optional[Dispatcher] = None
        self.http_client: Optional[httpx.AsyncClient] = None
        self.shutdown_event = asyncio.Event()
        self.checklist_cache = TTLCache(maxsize=100, ttl=1800)  # 30 min
        self.summary_cache = TTLCache(maxsize=50, ttl=3600)  # 1 hour
        self.pending_comment: Dict[int, str] = {}  # tg_id -> issue_key (awaiting comment text)

state = AppState()

# =============================================================================
# HTTP Client (singleton, lazy init)
# =============================================================================
async def get_http_client() -> httpx.AsyncClient:
    """Get or create HTTP client (singleton pattern)."""
    if state.http_client is None or state.http_client.is_closed:
        state.http_client = httpx.AsyncClient(
            timeout=HTTP_TIMEOUT,
            limits=HTTP_LIMITS,
            http2=False  # HTTP/2 uses more memory
        )
    return state.http_client


async def close_http_client() -> None:
    """Close HTTP client."""
    if state.http_client and not state.http_client.is_closed:
        await state.http_client.aclose()
        state.http_client = None

# =============================================================================
# FastAPI App
# =============================================================================
app = FastAPI(title="Tracker Bot", docs_url=None, redoc_url=None)
router = Router(name="main_router")


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "tracker-bot",
        "bot_active": state.bot is not None
    }


@app.get("/ping")
async def ping():
    """Keep-alive endpoint."""
    return "pong"


@app.on_event("shutdown")
async def app_shutdown():
    """Cleanup on app shutdown."""
    await close_http_client()

# =============================================================================
# Helpers
# =============================================================================
def fmt_item(item: dict) -> str:
    """Format checklist item for display."""
    mark = "✅" if item.get("checked") else "⬜"
    text = (item.get("text") or "").strip().replace("\n", " ")[:100]
    return f"{mark} {text}"


def fmt_date(date_str: Optional[str]) -> str:
    """Format ISO date to DD.MM.YYYY HH:MM."""
    if not date_str:
        return ""
    try:
        clean = date_str.replace("Z", "+00:00")
        # Fix timezone format: +0300 -> +03:00
        if "+" in clean and ":" not in clean.split("+")[-1]:
            parts = clean.rsplit("+", 1)
            if len(parts[1]) == 4:
                clean = f"{parts[0]}+{parts[1][:2]}:{parts[1][2:]}"
        return datetime.fromisoformat(clean).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return date_str[:16] if len(date_str) > 16 else date_str


def escape_md(text: str) -> str:
    """Escape special characters for Telegram Markdown."""
    for ch in ('_', '*', '`', '['):
        text = text.replace(ch, f'\\{ch}')
    return text


def fmt_issue_link(issue: dict, prefix: str = "") -> str:
    """Format issue as Markdown hyperlink: [KEY: Summary (date)](url)"""
    key = issue.get("key", "")
    summary = escape_md((issue.get("summary") or "")[:60])
    url = issue.get("url") or f"https://tracker.yandex.ru/{key}"
    date_str = fmt_date(issue.get("updatedAt"))
    
    if date_str:
        text = f"{prefix}[{key}: {summary} ({date_str})]({url})"
    else:
        text = f"{prefix}[{key}: {summary}]({url})"
    return text


def parse_response(r: httpx.Response) -> dict:
    """Parse HTTP response to dict."""
    if "application/json" in r.headers.get("content-type", ""):
        try:
            return r.json()
        except Exception:
            pass
    return {"raw": r.text}


async def api_request(method: str, path: str, params: dict, timeout: httpx.Timeout = None) -> Tuple[int, dict]:
    """Make API request with error handling."""
    client = await get_http_client()
    try:
        if method == "GET":
            r = await client.get(f"{BASE_URL}{path}", params=params, timeout=timeout)
        else:
            r = await client.post(f"{BASE_URL}{path}", params=params, timeout=timeout)
        return r.status_code, parse_response(r)
    except httpx.TimeoutException:
        return 504, {"error": "Timeout"}
    except Exception as e:
        return 500, {"error": str(e)[:200]}


def require_base_url(func):
    """Decorator to check BASE_URL."""
    @wraps(func)
    async def wrapper(m: Message, *args, **kwargs):
        if not BASE_URL:
            await m.answer("❌ BASE_URL не задан")
            return
        return await func(m, *args, **kwargs)
    return wrapper

# =============================================================================
# Keyboard Builders
# =============================================================================
def kb_settings_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Очереди", callback_data="st:queues")
    kb.button(text="Период", callback_data="st:days")
    kb.button(text="Лимит", callback_data="st:limit")
    kb.button(text="🔔 Напоминание", callback_data="st:reminder")
    kb.button(text="Закрыть", callback_data="st:close")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def kb_settings_queues(queues: List[str]) -> InlineKeyboardMarkup:
    qs = {q.upper() for q in queues}
    kb = InlineKeyboardBuilder()
    for q in ["INV", "DOC", "HR"]:
        kb.button(text=f"{'✅' if q in qs else '⬜'} {q}", callback_data=f"st:qtoggle:{q}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(3, 1)
    return kb.as_markup()


def kb_settings_days(days: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for d in [7, 15, 30, 90, 180]:
        kb.button(text=f"{'✅' if days == d else '⬜'} {d}д", callback_data=f"st:dset:{d}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(3, 2)
    return kb.as_markup()


def kb_settings_limit(limit: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for n in [5, 10, 15, 20, 30, 50]:
        kb.button(text=f"{'✅' if limit == n else '⬜'} {n}", callback_data=f"st:lset:{n}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(3, 3)
    return kb.as_markup()


def kb_settings_reminder(reminder: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    options = [(0, "Откл"), (1, "1ч"), (3, "3ч"), (6, "6ч")]
    for val, label in options:
        kb.button(text=f"{'✅' if reminder == val else '⬜'} {label}", callback_data=f"st:rset:{val}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(4, 1)
    return kb.as_markup()


def render_settings_text(queues: List[str], days: int, limit: int, reminder: int = 0) -> str:
    q = ", ".join(queues) if queues else "(все)"
    r = {0: "Откл", 1: "1ч", 3: "3ч", 6: "6ч"}.get(reminder, "Откл")
    return (
        f"⚙️ Настройки:\n"
        f"• Очереди: {q}\n"
        f"• Период: {days} дней\n"
        f"• Лимит: {limit}\n"
        f"• 🔔 Напоминание: {r}\n\n"
        "Выбери параметр:"
    )

# =============================================================================
# Checklist Helpers
# =============================================================================
async def get_settings(tg_id: int) -> Optional[Tuple[List[str], int, int, int]]:
    """Get user settings: (queues, days, limit, reminder)."""
    sc, data = await api_request("GET", "/tg/settings", {"tg": tg_id})
    if sc != 200:
        return None
    return (
        data.get("queues") or [],
        int(data.get("days", 30)),
        int(data.get("limit", 10)),
        int(data.get("reminder", 0))
    )


def build_checklist_response(
    issues: List[dict],
    header: str,
    include_checked: bool = True,
    add_buttons: bool = False
) -> Tuple[str, Optional[InlineKeyboardMarkup], Dict[int, Tuple[str, str]]]:
    """Build checklist text, keyboard, and item mapping."""
    lines = [header]
    kb = InlineKeyboardBuilder() if add_buttons else None
    item_mapping = {}
    item_num = 1
    
    for idx, issue in enumerate(issues, 1):
        lines.append(f"\n{idx}. {fmt_issue_link(issue)}")
        
        for item in issue.get("items", []):
            is_checked = item.get("checked", False)
            if include_checked or not is_checked:
                lines.append(f"  {fmt_item(item)}")
                item_mapping[item_num] = (issue.get("key"), item.get("id"))
                if kb and not is_checked:
                    kb.button(text=f"✅ {item_num}", callback_data=f"chk:{issue.get('key')}:{item.get('id')}:{item_num}")
                item_num += 1
    
    if kb:
        kb.adjust(4)
    
    text = "\n".join(lines)
    return text, kb.as_markup() if kb else None, item_mapping

# =============================================================================
# Bot Handlers
# =============================================================================
@router.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer("Привет! Я работаю с Yandex Tracker.\n\n/menu — все команды")


@router.message(Command("menu"))
async def cmd_menu(m: Message):
    await m.answer(
        "📋 *Меню:*\n\n"
        "🔗 /connect — привязать аккаунт\n"
        "👤 /me — проверить доступ\n"
        "⚙️ /settings — настройки\n\n"
        "✅ /cl\\_my — мои чеклисты\n"
        "❔ /cl\\_my\\_open — ожидают согласование\n"
        "✔️ /done N — отметить пункт\n\n"
        "📣 /mentions — требующие ответа\n"
        "🤖 /summary ISSUE — резюме (ИИ)",
        parse_mode="Markdown"
    )


@router.message(Command("connect"))
@require_base_url
async def cmd_connect(m: Message):
    url = f"{BASE_URL}/oauth/start?tg={m.from_user.id}"
    await m.answer(f"Открой ссылку:\n{url}\n\nПосле — /me")


@router.message(Command("me"))
@require_base_url
async def cmd_me(m: Message):
    sc, data = await api_request("GET", "/tracker/me_by_tg", {"tg": m.from_user.id})
    if sc != 200:
        await m.answer(f"❌ Ошибка {sc}: {data}")
        return
    
    inner_sc = data.get("status_code")
    if inner_sc == 200:
        user = data.get("response", {})
        login = user.get("login") or user.get("display") or "unknown"
        await m.answer(f"✅ Tracker: {login}")
    else:
        await m.answer(f"❌ Tracker: {inner_sc} — {data.get('response')}")


@router.message(Command("settings"))
@require_base_url
async def cmd_settings(m: Message):
    settings = await get_settings(m.from_user.id)
    if not settings:
        await m.answer("❌ Не удалось получить настройки")
        return
    queues, days, limit, reminder = settings
    await m.answer(render_settings_text(queues, days, limit, reminder), reply_markup=kb_settings_main())


@router.message(Command("cl_my"))
@require_base_url
async def cmd_cl_my(m: Message):
    tg_id = m.from_user.id
    settings = await get_settings(tg_id)
    limit = settings[2] if settings else 10
    
    sc, data = await api_request("GET", "/tracker/checklist/assigned", {"tg": tg_id, "limit": limit}, HTTP_TIMEOUT_LONG)
    if sc != 200:
        await m.answer(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    if not issues:
        days = data.get("settings", {}).get("days", 30)
        await m.answer(f"Нет задач за {days} дней")
        return
    
    text, _, item_mapping = build_checklist_response(issues, "📋 *Мои чеклисты:*")
    state.checklist_cache.set(f"cl:{tg_id}", item_mapping)
    
    # Split long messages
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        await m.answer(chunk, parse_mode="Markdown")


@router.message(Command("mentions"))
@require_base_url
async def cmd_mentions(m: Message):
    """Get issues where user was mentioned (summoned)."""
    tg_id = m.from_user.id
    settings = await get_settings(tg_id)
    limit = settings[2] if settings else 10
    
    sc, data = await api_request("GET", "/tracker/summons", {"tg": tg_id, "limit": limit}, HTTP_TIMEOUT_LONG)
    if sc != 200:
        await m.answer(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    if not issues:
        days = data.get("settings", {}).get("days", 30)
        await m.answer(f"📣 Нет упоминаний за {days} дней")
        return

    lines = ["📣 *Требующие ответа:*"]
    for idx, issue in enumerate(issues, 1):
        status = "✅" if issue.get("has_responded") else "⏳"
        lines.append(f"\n{idx}. {fmt_issue_link(issue, prefix=f'{status} ')}")
    
    lines.append("\n_✅ — вы ответили, ⏳ — ожидает ответа_")
    
    text = "\n".join(lines)
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        await m.answer(chunk, parse_mode="Markdown")


@router.message(Command("cl_my_open"))
@require_base_url
async def cmd_cl_my_open(m: Message):
    tg_id = m.from_user.id
    settings = await get_settings(tg_id)
    limit = settings[2] if settings else 10
    
    sc, data = await api_request("GET", "/tracker/checklist/assigned_unchecked", {"tg": tg_id, "limit": limit}, HTTP_TIMEOUT_LONG)
    if sc != 200:
        await m.answer(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    if not issues:
        days = data.get("settings", {}).get("days", 30)
        await m.answer(f"Нет пунктов за {days} дней")
        return
    
    text, keyboard, item_mapping = build_checklist_response(
        issues, "❔ *Ожидают согласование:*", include_checked=False, add_buttons=True
    )
    state.checklist_cache.set(f"cl:{tg_id}", item_mapping)
    
    if len(text) > 4000:
        await m.answer(text[:4000], reply_markup=keyboard, parse_mode="Markdown")
        await m.answer(text[4000:], parse_mode="Markdown")
    else:
        await m.answer(text, reply_markup=keyboard, parse_mode="Markdown")


@router.message(Command("done"))
@require_base_url
async def cmd_done(m: Message):
    """Mark checklist item by number."""
    parts = (m.text or "").split()
    if len(parts) != 2:
        await m.answer("Использование: /done N")
        return
    
    try:
        num = int(parts[1])
    except ValueError:
        await m.answer("❌ N должно быть числом")
        return

    tg_id = m.from_user.id
    item_mapping = state.checklist_cache.get(f"cl:{tg_id}")
    if not item_mapping or num not in item_mapping:
        await m.answer("❌ Сначала выполните /cl_my или /cl_my_open")
        return

    issue_key, item_id = item_mapping[num]
    sc, data = await api_request("POST", "/tracker/checklist/check", {
        "tg": tg_id, "issue": issue_key, "item": item_id, "checked": True
    })
    
    if sc == 200:
        await m.answer(f"✅ Отмечен: {issue_key} #{num}")
    else:
        await m.answer(f"❌ Ошибка: {data.get('error', data)}"[:200])


@router.message(Command("cl_done"))
@require_base_url
async def cmd_cl_done(m: Message):
    """Mark checklist item by issue key and item id."""
    parts = (m.text or "").split()
    if len(parts) != 3:
        await m.answer("Использование: /cl_done ISSUE-KEY ITEM_ID")
        return

    _, issue_key, item_id = parts
    sc, data = await api_request("POST", "/tracker/checklist/check", {
        "tg": m.from_user.id, "issue": issue_key, "item": item_id, "checked": True
    })
    
    if sc == 200:
        await m.answer(f"✅ Отмечен: {issue_key} / {item_id}")
    else:
        await m.answer(f"❌ Ошибка {sc}: {data}"[:200])


def kb_summary_actions(issue_key: str) -> InlineKeyboardMarkup:
    """Build keyboard for summary actions."""
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data=f"sum:refresh:{issue_key}")
    kb.button(text="💬 Комментарий", callback_data=f"sum:comment:{issue_key}")
    kb.button(text="📋 Чеклисты", callback_data=f"sum:checklist:{issue_key}")
    kb.adjust(3)
    return kb.as_markup()


@router.message(Command("summary"))
@require_base_url
async def cmd_summary(m: Message):
    """Generate AI summary for issue."""
    parts = (m.text or "").split()
    if len(parts) != 2:
        await m.answer("Использование: /summary ISSUE-KEY")
        return

    issue_key = parts[1].upper().strip()
    tg_id = m.from_user.id
    
    # Check cache (but still show buttons)
    cached = state.summary_cache.get(issue_key)
    if cached:
        await m.answer(
            f"📋 {issue_key} (кэш):\n\n{cached['summary']}\n\n🔗 {cached['url']}",
            reply_markup=kb_summary_actions(issue_key)
        )
        return
    
    loading = await m.answer("🤖 Генерирую резюме...")
    
    sc, data = await api_request("GET", f"/tracker/issue/{issue_key}/summary", {"tg": tg_id}, HTTP_TIMEOUT_LONG)
    
    if sc != 200:
        error_map = {401: "Ошибка авторизации. /connect", 404: f"{issue_key} не найден"}
        await loading.edit_text(f"❌ {error_map.get(sc, data.get('error', 'Ошибка'))}"[:500])
        return
    
    summary = data.get("summary", "")
    url = data.get("issue_url", f"https://tracker.yandex.ru/{issue_key}")
    
    if not summary:
        await loading.edit_text("❌ Не удалось сгенерировать резюме")
        return

    state.summary_cache.set(issue_key, {"summary": summary, "url": url})
    
    text = f"📋 {issue_key}:\n\n{summary}\n\n🔗 {url}"
    if len(text) > 4000:
        await loading.edit_text(text[:4000])
        await m.answer(text[4000:], reply_markup=kb_summary_actions(issue_key))
    else:
        await loading.edit_text(text, reply_markup=kb_summary_actions(issue_key))

# =============================================================================
# Callback Handlers
# =============================================================================
@router.callback_query()
async def handle_callback(c: CallbackQuery):
    """Handle all callback queries."""
    data = c.data or ""
    
    # Checklist item check
    if data.startswith("chk:"):
        await handle_check_callback(c)
        return
    
    # Settings callbacks
    if data.startswith("st:"):
        await handle_settings_callback(c)
        return
    
    # Summary action callbacks
    if data.startswith("sum:"):
        await handle_summary_callback(c)
        return

    await c.answer()


async def handle_check_callback(c: CallbackQuery):
    """Handle checklist item check."""
    parts = c.data.split(":")
    if len(parts) < 4:
        await c.answer("❌ Ошибка", show_alert=True)
        return

    _, issue_key, item_id, item_num = parts[:4]
    tg_id = c.from_user.id

    sc, data = await api_request("POST", "/tracker/checklist/check", {
        "tg": tg_id, "issue": issue_key, "item": item_id, "checked": True
    })
    
    if sc != 200:
        await c.answer(f"❌ {data.get('error', 'Ошибка')}"[:100], show_alert=True)
        return

    await c.answer("✅ Отмечено!")
    
    # Update message
    if c.message:
        text = (c.message.text or "").replace("⬜", "✅", 1)
        # Remove clicked button
        if c.message.reply_markup:
            kb = InlineKeyboardBuilder()
            for row in c.message.reply_markup.inline_keyboard:
                for btn in row:
                    if btn.callback_data != c.data:
                        kb.button(text=btn.text, callback_data=btn.callback_data)
            kb.adjust(4)
            new_markup = kb.as_markup() if kb.buttons else None
            try:
                await c.message.edit_text(text, reply_markup=new_markup)
            except Exception:
                pass


async def handle_summary_callback(c: CallbackQuery):
    """Handle summary action callbacks."""
    parts = c.data.split(":", 2)
    if len(parts) < 3:
        await c.answer("❌ Ошибка", show_alert=True)
        return
    
    _, action, issue_key = parts
    tg_id = c.from_user.id
    
    if action == "refresh":
        # Clear cache and regenerate
        state.summary_cache.data.pop(issue_key, None)
        await c.answer("🔄 Обновляю...")
        
        # Send loading message as reply (more reliable than edit)
        loading_msg = None
        try:
            if c.message:
                loading_msg = await c.message.reply(f"🤖 Генерирую резюме для {issue_key}...")
        except Exception as e:
            logger.error(f"Failed to send loading message: {e}")
        
        sc, data = await api_request("GET", f"/tracker/issue/{issue_key}/summary", {"tg": tg_id}, HTTP_TIMEOUT_LONG)
        logger.info(f"Summary refresh for {issue_key}: status={sc}")
        
        if sc != 200:
            error_map = {401: "Ошибка авторизации. /connect", 404: f"{issue_key} не найден"}
            err_detail = data.get('error', f'Ошибка {sc}') if isinstance(data, dict) else f'Ошибка {sc}'
            error_text = f"❌ {error_map.get(sc, err_detail)}"[:500]
            try:
                if loading_msg:
                    await loading_msg.edit_text(error_text, reply_markup=kb_summary_actions(issue_key))
                elif c.message:
                    await c.message.reply(error_text, reply_markup=kb_summary_actions(issue_key))
            except Exception as e:
                logger.error(f"Failed to send error: {e}")
            return
        
        summary = data.get("summary", "")
        url = data.get("issue_url", f"https://tracker.yandex.ru/{issue_key}")
        
        if summary:
            state.summary_cache.set(issue_key, {"summary": summary, "url": url})
            text = f"📋 {issue_key}:\n\n{summary}\n\n🔗 {url}"
            try:
                if loading_msg:
                    await loading_msg.edit_text(text[:4000], reply_markup=kb_summary_actions(issue_key))
                elif c.message:
                    await c.message.reply(text[:4000], reply_markup=kb_summary_actions(issue_key))
            except Exception as e:
                logger.error(f"Failed to send summary: {e}")
        else:
            try:
                msg = "❌ Не удалось сгенерировать резюме"
                if loading_msg:
                    await loading_msg.edit_text(msg, reply_markup=kb_summary_actions(issue_key))
                elif c.message:
                    await c.message.reply(msg, reply_markup=kb_summary_actions(issue_key))
            except Exception as e:
                logger.error(f"Failed to send error: {e}")
        return
    
    if action == "comment":
        # Set pending comment state
        state.pending_comment[tg_id] = issue_key
        await c.answer()
        if c.message:
            kb = InlineKeyboardBuilder()
            kb.button(text="❌ Отмена", callback_data=f"sum:cancel_comment:{issue_key}")
            await c.message.reply(
                f"💬 Напишите комментарий для *{issue_key}*:\n\n_Отправьте текст следующим сообщением_",
                parse_mode="Markdown",
                reply_markup=kb.as_markup()
            )
        return
    
    if action == "cancel_comment":
        state.pending_comment.pop(tg_id, None)
        await c.answer("Отменено")
        if c.message:
            await c.message.delete()
        return
    
    if action == "checklist":
        # Get checklists for specific issue
        await c.answer("📋 Загружаю...")
        
        sc, data = await api_request("GET", f"/tracker/issue/{issue_key}/checklist", {"tg": tg_id})
        if sc != 200:
            error_msg = data.get("error", f"Ошибка {sc}") if isinstance(data, dict) else f"Ошибка {sc}"
            await c.answer(f"❌ {error_msg}"[:100], show_alert=True)
            return
        
        items = data.get("checklist_items") or []
        
        if not items:
            await c.answer("📋 Нет чеклистов в этой задаче", show_alert=True)
            return
        
        lines = [f"📋 *{issue_key}* — чеклисты:\n"]
        kb = InlineKeyboardBuilder()
        
        for idx, item in enumerate(items[:15], 1):
            mark = "✅" if item.get("checked") else "⬜"
            text = (item.get("text") or "")[:40]
            
            # Get assignee display name
            assignee = item.get("assignee") or {}
            assignee_name = assignee.get("display") or assignee.get("login") or ""
            if assignee_name:
                lines.append(f"{mark} {text} — _{assignee_name}_")
            else:
                lines.append(f"{mark} {text}")
            
            if not item.get("checked"):
                item_id = item.get("id")
                if item_id:
                    kb.button(text=f"✓{idx}", callback_data=f"chk:{issue_key}:{item_id}:{idx}")
        
        kb.adjust(5)
        
        try:
            if c.message:
                await c.message.reply(
                    "\n".join(lines),
                    parse_mode="Markdown",
                    reply_markup=kb.as_markup() if kb.buttons else None
                )
        except Exception as e:
            logger.error(f"Failed to send checklist: {e}")
        await c.answer()
        return
    
    await c.answer()


async def handle_settings_callback(c: CallbackQuery):
    """Handle settings callbacks."""
    if not BASE_URL:
        await c.answer("❌ BASE_URL не задан", show_alert=True)
        return
    
    tg_id = c.from_user.id
    parts = c.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""
    arg = parts[2] if len(parts) > 2 else ""
    
    # Get current settings
    sc, data = await api_request("GET", "/tg/settings", {"tg": tg_id})
    if sc != 200:
        await c.answer(f"❌ Ошибка {sc}", show_alert=True)
        return
    
    queues = data.get("queues") or []
    days = int(data.get("days", 30))
    limit = int(data.get("limit", 10))
    reminder = int(data.get("reminder", 0))

    if action == "close":
        if c.message:
            await c.message.edit_reply_markup(reply_markup=None)
        await c.answer()
        return

    if action == "back":
        if c.message:
            await c.message.edit_text(render_settings_text(queues, days, limit, reminder), reply_markup=kb_settings_main())
        await c.answer()
        return

    if action == "queues":
        if c.message:
            await c.message.edit_text("Очереди:", reply_markup=kb_settings_queues(queues))
        await c.answer()
        return

    if action == "days":
        if c.message:
            await c.message.edit_text("Период:", reply_markup=kb_settings_days(days))
        await c.answer()
        return
    
    if action == "limit":
        if c.message:
            await c.message.edit_text("Лимит:", reply_markup=kb_settings_limit(limit))
        await c.answer()
        return
    
    if action == "reminder":
        if c.message:
            await c.message.edit_text(
                "🔔 Напоминание (09:00-19:00):\n\n"
                "Бот будет присылать список:\n"
                "• Неотмеченные пункты чеклистов\n"
                "• Упоминания без ответа",
                reply_markup=kb_settings_reminder(reminder)
            )
        await c.answer()
        return

    if action == "qtoggle":
        q = arg.upper()
        qs = [x.upper() for x in queues]
        qs = [x for x in qs if x != q] if q in qs else qs + [q]
        sc2, data2 = await api_request("POST", "/tg/settings/queues", {"tg": tg_id, "queues": ",".join(qs)})
        if sc2 == 200 and c.message:
            await c.message.edit_reply_markup(reply_markup=kb_settings_queues(data2.get("queues", [])))
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "dset":
        try:
            d = int(arg)
        except ValueError:
            await c.answer("❌ Ошибка", show_alert=True)
            return
        sc2, data2 = await api_request("POST", "/tg/settings/days", {"tg": tg_id, "days": d})
        if sc2 == 200 and c.message:
            await c.message.edit_reply_markup(reply_markup=kb_settings_days(int(data2.get("days", d))))
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "lset":
        try:
            n = int(arg)
        except ValueError:
            await c.answer("❌ Ошибка", show_alert=True)
            return
        sc2, data2 = await api_request("POST", "/tg/settings/limit", {"tg": tg_id, "limit": n})
        if sc2 == 200 and c.message:
            await c.message.edit_reply_markup(reply_markup=kb_settings_limit(int(data2.get("limit", n))))
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "rset":
        try:
            h = int(arg)
        except ValueError:
            await c.answer("❌ Ошибка", show_alert=True)
            return
        sc2, data2 = await api_request("POST", "/tg/settings/reminder", {"tg": tg_id, "hours": h})
        if sc2 == 200 and c.message:
            await c.message.edit_reply_markup(reply_markup=kb_settings_reminder(int(data2.get("reminder", h))))
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    await c.answer()


# =============================================================================
# Text Message Handler (for comments)
# =============================================================================
@router.message(F.text & ~F.text.startswith("/"))
async def handle_text_message(m: Message):
    """Handle plain text messages (for comments), excluding commands."""
    if not m.text or not m.from_user:
        return
    
    tg_id = m.from_user.id
    
    # Check if user is awaiting comment input
    issue_key = state.pending_comment.get(tg_id)
    if not issue_key:
        # No pending comment, ignore
        return
    
    # Clear pending state
    state.pending_comment.pop(tg_id, None)
    
    comment_text = m.text.strip()
    if not comment_text:
        await m.answer("❌ Комментарий не может быть пустым")
        return
    
    # Send comment
    loading = await m.answer("💬 Отправляю комментарий...")
    
    sc, data = await api_request("POST", f"/tracker/issue/{issue_key}/comment", {
        "tg": tg_id,
        "text": comment_text
    })
    
    if sc != 200:
        error_msg = data.get("error", "Ошибка отправки")
        await loading.edit_text(f"❌ {error_msg}"[:200])
        return
    
    await loading.edit_text(f"✅ Комментарий добавлен к *{issue_key}*", parse_mode="Markdown")


# =============================================================================
# Bot Setup and Run
# =============================================================================
async def setup_bot_commands(bot: Bot):
    """Set up bot commands menu."""
    await bot.set_my_commands([
        BotCommand(command="menu", description="📋 Меню"),
        BotCommand(command="connect", description="🔗 Привязать аккаунт"),
        BotCommand(command="me", description="👤 Проверить доступ"),
        BotCommand(command="settings", description="⚙️ Настройки"),
        BotCommand(command="cl_my", description="✅ Мои чеклисты"),
        BotCommand(command="cl_my_open", description="❔ Ожидают согласование"),
        BotCommand(command="done", description="✔️ Отметить пункт"),
        BotCommand(command="mentions", description="📣 Требующие ответа"),
        BotCommand(command="summary", description="🤖 Резюме (ИИ)"),
    ])


async def run_bot():
    """Run bot polling."""
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set")
    
    bot = Bot(token=BOT_TOKEN)
    state.bot = bot
    await setup_bot_commands(bot)
    
    # Clear old updates
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(1)
    except Exception as e:
        logger.warning(f"Could not delete webhook: {e}")
    
    # Reuse existing dispatcher or create new one
    # This prevents "Router is already attached" error on restart
    if state.dispatcher is None:
        dp = Dispatcher()
        dp.include_router(router)
        state.dispatcher = dp
    else:
        dp = state.dispatcher
    
    # Start polling with retry
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await dp.start_polling(
                bot,
                close_bot_session=False,
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True
            )
            break
        except Exception as e:
            if "Conflict" in str(e) and attempt < max_retries - 1:
                logger.info(f"Conflict, retry {attempt + 1}/{max_retries}")
                await asyncio.sleep(5 * (attempt + 1))
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                except Exception:
                    pass
            else:
                raise


async def run_web():
    """Run web server."""
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def keep_alive():
    """Keep service alive on Render (pings every 10 min)."""
    await asyncio.sleep(10)  # Wait for server start
    
    while not state.shutdown_event.is_set():
        try:
            client = await get_http_client()
            await client.get(f"http://localhost:{PORT}/ping")
        except Exception:
            pass
        
        try:
            await asyncio.wait_for(state.shutdown_event.wait(), timeout=KEEP_ALIVE_INTERVAL)
            break
        except asyncio.TimeoutError:
            continue


# Track last reminder time per user
_last_reminder: Dict[int, float] = {}


async def reminder_worker():
    """Send periodic reminders to users with reminder enabled (09:00-19:00)."""
    await asyncio.sleep(60)  # Wait for services to start
    
    while not state.shutdown_event.is_set():
        try:
            # Check if we're in working hours (09:00-19:00)
            now = datetime.now()
            if not (9 <= now.hour < 19):
                # Outside working hours, wait and check again
                await asyncio.sleep(300)  # 5 min
                continue
            
            if not state.bot or not BASE_URL:
                await asyncio.sleep(60)
                continue
            
            # Get users with reminder enabled
            sc, data = await api_request("GET", "/tg/users_with_reminder", {})
            if sc != 200 or not data.get("users"):
                await asyncio.sleep(300)
                continue
            
            for user in data["users"]:
                if state.shutdown_event.is_set():
                    break
                
                tg_id = user.get("tg_id")
                reminder_hours = user.get("reminder_hours", 0)
                
                if not tg_id or reminder_hours <= 0:
                    continue
                
                # Check if enough time passed since last reminder
                last_time = _last_reminder.get(tg_id, 0)
                if time.time() - last_time < reminder_hours * 3600:
                    continue
                
                # Build reminder message
                lines = ["🔔 *Напоминание:*\n"]
                has_items = False
                
                # Get unchecked checklists
                try:
                    sc1, data1 = await api_request("GET", "/tracker/checklist/assigned_unchecked", {"tg": tg_id, "limit": 5}, HTTP_TIMEOUT_LONG)
                    if sc1 == 200:
                        issues = data1.get("issues", [])
                        if issues:
                            has_items = True
                            lines.append("❔ *Ожидают согласование:*")
                            for issue in issues[:3]:
                                lines.append(f"• {issue.get('key')} — {issue.get('summary')[:50]}")
                            if len(issues) > 3:
                                lines.append(f"...и ещё {len(issues) - 3}")
                            lines.append("")
                except Exception:
                    pass
                
                # Get mentions without response
                try:
                    sc2, data2 = await api_request("GET", "/tracker/summons", {"tg": tg_id, "limit": 5}, HTTP_TIMEOUT_LONG)
                    if sc2 == 200:
                        issues = [i for i in data2.get("issues", []) if not i.get("has_responded")]
                        if issues:
                            has_items = True
                            lines.append("📣 *Требуют ответа:*")
                            for issue in issues[:3]:
                                lines.append(f"• {issue.get('key')} — {issue.get('summary')[:50]}")
                            if len(issues) > 3:
                                lines.append(f"...и ещё {len(issues) - 3}")
                except Exception:
                    pass
                
                # Send reminder only if there are items
                if has_items:
                    try:
                        text = "\n".join(lines)
                        await state.bot.send_message(tg_id, text, parse_mode="Markdown")
                        _last_reminder[tg_id] = time.time()
                        logger.info(f"Sent reminder to {tg_id}")
                    except Exception as e:
                        logger.warning(f"Failed to send reminder to {tg_id}: {e}")
                
                # Small delay between users
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Reminder worker error: {e}")
        
        # Wait before next check cycle
        try:
            await asyncio.wait_for(state.shutdown_event.wait(), timeout=300)  # 5 min
            break
        except asyncio.TimeoutError:
            continue


async def shutdown():
    """Graceful shutdown."""
    logger.info("Shutting down...")
    state.shutdown_event.set()
    
    if state.bot:
        try:
            await state.bot.delete_webhook(drop_pending_updates=True)
            await state.bot.session.close()
        except Exception as e:
            logger.warning(f"Bot shutdown error: {e}")
    
    await close_http_client()
    logger.info("Shutdown complete")


def setup_signals():
    """Setup signal handlers."""
    def handler(sig, frame):
        logger.info(f"Signal {sig} received")
        asyncio.create_task(shutdown())
    
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


async def main():
    """Main entry point."""
    setup_signals()
    logger.info("Starting services...")
    
    tasks = {
        "web": asyncio.create_task(run_web()),
        "bot": asyncio.create_task(run_bot()),
        "keepalive": asyncio.create_task(keep_alive()),
        "reminder": asyncio.create_task(reminder_worker()),
    }
    
    try:
        while not state.shutdown_event.is_set():
            await asyncio.sleep(5)
            
            # Restart failed tasks
            for name, task in list(tasks.items()):
                if task.done() and not state.shutdown_event.is_set():
                    exc = task.exception()
                    if exc:
                        logger.error(f"{name} failed: {exc}")
                    logger.info(f"Restarting {name}...")
                    await asyncio.sleep(3)
                    if name == "web":
                        tasks[name] = asyncio.create_task(run_web())
                    elif name == "bot":
                        tasks[name] = asyncio.create_task(run_bot())
                    elif name == "keepalive":
                        tasks[name] = asyncio.create_task(keep_alive())
                    elif name == "reminder":
                        tasks[name] = asyncio.create_task(reminder_worker())
    except asyncio.CancelledError:
        pass
    finally:
        # Cancel all tasks
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
