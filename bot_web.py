"""
Telegram Bot for Yandex Tracker integration.
Optimized for low-resource environments (1GB RAM).
"""
import asyncio
import logging
import re
import time
from collections import OrderedDict
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import uvicorn
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, CallbackQuery, BotCommand
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import settings
from http_client import get_client, close_client, get_timeout

# =============================================================================
# Logging Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
for name in ["aiogram", "aiogram.event", "aiogram.dispatcher", "aiogram.polling", "httpx"]:
    logging.getLogger(name).setLevel(logging.WARNING)


# =============================================================================
# LRU Cache with TTL (memory-efficient)
# =============================================================================
class TTLCache:
    """LRU cache with TTL and bounded size."""
    
    __slots__ = ('_cache', '_maxsize', '_ttl')
    
    def __init__(self, maxsize: int, ttl: int):
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
        self._cache.move_to_end(key)
        return value
    
    def set(self, key: str, value: Any) -> None:
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = (value, time.time())
        while len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)
    
    def pop(self, key: str, default: Any = None) -> Any:
        if key in self._cache:
            value, _ = self._cache.pop(key)
            return value
        return default
    
    def cleanup_expired(self) -> int:
        now = time.time()
        expired = [k for k, (_, ts) in self._cache.items() if now - ts > self._ttl]
        for k in expired:
            del self._cache[k]
        return len(expired)


# =============================================================================
# Pending State with TTL (bounded)
# =============================================================================
class PendingState:
    """Dict-like container with automatic expiration."""
    
    __slots__ = ('_data', '_max_age', '_maxsize')
    
    def __init__(self, max_age: int = 600, maxsize: int = 100):
        self._data: Dict[int, Tuple[Any, float]] = {}
        self._max_age = max_age
        self._maxsize = maxsize
    
    def __setitem__(self, key: int, value: Any) -> None:
        self._cleanup()
        self._data[key] = (value, time.time())
    
    def get(self, key: int, default: Any = None) -> Any:
        if key in self._data:
            value, ts = self._data[key]
            if time.time() - ts <= self._max_age:
                return value
            del self._data[key]
        return default
    
    def pop(self, key: int, default: Any = None) -> Any:
        if key in self._data:
            value, ts = self._data.pop(key)
            if time.time() - ts <= self._max_age:
                return value
        return default
    
    def _cleanup(self) -> None:
        if len(self._data) > self._maxsize // 2:
            now = time.time()
            expired = [k for k, (_, ts) in self._data.items() if now - ts > self._max_age]
            for k in expired:
                del self._data[k]


# =============================================================================
# Application State
# =============================================================================
class AppState:
    """Application state container."""
    
    __slots__ = (
        'bot', 'dispatcher', 'shutdown_event',
        'checklist_cache', 'summary_cache',
        'pending_comment', 'pending_summary', 'pending_ai_search', 
        'pending_new_issue', 'last_reminder'
    )
    
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dispatcher: Optional[Dispatcher] = None
        self.shutdown_event = asyncio.Event()
        
        # Caches with config-based sizes
        self.checklist_cache = TTLCache(
            maxsize=settings.cache.checklist_size,
            ttl=settings.cache.checklist_ttl
        )
        self.summary_cache = TTLCache(
            maxsize=settings.cache.summary_size,
            ttl=settings.cache.summary_ttl
        )
        self.pending_comment = PendingState(max_age=settings.cache.pending_state_ttl)
        self.pending_summary = PendingState(max_age=settings.cache.pending_state_ttl)
        self.pending_ai_search = PendingState(max_age=settings.cache.pending_state_ttl)
        self.pending_new_issue: Dict[int, dict] = {}  # tg_id -> issue draft
        self.last_reminder: Dict[int, float] = {}


state = AppState()


# =============================================================================
# Import FastAPI app from main.py
# =============================================================================
from main import app

router = Router(name="main_router")


# Override root endpoint
@app.get("/", include_in_schema=False)
async def root_with_bot_status():
    return {
        "status": "ok",
        "service": "tracker-bot",
        "version": "2.1.0",
        "bot_active": state.bot is not None
    }


@app.on_event("shutdown")
async def bot_app_shutdown():
    await close_client()


# =============================================================================
# Helpers
# =============================================================================
def fmt_item(item: dict, highlight_mine: bool = False) -> str:
    """
    Format checklist item.
    
    Args:
        item: Checklist item dict with text, checked, assignee, is_mine fields
        highlight_mine: If True, highlight user's own items with "👤 *Вы*"
    """
    mark = "✅" if item.get("checked") else "⬜"
    text = (item.get("text") or "").strip().replace("\n", " ")[:80]
    assignee = item.get("assignee") or {}
    name = assignee.get("display") or assignee.get("login") or ""
    
    if highlight_mine and item.get("is_mine", False):
        return f"{mark} {text} — 👤 *Вы*"
    elif name:
        suffix = f"_{name}_" if not highlight_mine else name
        return f"{mark} {text} — {suffix}"
    return f"{mark} {text}"


def fmt_date(date_str: Optional[str]) -> str:
    """Format ISO date to DD.MM.YYYY HH:MM."""
    if not date_str:
        return ""
    try:
        clean = date_str.replace("Z", "+00:00")
        if "+" in clean and ":" not in clean.split("+")[-1]:
            parts = clean.rsplit("+", 1)
            if len(parts[1]) == 4:
                clean = f"{parts[0]}+{parts[1][:2]}:{parts[1][2:]}"
        return datetime.fromisoformat(clean).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return date_str[:16] if len(date_str) > 16 else date_str


def normalize_issue_key(text: str) -> Optional[str]:
    """
    Normalize issue key from various formats.
    
    Examples:
        inv123 → INV-123
        INV123 → INV-123
        doc 123 → DOC-123
        DOC 123 → DOC-123
        inv-123 → INV-123
        INV-123 → INV-123
    """
    text = text.strip().upper()
    # Remove extra spaces, hyphens, underscores
    text = re.sub(r'[\s\-_]+', '', text)
    # Match: letters followed by digits
    match = re.match(r'^([A-ZА-ЯЁ]+)(\d+)$', text)
    if match:
        queue, number = match.groups()
        return f"{queue}-{number}"
    return None


def escape_md(text: str) -> str:
    """Escape special characters for Telegram Markdown link text."""
    return text.replace("[", "(").replace("]", ")")


def fmt_issue_link(issue: dict, prefix: str = "", show_date: bool = True) -> str:
    """Format issue as Markdown hyperlink."""
    key = issue.get("key", "")
    summary = escape_md((issue.get("summary") or "")[:55])
    url = issue.get("url") or f"https://tracker.yandex.ru/{key}"
    date_str = fmt_date(issue.get("updatedAt")) if show_date else ""
    
    link = f"{prefix}[{key}: {summary}]({url})"
    return f"{link} ({date_str})" if date_str else link


def parse_response(r) -> dict:
    """Parse HTTP response to dict."""
    if "application/json" in r.headers.get("content-type", ""):
        try:
            return r.json()
        except Exception:
            pass
    return {"raw": r.text[:500] if r.text else ""}


async def api_request(
    method: str,
    path: str,
    params: dict,
    long_timeout: bool = False
) -> Tuple[int, dict]:
    """Make API request with error handling."""
    client = await get_client()
    timeout = get_timeout(long=long_timeout)
    url = f"{settings.base_url}{path}"
    
    try:
        if method == "GET":
            r = await client.get(url, params=params, timeout=timeout)
        else:
            r = await client.post(url, params=params, timeout=timeout)
        return r.status_code, parse_response(r)
    except asyncio.TimeoutError:
        return 504, {"error": "Timeout"}
    except Exception as e:
        return 500, {"error": str(e)[:200]}


def require_base_url(func):
    """Decorator to check BASE_URL."""
    @wraps(func)
    async def wrapper(m: Message, *args, **kwargs):
        try:
            if not settings.base_url:
                await m.answer("❌ BASE_URL не задан")
                return
            return await func(m, *args, **kwargs)
        except Exception as e:
            logger.error(f"Handler {func.__name__} error: {type(e).__name__}: {e}")
            try:
                await m.answer(f"❌ Ошибка: {type(e).__name__}")
            except Exception:
                pass
    return wrapper


# =============================================================================
# Keyboards
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
    for q in ["INV", "DOC", "HR", "BB", "KOMDEP", "FINANCE", "BDEV"]:
        kb.button(text=f"{'✅' if q in qs else '⬜'} {q}", callback_data=f"st:qtoggle:{q}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(4, 4, 1)  # 4 + 3 queues + back button
    return kb.as_markup()


# New issue keyboards
QUEUES_LIST = ["INV", "DOC", "HR", "BB", "KOMDEP", "FINANCE", "BDEV"]


def kb_new_issue_queue() -> InlineKeyboardMarkup:
    """Queue selection keyboard for new issue."""
    kb = InlineKeyboardBuilder()
    for q in QUEUES_LIST:
        kb.button(text=q, callback_data=f"new:queue:{q}")
    kb.button(text="❌ Отмена", callback_data="new:cancel")
    kb.adjust(4, 3, 1)
    return kb.as_markup()


def kb_new_issue_assignee() -> InlineKeyboardMarkup:
    """Assignee selection keyboard."""
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 На себя", callback_data="new:assignee:me")
    kb.button(text="🔍 Ввести логин", callback_data="new:assignee:input")
    kb.button(text="⏭ Пропустить", callback_data="new:assignee:skip")
    kb.adjust(2, 1)
    return kb.as_markup()


def kb_new_issue_confirm(draft: dict) -> InlineKeyboardMarkup:
    """Confirmation keyboard for new issue."""
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Создать", callback_data="new:confirm")
    kb.button(text="✏️ Изменить", callback_data="new:edit")
    kb.button(text="❌ Отмена", callback_data="new:cancel")
    kb.adjust(2, 1)
    return kb.as_markup()


def kb_new_issue_edit() -> InlineKeyboardMarkup:
    """Edit fields keyboard."""
    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Очередь", callback_data="new:edit:queue")
    kb.button(text="📋 Название", callback_data="new:edit:summary")
    kb.button(text="📄 Описание", callback_data="new:edit:description")
    kb.button(text="👤 Исполнитель", callback_data="new:edit:assignee")
    kb.button(text="📣 Нужен ответ от", callback_data="new:edit:pending")
    kb.button(text="⬅️ Назад", callback_data="new:back")
    kb.adjust(3, 2, 1)
    return kb.as_markup()


def render_new_issue_draft(draft: dict) -> str:
    """Render issue draft for display."""
    lines = ["✅ Подтвердите создание:\n"]
    lines.append(f"📝 Очередь: {draft.get('queue', '—')}")
    lines.append(f"📋 Название: {draft.get('summary', '—')}")
    
    desc = draft.get('description', '')
    if desc:
        lines.append(f"📄 Описание: {desc[:100]}{'...' if len(desc) > 100 else ''}")
    else:
        lines.append("📄 Описание: —")
    
    lines.append(f"👤 Исполнитель: {draft.get('assignee', '—') or '—'}")
    lines.append(f"📣 Нужен ответ от: {draft.get('pending_reply_from', '—') or '—'}")
    lines.append("👁 Наблюдатели: вы")
    
    return "\n".join(lines)


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
    for val, label in [(0, "Откл"), (1, "1ч"), (3, "3ч"), (6, "6ч")]:
        kb.button(text=f"{'✅' if reminder == val else '⬜'} {label}", callback_data=f"st:rset:{val}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(4, 1)
    return kb.as_markup()


def kb_summary_actions(issue_key: str, extended: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data=f"sum:refresh:{issue_key}")
    if not extended:
        kb.button(text="📋 Подробнее", callback_data=f"sum:extended:{issue_key}")
    kb.button(text="💬 Комментарий", callback_data=f"sum:comment:{issue_key}")
    kb.adjust(3)
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
    add_buttons: bool = False,
    show_all_items: bool = False,
    add_comment_buttons: bool = False
) -> Tuple[str, Optional[InlineKeyboardMarkup], Dict[int, Tuple[str, str]]]:
    """Build checklist text, keyboard, and item mapping."""
    lines = [header]
    kb = InlineKeyboardBuilder() if add_buttons else None
    item_mapping: Dict[int, Tuple[str, str]] = {}
    item_num = 1
    
    for idx, issue in enumerate(issues, 1):
        lines.append(f"\n{idx}. {fmt_issue_link(issue)}")
        
        if show_all_items and issue.get("all_items"):
            lines.append("   📋 *Все пункты чеклиста:*")
            for item in issue.get("all_items", []):
                is_checked = item.get("checked", False)
                is_mine = item.get("is_mine", False)
                lines.append(f"   {fmt_item(item, highlight_mine=True)}")
                if is_mine and not is_checked:
                    item_mapping[item_num] = (issue.get("key"), item.get("id"))
                    if kb:
                        kb.button(
                            text=f"✅ {item_num}",
                            callback_data=f"chk:{issue.get('key')}:{item.get('id')}:{item_num}"
                        )
                    item_num += 1
            # Add comment button for each issue
            if kb and add_comment_buttons:
                kb.button(
                    text=f"💬 {idx}",
                    callback_data=f"cmt:{issue.get('key')}"
                )
        else:
            for item in issue.get("items", []):
                is_checked = item.get("checked", False)
                if include_checked or not is_checked:
                    lines.append(f"  {fmt_item(item)}")
                    item_mapping[item_num] = (issue.get("key"), item.get("id"))
                    if kb and not is_checked:
                        kb.button(
                            text=f"✅ {item_num}",
                            callback_data=f"chk:{issue.get('key')}:{item.get('id')}:{item_num}"
                        )
                    item_num += 1
    
    if kb:
        kb.adjust(5)
    
    if add_comment_buttons and issues:
        lines.append("\n_💬 N — комментировать задачу N_")
    
    return "\n".join(lines), kb.as_markup() if kb else None, item_mapping


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
        "✅ /cl\\_my — задачи с моим ОК\n"
        "❓ /cl\\_my\\_open — ожидают согласование\n\n"
        "📣 /mentions — требующие ответа\n"
        "🤖 /summary ISSUE — резюме (ИИ)\n"
        "🔍 /ai ЗАПРОС — поиск (ИИ)\n"
        "📝 /new — создать задачу",
        parse_mode="Markdown"
    )


@router.message(Command("connect"))
@require_base_url
async def cmd_connect(m: Message):
    url = f"{settings.base_url}/oauth/start?tg={m.from_user.id}"
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
    user_settings = await get_settings(m.from_user.id)
    if not user_settings:
        await m.answer("❌ Не удалось получить настройки")
        return
    queues, days, limit, reminder = user_settings
    await m.answer(render_settings_text(queues, days, limit, reminder), reply_markup=kb_settings_main())


@router.message(Command("cl_my"))
@require_base_url
async def cmd_cl_my(m: Message):
    tg_id = m.from_user.id
    user_settings = await get_settings(tg_id)
    limit = user_settings[2] if user_settings else 10
    
    sc, data = await api_request(
        "GET", "/tracker/checklist/assigned",
        {"tg": tg_id, "limit": limit},
        long_timeout=True
    )
    if sc != 200:
        await m.answer(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    if not issues:
        days = data.get("settings", {}).get("days", 30)
        await m.answer(f"Нет задач за {days} дней")
        return
    
    text, _, item_mapping = build_checklist_response(issues, "✅ *Задачи с моим ОК:*")
    state.checklist_cache.set(f"cl:{tg_id}", item_mapping)
    
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        await m.answer(chunk, parse_mode="Markdown")


@router.message(Command("mentions"))
@require_base_url
async def cmd_mentions(m: Message):
    """Get issues where user was mentioned."""
    tg_id = m.from_user.id
    user_settings = await get_settings(tg_id)
    limit = user_settings[2] if user_settings else 10
    
    sc, data = await api_request(
        "GET", "/tracker/summons",
        {"tg": tg_id, "limit": limit},
        long_timeout=True
    )
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
    user_settings = await get_settings(tg_id)
    limit = user_settings[2] if user_settings else 10
    
    sc, data = await api_request(
        "GET", "/tracker/checklist/assigned_unchecked",
        {"tg": tg_id, "limit": limit},
        long_timeout=True
    )
    if sc != 200:
        await m.answer(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    if not issues:
        days = data.get("settings", {}).get("days", 30)
        await m.answer(f"Нет пунктов за {days} дней")
        return
    
    text, keyboard, item_mapping = build_checklist_response(
        issues, "❓ *Ожидают согласование:*",
        include_checked=False, add_buttons=True, show_all_items=True,
        add_comment_buttons=True
    )
    state.checklist_cache.set(f"cl:{tg_id}", item_mapping)
    
    if len(text) > 4000:
        await m.answer(text[:4000], reply_markup=keyboard, parse_mode="Markdown")
        await m.answer(text[4000:], parse_mode="Markdown")
    else:
        await m.answer(text, reply_markup=keyboard, parse_mode="Markdown")


# TODO: временно отключено
# @router.message(Command("done"))
# @require_base_url
# async def cmd_done(m: Message):
#     """Mark checklist item by number."""
#     parts = (m.text or "").split()
#     if len(parts) != 2:
#         await m.answer("Использование: /done N")
#         return
#     
#     try:
#         num = int(parts[1])
#     except ValueError:
#         await m.answer("❌ N должно быть числом")
#         return
#
#     tg_id = m.from_user.id
#     item_mapping = state.checklist_cache.get(f"cl:{tg_id}")
#     if not item_mapping or num not in item_mapping:
#         await m.answer("❌ Сначала выполните /cl_my или /cl_my_open")
#         return
#
#     issue_key, item_id = item_mapping[num]
#     sc, data = await api_request("POST", "/tracker/checklist/check", {
#         "tg": tg_id, "issue": issue_key, "item": item_id, "checked": True
#     })
#     
#     if sc == 200:
#         await m.answer(f"✅ Отмечен: {issue_key} #{num}")
#     else:
#         await m.answer(f"❌ Ошибка: {data.get('error', data)}"[:200])


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


async def process_summary(m: Message, issue_key: str, tg_id: int):
    """Process summary request for an issue."""
    # Check cache
    cached = state.summary_cache.get(issue_key)
    if cached:
        await m.answer(
            f"📋 {issue_key} (кэш):\n\n{cached['summary']}\n\n🔗 {cached['url']}",
            reply_markup=kb_summary_actions(issue_key)
        )
        return
    
    loading = await m.answer("🤖 Генерирую резюме...")
    
    try:
        sc, data = await api_request(
            "GET", f"/tracker/issue/{issue_key}/summary",
            {"tg": tg_id},
            long_timeout=True
        )
    except Exception as e:
        await loading.edit_text(f"❌ Ошибка запроса: {str(e)[:100]}")
        return
    
    if sc != 200:
        error_msg = data.get('error', str(data)[:100]) if isinstance(data, dict) else str(data)[:100]
        error_map = {401: "Ошибка авторизации. /connect", 404: f"{issue_key} не найден"}
        await loading.edit_text(f"❌ {error_map.get(sc, error_msg)}"[:500])
        return
    
    summary = data.get("summary", "") if isinstance(data, dict) else ""
    url = data.get("issue_url", f"https://tracker.yandex.ru/{issue_key}") if isinstance(data, dict) else f"https://tracker.yandex.ru/{issue_key}"
    
    if not summary:
        err_detail = data.get("error", "Пустой ответ") if isinstance(data, dict) else "Неверный формат"
        await loading.edit_text(f"❌ Не удалось сгенерировать резюме: {err_detail}"[:500])
        return

    state.summary_cache.set(issue_key, {"summary": summary, "url": url})
    
    text = f"📋 {issue_key}:\n\n{summary}\n\n🔗 {url}"
    if len(text) > 4000:
        await loading.edit_text(text[:4000])
        await m.answer(text[4000:], reply_markup=kb_summary_actions(issue_key))
    else:
        await loading.edit_text(text, reply_markup=kb_summary_actions(issue_key))


@router.message(Command("summary"))
@require_base_url
async def cmd_summary(m: Message):
    """Generate AI summary for issue."""
    from aiogram.types import ForceReply
    
    parts = (m.text or "").split()
    if len(parts) < 2:
        state.pending_summary[m.from_user.id] = True
        await m.answer(
            "🤖 Введите ключ задачи (например: INV-123):",
            reply_markup=ForceReply(input_field_placeholder="INV-123")
        )
        return

    issue_key = normalize_issue_key(parts[1])
    if not issue_key:
        await m.answer("❌ Неверный формат. Примеры: INV-123, inv123, DOC 45")
        return
    await process_summary(m, issue_key, m.from_user.id)


@router.message(Command("ai"))
@require_base_url
async def cmd_ai_search(m: Message):
    """AI-powered search for issues."""
    from aiogram.types import ForceReply
    logger.info(f"cmd_ai_search called: tg={m.from_user.id}, text={m.text[:50] if m.text else ''}")
    
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        try:
            await m.answer(
                "🔍 AI-поиск по задачам\n\n"
                "Примеры запросов:\n"
                "• /ai мои задачи\n"
                "• /ai срочные баги\n"
                "• /ai просроченные\n"
                "• /ai мои согласования\n\n"
                "⚠️ Для чеклистов: /cl_my, /cl_my_open\n"
                "⚠️ Для призывов: /mentions",
                reply_markup=ForceReply(input_field_placeholder="Что ищем?")
            )
        except Exception as e:
            logger.error(f"cmd_ai_search answer error: {e}")
            await m.answer("🔍 Введите поисковый запрос после /ai")
        state.pending_ai_search[m.from_user.id] = True
        return

    query = parts[1].strip()
    await process_ai_search(m, query, m.from_user.id)


@router.message(Command("new"))
@require_base_url
async def cmd_new_issue(m: Message):
    """Start new issue creation dialog."""
    tg_id = m.from_user.id
    
    # Initialize draft
    state.pending_new_issue[tg_id] = {
        "step": "queue",
        "queue": "",
        "summary": "",
        "description": "",
        "assignee": "",
        "pending_reply_from": "",
        "message_id": None
    }
    
    msg = await m.answer(
        "📝 Создание задачи\n\nВыберите очередь:",
        reply_markup=kb_new_issue_queue()
    )
    state.pending_new_issue[tg_id]["message_id"] = msg.message_id


async def process_ai_search(m: Message, query: str, tg_id: int):
    """Process AI search request."""
    query_lower = query.lower()
    
    # Detect checklist/summons queries and show hint
    checklist_keywords = ["чеклист", "checklist", "пункт", "согласован"]
    summons_keywords = ["призвали", "призыв", "упомянули", "упоминани", "summon", "mention"]
    
    hint = ""
    if any(kw in query_lower for kw in checklist_keywords):
        hint = "\n\n💡 Для чеклистов лучше: /cl_my или /cl_my_open"
    elif any(kw in query_lower for kw in summons_keywords):
        hint = "\n\n💡 Для призывов лучше: /mentions"
    
    user_settings = await get_settings(tg_id)
    limit = user_settings[2] if user_settings else 10
    
    loading = await m.answer("🔍 Ищу..." + hint)
    logger.info(f"AI search: tg={tg_id}, query={query[:50]}")
    
    try:
        sc, data = await api_request(
            "GET", "/tracker/ai_search",
            {"tg": tg_id, "q": query, "limit": limit},
            long_timeout=True
        )
        logger.info(f"AI search result: sc={sc}, issues={len(data.get('issues', [])) if isinstance(data, dict) else 0}")
    except Exception as e:
        logger.error(f"AI search exception: {type(e).__name__}: {e}")
        await loading.edit_text(f"❌ Ошибка: {str(e)[:100]}")
        return
    
    if sc != 200:
        error_msg = data.get('error', str(data)[:100]) if isinstance(data, dict) else str(data)[:100]
        logger.warning(f"AI search failed: sc={sc}, error={error_msg}")
        await loading.edit_text(f"❌ {error_msg}"[:500])
        return
    
    # Handle redirects for checklist/summons
    if isinstance(data, dict) and data.get("redirect"):
        redirect = data["redirect"]
        if redirect == "checklist":
            await loading.edit_text(
                "📋 Для поиска по чеклистам используйте:\n"
                "• /cl_my — задачи с моим ОК\n"
                "• /cl_my_open — ожидают согласования"
            )
        elif redirect == "summons":
            await loading.edit_text(
                "📣 Для поиска призывов используйте:\n"
                "• /mentions — задачи где вас призвали"
            )
        else:
            await loading.edit_text(data.get("message", "Используйте специальную команду"))
        return
    
    issues = data.get("issues", []) if isinstance(data, dict) else []
    
    if not issues:
        await loading.edit_text("🔍 Ничего не найдено")
        return
    
    lines = [f"🔍 *Найдено {len(issues)} задач:*\n"]
    
    for idx, issue in enumerate(issues, 1):
        key = issue.get("key", "")
        summary = escape_md((issue.get("summary") or "")[:50])
        status = issue.get("status", "")
        description = (issue.get("description") or "")[:80]
        url = issue.get("url", f"https://tracker.yandex.ru/{key}")
        
        line = f"{idx}. [{key}: {summary}]({url})"
        if status:
            line += f" _{status}_"
        lines.append(line)
        
        if description:
            lines.append(f"   _{description}_")
    
    
    text = "\n".join(lines)
    
    if len(text) > 4000:
        await loading.edit_text(text[:4000], parse_mode="Markdown")
        await m.answer(text[4000:], parse_mode="Markdown")
    else:
        await loading.edit_text(text, parse_mode="Markdown")


# =============================================================================
# Callback Handlers
# =============================================================================
@router.callback_query()
async def handle_callback(c: CallbackQuery):
    """Handle all callback queries."""
    data = c.data or ""
    
    if data.startswith("chk:"):
        await handle_check_callback(c)
    elif data.startswith("cmt_cancel:"):
        # Cancel comment
        state.pending_comment.pop(c.from_user.id, None)
        await c.answer("Отменено")
        if c.message:
            try:
                await c.message.delete()
            except Exception:
                pass
    elif data.startswith("cmt:"):
        await handle_comment_callback(c)
    elif data.startswith("st:"):
        await handle_settings_callback(c)
    elif data.startswith("sum:"):
        await handle_summary_callback(c)
    elif data.startswith("new:"):
        await handle_new_issue_callback(c)
    else:
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
    
    if c.message:
        text = (c.message.text or "").replace("⬜", "✅", 1)
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


async def handle_comment_callback(c: CallbackQuery):
    """Handle comment button from checklist."""
    parts = c.data.split(":", 1)
    if len(parts) < 2:
        await c.answer("❌ Ошибка", show_alert=True)
        return
    
    _, issue_key = parts
    tg_id = c.from_user.id
    
    # Store pending comment
    state.pending_comment[tg_id] = issue_key
    await c.answer()
    
    if c.message:
        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Отмена", callback_data=f"cmt_cancel:{issue_key}")
        await c.message.reply(
            f"💬 Напишите комментарий для *{issue_key}*:\n\n_Отправьте текст следующим сообщением_",
            parse_mode="Markdown",
            reply_markup=kb.as_markup()
        )


async def handle_summary_callback(c: CallbackQuery):
    """Handle summary action callbacks."""
    parts = c.data.split(":", 2)
    if len(parts) < 3:
        await c.answer("❌ Ошибка", show_alert=True)
        return

    _, action, issue_key = parts
    tg_id = c.from_user.id

    if action == "refresh":
        state.summary_cache.pop(issue_key)
        await c.answer("🔄 Обновляю...")
        
        loading_msg = await c.message.reply(f"🤖 Генерирую резюме для {issue_key}...") if c.message else None
        
        try:
            sc, data = await api_request(
                "GET", f"/tracker/issue/{issue_key}/summary",
                {"tg": tg_id},
                long_timeout=True
            )
        except Exception as e:
            if loading_msg:
                await loading_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")
            return
        
        if sc != 200 or not isinstance(data, dict):
            err = data.get('error', f'Ошибка {sc}') if isinstance(data, dict) else f'Ошибка {sc}'
            if loading_msg:
                await loading_msg.edit_text(f"❌ {err}"[:300], reply_markup=kb_summary_actions(issue_key))
            return
        
        summary = data.get("summary", "")
        url = data.get("issue_url", f"https://tracker.yandex.ru/{issue_key}")
        
        if not summary:
            if loading_msg:
                await loading_msg.edit_text("❌ Пустой ответ от AI", reply_markup=kb_summary_actions(issue_key))
            return
        
        state.summary_cache.set(issue_key, {"summary": summary, "url": url})
        text = f"📋 {issue_key}:\n\n{summary}\n\n🔗 {url}"
        if loading_msg:
            await loading_msg.edit_text(text[:4000], reply_markup=kb_summary_actions(issue_key))
        return
    
    if action == "extended":
        await c.answer("📋 Генерирую расширенное резюме...")
        
        loading_msg = await c.message.reply(f"🤖 Генерирую расширенное резюме для {issue_key}...") if c.message else None
        
        try:
            sc, data = await api_request(
                "GET", f"/tracker/issue/{issue_key}/summary",
                {"tg": tg_id, "extended": "true"},
                long_timeout=True
            )
        except Exception as e:
            if loading_msg:
                await loading_msg.edit_text(f"❌ Ошибка: {str(e)[:100]}")
            return
        
        if sc != 200 or not isinstance(data, dict):
            err = data.get('error', f'Ошибка {sc}') if isinstance(data, dict) else f'Ошибка {sc}'
            if loading_msg:
                await loading_msg.edit_text(f"❌ {err}"[:300], reply_markup=kb_summary_actions(issue_key, extended=True))
            return
        
        summary = data.get("summary", "")
        url = data.get("issue_url", f"https://tracker.yandex.ru/{issue_key}")
        
        if not summary:
            if loading_msg:
                await loading_msg.edit_text("❌ Пустой ответ от AI", reply_markup=kb_summary_actions(issue_key, extended=True))
            return
        
        text = f"📋 {issue_key} (подробно):\n\n{summary}\n\n🔗 {url}"
        if loading_msg:
            await loading_msg.edit_text(text[:4000], reply_markup=kb_summary_actions(issue_key, extended=True))
        return
    
    if action == "comment":
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
            try:
                await c.message.delete()
            except Exception:
                pass
        return
    
    if action == "checklist":
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
            assignee = item.get("assignee") or {}
            name = assignee.get("display") or assignee.get("login") or ""
            if name:
                lines.append(f"{mark} {text} — _{name}_")
            else:
                lines.append(f"{mark} {text}")
            
            if not item.get("checked"):
                item_id = item.get("id")
                if item_id:
                    kb.button(text=f"✓{idx}", callback_data=f"chk:{issue_key}:{item_id}:{idx}")
        
        kb.adjust(5)
        
        if c.message:
            try:
                await c.message.reply(
                    "\n".join(lines),
                    parse_mode="Markdown",
                    reply_markup=kb.as_markup() if kb.buttons else None
                )
            except Exception:
                pass
        await c.answer()
        return

    await c.answer()


async def handle_new_issue_callback(c: CallbackQuery):
    """Handle new issue dialog callbacks."""
    tg_id = c.from_user.id
    data = c.data or ""
    parts = data.split(":")
    
    if len(parts) < 2:
        await c.answer("❌ Ошибка", show_alert=True)
        return
    
    action = parts[1]
    draft = state.pending_new_issue.get(tg_id, {})
    
    # Cancel action
    if action == "cancel":
        state.pending_new_issue.pop(tg_id, None)
        await c.answer("Отменено")
        if c.message:
            try:
                await c.message.delete()
            except Exception:
                pass
        return
    
    # Queue selection
    if action == "queue" and len(parts) >= 3:
        queue = parts[2].upper()
        draft["queue"] = queue
        draft["step"] = "summary"
        state.pending_new_issue[tg_id] = draft
        await c.answer()
        if c.message:
            from aiogram.types import ForceReply
            await c.message.edit_text(f"📝 Очередь: {queue}\n\nВведите название задачи:")
            await c.message.answer("📋 Название:", reply_markup=ForceReply(input_field_placeholder="Название задачи"))
        return
    
    # Assignee selection
    if action == "assignee" and len(parts) >= 3:
        choice = parts[2]
        if choice == "me":
            # Get user's tracker login
            sc, data_resp = await api_request("GET", "/tracker/user_by_tg", {"tg": tg_id})
            if sc == 200 and isinstance(data_resp, dict):
                login = data_resp.get("tracker_login", "")
                draft["assignee"] = login
            else:
                draft["assignee"] = ""
        elif choice == "skip":
            draft["assignee"] = ""
        elif choice == "input":
            draft["step"] = "assignee_input"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                from aiogram.types import ForceReply
                await c.message.edit_text("👤 Введите логин исполнителя:")
                await c.message.answer("Логин:", reply_markup=ForceReply(input_field_placeholder="login"))
            return
        
        draft["step"] = "pending_reply"
        state.pending_new_issue[tg_id] = draft
        await c.answer()
        if c.message:
            from aiogram.types import ForceReply
            assignee_text = f"@{draft['assignee']}" if draft.get("assignee") else "—"
            await c.message.edit_text(
                f"📝 Очередь: {draft.get('queue')}\n"
                f"📋 Название: {draft.get('summary')}\n"
                f"📄 Описание: {draft.get('description') or '—'}\n"
                f"👤 Исполнитель: {assignee_text}\n\n"
                f"Нужен ответ от? (введите логин или '-' чтобы пропустить):"
            )
            await c.message.answer("📣 Нужен ответ от:", reply_markup=ForceReply(input_field_placeholder="login или -"))
        return
    
    # Confirm creation
    if action == "confirm":
        await c.answer("⏳ Создаю...")
        
        # Get user's login for followers
        sc, user_data = await api_request("GET", "/tracker/user_by_tg", {"tg": tg_id})
        my_login = ""
        if sc == 200 and isinstance(user_data, dict):
            my_login = user_data.get("tracker_login", "")
        
        followers = [my_login] if my_login else []
        
        sc, result = await api_request(
            "POST", "/tracker/issue/create",
            {
                "tg": tg_id,
                "queue": draft.get("queue", ""),
                "summary": draft.get("summary", ""),
                "description": draft.get("description", ""),
                "assignee": draft.get("assignee", ""),
                "pending_reply_from": draft.get("pending_reply_from", ""),
                "followers": ",".join(followers)
            }
        )
        
        state.pending_new_issue.pop(tg_id, None)
        
        if sc not in (200, 201):
            error = result.get("error", "Ошибка создания") if isinstance(result, dict) else str(result)
            if c.message:
                await c.message.edit_text(f"❌ {error}"[:500])
            return
        
        issue_key = result.get("issue_key", "") if isinstance(result, dict) else ""
        issue_url = result.get("issue_url", f"https://tracker.yandex.ru/{issue_key}") if isinstance(result, dict) else ""
        
        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Резюме", callback_data=f"sum:refresh:{issue_key}")
        kb.button(text="💬 Комментарий", callback_data=f"sum:comment:{issue_key}")
        kb.adjust(2)
        
        if c.message:
            await c.message.edit_text(
                f"✅ Задача создана!\n\n"
                f"{issue_key}: {draft.get('summary', '')}\n"
                f"🔗 {issue_url}",
                reply_markup=kb.as_markup()
            )
        return
    
    # Edit menu
    if action == "edit":
        await c.answer()
        if c.message:
            await c.message.edit_text(
                "Что изменить?",
                reply_markup=kb_new_issue_edit()
            )
        return
    
    # Back to confirm
    if action == "back":
        draft["step"] = "confirm"
        state.pending_new_issue[tg_id] = draft
        await c.answer()
        if c.message:
            await c.message.edit_text(
                render_new_issue_draft(draft),
                reply_markup=kb_new_issue_confirm(draft)
            )
        return
    
    # Edit specific field
    if action == "edit" and len(parts) >= 3:
        field = parts[2]
        from aiogram.types import ForceReply
        
        if field == "queue":
            draft["step"] = "queue"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📝 Выберите очередь:", reply_markup=kb_new_issue_queue())
        elif field == "summary":
            draft["step"] = "summary"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📋 Введите название:")
                await c.message.answer("Название:", reply_markup=ForceReply(input_field_placeholder="Название"))
        elif field == "description":
            draft["step"] = "description"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📄 Введите описание:")
                await c.message.answer("Описание:", reply_markup=ForceReply(input_field_placeholder="Описание"))
        elif field == "assignee":
            draft["step"] = "assignee"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("👤 Назначить исполнителя?", reply_markup=kb_new_issue_assignee())
        elif field == "pending":
            draft["step"] = "pending_reply"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📣 Введите логин:")
                await c.message.answer("Нужен ответ от:", reply_markup=ForceReply(input_field_placeholder="login"))
        return
    
    await c.answer()


async def handle_settings_callback(c: CallbackQuery):
    """Handle settings callbacks."""
    if not settings.base_url:
        await c.answer("❌ BASE_URL не задан", show_alert=True)
        return
    
    tg_id = c.from_user.id
    parts = c.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""
    arg = parts[2] if len(parts) > 2 else ""
    
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
            await c.message.edit_text(
                render_settings_text(queues, days, limit, reminder),
                reply_markup=kb_settings_main()
            )
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
# Text Message Handler
# =============================================================================
@router.message(F.text)
async def handle_text_message(m: Message):
    """Handle plain text messages for pending inputs."""
    if not m.text or not m.from_user:
        return

    if m.text.startswith("/"):
        return

    tg_id = m.from_user.id
    text = m.text.strip()
    
    # Check if awaiting summary issue key
    if state.pending_summary.pop(tg_id, None):
        issue_key = normalize_issue_key(text)
        if not issue_key:
            await m.answer("❌ Неверный формат. Примеры: INV-123, inv123, DOC 45")
            return
        await process_summary(m, issue_key, tg_id)
        return
    
    # Check if awaiting AI search query
    if state.pending_ai_search.pop(tg_id, None):
        if len(text) < 2:
            await m.answer("❌ Слишком короткий запрос")
            return
        await process_ai_search(m, text, tg_id)
        return
    
    # Check if awaiting new issue input
    draft = state.pending_new_issue.get(tg_id)
    if draft:
        step = draft.get("step", "")
        
        if step == "summary":
            if len(text) < 3:
                await m.answer("❌ Название слишком короткое (минимум 3 символа)")
                return
            draft["summary"] = text[:500]
            draft["step"] = "description"
            state.pending_new_issue[tg_id] = draft
            from aiogram.types import ForceReply
            await m.answer(
                f"📝 Очередь: {draft.get('queue')}\n"
                f"📋 Название: {text[:50]}{'...' if len(text) > 50 else ''}\n\n"
                f"Введите описание (или '-' чтобы пропустить):",
                reply_markup=ForceReply(input_field_placeholder="Описание или -")
            )
            return
        
        if step == "description":
            draft["description"] = "" if text == "-" else text[:2000]
            draft["step"] = "assignee"
            state.pending_new_issue[tg_id] = draft
            await m.answer(
                f"📝 Очередь: {draft.get('queue')}\n"
                f"📋 Название: {draft.get('summary', '')[:50]}\n"
                f"📄 Описание: {(draft.get('description') or '—')[:50]}\n\n"
                f"Назначить исполнителя?",
                reply_markup=kb_new_issue_assignee()
            )
            return
        
        if step == "assignee_input":
            draft["assignee"] = text.strip().replace("@", "")
            draft["step"] = "pending_reply"
            state.pending_new_issue[tg_id] = draft
            from aiogram.types import ForceReply
            await m.answer(
                f"👤 Исполнитель: @{draft['assignee']}\n\n"
                f"Нужен ответ от? (введите логин или '-' чтобы пропустить):",
                reply_markup=ForceReply(input_field_placeholder="login или -")
            )
            return
        
        if step == "pending_reply":
            draft["pending_reply_from"] = "" if text == "-" else text.strip().replace("@", "")
            draft["step"] = "confirm"
            state.pending_new_issue[tg_id] = draft
            await m.answer(
                render_new_issue_draft(draft),
                reply_markup=kb_new_issue_confirm(draft)
            )
            return
        
        # If editing specific field, go back to confirm
        if step in ("edit_summary", "edit_description", "edit_assignee", "edit_pending"):
            field = step.replace("edit_", "")
            if field == "summary":
                draft["summary"] = text[:500]
            elif field == "description":
                draft["description"] = "" if text == "-" else text[:2000]
            elif field == "assignee":
                draft["assignee"] = text.strip().replace("@", "")
            elif field == "pending":
                draft["pending_reply_from"] = "" if text == "-" else text.strip().replace("@", "")
            
            draft["step"] = "confirm"
            state.pending_new_issue[tg_id] = draft
            await m.answer(
                render_new_issue_draft(draft),
                reply_markup=kb_new_issue_confirm(draft)
            )
            return
    
    # Check if awaiting comment input
    issue_key = state.pending_comment.pop(tg_id, None)
    if issue_key:
        if not text:
            await m.answer("❌ Комментарий не может быть пустым")
            return
        loading = await m.answer("💬 Отправляю комментарий...")
        sc, data = await api_request("POST", f"/tracker/issue/{issue_key}/comment", {"tg": tg_id, "text": text})
        if sc != 200:
            await loading.edit_text(f"❌ {data.get('error', 'Ошибка')}"[:200])
        else:
            await loading.edit_text(f"✅ Комментарий добавлен к *{issue_key}*", parse_mode="Markdown")
        return


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
        BotCommand(command="cl_my", description="✅ Задачи с моим ОК"),
        BotCommand(command="cl_my_open", description="❓ Ожидают согласование"),
        # BotCommand(command="done", description="✔️ Отметить пункт"),  # TODO: временно отключено
        BotCommand(command="mentions", description="📣 Требующие ответа"),
        BotCommand(command="summary", description="🤖 Резюме (ИИ)"),
        BotCommand(command="ai", description="🔍 Поиск (ИИ)"),
        BotCommand(command="new", description="📝 Создать задачу"),
    ])


async def run_bot():
    """Run bot polling."""
    if not settings.bot:
        raise RuntimeError("BOT_TOKEN not set")
    
    if state.bot:
        try:
            await state.bot.session.close()
        except Exception:
            pass

    bot = Bot(token=settings.bot.token)
    state.bot = bot
    await setup_bot_commands(bot)
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(2)
    except Exception:
        pass
    
    if state.dispatcher is None:
        dp = Dispatcher()
        dp.include_router(router)
        state.dispatcher = dp
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            await state.dispatcher.start_polling(
                bot,
                close_bot_session=False,
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True,
                polling_timeout=30
            )
            break
        except Exception as e:
            error_str = str(e)
            if ("Conflict" in error_str or "terminated" in error_str) and attempt < max_retries - 1:
                await asyncio.sleep(5 * (attempt + 1))
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                except Exception:
                    pass
            else:
                raise


async def run_web():
    """Run web server."""
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def keep_alive():
    """Keep service alive and cleanup caches."""
    await asyncio.sleep(10)
    
    interval = settings.bot.keep_alive_interval if settings.bot else 300
    
    while not state.shutdown_event.is_set():
        try:
            client = await get_client()
            await client.get(f"http://localhost:{settings.port}/ping")
        except Exception:
            pass
        
        # Cleanup expired cache entries
        state.checklist_cache.cleanup_expired()
        state.summary_cache.cleanup_expired()
        
        # Cleanup old reminder timestamps (keep only active users)
        if len(state.last_reminder) > 1000:
            cutoff = time.time() - 86400 * 7  # 7 days
            state.last_reminder = {
                k: v for k, v in state.last_reminder.items()
                if v > cutoff
            }
        
        try:
            await asyncio.wait_for(state.shutdown_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            continue


async def reminder_worker():
    """Send periodic reminders (09:00-19:00)."""
    await asyncio.sleep(60)
    
    interval = settings.bot.reminder_check_interval if settings.bot else 300
    
    while not state.shutdown_event.is_set():
        try:
            now = datetime.now()
            if not (9 <= now.hour < 19):
                await asyncio.sleep(300)
                continue
            
            if not state.bot or not settings.base_url:
                await asyncio.sleep(60)
                continue
            
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
                
                last_time = state.last_reminder.get(tg_id, 0)
                if time.time() - last_time < reminder_hours * 3600:
                    continue
                
                lines = ["🔔 *Напоминание:*\n"]
                has_items = False
                
                try:
                    sc1, data1 = await api_request(
                        "GET", "/tracker/checklist/assigned_unchecked",
                        {"tg": tg_id, "limit": 5},
                        long_timeout=True
                    )
                    if sc1 == 200:
                        issues = data1.get("issues", [])
                        if issues:
                            has_items = True
                            lines.append("❓ *Ожидают согласование:*")
                            for idx, issue in enumerate(issues[:3], 1):
                                lines.append(f"{idx}. {fmt_issue_link(issue, show_date=False)}")
                            if len(issues) > 3:
                                lines.append(f"_...и ещё {len(issues) - 3}_")
                            lines.append("")
                except Exception:
                    pass
                
                try:
                    sc2, data2 = await api_request(
                        "GET", "/tracker/summons",
                        {"tg": tg_id, "limit": 5},
                        long_timeout=True
                    )
                    if sc2 == 200:
                        issues = [i for i in data2.get("issues", []) if not i.get("has_responded")]
                        if issues:
                            has_items = True
                            lines.append("📣 *Требуют ответа:*")
                            for idx, issue in enumerate(issues[:3], 1):
                                lines.append(f"{idx}. ⏳ {fmt_issue_link(issue, show_date=False)}")
                            if len(issues) > 3:
                                lines.append(f"_...и ещё {len(issues) - 3}_")
                except Exception:
                    pass
                
                if has_items:
                    try:
                        await state.bot.send_message(tg_id, "\n".join(lines), parse_mode="Markdown")
                        state.last_reminder[tg_id] = time.time()
                    except Exception:
                        pass
                
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.debug(f"Reminder worker error: {e}")
        
        try:
            await asyncio.wait_for(state.shutdown_event.wait(), timeout=interval)
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
        except Exception:
            pass
    
    await close_client()
    logger.info("Shutdown complete")


async def main():
    """Main entry point."""
    tasks = {
        "web": asyncio.create_task(run_web()),
        "bot": asyncio.create_task(run_bot()),
        "keepalive": asyncio.create_task(keep_alive()),
        "reminder": asyncio.create_task(reminder_worker()),
    }
    
    logger.info(f"Starting tracker-bot on port {settings.port}")
    
    try:
        while not state.shutdown_event.is_set():
            await asyncio.sleep(5)
            
            for name, task in list(tasks.items()):
                if task.done() and not state.shutdown_event.is_set():
                    exc = task.exception() if not task.cancelled() else None
                    if exc:
                        logger.warning(f"Task {name} failed: {exc}")
                    
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
    except KeyboardInterrupt:
        pass
    finally:
        for task in tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
