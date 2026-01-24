"""
Telegram Bot for Yandex Tracker integration.
Optimized for low-resource environments (1GB RAM).
"""
import asyncio
import logging
import re
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import uvicorn
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, CallbackQuery, BotCommand
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import settings
from http_client import get_client, close_client, get_timeout
from formatters import (
    format_issue_list, safe_edit_markdown, safe_send_markdown,
    strip_markdown, escape_md, fmt_date, fmt_issue_link,
    FORMAT_MORNING, FORMAT_EVENING, FORMAT_REMINDER, FORMAT_WORKER,
    ListFormatConfig
)

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
class ChatHistory:
    """Chat history storage with TTL and message limit."""
    
    def __init__(self, max_messages: int = 10, ttl: int = 3600):
        self._history: Dict[int, List[dict]] = {}
        self._timestamps: Dict[int, float] = {}
        self._last_issue: Dict[int, str] = {}  # Last discussed issue key
        self._max_messages = max_messages  # 5 pairs = 10 messages
        self._ttl = ttl  # 1 hour
    
    def add(self, user_id: int, role: str, content: str):
        """Add message to history."""
        now = time.time()
        # Clear expired
        if user_id in self._timestamps and now - self._timestamps[user_id] > self._ttl:
            self._history[user_id] = []
            self._last_issue.pop(user_id, None)
        
        if user_id not in self._history:
            self._history[user_id] = []
        
        self._history[user_id].append({"role": role, "content": content})
        self._timestamps[user_id] = now
        
        # Limit history size
        if len(self._history[user_id]) > self._max_messages:
            self._history[user_id] = self._history[user_id][-self._max_messages:]
    
    def get(self, user_id: int) -> List[dict]:
        """Get history for user."""
        now = time.time()
        if user_id in self._timestamps and now - self._timestamps[user_id] > self._ttl:
            self._history[user_id] = []
            self._last_issue.pop(user_id, None)
            return []
        return self._history.get(user_id, [])
    
    def set_last_issue(self, user_id: int, issue_key: str):
        """Remember last discussed issue."""
        self._last_issue[user_id] = issue_key
        self._timestamps[user_id] = time.time()
    
    def get_last_issue(self, user_id: int) -> Optional[str]:
        """Get last discussed issue key."""
        now = time.time()
        if user_id in self._timestamps and now - self._timestamps[user_id] > self._ttl:
            return None
        return self._last_issue.get(user_id)
    
    def clear(self, user_id: int):
        """Clear history for user."""
        self._history.pop(user_id, None)
        self._timestamps.pop(user_id, None)
        self._last_issue.pop(user_id, None)


class AppState:
    """Application state container."""
    
    __slots__ = (
        'bot', 'dispatcher', 'shutdown_event',
        'checklist_cache', 'summary_cache',
        'pending_comment', 'pending_summary', 'pending_ai_search', 
        'pending_new_issue', 'pending_stats_dates', 'last_reminder',
        'chat_history'
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
        self.pending_stats_dates: Dict[int, dict] = {}  # tg_id -> {queue, msg_id}
        self.last_reminder: Dict[int, float] = {}
        self.chat_history = ChatHistory(max_messages=10, ttl=3600)  # 5 pairs, 1 hour TTL


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


# Note: shutdown is handled via lifespan in main.py


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


# fmt_date moved to formatters.py


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


# fmt_issue_link moved to formatters.py


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
    kb.button(text="🌅 Утренний отчёт", callback_data="st:morning")
    kb.button(text="🌆 Вечерний отчёт", callback_data="st:evening")
    kb.button(text="📊 Итоговый отчёт", callback_data="st:report")
    kb.button(text="Закрыть", callback_data="st:close")
    kb.adjust(2, 2, 3, 1)
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


def kb_new_issue_back(prev_step: str) -> InlineKeyboardMarkup:
    """Back button keyboard for text input steps."""
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data=f"new:goback:{prev_step}")
    kb.button(text="❌ Отмена", callback_data="new:cancel")
    kb.adjust(2)
    return kb.as_markup()


def kb_new_issue_assignee() -> InlineKeyboardMarkup:
    """Assignee selection keyboard."""
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 На себя", callback_data="new:assignee:me")
    kb.button(text="🔍 Ввести логин", callback_data="new:assignee:input")
    kb.button(text="⏭ Пропустить", callback_data="new:assignee:skip")
    kb.button(text="⬅️ Назад", callback_data="new:goback:description")
    kb.adjust(2, 1, 1)
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


def kb_settings_morning(enabled: bool, queue: str, limit: int) -> InlineKeyboardMarkup:
    """Morning report settings keyboard."""
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"{'✅' if enabled else '❌'} {'Включён' if enabled else 'Выключен'}",
        callback_data=f"st:morning_toggle"
    )
    kb.button(text=f"📋 Очередь: {queue or '—'}", callback_data="st:morning_queue")
    kb.button(text=f"🔢 Лимит: {limit}", callback_data="st:morning_limit")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(1, 2, 1)
    return kb.as_markup()


def kb_morning_queue_select(current: str) -> InlineKeyboardMarkup:
    """Queue selection for morning report."""
    kb = InlineKeyboardBuilder()
    for q in QUEUES_LIST:
        kb.button(text=f"{'✅' if q == current else '⬜'} {q}", callback_data=f"st:morning_qset:{q}")
    kb.button(text="Назад", callback_data="st:morning")
    kb.adjust(4, 3, 1)
    return kb.as_markup()


def kb_morning_limit_select(current: int) -> InlineKeyboardMarkup:
    """Limit selection for morning report."""
    kb = InlineKeyboardBuilder()
    for n in [5, 10, 20]:
        kb.button(text=f"{'✅' if n == current else '⬜'} {n}", callback_data=f"st:morning_lset:{n}")
    kb.button(text="Назад", callback_data="st:morning")
    kb.adjust(3, 1)
    return kb.as_markup()


def kb_settings_evening(enabled: bool, queue: str) -> InlineKeyboardMarkup:
    """Evening report settings keyboard."""
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"{'✅' if enabled else '❌'} {'Включён' if enabled else 'Выключен'}",
        callback_data=f"st:evening_toggle"
    )
    kb.button(text=f"📋 Очередь: {queue or '(= утренняя)'}", callback_data="st:evening_info")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def kb_settings_report(enabled: bool, queue: str, period: str) -> InlineKeyboardMarkup:
    """Итоговый отчёт settings keyboard."""
    period_names = {"today": "сегодня", "week": "неделя", "month": "месяц"}
    kb = InlineKeyboardBuilder()
    kb.button(
        text=f"{'✅' if enabled else '❌'} Авто (19:00): {'Вкл' if enabled else 'Выкл'}",
        callback_data="st:report_toggle"
    )
    kb.button(text=f"📋 Очередь: {queue or '—'}", callback_data="st:report_queue")
    kb.button(text=f"📅 Период: {period_names.get(period, period)}", callback_data="st:report_period")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(1, 2, 1)
    return kb.as_markup()


def kb_report_queue_select(current: str) -> InlineKeyboardMarkup:
    """Queue selection for итоговый отчёт."""
    kb = InlineKeyboardBuilder()
    for q in QUEUES_LIST:
        kb.button(text=f"{'✅' if q == current else '⬜'} {q}", callback_data=f"st:report_qset:{q}")
    kb.button(text="Назад", callback_data="st:report")
    kb.adjust(4, 3, 1)
    return kb.as_markup()


def kb_report_period_select(current: str) -> InlineKeyboardMarkup:
    """Period selection for итоговый отчёт settings."""
    kb = InlineKeyboardBuilder()
    for val, label in [("today", "Сегодня"), ("week", "Неделя"), ("month", "Месяц")]:
        kb.button(text=f"{'✅' if val == current else '⬜'} {label}", callback_data=f"st:report_pset:{val}")
    kb.button(text="Назад", callback_data="st:report")
    kb.adjust(3, 1)
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


async def get_full_settings(tg_id: int) -> Optional[dict]:
    """Get all user settings as a dict."""
    sc, data = await api_request("GET", "/tg/settings", {"tg": tg_id})
    if sc != 200:
        return None
    return {
        "queues": data.get("queues") or [],
        "days": int(data.get("days", 30)),
        "limit": int(data.get("limit", 10)),
        "reminder": int(data.get("reminder", 0)),
        "morning_enabled": bool(data.get("morning_report_enabled", False)),
        "morning_queue": data.get("morning_report_queue", ""),
        "morning_limit": int(data.get("morning_report_limit", 10)),
        "evening_enabled": bool(data.get("evening_report_enabled", False)),
        "report_enabled": bool(data.get("report_enabled", False)),
        "report_queue": data.get("report_queue", ""),
        "report_period": data.get("report_period", "week"),
    }


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
        # Show status if available
        status = issue.get("status")
        if status:
            lines.append(f"   _{escape_md(status)}_")
        
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
    kb = InlineKeyboardBuilder()
    kb.button(text="🌅 Утренний", callback_data="report:morning")
    kb.button(text="🌆 Вечерний", callback_data="report:evening")
    kb.button(text="📊 Отчёт", callback_data="report:stats")
    kb.adjust(3)
    
    await m.answer(
        "📋 *Меню:*\n\n"
        "🔗 /connect — привязать аккаунт\n"
        "👤 /me — проверить доступ\n"
        "⚙️ /settings — настройки\n\n"
        "✅ /cl\\_my — задачи с моим ОК\n"
        "❓ /cl\\_my\\_open — ждут моего ОК\n"
        "📣 /mentions — требующие ответа\n\n"
        "🤖 /summary ISSUE — резюме (ИИ)\n"
        "🔍 /ai ЗАПРОС — поиск (ИИ)\n"
        "📝 /new — создать задачу",
        parse_mode="Markdown",
        reply_markup=kb.as_markup()
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
        responded_icon = "✅" if issue.get("has_responded") else "⏳"
        lines.append(f"\n{idx}. {fmt_issue_link(issue, prefix=f'{responded_icon} ')}")
        # Show status if available
        issue_status = issue.get("status")
        if issue_status:
            lines.append(f"   _{escape_md(issue_status)}_")
    
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
        issues, "❓ *Ждут моего ОК:*",
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


@router.message(Command("morning"))
@require_base_url
async def cmd_morning(m: Message):
    """Get morning report: open issues in queue."""
    tg_id = m.from_user.id
    full_settings = await get_full_settings(tg_id)
    
    queue = full_settings.get("morning_queue", "") if full_settings else ""
    limit = full_settings.get("morning_limit", 10) if full_settings else 10
    
    if not queue:
        await m.answer("❌ Очередь не настроена. /settings → Утренний отчёт")
        return
    
    loading = await m.answer(f"🌅 Загружаю открытые задачи {queue}...")
    
    moscow_tz = timezone(timedelta(hours=3))
    today_str = datetime.now(moscow_tz).strftime("%d.%m.%Y")
    
    sc, data = await api_request(
        "GET", "/tracker/morning_report",
        {"tg": tg_id, "queue": queue, "limit": limit, "date_offset": 0},
        long_timeout=True
    )
    
    if sc != 200:
        await loading.edit_text(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    count = data.get("count", 0)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📆 Вчера", callback_data="report:morning:1")
    kb.adjust(1)
    
    if not issues:
        text = f"🌅 *{queue}* ({today_str}): нет открытых задач"
    else:
        title = f"🌅 *Утренний отчёт — {queue}* ({today_str}, {count} задач)\n"
        text = format_issue_list(issues, title, FORMAT_MORNING)
    
    await safe_edit_markdown(loading, text, reply_markup=kb.as_markup())


@router.message(Command("evening"))
@require_base_url
async def cmd_evening(m: Message):
    """Get evening report: issues closed today."""
    tg_id = m.from_user.id
    full_settings = await get_full_settings(tg_id)
    
    queue = full_settings.get("morning_queue", "") if full_settings else ""
    
    if not queue:
        await m.answer("❌ Очередь не настроена. /settings → Утренний отчёт")
        return
    
    loading = await m.answer(f"🌆 Загружаю закрытые сегодня задачи {queue}...")
    
    moscow_tz = timezone(timedelta(hours=3))
    today_str = datetime.now(moscow_tz).strftime("%d.%m.%Y")
    
    sc, data = await api_request(
        "GET", "/tracker/evening_report",
        {"tg": tg_id, "queue": queue, "date_offset": 0},
        long_timeout=True
    )
    
    if sc != 200:
        await loading.edit_text(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    issues = data.get("issues", [])
    count = data.get("count", 0)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📆 Вчера", callback_data="report:evening:1")
    kb.adjust(1)
    
    if not issues:
        text = f"🌆 *{queue}* ({today_str}): ничего не закрыто"
    else:
        title = f"🌆 *Вечерний отчёт — {queue}* ({today_str}, {count} закрыто)\n"
        text = format_issue_list(issues, title, FORMAT_EVENING)
    
    await safe_edit_markdown(loading, text, reply_markup=kb.as_markup())


@router.message(Command("report"))
@require_base_url
async def cmd_report(m: Message):
    """Get итоговый отчёт."""
    await m.answer(
        "📊 *Итоговый отчёт*\n\nВыберите очередь:",
        parse_mode="Markdown",
        reply_markup=kb_stats_queue()
    )


def kb_stats_queue() -> InlineKeyboardMarkup:
    """Queue selection for stats."""
    kb = InlineKeyboardBuilder()
    for q in QUEUES_LIST:
        kb.button(text=q, callback_data=f"stats:queue:{q}")
    kb.button(text="❌ Отмена", callback_data="stats:cancel")
    kb.adjust(4, 3, 1)
    return kb.as_markup()


def kb_stats_period(queue: str) -> InlineKeyboardMarkup:
    """Period selection for stats."""
    kb = InlineKeyboardBuilder()
    kb.button(text="📆 Сегодня", callback_data=f"stats:period:{queue}:today")
    kb.button(text="📅 Неделя", callback_data=f"stats:period:{queue}:week")
    kb.button(text="🗓 Месяц", callback_data=f"stats:period:{queue}:month")
    kb.button(text="📆 Выбрать даты", callback_data=f"stats:custom:{queue}")
    kb.button(text="⬅️ Назад", callback_data="stats:back")
    kb.adjust(3, 1, 1)
    return kb.as_markup()


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
                "• /cl_my_open — ждут моего ОК"
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
        status = escape_md(issue.get("status", ""))
        description = escape_md((issue.get("description") or "")[:80])
        url = issue.get("url", f"https://tracker.yandex.ru/{key}")
        
        line = f"{idx}. [{key}: {summary}]({url})"
        if status:
            line += f" _{status}_"
        lines.append(line)
        
        if description:
            lines.append(f"   _{description}_")
    
    
    text = "\n".join(lines)
    
    try:
        if len(text) > 4000:
            await loading.edit_text(text[:4000], parse_mode="Markdown")
            await m.answer(text[4000:], parse_mode="Markdown")
        else:
            await loading.edit_text(text, parse_mode="Markdown")
    except Exception:
        # Fallback without Markdown
        plain = text.replace("*", "").replace("_", "")
        await loading.edit_text(plain[:4000])


async def process_custom_stats(m: Message, text: str, pending: dict):
    """Process custom date range for stats."""
    import re
    tg_id = m.from_user.id
    queue = pending.get("queue", "")
    
    # Parse date range: DD.MM.YYYY — DD.MM.YYYY or DD.MM.YYYY - DD.MM.YYYY
    pattern = r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s*[-—]\s*(\d{1,2})\.(\d{1,2})\.(\d{4})"
    match = re.search(pattern, text)
    
    if not match:
        await m.answer(
            "❌ Неверный формат дат.\n\n"
            "Пример: `15.01.2026 — 23.01.2026`",
            parse_mode="Markdown"
        )
        return
    
    try:
        d1, m1, y1 = int(match.group(1)), int(match.group(2)), int(match.group(3))
        d2, m2, y2 = int(match.group(4)), int(match.group(5)), int(match.group(6))
        
        date_from = datetime(y1, m1, d1)
        date_to = datetime(y2, m2, d2)
        
        if date_from > date_to:
            date_from, date_to = date_to, date_from
    except ValueError:
        await m.answer("❌ Неверная дата")
        return
    
    loading = await m.answer("⏳ Загружаю...")
    
    # Request with custom dates
    sc, data = await api_request(
        "GET", "/tracker/queue_stats",
        {
            "tg": tg_id, 
            "queue": queue, 
            "period": "custom",
            "date_from": date_from.strftime("%Y-%m-%d"),
            "date_to": date_to.strftime("%Y-%m-%d")
        },
        long_timeout=True
    )
    
    if sc != 200:
        await loading.edit_text(f"❌ Ошибка {sc}: {data.get('error', data)}"[:500])
        return
    
    created = data.get("created", 0)
    in_progress = data.get("in_progress", 0)
    closed = data.get("closed", 0)
    
    period_text = f"{date_from.strftime('%d.%m.%Y')} — {date_to.strftime('%d.%m.%Y')}"
    
    result_text = (
        f"📊 *Итоговый отчёт — {queue}* ({period_text})\n\n"
        f"📝 Создано: {created}\n"
        f"🔄 В работе: {in_progress}\n"
        f"✅ Закрыто: {closed}"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 Другой период", callback_data=f"stats:queue:{queue}")
    kb.button(text="📋 Другая очередь", callback_data="stats:back")
    kb.adjust(2)
    
    try:
        await loading.edit_text(result_text, parse_mode="Markdown", reply_markup=kb.as_markup())
    except Exception:
        await loading.edit_text(result_text.replace("*", "").replace("_", ""), reply_markup=kb.as_markup())


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
    elif data.startswith("stats:"):
        await handle_stats_callback(c)
    elif data.startswith("report:"):
        await handle_report_callback(c)
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
    
    # Go back to previous step
    if action == "goback" and len(parts) >= 3:
        prev_step = parts[2]
        await c.answer()
        
        if prev_step == "queue":
            draft["step"] = "queue"
            state.pending_new_issue[tg_id] = draft
            if c.message:
                await c.message.edit_text("📝 Создание задачи\n\nВыберите очередь:", reply_markup=kb_new_issue_queue())
        elif prev_step == "summary":
            draft["step"] = "summary"
            state.pending_new_issue[tg_id] = draft
            if c.message:
                from aiogram.types import ForceReply
                await c.message.edit_text(
                    f"📝 Очередь: {draft.get('queue')}\n\nВведите название задачи:",
                    reply_markup=kb_new_issue_back("queue")
                )
                await c.message.answer("📋 Название:", reply_markup=ForceReply(input_field_placeholder="Название задачи"))
        elif prev_step == "description":
            draft["step"] = "description"
            state.pending_new_issue[tg_id] = draft
            if c.message:
                from aiogram.types import ForceReply
                await c.message.edit_text(
                    f"📝 Очередь: {draft.get('queue')}\n"
                    f"📋 Название: {draft.get('summary', '')[:50]}\n\n"
                    f"Введите описание (или '-' чтобы пропустить):",
                    reply_markup=kb_new_issue_back("summary")
                )
                await c.message.answer("📄 Описание:", reply_markup=ForceReply(input_field_placeholder="Описание или -"))
        elif prev_step == "assignee":
            draft["step"] = "assignee"
            state.pending_new_issue[tg_id] = draft
            if c.message:
                await c.message.edit_text(
                    f"📝 Очередь: {draft.get('queue')}\n"
                    f"📋 Название: {draft.get('summary', '')[:50]}\n"
                    f"📄 Описание: {(draft.get('description') or '—')[:50]}\n\n"
                    f"Назначить исполнителя?",
                    reply_markup=kb_new_issue_assignee()
                )
        elif prev_step == "pending":
            draft["step"] = "pending_reply"
            state.pending_new_issue[tg_id] = draft
            if c.message:
                from aiogram.types import ForceReply
                assignee_text = f"@{draft.get('assignee')}" if draft.get('assignee') else "—"
                await c.message.edit_text(
                    f"📝 Очередь: {draft.get('queue')}\n"
                    f"📋 Название: {draft.get('summary', '')[:50]}\n"
                    f"📄 Описание: {(draft.get('description') or '—')[:50]}\n"
                    f"👤 Исполнитель: {assignee_text}\n\n"
                    f"📣 Нужен ответ от?\n(введите логин Tracker, например: ivanov или '-' чтобы пропустить)",
                    reply_markup=kb_new_issue_back("assignee")
                )
                await c.message.answer("Логин Tracker:", reply_markup=ForceReply(input_field_placeholder="login или -"))
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
            await c.message.edit_text(
                f"📝 Очередь: {queue}\n\nВведите название задачи:",
                reply_markup=kb_new_issue_back("queue")
            )
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
                await c.message.edit_text(
                    "👤 Введите логин исполнителя\n(логин Tracker, например: ivanov)",
                    reply_markup=kb_new_issue_back("description")
                )
                await c.message.answer("Логин Tracker:", reply_markup=ForceReply(input_field_placeholder="login"))
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
                f"📣 Нужен ответ от?\n(введите логин Tracker, например: ivanov или '-' чтобы пропустить)",
                reply_markup=kb_new_issue_back("assignee")
            )
            await c.message.answer("Логин Tracker:", reply_markup=ForceReply(input_field_placeholder="login или -"))
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
    
    # Edit specific field (new:edit:queue, new:edit:summary, etc.)
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
            draft["step"] = "edit_summary"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📋 Введите новое название:")
                await c.message.answer("Название:", reply_markup=ForceReply(input_field_placeholder="Название"))
        elif field == "description":
            draft["step"] = "edit_description"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📄 Введите новое описание (или '-'):")
                await c.message.answer("Описание:", reply_markup=ForceReply(input_field_placeholder="Описание"))
        elif field == "assignee":
            draft["step"] = "assignee"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("👤 Назначить исполнителя?", reply_markup=kb_new_issue_assignee())
        elif field == "pending":
            draft["step"] = "edit_pending"
            state.pending_new_issue[tg_id] = draft
            await c.answer()
            if c.message:
                await c.message.edit_text("📣 Нужен ответ от?\n(введите логин Tracker, например: ivanov или '-' чтобы пропустить)")
                await c.message.answer("Логин Tracker:", reply_markup=ForceReply(input_field_placeholder="login или -"))
        return
    
    # Edit menu (just "new:edit" without field)
    if action == "edit":
        await c.answer()
        if c.message:
            await c.message.edit_text(
                "Что изменить?",
                reply_markup=kb_new_issue_edit()
            )
        return
    
    await c.answer()


async def handle_report_callback(c: CallbackQuery):
    """Handle report callbacks from menu."""
    tg_id = c.from_user.id
    data = c.data or ""
    parts = data.split(":")
    
    if len(parts) < 2:
        await c.answer()
        return
    
    action = parts[1]
    date_offset = int(parts[2]) if len(parts) > 2 else 0  # 0 = today, 1 = yesterday, etc.
    
    full_settings = await get_full_settings(tg_id)
    queue = full_settings.get("morning_queue", "") if full_settings else ""
    limit = full_settings.get("morning_limit", 10) if full_settings else 10
    
    if not queue:
        await c.answer("❌ Очередь не настроена. /settings → Утренний отчёт", show_alert=True)
        return
    
    if action == "morning":
        await c.answer("⏳ Загружаю...")
        
        # Calculate date for display
        moscow_tz = timezone(timedelta(hours=3))
        target_date = datetime.now(moscow_tz) - timedelta(days=date_offset)
        date_str = target_date.strftime("%d.%m.%Y")
        
        sc, data_resp = await api_request(
            "GET", "/tracker/morning_report",
            {"tg": tg_id, "queue": queue, "limit": limit, "date_offset": date_offset},
            long_timeout=True
        )
        
        if sc != 200:
            if c.message:
                await c.message.edit_text(f"❌ Ошибка {sc}: {data_resp.get('error', data_resp)}"[:500])
            return
        
        issues = data_resp.get("issues", [])
        count = data_resp.get("count", 0)
        
        if not issues:
            text = f"🌅 *{queue}* ({date_str}): нет открытых задач"
        else:
            title = f"🌅 *Утренний отчёт — {queue}* ({date_str}, {count} задач)\n"
            text = format_issue_list(issues, title, FORMAT_MORNING)
        
        kb = InlineKeyboardBuilder()
        if date_offset == 0:
            kb.button(text="📆 Вчера", callback_data="report:morning:1")
        else:
            if date_offset < 7:
                kb.button(text="◀️ Раньше", callback_data=f"report:morning:{date_offset + 1}")
            kb.button(text="📅 Сегодня", callback_data="report:morning:0")
        kb.adjust(2)
        
        if c.message:
            await safe_edit_markdown(c.message, text, reply_markup=kb.as_markup())
        return
    
    if action == "evening":
        await c.answer("⏳ Загружаю...")
        
        moscow_tz = timezone(timedelta(hours=3))
        target_date = datetime.now(moscow_tz) - timedelta(days=date_offset)
        date_str = target_date.strftime("%d.%m.%Y")
        
        sc, data_resp = await api_request(
            "GET", "/tracker/evening_report",
            {"tg": tg_id, "queue": queue, "date_offset": date_offset},
            long_timeout=True
        )
        
        if sc != 200:
            if c.message:
                await c.message.edit_text(f"❌ Ошибка {sc}: {data_resp.get('error', data_resp)}"[:500])
            return
        
        issues = data_resp.get("issues", [])
        count = data_resp.get("count", 0)
        
        if not issues:
            text = f"🌆 *{queue}* ({date_str}): ничего не закрыто"
        else:
            title = f"🌆 *Вечерний отчёт — {queue}* ({date_str}, {count} закрыто)\n"
            text = format_issue_list(issues, title, FORMAT_EVENING)
        
        kb = InlineKeyboardBuilder()
        if date_offset == 0:
            kb.button(text="📆 Вчера", callback_data="report:evening:1")
        else:
            if date_offset < 7:
                kb.button(text="◀️ Раньше", callback_data=f"report:evening:{date_offset + 1}")
            kb.button(text="📅 Сегодня", callback_data="report:evening:0")
        kb.adjust(2)
        
        if c.message:
            await safe_edit_markdown(c.message, text, reply_markup=kb.as_markup())
        return
    
    if action == "stats":
        await c.answer()
        if c.message:
            await c.message.edit_text(
                "📊 *Итоговый отчёт*\n\nВыберите очередь:",
                parse_mode="Markdown",
                reply_markup=kb_stats_queue()
            )
        return
    
    await c.answer()


async def handle_stats_callback(c: CallbackQuery):
    """Handle stats callbacks."""
    tg_id = c.from_user.id
    data = c.data or ""
    parts = data.split(":")
    
    if len(parts) < 2:
        await c.answer()
        return
    
    action = parts[1]
    
    # Cancel
    if action == "cancel":
        await c.answer("Отменено")
        if c.message:
            try:
                await c.message.delete()
            except Exception:
                pass
        return
    
    # Back to queue selection
    if action == "back":
        await c.answer()
        if c.message:
            await c.message.edit_text(
                "📊 *Итоговый отчёт*\n\nВыберите очередь:",
                parse_mode="Markdown",
                reply_markup=kb_stats_queue()
            )
        return
    
    # Queue selected
    if action == "queue" and len(parts) >= 3:
        queue = parts[2].upper()
        await c.answer()
        if c.message:
            await c.message.edit_text(
                f"📊 *Итоговый отчёт — {queue}*\n\nВыберите период:",
                parse_mode="Markdown",
                reply_markup=kb_stats_period(queue)
            )
        return
    
    # Custom date range
    if action == "custom" and len(parts) >= 3:
        queue = parts[2].upper()
        await c.answer()
        if c.message:
            state.pending_stats_dates[tg_id] = {"queue": queue, "msg_id": c.message.message_id}
            await c.message.edit_text(
                f"📊 *Итоговый отчёт — {queue}*\n\n"
                "Введите период в формате:\n"
                "`ДД.ММ.ГГГГ — ДД.ММ.ГГГГ`\n\n"
                "Пример: `15.01.2026 — 23.01.2026`",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardBuilder().button(
                    text="⬅️ Назад", callback_data=f"stats:queue:{queue}"
                ).as_markup()
            )
        return
    
    # Period selected - show stats
    if action == "period" and len(parts) >= 4:
        queue = parts[2].upper()
        period = parts[3]
        await c.answer("⏳ Загружаю...")
        
        sc, data_resp = await api_request(
            "GET", "/tracker/queue_stats",
            {"tg": tg_id, "queue": queue, "period": period},
            long_timeout=True
        )
        
        if sc != 200:
            if c.message:
                await c.message.edit_text(f"❌ Ошибка {sc}: {data_resp.get('error', data_resp)}"[:500])
            return
        
        created = data_resp.get("created", 0)
        in_progress = data_resp.get("in_progress", 0)
        closed = data_resp.get("closed", 0)
        
        # Calculate date range for display
        moscow_tz = timezone(timedelta(hours=3))
        now = datetime.now(moscow_tz)
        today_str = now.strftime("%d.%m")
        
        if period == "today":
            period_text = f"сегодня {today_str}"
        elif period == "week":
            week_ago = now - timedelta(days=7)
            period_text = f"за неделю {week_ago.strftime('%d.%m')} — {today_str}"
        elif period == "month":
            month_ago = now - timedelta(days=30)
            period_text = f"за месяц {month_ago.strftime('%d.%m')} — {today_str}"
        else:
            period_text = period
        
        text = (
            f"📊 *Итоговый отчёт — {queue}* ({period_text})\n\n"
            f"📝 Создано: {created}\n"
            f"🔄 В работе: {in_progress}\n"
            f"✅ Закрыто: {closed}"
        )
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Обновить", callback_data=f"stats:period:{queue}:{period}")
        kb.button(text="⬅️ Другой период", callback_data=f"stats:queue:{queue}")
        kb.button(text="📋 Другая очередь", callback_data="stats:back")
        kb.adjust(1, 2)
        
        if c.message:
            try:
                await c.message.edit_text(text, parse_mode="Markdown", reply_markup=kb.as_markup())
            except Exception:
                await c.message.edit_text(text.replace("*", "").replace("_", ""), reply_markup=kb.as_markup())
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

    # Morning report settings
    if action == "morning":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "🌅 *Утренний отчёт* (09:00)\n\n"
                "Список открытых задач из выбранной очереди.",
                parse_mode="Markdown",
                reply_markup=kb_settings_morning(
                    full_settings["morning_enabled"],
                    full_settings["morning_queue"],
                    full_settings["morning_limit"]
                )
            )
        await c.answer()
        return

    if action == "morning_toggle":
        full_settings = await get_full_settings(tg_id)
        if full_settings:
            new_val = not full_settings["morning_enabled"]
            sc2, _ = await api_request("POST", "/tg/settings/morning_enabled", {"tg": tg_id, "enabled": new_val})
            if sc2 == 200 and c.message:
                await c.message.edit_reply_markup(
                    reply_markup=kb_settings_morning(new_val, full_settings["morning_queue"], full_settings["morning_limit"])
                )
            await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "morning_queue":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "🌅 Выберите очередь для утреннего отчёта:",
                reply_markup=kb_morning_queue_select(full_settings["morning_queue"])
            )
        await c.answer()
        return

    if action == "morning_qset":
        q = arg.upper()
        sc2, _ = await api_request("POST", "/tg/settings/morning_queue", {"tg": tg_id, "queue": q})
        if sc2 == 200:
            full_settings = await get_full_settings(tg_id)
            if full_settings and c.message:
                await c.message.edit_text(
                    "🌅 *Утренний отчёт* (09:00)\n\n"
                    "Список открытых задач из выбранной очереди.",
                    parse_mode="Markdown",
                    reply_markup=kb_settings_morning(
                        full_settings["morning_enabled"],
                        full_settings["morning_queue"],
                        full_settings["morning_limit"]
                    )
                )
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "morning_limit":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "🌅 Выберите лимит задач:",
                reply_markup=kb_morning_limit_select(full_settings["morning_limit"])
            )
        await c.answer()
        return

    if action == "morning_lset":
        try:
            n = int(arg)
        except ValueError:
            await c.answer("❌ Ошибка", show_alert=True)
            return
        sc2, _ = await api_request("POST", "/tg/settings/morning_limit", {"tg": tg_id, "limit": n})
        if sc2 == 200:
            full_settings = await get_full_settings(tg_id)
            if full_settings and c.message:
                await c.message.edit_text(
                    "🌅 *Утренний отчёт* (09:00)\n\n"
                    "Список открытых задач из выбранной очереди.",
                    parse_mode="Markdown",
                    reply_markup=kb_settings_morning(
                        full_settings["morning_enabled"],
                        full_settings["morning_queue"],
                        full_settings["morning_limit"]
                    )
                )
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    # Evening report settings
    if action == "evening":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "🌆 *Вечерний отчёт* (19:00)\n\n"
                "Список закрытых сегодня задач.\n"
                "Очередь = утренняя.",
                parse_mode="Markdown",
                reply_markup=kb_settings_evening(
                    full_settings["evening_enabled"],
                    full_settings["morning_queue"]
                )
            )
        await c.answer()
        return

    if action == "evening_toggle":
        full_settings = await get_full_settings(tg_id)
        if full_settings:
            new_val = not full_settings["evening_enabled"]
            sc2, _ = await api_request("POST", "/tg/settings/evening_enabled", {"tg": tg_id, "enabled": new_val})
            if sc2 == 200 and c.message:
                await c.message.edit_reply_markup(
                    reply_markup=kb_settings_evening(new_val, full_settings["morning_queue"])
                )
            await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "evening_info":
        await c.answer("Очередь берётся из утреннего отчёта", show_alert=True)
        return

    # Итоговый отчёт settings
    if action == "report":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "📊 *Итоговый отчёт*\n\n"
                "Статистика: создано, в работе, закрыто.\n"
                "Авто-отправка в 19:00 вместе с вечерним.",
                parse_mode="Markdown",
                reply_markup=kb_settings_report(
                    full_settings["report_enabled"],
                    full_settings["report_queue"],
                    full_settings["report_period"]
                )
            )
        await c.answer()
        return

    if action == "report_toggle":
        full_settings = await get_full_settings(tg_id)
        if full_settings:
            new_val = not full_settings["report_enabled"]
            sc2, _ = await api_request("POST", "/tg/settings/report_enabled", {"tg": tg_id, "enabled": new_val})
            if sc2 == 200 and c.message:
                await c.message.edit_reply_markup(
                    reply_markup=kb_settings_report(new_val, full_settings["report_queue"], full_settings["report_period"])
                )
            await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "report_queue":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "📊 Выберите очередь для итогового отчёта:",
                reply_markup=kb_report_queue_select(full_settings["report_queue"])
            )
        await c.answer()
        return

    if action == "report_qset":
        q = arg.upper()
        sc2, _ = await api_request("POST", "/tg/settings/report_queue", {"tg": tg_id, "queue": q})
        if sc2 == 200:
            full_settings = await get_full_settings(tg_id)
            if full_settings and c.message:
                await c.message.edit_text(
                    "📊 *Итоговый отчёт*\n\n"
                    "Статистика: создано, в работе, закрыто.\n"
                    "Авто-отправка в 19:00 вместе с вечерним.",
                    parse_mode="Markdown",
                    reply_markup=kb_settings_report(
                        full_settings["report_enabled"],
                        full_settings["report_queue"],
                        full_settings["report_period"]
                    )
                )
        await c.answer("✅" if sc2 == 200 else f"❌ {sc2}")
        return

    if action == "report_period":
        full_settings = await get_full_settings(tg_id)
        if full_settings and c.message:
            await c.message.edit_text(
                "📊 Выберите период по умолчанию:",
                reply_markup=kb_report_period_select(full_settings["report_period"])
            )
        await c.answer()
        return

    if action == "report_pset":
        sc2, _ = await api_request("POST", "/tg/settings/report_period", {"tg": tg_id, "period": arg})
        if sc2 == 200:
            full_settings = await get_full_settings(tg_id)
            if full_settings and c.message:
                await c.message.edit_text(
                    "📊 *Итоговый отчёт*\n\n"
                    "Статистика: создано, в работе, закрыто.\n"
                    "Авто-отправка в 19:00 вместе с вечерним.",
                    parse_mode="Markdown",
                    reply_markup=kb_settings_report(
                        full_settings["report_enabled"],
                        full_settings["report_queue"],
                        full_settings["report_period"]
                    )
                )
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
    
    # Check if awaiting custom stats date range
    stats_pending = state.pending_stats_dates.pop(tg_id, None)
    if stats_pending:
        await process_custom_stats(m, text, stats_pending)
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
                reply_markup=kb_new_issue_back("summary")
            )
            await m.answer("📄 Описание:", reply_markup=ForceReply(input_field_placeholder="Описание или -"))
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
                f"📣 Нужен ответ от?\n(введите логин Tracker, например: ivanov или '-' чтобы пропустить)",
                reply_markup=kb_new_issue_back("assignee")
            )
            await m.answer("Логин Tracker:", reply_markup=ForceReply(input_field_placeholder="login или -"))
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
    
    # =========================================================================
    # AI Chat - handle all other text messages
    # =========================================================================
    await process_chat_message(m, text, tg_id)


async def process_chat_message(m: Message, text: str, tg_id: int):
    """Process chat message with AI assistant."""
    import re
    from ai_service import chat_with_ai, _format_issue_context, generate_search_query
    
    # Show typing indicator
    loading = await m.answer("🤔 Думаю...")
    
    try:
        # Check for issue keys in message (e.g., INV-123, DOC-45, inv123, doc45)
        # Pattern 1: with hyphen (INV-123)
        issue_pattern1 = r'\b([A-Z]{2,10}-\d+)\b'
        # Pattern 2: without hyphen (INV123, doc125)
        issue_pattern2 = r'\b([A-Z]{2,10})(\d+)\b'
        
        issue_keys = re.findall(issue_pattern1, text.upper())
        issue_mentioned_directly = bool(issue_keys)
        
        # Also check for keys without hyphen
        if not issue_keys:
            matches = re.findall(issue_pattern2, text.upper())
            issue_keys = [f"{m[0]}-{m[1]}" for m in matches]
            issue_mentioned_directly = bool(issue_keys)
        
        # If no issue mentioned, check last discussed issue
        if not issue_keys:
            last_issue = state.chat_history.get_last_issue(tg_id)
            if last_issue:
                issue_keys = [last_issue]
                logger.info(f"Using last issue from context: {last_issue}")
        
        # Get user settings for search constraints
        user_settings = await get_settings(tg_id)
        queues = []
        days = 30
        if user_settings:
            queues_raw = user_settings[0]
            # Handle both string and list formats
            if isinstance(queues_raw, list):
                queues = queues_raw
            elif isinstance(queues_raw, str) and queues_raw:
                queues = [q.strip() for q in queues_raw.split(",") if q.strip()]
            days = user_settings[1] or 30
        
        # Build context from various sources
        issue_context = None
        search_results = None
        
        # 1. If specific issue mentioned - get its data
        if issue_keys:
            issue_key = issue_keys[0]
            try:
                sc, data = await api_request(
                    "GET", f"/tracker/issue/{issue_key}",
                    {"tg": tg_id},
                    long_timeout=True
                )
                if sc == 200 and data.get("key"):
                    issue_context = _format_issue_context(data)
                    # Remember this issue for follow-up questions
                    state.chat_history.set_last_issue(tg_id, issue_key)
                    logger.info(f"Loaded issue {issue_key}, set as last issue")
            except Exception as e:
                logger.warning(f"Failed to get issue {issue_key}: {e}")
        
        # 2. Check if user is asking for search/list/stats
        search_keywords = [
            "покажи", "найди", "список", "сколько", "последние",
            "открытые", "закрытые", "статистика", "поиск", "найти",
            "незавершённых", "незавершенных", "активных", "в работе",
            "задач", "задачи", "зада", "очередь", "очереди", "тикет",
            "все ", "первые", "новые", "старые"
        ]
        # Only skip search if issue was directly mentioned in this message
        needs_search = any(kw in text.lower() for kw in search_keywords) and not issue_mentioned_directly
        
        if needs_search:
            try:
                # Generate YQL query from natural language
                logger.info(f"Generating YQL for: {text[:50]}, queues={queues}, days={days}")
                yql_query, err = await generate_search_query(text, queues, days)
                logger.info(f"YQL result: query={yql_query}, err={err}")
                
                if yql_query and not err:
                    # Handle special commands
                    if yql_query == "CHECKLIST":
                        sc, data = await api_request(
                            "GET", "/tracker/checklist/assigned",
                            {"tg": tg_id, "limit": 10},
                            long_timeout=True
                        )
                        if sc == 200:
                            issues = data.get("issues", [])
                            if issues:
                                search_results = _format_search_results(issues, "Задачи с чеклистами")
                            else:
                                search_results = "Задач с чеклистами не найдено."
                    elif yql_query == "SUMMONS":
                        sc, data = await api_request(
                            "GET", "/tracker/summons",
                            {"tg": tg_id, "limit": 10},
                            long_timeout=True
                        )
                        if sc == 200:
                            issues = data.get("issues", [])
                            if issues:
                                search_results = _format_search_results(issues, "Требующие ответа")
                            else:
                                search_results = "Задач, требующих ответа, не найдено."
                    else:
                        # Clean up YQL query - remove Sort by if present (it's an API param, not YQL)
                        clean_query = yql_query
                        if "Sort by:" in clean_query:
                            # Remove Sort by clause
                            import re as re_clean
                            clean_query = re_clean.sub(r'\s*\(?\s*Sort by:[^)]*\)?\s*AND\s*', '', clean_query)
                            clean_query = re_clean.sub(r'\s*AND\s*\(?\s*Sort by:[^)]*\)?\s*', '', clean_query)
                            clean_query = re_clean.sub(r'\(?\s*Sort by:[^)]*\)?\s*', '', clean_query)
                        
                        logger.info(f"Executing search: {clean_query}")
                        # Execute YQL search
                        sc, data = await api_request(
                            "GET", "/tracker/search",
                            {"tg": tg_id, "query": clean_query, "limit": 10},
                            long_timeout=True
                        )
                        logger.info(f"Search result: sc={sc}, issues={len(data.get('issues', []))}")
                        if sc == 200:
                            issues = data.get("issues", [])
                            if issues:
                                search_results = _format_search_results(issues, f"Результаты поиска ({len(issues)})")
                            else:
                                search_results = f"По запросу ничего не найдено.\nYQL: {clean_query}"
            except Exception as e:
                logger.warning(f"Search failed: {e}")
        
        # Build full context for AI
        full_context = ""
        if issue_context:
            full_context += f"\n\n{issue_context}"
        if search_results:
            full_context += f"\n\nРезультаты из Tracker:\n{search_results}"
        
        logger.info(f"AI context length: {len(full_context)}, has_issue={bool(issue_context)}, has_search={bool(search_results)}")
        
        # Get history
        history = state.chat_history.get(tg_id)
        
        # Call AI with context
        response, error = await chat_with_ai(text, history, full_context if full_context else None)
        logger.info(f"AI response: len={len(response) if response else 0}, error={error}")
        
        if error:
            await loading.edit_text(error)
            return
        
        if response:
            # Save to history
            state.chat_history.add(tg_id, "user", text)
            state.chat_history.add(tg_id, "assistant", response)
            
            # Send response (split if too long)
            await loading.delete()
            for chunk in [response[i:i+4000] for i in range(0, len(response), 4000)]:
                await m.answer(chunk)
        else:
            await loading.edit_text("❌ Не удалось получить ответ")
    
    except Exception as e:
        import traceback
        logger.error(f"Chat processing error: {e}\n{traceback.format_exc()}")
        try:
            await loading.edit_text(f"❌ Ошибка: {str(e)[:100]}")
        except Exception:
            pass


def _format_search_results(issues: list, title: str) -> str:
    """Format search results for AI context."""
    lines = [f"{title}:"]
    for i, issue in enumerate(issues[:10], 1):
        key = issue.get("key", "")
        summary = issue.get("summary", "")[:60]
        status = ""
        if isinstance(issue.get("status"), dict):
            status = issue["status"].get("display", "")
        elif isinstance(issue.get("status"), str):
            status = issue.get("status", "")
        updated = issue.get("updatedAt", "")[:10] if issue.get("updatedAt") else ""
        lines.append(f"{i}. {key}: {summary} [{status}] ({updated})")
    return "\n".join(lines)


@router.message(Command("clear"))
async def cmd_clear(m: Message):
    """Clear chat history."""
    if not m.from_user:
        return
    tg_id = m.from_user.id
    state.chat_history.clear(tg_id)
    await m.answer("🗑️ История чата очищена")


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
        BotCommand(command="cl_my_open", description="❓ Ждут моего ОК"),
        BotCommand(command="mentions", description="📣 Требующие ответа"),
        BotCommand(command="morning", description="🌅 Утренний отчёт"),
        BotCommand(command="evening", description="🌆 Вечерний отчёт"),
        BotCommand(command="report", description="📊 Итоговый отчёт"),
        BotCommand(command="summary", description="🤖 Резюме (ИИ)"),
        BotCommand(command="ai", description="🔍 Поиск (ИИ)"),
        BotCommand(command="new", description="📝 Создать задачу"),
        BotCommand(command="clear", description="🗑️ Очистить историю чата"),
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
        await asyncio.sleep(5)  # Wait for old instance to fully stop
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
                logger.warning(f"Bot conflict, retrying in {10 * (attempt + 1)}s... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(10 * (attempt + 1))  # 10, 20, 30, 40 seconds
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
            moscow_tz = timezone(timedelta(hours=3))
            now = datetime.now(moscow_tz)
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
                            lines.append("❓ *Ждут моего ОК:*")
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


async def morning_report_worker():
    """Send morning reports at 09:00 Moscow time."""
    await asyncio.sleep(120)  # Wait for startup
    
    last_sent_date: Dict[int, str] = {}  # tg_id -> date string
    
    while not state.shutdown_event.is_set():
        try:
            moscow_tz = timezone(timedelta(hours=3))
            now = datetime.now(moscow_tz)
            today_str = now.strftime("%Y-%m-%d")
            
            # Send between 09:00 and 09:30
            if now.hour == 9 and now.minute < 30:
                if state.bot and settings.base_url:
                    # Get users with morning report enabled
                    sc, data = await api_request("GET", "/tg/users_with_morning_report", {})
                    if sc == 200:
                        for user in data.get("users", []):
                            tg_id = user.get("tg_id")
                            queue = user.get("morning_report_queue", "")
                            limit = user.get("morning_report_limit", 10)
                            
                            if not tg_id or not queue:
                                continue
                            
                            # Check if already sent today
                            if last_sent_date.get(tg_id) == today_str:
                                continue
                            
                            try:
                                sc2, data2 = await api_request(
                                    "GET", "/tracker/morning_report",
                                    {"tg": tg_id, "queue": queue, "limit": limit},
                                    long_timeout=True
                                )
                                
                                if sc2 == 200:
                                    issues = data2.get("issues", [])
                                    count = data2.get("count", 0)
                                    
                                    if issues:
                                        title = f"🌅 *Утренний отчёт — {queue}* ({count} задач)\n"
                                        text = format_issue_list(issues, title, FORMAT_WORKER)
                                        await safe_send_markdown(state.bot, tg_id, text)
                                    
                                    last_sent_date[tg_id] = today_str
                            except Exception:
                                pass
                            
                            await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"Morning report worker error: {e}")
        
        # Check every 5 minutes
        try:
            await asyncio.wait_for(state.shutdown_event.wait(), timeout=300)
            break
        except asyncio.TimeoutError:
            continue


async def evening_report_worker():
    """Send evening reports and итоговый отчёт at 19:00 Moscow time."""
    await asyncio.sleep(180)  # Wait for startup
    
    last_sent_evening: Dict[int, str] = {}  # tg_id -> date string
    last_sent_report: Dict[int, str] = {}  # tg_id -> date string
    
    while not state.shutdown_event.is_set():
        try:
            moscow_tz = timezone(timedelta(hours=3))
            now = datetime.now(moscow_tz)
            today_str = now.strftime("%Y-%m-%d")
            
            # Send between 19:00 and 19:30
            if now.hour == 19 and now.minute < 30:
                if state.bot and settings.base_url:
                    # --- Send evening reports ---
                    sc, data = await api_request("GET", "/tg/users_with_evening_report", {})
                    if sc == 200:
                        for user in data.get("users", []):
                            tg_id = user.get("tg_id")
                            queue = user.get("queue", "")
                            
                            if not tg_id or not queue:
                                continue
                            
                            if last_sent_evening.get(tg_id) == today_str:
                                continue
                            
                            try:
                                sc2, data2 = await api_request(
                                    "GET", "/tracker/evening_report",
                                    {"tg": tg_id, "queue": queue},
                                    long_timeout=True
                                )
                                
                                if sc2 == 200:
                                    issues = data2.get("issues", [])
                                    count = data2.get("count", 0)
                                    
                                    if issues:
                                        title = f"🌆 *Вечерний отчёт — {queue}* ({count} закрыто)\n"
                                        text = format_issue_list(issues, title, FORMAT_WORKER)
                                    else:
                                        text = f"🌆 *Вечерний отчёт — {queue}*\n\nСегодня ничего не закрыто"
                                    
                                    await safe_send_markdown(state.bot, tg_id, text)
                                    
                                    last_sent_evening[tg_id] = today_str
                            except Exception:
                                pass
                            
                            await asyncio.sleep(0.5)
                    
                    # --- Send итоговый отчёт ---
                    sc_r, data_r = await api_request("GET", "/tg/users_with_report", {})
                    if sc_r == 200:
                        for user in data_r.get("users", []):
                            tg_id = user.get("tg_id")
                            queue = user.get("report_queue", "")
                            period = user.get("report_period", "week")
                            
                            if not tg_id or not queue:
                                continue
                            
                            if last_sent_report.get(tg_id) == today_str:
                                continue
                            
                            try:
                                sc3, data3 = await api_request(
                                    "GET", "/tracker/queue_stats",
                                    {"tg": tg_id, "queue": queue, "period": period},
                                    long_timeout=True
                                )
                                
                                if sc3 == 200:
                                    created = data3.get("created", 0)
                                    in_progress = data3.get("in_progress", 0)
                                    closed = data3.get("closed", 0)
                                    
                                    today_fmt = now.strftime("%d.%m")
                                    if period == "today":
                                        period_text = f"сегодня {today_fmt}"
                                    elif period == "week":
                                        week_ago = now - timedelta(days=7)
                                        period_text = f"за неделю {week_ago.strftime('%d.%m')} — {today_fmt}"
                                    elif period == "month":
                                        month_ago = now - timedelta(days=30)
                                        period_text = f"за месяц {month_ago.strftime('%d.%m')} — {today_fmt}"
                                    else:
                                        period_text = period
                                    
                                    text = (
                                        f"📊 *Итоговый отчёт — {queue}* ({period_text})\n\n"
                                        f"📝 Создано: {created}\n"
                                        f"🔄 В работе: {in_progress}\n"
                                        f"✅ Закрыто: {closed}"
                                    )
                                    
                                    await state.bot.send_message(
                                        tg_id, text, parse_mode="Markdown"
                                    )
                                    
                                    last_sent_report[tg_id] = today_str
                            except Exception:
                                pass
                            
                            await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"Evening report worker error: {e}")
        
        # Check every 5 minutes
        try:
            await asyncio.wait_for(state.shutdown_event.wait(), timeout=300)
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
        "morning_report": asyncio.create_task(morning_report_worker()),
        "evening_report": asyncio.create_task(evening_report_worker()),
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
