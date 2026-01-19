import os
import asyncio
import re
import time
from datetime import datetime
from functools import wraps
from typing import Optional, Tuple, Dict, List, Any

import httpx
from fastapi import FastAPI
import uvicorn

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, CallbackQuery, BotCommand
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")  # auth-service url
PORT = int(os.getenv("PORT", "10000"))

router = Router()
app = FastAPI()

# Cache for last checklist results (per user)
_last_checklist_cache: Dict[int, Dict[str, Any]] = {}  # tg_id -> {"issues": [...], "item_mapping": {num: (issue_key, item_id)}}

# Cache for issue summaries (per issue_key)
_summary_cache: Dict[str, Dict[str, Any]] = {}  # issue_key -> {"summary": "...", "updated_at": timestamp}

# HTTP client constants
HTTP_LIMITS = httpx.Limits(max_keepalive_connections=5, max_connections=10)
HTTP_TIMEOUT_SHORT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
HTTP_TIMEOUT_LONG = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=5.0)
HTTP_TIMEOUT_DEFAULT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)


@app.get("/ping")
async def ping():
    return "pong"


# =========================
# Helpers
# =========================
def _fmt_item(item: dict) -> str:
    """Format checklist item for display"""
    mark = "✅" if item.get("checked", False) else "⬜"
    text = (item.get("text") or "").strip().replace("\n", " ")
    if len(text) > 100:
        text = text[:97] + "..."
    return f"{mark} {text}"


def _fmt_date(date_str: Optional[str]) -> str:
    """Format ISO date string to readable format (DD.MM.YYYY HH:MM)"""
    if not date_str:
        return ""
    try:
        # Normalize timezone format: "2024-01-15T10:30:00.000+0300" -> "2024-01-15T10:30:00+03:00"
        clean_date = date_str.replace("Z", "+00:00")
        if "+" in clean_date:
            parts = clean_date.split("+", 1)
            if len(parts) == 2 and len(parts[1]) == 4 and ":" not in parts[1]:
                # Format: +0300 -> +03:00
                clean_date = f"{parts[0]}+{parts[1][:2]}:{parts[1][2:]}"
        dt = datetime.fromisoformat(clean_date)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        try:
            # Fallback: parse without timezone
            dt = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            return date_str[:16] if len(date_str) > 16 else date_str


def _parse_response(r: httpx.Response) -> dict:
    """Parse HTTP response to dict"""
    content_type = r.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}
    return {"raw": r.text}


async def _api_get(path: str, params: dict) -> Tuple[int, dict]:
    """Make GET request to API"""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SHORT, limits=HTTP_LIMITS) as client:
        r = await client.get(f"{BASE_URL}{path}", params=params)
    return r.status_code, _parse_response(r)


async def _api_post(path: str, params: dict) -> Tuple[int, dict]:
    """Make POST request to API"""
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SHORT, limits=HTTP_LIMITS) as client:
        r = await client.post(f"{BASE_URL}{path}", params=params)
    return r.status_code, _parse_response(r)


def _kb_settings_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Очереди", callback_data="st:queues")
    kb.button(text="Период", callback_data="st:days")
    kb.button(text="Лимит", callback_data="st:limit")
    kb.button(text="Закрыть", callback_data="st:close")
    kb.adjust(2, 1, 1)
    return kb.as_markup()


def _kb_settings_queues(queues: list[str]) -> InlineKeyboardMarkup:
    qs = {q.upper() for q in queues}
    kb = InlineKeyboardBuilder()
    for q in ["INV", "DOC", "HR"]:
        mark = "✅" if q in qs else "⬜"
        kb.button(text=f"{mark} {q}", callback_data=f"st:qtoggle:{q}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(3, 1)
    return kb.as_markup()


def _kb_settings_days(days: int) -> InlineKeyboardMarkup:
    options = [7, 15, 30, 90, 180]
    kb = InlineKeyboardBuilder()
    for d in options:
        mark = "✅" if int(days) == d else "⬜"
        kb.button(text=f"{mark} {d}д", callback_data=f"st:dset:{d}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(3, 2)
    return kb.as_markup()


def _kb_settings_limit(limit: int) -> InlineKeyboardMarkup:
    options = [5, 10, 15, 20, 30, 50]
    kb = InlineKeyboardBuilder()
    for l in options:
        mark = "✅" if int(limit) == l else "⬜"
        kb.button(text=f"{mark} {l}", callback_data=f"st:lset:{l}")
    kb.button(text="Назад", callback_data="st:back")
    kb.adjust(3, 3)
    return kb.as_markup()


def _render_settings_text(queues: list[str], days: int, limit: int) -> str:
    q = ", ".join(queues) if queues else "(все очереди)"
    return (
        "Настройки поиска чеклистов:\n"
        f"• Очереди: {q}\n"
        f"• Период: {days} дней\n"
        f"• Лимит результатов: {limit}\n\n"
        "Выбери, что изменить:"
    )


async def _get_settings(tg_id: int) -> Optional[Tuple[List[str], int, int]]:
    """Get user settings from API"""
    sc, data = await _api_get("/tg/settings", {"tg": tg_id})
    if sc != 200:
        return None
    queues = data.get("queues", []) or []
    days = int(data.get("days", 30))
    limit = int(data.get("limit", 10))
    return queues, days, limit


def _build_checklist_lines(
    issues: List[dict],
    header: str,
    include_checked: bool = True,
    add_buttons: bool = False
) -> Tuple[List[str], Optional[InlineKeyboardBuilder], Dict[int, Tuple[str, str]]]:
    """Build checklist response text and optional keyboard"""
    lines = [header]
    kb = InlineKeyboardBuilder() if add_buttons else None
    issue_counter = 1
    item_counter = 1
    item_mapping = {}
    
    for iss in issues:
        updated = _fmt_date(iss.get("updatedAt"))
        date_str = f" (обновлено: {updated})" if updated else ""
        lines.append(f"\n{issue_counter}. {iss.get('key')} — {iss.get('summary')}{date_str}\n{iss.get('url')}")
        
        for item in iss.get("items", []):
            if include_checked or not item.get("checked", False):
                lines.append("  " + _fmt_item(item))
                issue_key = iss.get('key')
                item_id = item.get('id')
                item_mapping[item_counter] = (issue_key, item_id)
                
                if add_buttons and not item.get("checked", False):
                    kb.button(text=f"✅ {item_counter}", callback_data=f"check:{issue_key}:{item_id}:{item_counter}")
                item_counter += 1
        issue_counter += 1
    
    if kb:
        kb.adjust(3)
    
    return lines, kb, item_mapping


def _require_base_url(func):
    """Decorator to check BASE_URL before handler execution"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if not BASE_URL:
            message = args[0] if args else None
            if message and hasattr(message, 'answer'):
                await message.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
            return
        return await func(*args, **kwargs)
    return wrapper


# =========================
# Bot handlers
# =========================
@router.message(Command("start"))
async def start(m: Message):
    await m.answer(
        "Привет! Я работаю с Yandex Tracker.\n\n"
        "Используй /menu для просмотра всех команд."
    )


@router.message(Command("menu"))
async def menu(m: Message):
    menu_text = (
        "📋 Меню команд:\n\n"
        "🔗 Подключение:\n"
        "/connect — привязать аккаунт\n"
        "/me — проверить доступ\n\n"
        "⚙️ Настройки:\n"
        "/settings — настройки очередей, периода и лимита\n\n"
        "✅ Чеклисты:\n"
        "/cl_my — задачи, где ты назначен исполнителем пункта чеклиста\n"
        "/cl_my_open — ожидают мое согласование\n"
        "/cl_done ISSUE-KEY ITEM_ID — отметить пункт чеклиста\n\n"
        "🤖 ИИ функции:\n"
        "/summary ISSUE-KEY — составить резюме задачи"
    )
    await m.answer(menu_text)


@router.message(Command("connect"))
@_require_base_url
async def connect(m: Message):
    tg_id = m.from_user.id
    url = f"{BASE_URL}/oauth/start?tg={tg_id}"
    await m.answer(
        "Открой ссылку и заверши авторизацию:\n"
        f"{url}\n\n"
        "После этого вернись и напиши /me"
    )


@router.message(Command("me"))
@_require_base_url
async def me(m: Message):
    tg_id = m.from_user.id
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_DEFAULT) as client:
        r = await client.get(f"{BASE_URL}/tracker/me_by_tg", params={"tg": tg_id})

    data = _parse_response(r)
    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    sc = data.get("status_code")
    if sc == 200:
        user = data.get("response", {})
        login = user.get("login") or user.get("display") or user.get("uid") or "unknown"
        await m.answer(f"Ок. Tracker user: {login}")
    else:
        await m.answer(f"Tracker вернул {sc}: {data.get('response')}")


@router.message(Command("settings"))
@_require_base_url
async def settings_cmd(m: Message):
    tg_id = m.from_user.id
    settings = await _get_settings(tg_id)
    if settings is None:
        await m.answer("Ошибка: не удалось получить настройки")
        return

    queues, days, limit = settings
    await m.answer(_render_settings_text(queues, days, limit), reply_markup=_kb_settings_main())


@router.callback_query()
async def settings_callbacks(c: CallbackQuery):
    # Handle checklist item check callbacks
    if c.data and c.data.startswith("check:"):
        parts = c.data.split(":")
        if len(parts) >= 3:
            _, issue_key, item_id = parts[0], parts[1], parts[2]
            item_num = parts[3] if len(parts) > 3 else None
            tg_id = c.from_user.id
            
            try:
                # Call API to check the item
                # Note: FastAPI will convert string "true" to bool True
                sc, data = await _api_post("/tracker/checklist/check", {
                    "tg": str(tg_id),
                    "issue": issue_key,
                    "item": item_id,
                    "checked": True
                })
                
                if sc == 200:
                    await c.answer("✅ Пункт отмечен!", show_alert=False)
                    # Update message to show item as checked
                    if c.message:
                        text = c.message.text or ""
                        # Replace "⬜" with "✅" for this specific item
                        if item_num:
                            # Find the line with this item by searching near button number context
                            lines = text.split('\n')
                            new_lines = []
                            found = False
                            for i, line in enumerate(lines):
                                if not found and "⬜" in line:
                                    # Check if this line is near the button number in the text
                                    text_pos = text.find(line)
                                    if item_num in text[max(0, text_pos-200):text_pos+200]:
                                        new_lines.append(line.replace("⬜", "✅", 1))
                                        found = True
                                    else:
                                        new_lines.append(line)
                                else:
                                    new_lines.append(line)
                            new_text = '\n'.join(new_lines) if found else text.replace("⬜", "✅", 1)
                        else:
                            new_text = text.replace("⬜", "✅", 1)
                        
                        # Remove the button for this item
                        if c.message.reply_markup:
                            new_kb = InlineKeyboardBuilder()
                            for row in c.message.reply_markup.inline_keyboard:
                                for button in row:
                                    if button.callback_data != c.data:
                                        new_kb.button(text=button.text, callback_data=button.callback_data)
                            new_kb.adjust(3)
                            await c.message.edit_text(
                                new_text, 
                                reply_markup=new_kb.as_markup() if new_kb.buttons else None
                            )
                        else:
                            await c.message.edit_text(new_text)
                else:
                    error_msg = data.get("error", "Не удалось отметить") if isinstance(data, dict) else str(data)[:100]
                    await c.answer(f"Ошибка: {error_msg}", show_alert=True)
            except Exception as e:
                await c.answer(f"Ошибка: {str(e)[:100]}", show_alert=True)
        return
    
    # Handle settings callbacks
    if not c.data or not c.data.startswith("st:"):
        return

    if not BASE_URL:
        await c.answer("BASE_URL не задан", show_alert=True)
        return

    tg_id = c.from_user.id

    sc, data = await _api_get("/tg/settings", {"tg": tg_id})
    if sc != 200:
        await c.answer(f"Ошибка {sc}", show_alert=True)
        return

    queues = data.get("queues", []) or []
    days = int(data.get("days", 30))
    limit = int(data.get("limit", 10))

    parts = c.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""
    arg = parts[2] if len(parts) > 2 else ""

    if action == "close":
        if c.message:
            await c.message.edit_reply_markup(reply_markup=None)
        await c.answer("Ок")
        return

    if action == "back":
        if c.message:
            await c.message.edit_text(_render_settings_text(queues, days, limit), reply_markup=_kb_settings_main())
        await c.answer()
        return

    if action == "queues":
        if c.message:
            await c.message.edit_text(
                "Настройки → Очереди (нажми чтобы включить/выключить):",
                reply_markup=_kb_settings_queues(queues),
            )
        await c.answer()
        return

    if action == "days":
        if c.message:
            await c.message.edit_text(
                "Настройки → Период (за сколько дней искать обновлённые задачи):",
                reply_markup=_kb_settings_days(days),
            )
        await c.answer()
        return

    if action == "limit":
        if c.message:
            await c.message.edit_text(
                "Настройки → Лимит результатов (сколько задач выводить):",
                reply_markup=_kb_settings_limit(limit),
            )
        await c.answer()
        return

    if action == "qtoggle":
        q = arg.upper()
        qs = [x.upper() for x in queues]
        if q in qs:
            qs = [x for x in qs if x != q]
        else:
            qs.append(q)

        sc2, data2 = await _api_post("/tg/settings/queues", {"tg": tg_id, "queues": ",".join(qs)})
        if sc2 != 200:
            await c.answer(f"Ошибка {sc2}", show_alert=True)
            return

        queues2 = data2.get("queues", []) or []
        if c.message:
            await c.message.edit_reply_markup(reply_markup=_kb_settings_queues(queues2))
        await c.answer("Сохранено")
        return

    if action == "dset":
        try:
            d = int(arg)
        except Exception:
            await c.answer("Некорректное число", show_alert=True)
            return

        sc2, data2 = await _api_post("/tg/settings/days", {"tg": tg_id, "days": d})
        if sc2 != 200:
            await c.answer(f"Ошибка {sc2}", show_alert=True)
            return

        days2 = int(data2.get("days", d))
        if c.message:
            await c.message.edit_reply_markup(reply_markup=_kb_settings_days(days2))
        await c.answer("Сохранено")
        return

    if action == "lset":
        try:
            l = int(arg)
        except Exception:
            await c.answer("Некорректное число", show_alert=True)
            return

        sc2, data2 = await _api_post("/tg/settings/limit", {"tg": tg_id, "limit": l})
        if sc2 != 200:
            await c.answer(f"Ошибка {sc2}", show_alert=True)
            return

        limit2 = int(data2.get("limit", l))
        if c.message:
            await c.message.edit_reply_markup(reply_markup=_kb_settings_limit(limit2))
        await c.answer("Сохранено")
        return

    await c.answer()


async def _fetch_checklist(tg_id: int, endpoint: str, limit: int) -> Tuple[Optional[dict], Optional[str]]:
    """Fetch checklist data from API"""
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_LONG, limits=HTTP_LIMITS) as client:
            r = await client.get(f"{BASE_URL}/tracker/checklist/{endpoint}", params={"tg": tg_id, "limit": limit})
        
        data = _parse_response(r)
        if r.status_code != 200:
            return None, f"Ошибка {r.status_code}: {str(data)[:500]}"
        return data, None
    except httpx.TimeoutException:
        return None, "⏱ Превышено время ожидания ответа от сервера. Попробуйте позже."
    except Exception as e:
        return None, f"❌ Произошла ошибка: {str(e)[:300]}"


@router.message(Command("cl_my"))
@_require_base_url
async def cl_my(m: Message):
    try:
        tg_id = m.from_user.id
        settings = await _get_settings(tg_id)
        limit = settings[2] if settings else 10
        
        data, error = await _fetch_checklist(tg_id, "assigned", limit)
        if error:
            await m.answer(error)
            return

        issues = data.get("issues", [])
        if not issues:
            days = data.get("settings", {}).get("days", 30)
            await m.answer(f"Не нашёл задач, где ты назначен исполнителем пункта чеклиста (в выборке за {days} дней).")
            return

        lines, _, item_mapping = _build_checklist_lines(issues, "Задачи с чеклистами, где ты исполнитель пункта:", include_checked=True)
        
        # Save to cache
        _last_checklist_cache[tg_id] = {"issues": issues, "item_mapping": item_mapping}
        
        await m.answer("\n".join(lines))
    except Exception as e:
        await m.answer(f"❌ Произошла ошибка: {str(e)[:300]}")


@router.message(Command("cl_my_open"))
@_require_base_url
async def cl_my_open(m: Message):
    try:
        tg_id = m.from_user.id
        settings = await _get_settings(tg_id)
        limit = settings[2] if settings else 10
        
        data, error = await _fetch_checklist(tg_id, "assigned_unchecked", limit)
        if error:
            await m.answer(error)
            return

        issues = data.get("issues", [])
        if not issues:
            days = data.get("settings", {}).get("days", 30)
            await m.answer(f"Не нашёл пунктов, ожидающих твоего согласования (в выборке за {days} дней).")
            return

        lines, kb, item_mapping = _build_checklist_lines(
            issues, "Ожидают мое согласование:", 
            include_checked=False, add_buttons=True
        )
        
        if not item_mapping:
            # No unchecked items
            await m.answer("\n".join(lines))
            return
        
        lines.append("\n\nНажмите кнопку с номером, чтобы отметить пункт")
        
        # Save to cache
        _last_checklist_cache[tg_id] = {"issues": issues, "item_mapping": item_mapping}
        
        # Split message if too long (Telegram limit is 4096 chars)
        message_text = "\n".join(lines)
        if len(message_text) > 4000:
            # Split into chunks - send first with buttons, rest without
            first_part = "\n".join(lines[:-1])  # All except last line
            await m.answer(first_part[:4000], reply_markup=kb.as_markup())
            if len(message_text) > 4000:
                await m.answer(message_text[4000:])
        else:
            await m.answer(message_text, reply_markup=kb.as_markup())
    except Exception as e:
        await m.answer(f"❌ Произошла ошибка: {str(e)[:300]}")


@router.message(Command("done"))
@_require_base_url
async def done_cmd(m: Message):
    """Mark checklist item by number from last /cl_my or /cl_my_open result"""
    parts = (m.text or "").split()
    if len(parts) != 2:
        await m.answer("Использование: /done N\nгде N — номер пункта из последнего списка (/cl_my или /cl_my_open)")
        return

    try:
        item_num = int(parts[1])
    except ValueError:
        await m.answer("Номер должен быть числом. Использование: /done N")
        return

    tg_id = m.from_user.id
    cache = _last_checklist_cache.get(tg_id)
    if not cache or not cache.get("item_mapping"):
        await m.answer("❌ Кэш пуст. Сначала выполните /cl_my или /cl_my_open")
        return

    item_mapping = cache.get("item_mapping", {})
    if item_num not in item_mapping:
        await m.answer(f"❌ Пункт с номером {item_num} не найден в последнем списке")
        return

    issue_key, item_id = item_mapping[item_num]

    try:
        sc, data = await _api_post("/tracker/checklist/check", {
            "tg": str(tg_id),
            "issue": issue_key,
            "item": item_id,
            "checked": True
        })

        if sc == 200:
            await m.answer(f"✅ Отмечен пункт {item_num} в задаче {issue_key}")
        else:
            error_msg = data.get("error", "Не удалось отметить") if isinstance(data, dict) else str(data)[:100]
            await m.answer(f"❌ Ошибка {sc}: {error_msg}")
    except Exception as e:
        await m.answer(f"❌ Произошла ошибка: {str(e)[:200]}")


@router.message(Command("summary"))
@_require_base_url
async def summary_cmd(m: Message):
    """Generate AI summary for issue"""
    parts = (m.text or "").split()
    if len(parts) != 2:
        await m.answer("Использование: /summary ISSUE-KEY\nПример: /summary INV-123")
        return
    
    issue_key = parts[1].upper().strip()
    tg_id = m.from_user.id
    
    # Проверяем кэш (кэш действителен 1 час)
    cache_key = issue_key
    if cache_key in _summary_cache:
        cached = _summary_cache[cache_key]
        cache_age = time.time() - cached.get("updated_at", 0)
        if cache_age < 3600:  # 1 час
            summary_text = cached.get("summary", "")
            issue_url = cached.get("issue_url", f"https://tracker.yandex.ru/{issue_key}")
            response_text = (
                f"📋 Резюме задачи {issue_key} (из кэша):\n\n"
                f"{summary_text}\n\n"
                f"🔗 {issue_url}"
            )
            await m.answer(response_text)
            return
    
    # Показываем индикатор загрузки
    loading_msg = await m.answer("🤖 Генерирую резюме...")
    
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_LONG, limits=HTTP_LIMITS) as client:
            r = await client.get(
                f"{BASE_URL}/tracker/issue/{issue_key}/summary",
                params={"tg": tg_id}
            )
        
        data = _parse_response(r)
        
        if r.status_code != 200:
            error_msg = data.get("error", "Неизвестная ошибка") if isinstance(data, dict) else str(data)[:200]
            
            # Детальные сообщения об ошибках
            if r.status_code == 401:
                await loading_msg.edit_text("❌ Ошибка авторизации. Выполните /connect для привязки аккаунта.")
            elif r.status_code == 404:
                await loading_msg.edit_text(f"❌ Задача {issue_key} не найдена или у вас нет доступа к ней.")
            elif r.status_code == 500:
                if "AI service" in error_msg or "GPTunneL" in error_msg:
                    await loading_msg.edit_text(f"❌ Ошибка ИИ-сервиса: {error_msg}\n\nПроверьте настройки GPTunneL API.")
                else:
                    await loading_msg.edit_text(f"❌ Внутренняя ошибка сервера: {error_msg}")
            else:
                await loading_msg.edit_text(f"❌ Ошибка {r.status_code}: {error_msg}")
            return
        
        summary = data.get("summary", "")
        issue_url = data.get("issue_url", f"https://tracker.yandex.ru/{issue_key}")
        
        if not summary:
            await loading_msg.edit_text("❌ Не удалось сгенерировать резюме. Попробуйте позже.")
            return
        
        # Сохраняем в кэш
        _summary_cache[cache_key] = {
            "summary": summary,
            "issue_url": issue_url,
            "updated_at": time.time()
        }
        
        # Форматируем ответ
        response_text = (
            f"📋 Резюме задачи {issue_key}:\n\n"
            f"{summary}\n\n"
            f"🔗 {issue_url}"
        )
        
        # Telegram ограничение на длину сообщения - разбиваем если нужно
        if len(response_text) > 4000:
            # Отправляем первую часть
            await loading_msg.edit_text(response_text[:4000])
            # Отправляем остальное
            await m.answer(response_text[4000:])
        else:
            await loading_msg.edit_text(response_text)
            
    except httpx.TimeoutException:
        await loading_msg.edit_text("⏱ Превышено время ожидания ответа от сервера. Попробуйте позже.")
    except Exception as e:
        await loading_msg.edit_text(f"❌ Произошла ошибка: {str(e)[:300]}")


@router.message(Command("cl_done"))
@_require_base_url
async def cl_done(m: Message):
    parts = (m.text or "").split()
    if len(parts) != 3:
        await m.answer("Использование: /cl_done ISSUE-KEY ITEM_ID")
        return

    _cmd, issue_key, item_id = parts
    tg_id = m.from_user.id

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_DEFAULT) as client:
        r = await client.post(
            f"{BASE_URL}/tracker/checklist/check",
            params={"tg": tg_id, "issue": issue_key, "item": item_id, "checked": True},
        )

    data = _parse_response(r)
    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    sc = data.get("status_code")
    if sc in (200, 204):
        await m.answer(f"Готово: отметил пункт чеклиста {item_id} в задаче {issue_key}")
    else:
        await m.answer(f"Tracker вернул {sc}: {data.get('response')}")
        await m.answer("Если это 404/405 — пришли сюда ответ целиком, я поправлю метод/URL под твою версию API.")


# =========================
# Run web + bot
# =========================
async def setup_bot_commands(bot: Bot):
    """Set up bot commands menu"""
    commands = [
        BotCommand(command="menu", description="📋 Показать меню команд"),
        BotCommand(command="connect", description="🔗 Привязать аккаунт"),
        BotCommand(command="me", description="👤 Проверить доступ"),
        BotCommand(command="settings", description="⚙️ Настройки"),
        BotCommand(command="cl_my", description="✅ Мои задачи с чеклистами"),
        BotCommand(command="cl_my_open", description="⬜ Ожидают мое согласование"),
        BotCommand(command="done", description="✅ Отметить пункт по номеру"),
        BotCommand(command="summary", description="🤖 Резюме задачи (ИИ)"),
    ]
    await bot.set_my_commands(commands)


async def run_bot():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=BOT_TOKEN)
    await setup_bot_commands(bot)
    
    # Очищаем старые обновления перед запуском polling
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print(f"Warning: Could not delete webhook: {e}")
    
    dp = Dispatcher()
    dp.include_router(router)
    
    # Используем close_bot_session=False чтобы избежать конфликтов
    try:
        await dp.start_polling(bot, close_bot_session=False, allowed_updates=["message", "callback_query"])
    except Exception as e:
        print(f"Bot polling error: {e}")
        raise


async def run_web():
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    # Запускаем веб-сервер и бота параллельно
    try:
        await asyncio.gather(run_web(), run_bot())
    except KeyboardInterrupt:
        print("Shutting down...")
    except Exception as e:
        print(f"Error in main: {e}")
        # Продолжаем работу веб-сервера даже если бот упал
        try:
            await run_web()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
