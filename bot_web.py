import os
import asyncio
import re
from datetime import datetime

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


@app.get("/ping")
async def ping():
    return "pong"


# =========================
# Helpers
# =========================
def _fmt_item(item: dict, num: int = None) -> str:
    checked = item.get("checked", False)
    mark = "✅" if checked else "⬜"
    text = (item.get("text") or "").strip()
    item_id = item.get("id")
    text = text.replace("\n", " ").strip()
    if len(text) > 100:
        text = text[:97] + "..."
    num_str = f"{num}. " if num is not None else ""
    return f"{num_str}{mark} {text}"


def _fmt_date(date_str: str | None) -> str:
    """Format ISO date string to readable format (DD.MM.YYYY HH:MM)"""
    if not date_str:
        return ""
    try:
        # Yandex Tracker typically uses ISO format: "2024-01-15T10:30:00.000+0300" or "2024-01-15T10:30:00Z"
        # Remove timezone info if present and parse
        clean_date = date_str.replace("Z", "+00:00")
        # Handle format like "2024-01-15T10:30:00.000+0300" -> "2024-01-15T10:30:00+03:00"
        if "+" in clean_date and len(clean_date.split("+")[1]) == 4:
            tz_part = clean_date.split("+")[1]
            clean_date = clean_date.replace(f"+{tz_part}", f"+{tz_part[:2]}:{tz_part[2:]}")
        dt = datetime.fromisoformat(clean_date)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        # Fallback: try simple format without timezone
        try:
            dt = datetime.strptime(date_str[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            # Last resort: return first 16 chars if available
            return date_str[:16] if len(date_str) > 16 else date_str


async def _api_get(path: str, params: dict) -> tuple[int, dict]:
    # Optimize HTTP client with connection pooling and shorter timeout
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    timeout = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        r = await client.get(f"{BASE_URL}{path}", params=params)
    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}
    return r.status_code, data


async def _api_post(path: str, params: dict) -> tuple[int, dict]:
    # Optimize HTTP client with connection pooling and shorter timeout
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    timeout = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        r = await client.post(f"{BASE_URL}{path}", params=params)
    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}
    return r.status_code, data


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


async def _get_settings(tg_id: int) -> tuple[list[str], int, int] | tuple[None, None, None]:
    sc, data = await _api_get("/tg/settings", {"tg": tg_id})
    if sc != 200:
        return None, None, None
    queues = data.get("queues", []) or []
    days = int(data.get("days", 30))
    limit = int(data.get("limit", 10))
    return queues, days, limit


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
        "/cl_my_open — только неотмеченные пункты\n"
        "/cl_done ISSUE-KEY ITEM_ID — отметить пункт чеклиста"
    )
    await m.answer(menu_text)


@router.message(Command("connect"))
async def connect(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    tg_id = m.from_user.id
    url = f"{BASE_URL}/oauth/start?tg={tg_id}"
    await m.answer(
        "Открой ссылку и заверши авторизацию:\n"
        f"{url}\n\n"
        "После этого вернись и напиши /me"
    )


@router.message(Command("me"))
async def me(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    tg_id = m.from_user.id
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE_URL}/tracker/me_by_tg", params={"tg": tg_id})

    try:
        data = r.json()
    except Exception:
        await m.answer(f"Сервер вернул не-JSON: {r.text[:500]}")
        return

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
async def settings_cmd(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    tg_id = m.from_user.id
    sc, data = await _api_get("/tg/settings", {"tg": tg_id})
    if sc != 200:
        await m.answer(f"Ошибка {sc}: {data}")
        return

    queues = data.get("queues", []) or []
    days = int(data.get("days", 30))
    limit = int(data.get("limit", 10))
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
                        # Replace the specific item's unchecked mark with checked
                        if item_num:
                            # Replace "N. ⬜" with "N. ✅" for this specific item
                            import re
                            pattern = rf"{re.escape(item_num)}\. ⬜"
                            new_text = re.sub(pattern, f"{item_num}. ✅", text, count=1)
                        else:
                            # Fallback: replace first unchecked
                            new_text = text.replace("⬜", "✅", 1)
                        
                        # Remove the button for this item
                        if c.message.reply_markup:
                            new_kb = InlineKeyboardBuilder()
                            for row in c.message.reply_markup.inline_keyboard:
                                for button in row:
                                    if button.callback_data != c.data:
                                        new_kb.button(
                                            text=button.text,
                                            callback_data=button.callback_data
                                        )
                            new_kb.adjust(3)
                            await c.message.edit_text(new_text, reply_markup=new_kb.as_markup() if new_kb.buttons else None)
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


@router.message(Command("cl_my"))
async def cl_my(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    try:
        tg_id = m.from_user.id
        # Get limit from settings
        sc, data = await _api_get("/tg/settings", {"tg": tg_id})
        limit = int(data.get("limit", 10)) if sc == 200 else 10
        
        # Optimize HTTP client for checklist requests
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        timeout = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=5.0)  # Longer read timeout for checklist processing
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            r = await client.get(f"{BASE_URL}/tracker/checklist/assigned", params={"tg": tg_id, "limit": limit})

        try:
            data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}
        except Exception as e:
            await m.answer(f"Ошибка парсинга ответа: {str(e)[:200]}")
            return

        if r.status_code != 200:
            await m.answer(f"Ошибка {r.status_code}: {str(data)[:500]}")
            return

        issues = data.get("issues", [])
        if not issues:
            settings = data.get("settings") or {}
            days = settings.get("days", 30)
            await m.answer(f"Не нашёл задач, где ты назначен исполнителем пункта чеклиста (в выборке за {days} дней).")
            return

        lines = ["Задачи с чеклистами, где ты исполнитель пункта:"]
        issue_counter = 1
        item_counter = 1
        for iss in issues:
            updated = _fmt_date(iss.get("updatedAt"))
            date_str = f" (обновлено: {updated})" if updated else ""
            # Add issue number before ISSUE-KEY
            lines.append(f"\n{issue_counter}. {iss.get('key')} — {iss.get('summary')}{date_str}\n{iss.get('url')}")
            for item in iss.get("items", []):
                lines.append("  " + _fmt_item(item, item_counter))
                item_counter += 1
            issue_counter += 1

        await m.answer("\n".join(lines))
    except httpx.TimeoutException:
        await m.answer("⏱ Превышено время ожидания ответа от сервера. Попробуйте позже.")
    except Exception as e:
        await m.answer(f"❌ Произошла ошибка: {str(e)[:300]}")


@router.message(Command("cl_my_open"))
async def cl_my_open(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    try:
        tg_id = m.from_user.id
        # Get limit from settings
        sc, data = await _api_get("/tg/settings", {"tg": tg_id})
        limit = int(data.get("limit", 10)) if sc == 200 else 10
        
        # Optimize HTTP client for checklist requests
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        timeout = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=5.0)  # Longer read timeout for checklist processing
        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            r = await client.get(f"{BASE_URL}/tracker/checklist/assigned_unchecked", params={"tg": tg_id, "limit": limit})

        try:
            data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}
        except Exception as e:
            await m.answer(f"Ошибка парсинга ответа: {str(e)[:200]}")
            return

        if r.status_code != 200:
            await m.answer(f"Ошибка {r.status_code}: {str(data)[:500]}")
            return

        issues = data.get("issues", [])
        if not issues:
            settings = data.get("settings") or {}
            days = settings.get("days", 30)
            await m.answer(f"Не нашёл неотмеченных пунктов чеклиста на тебе (в выборке за {days} дней).")
            return

        lines = ["Неотмеченные пункты чеклиста, где ты исполнитель:"]
        kb = InlineKeyboardBuilder()
        issue_counter = 1
        item_counter = 1
        item_mapping = {}  # Store mapping: counter -> (issue_key, item_id)
        
        for iss in issues:
            updated = _fmt_date(iss.get("updatedAt"))
            date_str = f" (обновлено: {updated})" if updated else ""
            # Add issue number before ISSUE-KEY
            lines.append(f"\n{issue_counter}. {iss.get('key')} — {iss.get('summary')}{date_str}\n{iss.get('url')}")
            
            for item in iss.get("items", []):
                if not item.get("checked", False):  # Only show unchecked items
                    lines.append("  " + _fmt_item(item, item_counter))
                    # Store mapping for callback
                    issue_key = iss.get('key')
                    item_id = item.get('id')
                    item_mapping[item_counter] = (issue_key, item_id)
                    # Add button for each unchecked item
                    button_text = f"✅ {item_counter}"
                    kb.button(text=button_text, callback_data=f"check:{issue_key}:{item_id}:{item_counter}")
                    item_counter += 1
            issue_counter += 1
        
        if item_counter == 1:
            # No unchecked items
            await m.answer("\n".join(lines))
            return
        
        lines.append(f"\n\nНажмите кнопку с номером, чтобы отметить пункт")
        kb.adjust(3)  # 3 buttons per row
        
        # Split message if too long (Telegram limit is 4096 chars)
        message_text = "\n".join(lines)
        if len(message_text) > 4000:
            # Split into chunks - send first with buttons, rest without
            first_part = "\n".join(lines[:len(lines)-1])  # All except last line
            await m.answer(first_part[:4000], reply_markup=kb.as_markup())
            if len(message_text) > 4000:
                await m.answer(message_text[4000:])
        else:
            await m.answer(message_text, reply_markup=kb.as_markup())
    except httpx.TimeoutException:
        await m.answer("⏱ Превышено время ожидания ответа от сервера. Попробуйте позже.")
    except Exception as e:
        await m.answer(f"❌ Произошла ошибка: {str(e)[:300]}")


@router.message(Command("cl_done"))
async def cl_done(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    parts = (m.text or "").split()
    if len(parts) != 3:
        await m.answer("Использование: /cl_done ISSUE-KEY ITEM_ID")
        return

    _cmd, issue_key, item_id = parts
    tg_id = m.from_user.id

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{BASE_URL}/tracker/checklist/check",
            params={"tg": tg_id, "issue": issue_key, "item": item_id, "checked": True},
        )

    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}

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
        BotCommand(command="cl_my_open", description="📝 Неотмеченные пункты"),
    ]
    await bot.set_my_commands(commands)


async def run_bot():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=BOT_TOKEN)
    await setup_bot_commands(bot)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


async def run_web():
    config = uvicorn.Config(app, host="0.0.0.0", port=PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await asyncio.gather(run_web(), run_bot())


if __name__ == "__main__":
    asyncio.run(main())
