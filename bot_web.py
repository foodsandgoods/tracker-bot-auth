import os
import asyncio

import httpx
from fastapi import FastAPI
import uvicorn

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, CallbackQuery
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
def _fmt_item(item: dict) -> str:
    checked = item.get("checked", False)
    mark = "✅" if checked else "⬜"
    text = (item.get("text") or "").strip()
    item_id = item.get("id")
    text = text.replace("\n", " ").strip()
    if len(text) > 120:
        text = text[:117] + "..."
    return f"{mark} {text} (id: {item_id})"


async def _api_get(path: str, params: dict) -> tuple[int, dict]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE_URL}{path}", params=params)
    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}
    return r.status_code, data


async def _api_post(path: str, params: dict) -> tuple[int, dict]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f"{BASE_URL}{path}", params=params)
    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}
    return r.status_code, data


def _kb_settings_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Очереди", callback_data="st:queues")
    kb.button(text="Период", callback_data="st:days")
    kb.button(text="Закрыть", callback_data="st:close")
    kb.adjust(2, 1)
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


def _render_settings_text(queues: list[str], days: int) -> str:
    q = ", ".join(queues) if queues else "(все очереди)"
    return (
        "Настройки поиска чеклистов:\n"
        f"• Очереди: {q}\n"
        f"• Период: {days} дней\n\n"
        "Выбери, что изменить:"
    )


async def _get_settings(tg_id: int) -> tuple[list[str], int] | tuple[None, None]:
    sc, data = await _api_get("/tg/settings", {"tg": tg_id})
    if sc != 200:
        return None, None
    queues = data.get("queues", []) or []
    days = int(data.get("days", 30))
    return queues, days


# =========================
# Bot handlers
# =========================
@router.message(Command("start"))
async def start(m: Message):
    await m.answer(
        "Привет! Я работаю с Yandex Tracker.\n\n"
        "/connect — привязать аккаунт\n"
        "/me — проверить доступ\n"
        "/settings — настройки очередей и периода\n\n"
        "Чеклисты:\n"
        "/cl_my — задачи, где ты назначен исполнителем пункта чеклиста\n"
        "/cl_my_open — только неотмеченные пункты\n"
        "/cl_done ISSUE-KEY ITEM_ID — отметить пункт чеклиста"
    )


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
    await m.answer(_render_settings_text(queues, days), reply_markup=_kb_settings_main())


@router.callback_query()
async def settings_callbacks(c: CallbackQuery):
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
            await c.message.edit_text(_render_settings_text(queues, days), reply_markup=_kb_settings_main())
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

    await c.answer()


@router.message(Command("cl_my"))
async def cl_my(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    tg_id = m.from_user.id
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{BASE_URL}/tracker/checklist/assigned", params={"tg": tg_id, "limit": 15})

    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}

    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    issues = data.get("issues", [])
    if not issues:
        settings = data.get("settings") or {}
        days = settings.get("days", 30)
        await m.answer(f"Не нашёл задач, где ты назначен исполнителем пункта чеклиста (в выборке за {days} дней).")
        return

    lines = ["Задачи с чеклистами, где ты исполнитель пункта:"]
    for iss in issues:
        lines.append(f"\n{iss.get('key')} — {iss.get('summary')}\n{iss.get('url')}")
        for item in iss.get("items", []):
            lines.append("  " + _fmt_item(item))

    await m.answer("\n".join(lines))


@router.message(Command("cl_my_open"))
async def cl_my_open(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    tg_id = m.from_user.id
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{BASE_URL}/tracker/checklist/assigned_unchecked", params={"tg": tg_id, "limit": 15})

    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}

    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    issues = data.get("issues", [])
    if not issues:
        settings = data.get("settings") or {}
        days = settings.get("days", 30)
        await m.answer(f"Не нашёл неотмеченных пунктов чеклиста на тебе (в выборке за {days} дней).")
        return

    lines = ["Неотмеченные пункты чеклиста, где ты исполнитель:"]
    for iss in issues:
        lines.append(f"\n{iss.get('key')} — {iss.get('summary')}\n{iss.get('url')}")
        for item in iss.get("items", []):
            lines.append("  " + _fmt_item(item))

    lines.append("\nЧтобы отметить пункт: /cl_done ISSUE-KEY ITEM_ID")
    await m.answer("\n".join(lines))


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
async def run_bot():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=BOT_TOKEN)
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
