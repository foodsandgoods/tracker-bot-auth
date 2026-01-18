import os
import asyncio

import httpx
from fastapi import FastAPI
import uvicorn

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")  # auth-service url
PORT = int(os.getenv("PORT", "10000"))

router = Router()
app = FastAPI()


@app.get("/ping")
async def ping():
    return "pong"


def _fmt_item(item: dict) -> str:
    checked = item.get("checked", False)
    mark = "✅" if checked else "⬜"
    text = (item.get("text") or "").strip()
    item_id = item.get("id")
    text = text.replace("\n", " ").strip()
    if len(text) > 120:
        text = text[:117] + "..."
    return f"{mark} {text} (id: {item_id})"


@router.message(Command("start"))
async def start(m: Message):
    await m.answer(
        "Привет! Я работаю с Yandex Tracker.\n\n"
        "/connect — привязать аккаунт\n"
        "/me — проверить доступ\n\n"
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


@router.message(Command("cl_my"))
async def cl_my(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан (адрес auth-сервиса).")
        return

    tg_id = m.from_user.id
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{BASE_URL}/tracker/checklist/assigned", params={"tg": tg_id, "limit": 10})

    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}

    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    issues = data.get("issues", [])
    if not issues:
        await m.answer("Не нашёл задач, где ты назначен исполнителем пункта чеклиста (в выборке за 30 дней).")
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
        r = await client.get(f"{BASE_URL}/tracker/checklist/assigned_unchecked", params={"tg": tg_id, "limit": 10})

    data = r.json() if "application/json" in r.headers.get("content-type", "") else {"raw": r.text}

    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    issues = data.get("issues", [])
    if not issues:
        await m.answer("Не нашёл неотмеченных пунктов чеклиста на тебе (в выборке за 30 дней).")
        return

    lines = ["Неотмеченные пункты чеклиста, где ты исполнитель:"]
    for iss in issues:
        lines.append(f"\n{iss.get('key')} — {iss.get('summary')}\n{iss.get('url')}")
        for item in iss.get("items", []):
            # здесь items уже только unchecked, но отметку оставим наглядно
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
        # часто тут будет 404/405 если отличается путь/метод — тогда пришли мне этот ответ
        await m.answer(f"Tracker вернул {sc}: {data.get('response')}")
        await m.answer("Если это 404/405 — пришли сюда ответ целиком, я поправлю метод/URL под твою версию API.")


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
