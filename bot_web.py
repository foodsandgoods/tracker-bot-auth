import os
import asyncio

import httpx
from fastapi import FastAPI
import uvicorn

from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

router = Router()
app = FastAPI()


@app.get("/ping")
async def ping():
    return "pong"


@router.message(Command("start"))
async def start(m: Message):
    await m.answer(
        "Привет! Я подключаю Yandex Tracker.\n\n"
        "/connect — привязать аккаунт\n"
        "/me — проверить доступ"
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

    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.get(f"{BASE_URL}/tracker/me_by_tg", params={"tg": tg_id})

    try:
        data = r.json()
    except Exception:
        await m.answer(f"Сервер вернул не-JSON: {r.text[:500]}")
        return

    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    status_code = data.get("status_code")
    if status_code == 200:
        user = data.get("response", {})
        login = user.get("login") or user.get("display") or user.get("uid") or "unknown"
        await m.answer(f"Ок. Tracker user: {login}")
    else:
        await m.answer(f"Tracker вернул {status_code}: {data.get('response')}")


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
