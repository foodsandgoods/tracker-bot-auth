import os
import asyncio

import httpx
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")

router = Router()


@router.message(Command("start"))
async def start(m: Message):
    await m.answer(
        "Привет! Я подключаю Yandex Tracker.\n\n"
        "1) /connect — привязать аккаунт\n"
        "2) /me — проверить доступ"
    )


@router.message(Command("connect"))
async def connect(m: Message):
    if not BASE_URL:
        await m.answer("Ошибка: BASE_URL не задан.")
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
        await m.answer("Ошибка: BASE_URL не задан.")
        return

    tg_id = m.from_user.id
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{BASE_URL}/tracker/me", params={"tg": tg_id})

    try:
        data = r.json()
    except Exception:
        await m.answer(f"Не смог прочитать ответ сервера: {r.text}")
        return

    if r.status_code != 200:
        await m.answer(f"Ошибка {r.status_code}: {data}")
        return

    status_code = data.get("status_code")
    if status_code == 200:
        user = data.get("response", {})
        await m.answer(f"Ок. Tracker user: {user.get('login', user.get('uid', 'unknown'))}")
    else:
        await m.answer(f"Tracker вернул {status_code}: {data.get('response')}")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
