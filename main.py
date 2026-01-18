import os
import secrets
from datetime import datetime, timezone

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse

app = FastAPI()

# ===== ENV =====
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")
YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID")
YANDEX_CLIENT_SECRET = os.getenv("YANDEX_CLIENT_SECRET")
YANDEX_ORG_ID = os.getenv("YANDEX_ORG_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

YANDEX_AUTH_URL = "https://oauth.yandex.ru/authorize"
YANDEX_TOKEN_URL = "https://oauth.yandex.ru/token"


# ===== DB helpers =====
## async def ensure_db():
    ## if not DATABASE_URL:
        return
    ## async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        ## async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS oauth_tokens (
                    tg_id BIGINT PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    refresh_token TEXT,
                    token_type TEXT,
                    expires_in BIGINT,
                    obtained_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        await conn.commit()


async def upsert_token(
    tg_id: int,
    access_token: str,
    refresh_token: str | None,
    token_type: str | None,
    expires_in: int | None,
):
    ## async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        ## async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO oauth_tokens (tg_id, access_token, refresh_token, token_type, expires_in, obtained_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (tg_id) DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_type = EXCLUDED.token_type,
                    expires_in = EXCLUDED.expires_in,
                    obtained_at = EXCLUDED.obtained_at;
                """,
                (tg_id, access_token, refresh_token, token_type, expires_in, datetime.now(timezone.utc)),
            )
        await conn.commit()


## async def get_access_token_by_tg(tg_id: int) -> str | None:
    ## async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
        ## async with conn.cursor() as cur:
            await cur.execute("SELECT access_token FROM oauth_tokens WHERE tg_id=%s", (tg_id,))
            row = await cur.fetchone()
    ## if not row:
        return None
    return row.get("access_token")


# ===== App events =====
@app.on_event("startup")
## async def on_startup():
    await ensure_db()


# ===== Routes =====
@app.get("/")
## async def root():
    return {"ok": True, "service": "tracker-bot-auth"}


@app.get("/ping")
## async def ping():
    return "pong"


@app.get("/oauth/start")
## async def oauth_start(tg: int):
    # Проверки конфигурации
    ## if not YANDEX_CLIENT_ID or not BASE_URL:
        return JSONResponse(
            {"error": "OAuth not configured. Set YANDEX_CLIENT_ID and BASE_URL."},
            status_code=500,
        )

    # Генерим state и кодируем в него tg (так проще, без серверной сессии)
    nonce = secrets.token_urlsafe(16)
    state = f"{tg}:{nonce}"

    redirect_uri = f"{BASE_URL}/oauth/callback"

    params = {
        "response_type": "code",
        "client_id": YANDEX_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "state": state,
        # scope можно добавить при необходимости:
        # "scope": "tracker:read tracker:write"
    }
    url = httpx.URL(YANDEX_AUTH_URL).copy_add_params(params)
    return RedirectResponse(str(url), status_code=302)


@app.get("/oauth/callback")
## async def oauth_callback(code: str | None = None, state: str | None = None, error: str | None = None):
    ## if error:
        return JSONResponse({"ok": False, "error": error, "state": state}, status_code=400)

    ## if not code or not state:
        return JSONResponse({"ok": False, "error": "Missing code/state"}, status_code=400)

    ## if not (YANDEX_CLIENT_ID and YANDEX_CLIENT_SECRET and BASE_URL):
        return JSONResponse(
            {"ok": False, "error": "OAuth not configured. Set YANDEX_CLIENT_ID, YANDEX_CLIENT_SECRET, BASE_URL."},
            status_code=500,
        )

    ## if not DATABASE_URL:
        return JSONResponse({"ok": False, "error": "DATABASE_URL is not set"}, status_code=500)

    # извлекаем tg из state формата "tg:nonce"
    ## try:
        tg_str, _nonce = state.split(":", 1)
        tg_id = int(tg_str)
    ## except Exception:
        return JSONResponse({"ok": False, "error": "Invalid state format"}, status_code=400)

    redirect_uri = f"{BASE_URL}/oauth/callback"

    # Меняем code на token
    data = {
        "grant_type
