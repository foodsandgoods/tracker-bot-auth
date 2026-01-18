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


# ===== DB =====
async def ensure_db() -> None:
    if not DATABASE_URL:
        return
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
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
) -> None:
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        async with conn.cursor() as cur:
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


async def get_access_token_by_tg(tg_id: int) -> str | None:
    async with await psycopg.AsyncConnection.connect(DATABASE_URL, row_factory=dict_row) as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT access_token FROM oauth_tokens WHERE tg_id=%s", (tg_id,))
            row = await cur.fetchone()
    if not row:
        return None
    return row.get("access_token")


@app.on_event("startup")
async def on_startup():
    await ensure_db()


# ===== ROUTES =====
@app.get("/")
async def root():
    return {"ok": True, "service": "tracker-bot-auth"}


@app.get("/ping")
async def ping():
    return "pong"


@app.get("/oauth/start")
async def oauth_start(tg: int):
    if not YANDEX_CLIENT_ID or not BASE_URL:
        return JSONResponse(
            {"error": "OAuth not configured. Set YANDEX_CLIENT_ID and BASE_URL."},
            status_code=500,
        )

    nonce = secrets.token_urlsafe(16)
    state = f"{tg}:{nonce}"
    redirect_uri = f"{BASE_URL}/oauth/callback"

    params = {
    "response_type": "code",
    "client_id": YANDEX_CLIENT_ID,
    "redirect_uri": redirect_uri,
    "state": state,
}

async with httpx.AsyncClient() as client:
    url = str(client.build_request("GET", YANDEX_AUTH_URL, params=params).url)

return RedirectResponse(url, status_code=302)


@app.get("/oauth/callback")
async def oauth_callback(code: str | None = None, state: str | None = None, error: str | None = None):
    if error:
        return JSONResponse({"ok": False, "error": error, "state": state}, status_code=400)

    if not code or not state:
        return JSONResponse({"ok": False, "error": "Missing code/state"}, status_code=400)

    if not (YANDEX_CLIENT_ID and YANDEX_CLIENT_SECRET and BASE_URL):
        return JSONResponse(
            {"ok": False, "error": "OAuth not configured. Set YANDEX_CLIENT_ID, YANDEX_CLIENT_SECRET, BASE_URL."},
            status_code=500,
        )

    if not DATABASE_URL:
        return JSONResponse({"ok": False, "error": "DATABASE_URL is not set"}, status_code=500)

    try:
        tg_str, _nonce = state.split(":", 1)
        tg_id = int(tg_str)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid state format"}, status_code=400)

    redirect_uri = f"{BASE_URL}/oauth/callback"

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": YANDEX_CLIENT_ID,
        "client_secret": YANDEX_CLIENT_SECRET,
        "redirect_uri": redirect_uri,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(YANDEX_TOKEN_URL, data=data)

    try:
        token_payload = r.json()
    except Exception:
        token_payload = {"raw": r.text}

    if r.status_code != 200:
        return JSONResponse(
            {"ok": False, "status_code": r.status_code, "token_response": token_payload},
            status_code=400,
        )

    access_token = token_payload.get("access_token")
    refresh_token = token_payload.get("refresh_token")
    token_type = token_payload.get("token_type")
    expires_in = token_payload.get("expires_in")

    if not access_token:
        return JSONResponse(
            {"ok": False, "error": "No access_token in token response", "token_response": token_payload},
            status_code=400,
        )

    await upsert_token(tg_id, access_token, refresh_token, token_type, expires_in)

    return JSONResponse(
        {"ok": True, "status_code": 200, "message": "Connected. Return to Telegram and run /me", "tg": tg_id}
    )


@app.get("/tracker/me")
async def tracker_me(token: str):
    if not YANDEX_ORG_ID:
        return JSONResponse({"error": "YANDEX_ORG_ID is not set"}, status_code=500)

    url = "https://api.tracker.yandex.net/v2/myself"
    headers = {"Authorization": f"OAuth {token}", "X-Org-Id": YANDEX_ORG_ID}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=headers)

    try:
        payload = r.json()
    except Exception:
        payload = {"raw": r.text}

    return {"status_code": r.status_code, "response": payload}


@app.get("/tracker/me_by_tg")
async def tracker_me_by_tg(tg: int):
    if not DATABASE_URL:
        return JSONResponse({"error": "DATABASE_URL is not set"}, status_code=500)
    if not YANDEX_ORG_ID:
        return JSONResponse({"error": "YANDEX_ORG_ID is not set"}, status_code=500)

    token = await get_access_token_by_tg(tg)
    if not token:
        return JSONResponse({"error": "No token for this tg. Use /connect first."}, status_code=401)

    url = "https://api.tracker.yandex.net/v2/myself"
    headers = {"Authorization": f"OAuth {token}", "X-Org-Id": YANDEX_ORG_ID}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=headers)

    try:
        payload = r.json()
    except Exception:
        payload = {"raw": r.text}

    return {"status_code": r.status_code, "response": payload}
