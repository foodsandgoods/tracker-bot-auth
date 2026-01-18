import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse


# =========================
# Config
# =========================
@dataclass(frozen=True)
class Settings:
    base_url: str
    yandex_client_id: str
    yandex_client_secret: str
    yandex_org_id: str
    database_url: str

    yandex_auth_url: str = "https://oauth.yandex.ru/authorize"
    yandex_token_url: str = "https://oauth.yandex.ru/token"
    tracker_myself_url: str = "https://api.tracker.yandex.net/v2/myself"

    http_timeout_seconds: float = 20.0


def load_settings() -> Settings:
    base_url = (os.getenv("BASE_URL") or "").rstrip("/")
    client_id = os.getenv("YANDEX_CLIENT_ID") or ""
    client_secret = os.getenv("YANDEX_CLIENT_SECRET") or ""
    org_id = os.getenv("YANDEX_ORG_ID") or ""
    db_url = os.getenv("DATABASE_URL") or ""

    return Settings(
        base_url=base_url,
        yandex_client_id=client_id,
        yandex_client_secret=client_secret,
        yandex_org_id=org_id,
        database_url=db_url,
    )


# =========================
# Storage (Postgres)
# =========================
class TokenStorage:
    def __init__(self, database_url: str):
        self.database_url = database_url

    async def ensure_schema(self) -> None:
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
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
        self,
        tg_id: int,
        access_token: str,
        refresh_token: Optional[str],
        token_type: Optional[str],
        expires_in: Optional[int],
    ) -> None:
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
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
                    (
                        tg_id,
                        access_token,
                        refresh_token,
                        token_type,
                        expires_in,
                        datetime.now(timezone.utc),
                    ),
                )
            await conn.commit()

    async def get_tokens(self, tg_id: int) -> Optional[dict]:
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT access_token, refresh_token FROM oauth_tokens WHERE tg_id=%s",
                    (tg_id,),
                )
                row = await cur.fetchone()
        return row


# =========================
# OAuth + Tracker clients
# =========================
class OAuthClient:
    def __init__(self, http: httpx.AsyncClient, settings: Settings):
        self.http = http
        self.settings = settings

    def build_authorize_url(self, tg_id: int) -> str:
        # state формата "tg:nonce" — без серверных сессий
        nonce = secrets.token_urlsafe(16)
        state = f"{tg_id}:{nonce}"

        redirect_uri = f"{self.settings.base_url}/oauth/callback"
        params = {
            "response_type": "code",
            "client_id": self.settings.yandex_client_id,
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"{self.settings.yandex_auth_url}?{urlencode(params)}"

    async def exchange_code(self, code: str) -> dict[str, Any]:
        redirect_uri = f"{self.settings.base_url}/oauth/callback"
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.settings.yandex_client_id,
            "client_secret": self.settings.yandex_client_secret,
            "redirect_uri": redirect_uri,
        }
        r = await self.http.post(self.settings.yandex_token_url, data=data)
        payload = _safe_json(r)
        if r.status_code != 200:
            raise RuntimeError(f"Token exchange failed: {r.status_code} {payload}")
        if "access_token" not in payload:
            raise RuntimeError(f"Token exchange response has no access_token: {payload}")
        return payload

    async def refresh(self, refresh_token: str) -> dict[str, Any]:
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.settings.yandex_client_id,
            "client_secret": self.settings.yandex_client_secret,
        }
        r = await self.http.post(self.settings.yandex_token_url, data=data)
        payload = _safe_json(r)
        if r.status_code != 200:
            raise RuntimeError(f"Token refresh failed: {r.status_code} {payload}")
        if "access_token" not in payload:
            raise RuntimeError(f"Refresh response has no access_token: {payload}")
        return payload


class TrackerClient:
    def __init__(self, http: httpx.AsyncClient, settings: Settings):
        self.http = http
        self.settings = settings

    async def myself(self, access_token: str) -> tuple[int, Any]:
        headers = {
            "Authorization": f"OAuth {access_token}",
            "X-Org-Id": self.settings.yandex_org_id,
        }
        r = await self.http.get(self.settings.tracker_myself_url, headers=headers)
        return r.status_code, _safe_json(r)


# =========================
# Service layer
# =========================
class TrackerService:
    def __init__(self, storage: TokenStorage, oauth: OAuthClient, tracker: TrackerClient):
        self.storage = storage
        self.oauth = oauth
        self.tracker = tracker

    async def myself_by_tg(self, tg_id: int) -> dict[str, Any]:
        tokens = await self.storage.get_tokens(tg_id)
        if not tokens or not tokens.get("access_token"):
            return {"http_status": 401, "body": {"error": "No token for this tg. Use /connect first."}}

        access = tokens["access_token"]
        status, payload = await self.tracker.myself(access)
        if status != 401:
            return {"http_status": 200, "body": {"status_code": status, "response": payload}}

        # access token invalid/expired -> refresh
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            return {
                "http_status": 401,
                "body": {"error": "Access token expired and no refresh_token stored. Reconnect with /connect."},
            }

        try:
            new_payload = await self.oauth.refresh(refresh_token)
        except Exception as e:
            return {"http_status": 401, "body": {"error": "Token refresh failed", "detail": str(e)}}

        new_access = new_payload.get("access_token")
        new_refresh = new_payload.get("refresh_token") or refresh_token
        new_type = new_payload.get("token_type")
        new_expires = new_payload.get("expires_in")

        await self.storage.upsert_token(tg_id, new_access, new_refresh, new_type, new_expires)

        status2, payload2 = await self.tracker.myself(new_access)
        return {"http_status": 200, "body": {"status_code": status2, "response": payload2}}


# =========================
# Utils
# =========================
def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}


def _require(settings: Settings) -> Optional[JSONResponse]:
    # Проверяем минимально необходимое для OAuth/Tracker
    missing = []
    if not settings.base_url:
        missing.append("BASE_URL")
    if not settings.yandex_client_id:
        missing.append("YANDEX_CLIENT_ID")
    if not settings.yandex_client_secret:
        missing.append("YANDEX_CLIENT_SECRET")
    if not settings.yandex_org_id:
        missing.append("YANDEX_ORG_ID")
    if not settings.database_url:
        missing.append("DATABASE_URL")

    if missing:
        return JSONResponse({"error": "Service not configured", "missing": missing}, status_code=500)
    return None


# =========================
# FastAPI app wiring
# =========================
app = FastAPI()

settings = load_settings()

# один клиент на всё приложение (быстрее и правильнее)
_http: Optional[httpx.AsyncClient] = None
_storage: Optional[TokenStorage] = None
_oauth: Optional[OAuthClient] = None
_tracker: Optional[TrackerClient] = None
_service: Optional[TrackerService] = None


@app.on_event("startup")
async def startup():
    global _http, _storage, _oauth, _tracker, _service

    # создаём общий http клиент
    _http = httpx.AsyncClient(timeout=settings.http_timeout_seconds)

    cfg_err = _require(settings)
    # даже если не настроено — поднимем сервис, но без OAuth он будет отдавать 500 с missing
    if settings.database_url:
        _storage = TokenStorage(settings.database_url)
        await _storage.ensure_schema()

    if not cfg_err and _http and _storage:
        _oauth = OAuthClient(_http, settings)
        _tracker = TrackerClient(_http, settings)
        _service = TrackerService(_storage, _oauth, _tracker)


@app.on_event("shutdown")
async def shutdown():
    global _http
    if _http:
        await _http.aclose()
        _http = None


# =========================
# Routes
# =========================
@app.get("/")
async def root():
    return {"ok": True, "service": "tracker-bot-auth"}


@app.get("/ping")
async def ping():
    return "pong"


@app.get("/oauth/start")
async def oauth_start(tg: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err

    assert _oauth is not None
    return RedirectResponse(_oauth.build_authorize_url(tg), status_code=302)


@app.get("/oauth/callback")
async def oauth_callback(code: str | None = None, state: str | None = None, error: str | None = None):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err

    if error:
        return JSONResponse({"ok": False, "error": error, "state": state}, status_code=400)
    if not code or not state:
        return JSONResponse({"ok": False, "error": "Missing code/state"}, status_code=400)

    # state = "tg:nonce"
    try:
        tg_str, _nonce = state.split(":", 1)
        tg_id = int(tg_str)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid state format"}, status_code=400)

    assert _oauth is not None
    assert _storage is not None

    try:
        token_payload = await _oauth.exchange_code(code)
    except Exception as e:
        return JSONResponse({"ok": False, "error": "Token exchange failed", "detail": str(e)}, status_code=400)

    access = token_payload.get("access_token")
    refresh = token_payload.get("refresh_token")
    token_type = token_payload.get("token_type")
    expires_in = token_payload.get("expires_in")

    await _storage.upsert_token(tg_id, access, refresh, token_type, expires_in)

    # Токены не возвращаем
    return JSONResponse(
        {"ok": True, "message": "Connected. Return to Telegram and run /me", "tg": tg_id},
        status_code=200,
    )


@app.get("/tracker/me")
async def tracker_me(token: str):
    # оставляем endpoint для отладки/ручных запросов
    if not settings.yandex_org_id:
        return JSONResponse({"error": "YANDEX_ORG_ID is not set"}, status_code=500)

    assert _tracker is not None
    status, payload = await _tracker.myself(token)
    return {"status_code": status, "response": payload}


@app.get("/tracker/me_by_tg")
async def tracker_me_by_tg(tg: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err

    assert _service is not None
    result = await _service.myself_by_tg(tg)
    return JSONResponse(result["body"], status_code=result["http_status"])
