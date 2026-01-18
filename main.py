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

    http_timeout_seconds: float = 25.0


def load_settings() -> Settings:
    return Settings(
        base_url=(os.getenv("BASE_URL") or "").rstrip("/"),
        yandex_client_id=os.getenv("YANDEX_CLIENT_ID") or "",
        yandex_client_secret=os.getenv("YANDEX_CLIENT_SECRET") or "",
        yandex_org_id=os.getenv("YANDEX_ORG_ID") or "",
        database_url=os.getenv("DATABASE_URL") or "",
    )


# =========================
# Utils
# =========================
def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}


def _require(settings: Settings) -> Optional[JSONResponse]:
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


def _parse_state(state: str) -> int:
    tg_str, _nonce = state.split(":", 1)
    return int(tg_str)


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
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tg_users (
                        tg_id BIGINT PRIMARY KEY,
                        tracker_login TEXT NOT NULL,
                        tracker_uid TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

    async def upsert_user(self, tg_id: int, tracker_login: str, tracker_uid: Optional[str]) -> None:
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO tg_users (tg_id, tracker_login, tracker_uid, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (tg_id) DO UPDATE SET
                        tracker_login = EXCLUDED.tracker_login,
                        tracker_uid = EXCLUDED.tracker_uid,
                        updated_at = NOW();
                    """,
                    (tg_id, tracker_login, tracker_uid),
                )
            await conn.commit()

    async def get_user(self, tg_id: int) -> Optional[dict]:
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT tracker_login, tracker_uid FROM tg_users WHERE tg_id=%s",
                    (tg_id,),
                )
                row = await cur.fetchone()
        return row


# =========================
# HTTP clients
# =========================
class OAuthClient:
    def __init__(self, http: httpx.AsyncClient, settings: Settings):
        self.http = http
        self.settings = settings

    def build_authorize_url(self, tg_id: int) -> str:
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

    def _headers(self, access_token: str) -> dict[str, str]:
        return {
            "Authorization": f"OAuth {access_token}",
            "X-Org-Id": self.settings.yandex_org_id,
        }

    async def myself(self, access_token: str) -> tuple[int, Any]:
        r = await self.http.get(self.settings.tracker_myself_url, headers=self._headers(access_token))
        return r.status_code, _safe_json(r)

    async def search_issues(self, access_token: str, query: str, limit: int = 50) -> tuple[int, Any]:
        """
        В некоторых инсталляциях Tracker поле perPage в JSON-body не принимается.
        Поэтому передаём пагинацию query-параметрами.
        """
        url = "https://api.tracker.yandex.net/v2/issues/_search"
        headers = {**self._headers(access_token), "Content-Type": "application/json"}

        params = {"page": 1, "perPage": limit}
        r = await self.http.post(url, headers=headers, params=params, json={"query": query})
        return r.status_code, _safe_json(r)

    async def get_checklist(self, access_token: str, issue_key: str) -> tuple[int, Any]:
        url_v2 = f"https://api.tracker.yandex.net/v2/issues/{issue_key}/checklist"
        url_v3 = f"https://api.tracker.yandex.net/v3/issues/{issue_key}/checklist"

        r = await self.http.get(url_v2, headers=self._headers(access_token))
        if r.status_code in (404, 405):
            r = await self.http.get(url_v3, headers=self._headers(access_token))
        return r.status_code, _safe_json(r)

    async def set_checklist_item_checked(
        self, access_token: str, issue_key: str, item_id: str, checked: bool
    ) -> tuple[int, Any]:
        url_v2 = f"https://api.tracker.yandex.net/v2/issues/{issue_key}/checklist/items/{item_id}"
        url_v3 = f"https://api.tracker.yandex.net/v3/issues/{issue_key}/checklist/items/{item_id}"

        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        body = {"checked": checked}

        r = await self.http.patch(url_v2, headers=headers, json=body)
        if r.status_code in (404, 405):
            r = await self.http.patch(url_v3, headers=headers, json=body)
        return r.status_code, _safe_json(r)


# =========================
# Service layer
# =========================
class TrackerService:
    def __init__(self, storage: TokenStorage, oauth: OAuthClient, tracker: TrackerClient):
        self.storage = storage
        self.oauth = oauth
        self.tracker = tracker

    async def _get_valid_access_token(self, tg_id: int) -> tuple[Optional[str], Optional[dict]]:
        tokens = await self.storage.get_tokens(tg_id)
        if not tokens or not tokens.get("access_token"):
            return None, {"http_status": 401, "body": {"error": "No token. Use /connect first."}}

        access = tokens["access_token"]

        status, me = await self.tracker.myself(access)
        if status != 401:
            if status == 200 and isinstance(me, dict):
                login = me.get("login")
                uid = me.get("trackerUid") or me.get("passportUid") or me.get("uid")
                if login:
                    await self.storage.upsert_user(tg_id, login, str(uid) if uid is not None else None)
            return access, None

        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            return None, {"http_status": 401, "body": {"error": "Expired token and no refresh_token. Reconnect."}}

        new_payload = await self.oauth.refresh(refresh_token)
        new_access = new_payload.get("access_token")
        new_refresh = new_payload.get("refresh_token") or refresh_token

        await self.storage.upsert_token(
            tg_id,
            new_access,
            new_refresh,
            new_payload.get("token_type"),
            new_payload.get("expires_in"),
        )

        status2, me2 = await self.tracker.myself(new_access)
        if status2 == 200 and isinstance(me2, dict):
            login2 = me2.get("login")
            uid2 = me2.get("trackerUid") or me2.get("passportUid") or me2.get("uid")
            if login2:
                await self.storage.upsert_user(tg_id, login2, str(uid2) if uid2 is not None else None)

        return new_access, None

    async def me_by_tg(self, tg_id: int) -> dict:
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err
        status, payload = await self.tracker.myself(access)
        return {"http_status": 200, "body": {"status_code": status, "response": payload}}

    async def user_by_tg(self, tg_id: int) -> dict:
        user = await self.storage.get_user(tg_id)
        if user:
            return {"http_status": 200, "body": user}

        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        user2 = await self.storage.get_user(tg_id)
        if user2:
            return {"http_status": 200, "body": user2}

        return {"http_status": 404, "body": {"error": "User not linked yet. Run /connect again."}}

    def _extract_checklist_items(self, checklist_payload: Any) -> list[dict]:
        if isinstance(checklist_payload, list):
            return checklist_payload
        if isinstance(checklist_payload, dict):
            if isinstance(checklist_payload.get("checklistItems"), list):
                return checklist_payload["checklistItems"]
            if isinstance(checklist_payload.get("items"), list):
                return checklist_payload["items"]
        return []

    async def checklist_assigned_issues(self, tg_id: int, only_unchecked: bool, limit: int = 10) -> dict:
        u = await self.user_by_tg(tg_id)
        if u["http_status"] != 200:
            return u

        login = (u["body"].get("tracker_login") or "").lower()
        if not login:
            return {"http_status": 500, "body": {"error": "tracker_login is empty for this tg"}}

        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        query = "(Queue: INV OR Queue: DOC OR Queue: HR) AND Updated: >= now()-15d"
        st, payload = await self.tracker.search_issues(access, query=query, limit=50)
        if st != 200:
            return {"http_status": 200, "body": {"status_code": st, "response": payload, "query": query}}

        issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
        result: list[dict] = []

        for it in issues:
            key = it.get("key")
            if not key:
                continue

            stc, checklist = await self.tracker.get_checklist(access, key)
            if stc != 200:
                continue

            items = self._extract_checklist_items(checklist)
            if not items:
                continue

            matched_items = []
            for ci in items:
                ass = ci.get("assignee") or {}
                ass_login = (ass.get("login") or "").lower()
                if ass_login != login:
                    continue
                if only_unchecked and ci.get("checked") is True:
                    continue

                matched_items.append(
                    {
                        "id": str(ci.get("id")),
                        "text": ci.get("text") or ci.get("textHtml") or "",
                        "checked": bool(ci.get("checked", False)),
                    }
                )

            if matched_items:
                result.append(
                    {
                        "key": key,
                        "summary": it.get("summary"),
                        "url": f"https://tracker.yandex.ru/{key}",
                        "items": matched_items,
                    }
                )

            if len(result) >= limit:
                break

        return {"http_status": 200, "body": {"status_code": 200, "issues": result, "query": query, "login": login}}

    async def checklist_item_check(self, tg_id: int, issue_key: str, item_id: str, checked: bool) -> dict:
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        st, payload = await self.tracker.set_checklist_item_checked(access, issue_key, item_id, checked)
        return {"http_status": 200, "body": {"status_code": st, "response": payload}}


# =========================
# FastAPI wiring
# =========================
app = FastAPI()
settings = load_settings()

_http: Optional[httpx.AsyncClient] = None
_storage: Optional[TokenStorage] = None
_oauth: Optional[OAuthClient] = None
_tracker: Optional[TrackerClient] = None
_service: Optional[TrackerService] = None


@app.on_event("startup")
async def startup():
    global _http, _storage, _oauth, _tracker, _service

    _http = httpx.AsyncClient(timeout=settings.http_timeout_seconds)

    if settings.database_url:
        _storage = TokenStorage(settings.database_url)
        await _storage.ensure_schema()

    cfg_err = _require(settings)
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

    try:
        tg_id = _parse_state(state)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid state format"}, status_code=400)

    assert _oauth is not None
    assert _storage is not None
    assert _tracker is not None

    try:
        token_payload = await _oauth.exchange_code(code)
    except Exception as e:
        return JSONResponse({"ok": False, "error": "Token exchange failed", "detail": str(e)}, status_code=400)

    access = token_payload.get("access_token")
    refresh = token_payload.get("refresh_token")
    token_type = token_payload.get("token_type")
    expires_in = token_payload.get("expires_in")

    await _storage.upsert_token(tg_id, access, refresh, token_type, expires_in)

    st, me = await _tracker.myself(access)
    if st == 200 and isinstance(me, dict):
        login = me.get("login")
        uid = me.get("trackerUid") or me.get("passportUid") or me.get("uid")
        if login:
            await _storage.upsert_user(tg_id, login, str(uid) if uid is not None else None)

    return JSONResponse(
        {"ok": True, "message": "Connected. Return to Telegram and run /me", "tg": tg_id},
        status_code=200,
    )


@app.get("/tracker/me_by_tg")
async def tracker_me_by_tg(tg: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.me_by_tg(tg)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/user_by_tg")
async def tracker_user_by_tg(tg: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.user_by_tg(tg)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/checklist/assigned")
async def checklist_assigned(tg: int, limit: int = 10):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.checklist_assigned_issues(tg, only_unchecked=False, limit=limit)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/checklist/assigned_unchecked")
async def checklist_assigned_unchecked(tg: int, limit: int = 10):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.checklist_assigned_issues(tg, only_unchecked=True, limit=limit)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tracker/checklist/check")
async def checklist_check(tg: int, issue: str, item: str, checked: bool = True):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.checklist_item_check(tg, issue_key=issue, item_id=item, checked=checked)
    return JSONResponse(result["body"], status_code=result["http_status"])
    
@app.get("/tracker/debug_checklist")
async def debug_checklist(tg: int, issue: str):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err

    assert _service is not None

    # Получаем валидный access token
    access, err = await _service._get_valid_access_token(tg)  # noqa: SLF001
    if err:
        return JSONResponse(err["body"], status_code=err["http_status"])

    # Достаём сырой checklist
    st, payload = await _service.tracker.get_checklist(access, issue)  # noqa: SLF001
    return {"status_code": st, "response": payload}
    
@app.get("/tracker/debug_issue")
async def debug_issue(tg: int, issue: str):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err

    assert _service is not None
    access, err = await _service._get_valid_access_token(tg)  # noqa: SLF001
    if err:
        return JSONResponse(err["body"], status_code=err["http_status"])

    assert _http is not None
    url = f"https://api.tracker.yandex.net/v2/issues/{issue}"
    headers = {"Authorization": f"OAuth {access}", "X-Org-Id": settings.yandex_org_id}

    r = await _http.get(url, headers=headers)
    return {"status_code": r.status_code, "response": _safe_json(r)}
