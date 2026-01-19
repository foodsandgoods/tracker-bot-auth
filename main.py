import asyncio
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


def _normalize_queues_csv(s: str) -> str:
    # "INV, DOC;HR" -> "INV,DOC,HR"
    raw = s.replace(";", ",").replace(" ", "")
    parts = [p for p in raw.split(",") if p]
    # верхний регистр, уникальные, сохраняем порядок
    seen = set()
    out = []
    for p in parts:
        p2 = p.upper()
        if p2 not in seen:
            out.append(p2)
            seen.add(p2)
    return ",".join(out)


def _queues_list(csv: str) -> list[str]:
    if not csv:
        return []
    return [p for p in csv.split(",") if p]


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
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tg_settings (
                        tg_id BIGINT PRIMARY KEY,
                        queues_csv TEXT NOT NULL DEFAULT '',
                        days INT NOT NULL DEFAULT 30,
                        limit_results INT NOT NULL DEFAULT 10,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
                # Add limit_results column if it doesn't exist (for existing databases)
                await cur.execute(
                    """
                    ALTER TABLE tg_settings
                    ADD COLUMN IF NOT EXISTS limit_results INT NOT NULL DEFAULT 10;
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
                await cur.execute("SELECT access_token, refresh_token FROM oauth_tokens WHERE tg_id=%s", (tg_id,))
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
                await cur.execute("SELECT tracker_login, tracker_uid FROM tg_users WHERE tg_id=%s", (tg_id,))
                row = await cur.fetchone()
        return row

    async def ensure_settings_row(self, tg_id: int) -> None:
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO tg_settings (tg_id)
                    VALUES (%s)
                    ON CONFLICT (tg_id) DO NOTHING;
                    """,
                    (tg_id,),
                )
            await conn.commit()

    async def get_settings(self, tg_id: int) -> dict:
        await self.ensure_settings_row(tg_id)
        async with await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT queues_csv, days, limit_results FROM tg_settings WHERE tg_id=%s", (tg_id,))
                row = await cur.fetchone()
        return row or {"queues_csv": "", "days": 30, "limit_results": 10}

    async def set_queues(self, tg_id: int, queues_csv: str) -> None:
        q = _normalize_queues_csv(queues_csv)
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO tg_settings (tg_id, queues_csv, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (tg_id) DO UPDATE SET
                        queues_csv = EXCLUDED.queues_csv,
                        updated_at = NOW();
                    """,
                    (tg_id, q),
                )
            await conn.commit()

    async def set_days(self, tg_id: int, days: int) -> None:
        days = max(1, min(int(days), 3650))
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO tg_settings (tg_id, days, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (tg_id) DO UPDATE SET
                        days = EXCLUDED.days,
                        updated_at = NOW();
                    """,
                    (tg_id, days),
                )
            await conn.commit()

    async def set_limit(self, tg_id: int, limit: int) -> None:
        limit = max(1, min(int(limit), 100))
        async with await psycopg.AsyncConnection.connect(self.database_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO tg_settings (tg_id, limit_results, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (tg_id) DO UPDATE SET
                        limit_results = EXCLUDED.limit_results,
                        updated_at = NOW();
                    """,
                    (tg_id, limit),
                )
            await conn.commit()


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
        return {"Authorization": f"OAuth {access_token}", "X-Org-Id": self.settings.yandex_org_id}

    async def myself(self, access_token: str) -> tuple[int, Any]:
        url = "https://api.tracker.yandex.net/v2/myself"
        r = await self.http.get(url, headers=self._headers(access_token))
        return r.status_code, _safe_json(r)

    async def search_issues(self, access_token: str, query: str, limit: int = 50) -> tuple[int, Any]:
        url = "https://api.tracker.yandex.net/v2/issues/_search"
        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        params = {"page": 1, "perPage": limit}
        r = await self.http.post(url, headers=headers, params=params, json={"query": query})
        return r.status_code, _safe_json(r)

    async def get_issue(self, access_token: str, issue_key: str) -> tuple[int, Any]:
        url = f"https://api.tracker.yandex.net/v2/issues/{issue_key}"
        r = await self.http.get(url, headers=self._headers(access_token))
        return r.status_code, _safe_json(r)

    async def get_issue_with_changelog(self, access_token: str, issue_key: str) -> tuple[int, Any]:
        """Get issue with changelog and comments"""
        url = f"https://api.tracker.yandex.net/v2/issues/{issue_key}"
        # Пробуем получить задачу с расширенными данными
        # Если не поддерживается, вернемся к обычному запросу
        try:
            params = {"expand": "changelog,comments"}
            r = await self.http.get(url, headers=self._headers(access_token), params=params)
            if r.status_code == 200:
                return r.status_code, _safe_json(r)
            # Если expand не поддерживается, пробуем без него
        except Exception:
            pass
        
        # Fallback: обычный запрос без expand
        r = await self.http.get(url, headers=self._headers(access_token))
        return r.status_code, _safe_json(r)

    async def patch_issue(self, access_token: str, issue_key: str, patch: dict) -> tuple[int, Any]:
        url = f"https://api.tracker.yandex.net/v2/issues/{issue_key}"
        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        r = await self.http.patch(url, headers=headers, json=patch)
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
        st, me = await self.tracker.myself(access)
        if st != 401:
            if st == 200 and isinstance(me, dict):
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
        await self.storage.upsert_token(tg_id, new_access, new_refresh, new_payload.get("token_type"), new_payload.get("expires_in"))

        st2, me2 = await self.tracker.myself(new_access)
        if st2 == 200 and isinstance(me2, dict):
            login2 = me2.get("login")
            uid2 = me2.get("trackerUid") or me2.get("passportUid") or me2.get("uid")
            if login2:
                await self.storage.upsert_user(tg_id, login2, str(uid2) if uid2 is not None else None)

        return new_access, None

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

    async def me_by_tg(self, tg_id: int) -> dict:
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err
        st, payload = await self.tracker.myself(access)
        return {"http_status": 200, "body": {"status_code": st, "response": payload}}

    async def settings_get(self, tg_id: int) -> dict:
        s = await self.storage.get_settings(tg_id)
        return {"http_status": 200, "body": {"queues": _queues_list(s.get("queues_csv", "")), "days": s.get("days", 30), "limit": s.get("limit_results", 10)}}

    async def settings_set_queues(self, tg_id: int, queues_csv: str) -> dict:
        await self.storage.set_queues(tg_id, queues_csv)
        return await self.settings_get(tg_id)

    async def settings_set_days(self, tg_id: int, days: int) -> dict:
        await self.storage.set_days(tg_id, days)
        return await self.settings_get(tg_id)

    async def settings_set_limit(self, tg_id: int, limit: int) -> dict:
        await self.storage.set_limit(tg_id, limit)
        return await self.settings_get(tg_id)

    def _build_candidate_query(self, queues: list[str], days: int) -> str:
        base = f"Updated: >= now()-{int(days)}d"
        if not queues:
            return base
        # Yandex Tracker syntax: Queue: KEY OR Queue: KEY2 OR Queue: KEY3
        # Each queue condition should be separate
        queue_conditions = [f"Queue: {x}" for x in queues]
        q = " OR ".join(queue_conditions)
        # Wrap in parentheses to ensure proper precedence
        return f"({q}) AND {base}"

    @staticmethod
    def _extract_checklist_items(issue_payload: Any) -> list[dict]:
        if isinstance(issue_payload, dict) and isinstance(issue_payload.get("checklistItems"), list):
            return issue_payload["checklistItems"]
        return []

    async def checklist_assigned_issues(self, tg_id: int, only_unchecked: bool, limit: int = 10) -> dict:
        u = await self.user_by_tg(tg_id)
        if u["http_status"] != 200:
            return u
        login = (u["body"].get("tracker_login") or "").lower()
        if not login:
            return {"http_status": 500, "body": {"error": "tracker_login is empty"}}

        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        s = await self.storage.get_settings(tg_id)
        queues = _queues_list(s.get("queues_csv", ""))
        days = int(s.get("days", 30))
        # Use limit from settings if not provided as parameter
        if limit == 10:  # default value
            limit = int(s.get("limit_results", 10))

        query = self._build_candidate_query(queues, days)
        # Increase search limit to get more results from all queues
        # Need more issues to find matches across all selected queues
        search_limit = min(max(50, limit * 5), 150)  # Between 50 and 150 issues
        st, payload = await self.tracker.search_issues(access, query=query, limit=search_limit)
        if st != 200:
            return {"http_status": 200, "body": {"status_code": st, "response": payload, "query": query}}

        issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
        
        # Early exit if no issues found
        if not issues:
            return {
                "http_status": 200,
                "body": {
                    "status_code": 200,
                    "issues": [],
                    "query": query,
                    "settings": {"queues": queues, "days": days},
                    "login": login,
                },
            }
        
        # Create lookup dict for O(1) access instead of O(n) search
        issues_by_key = {it.get("key"): it for it in issues if it.get("key")}
        issue_keys = list(issues_by_key.keys())
        
        # Process more issues to get results from all queues
        max_keys_to_process = min(len(issue_keys), 120)  # Increased to get more from all queues
        issue_keys = issue_keys[:max_keys_to_process]
        
        # Fetch all issues in parallel for better performance
        async def fetch_issue(key: str) -> tuple[str, dict | None]:
            try:
                sti, issue_full = await self.tracker.get_issue(access, key)
                if sti == 200 and isinstance(issue_full, dict):
                    return key, issue_full
            except Exception:
                # Silently handle errors to not slow down processing
                pass
            return key, None
        
        # Optimized batch size - balance between speed and resource usage
        batch_size = 20  # Increased for better parallelism
        result: list[dict] = []
        
        # Process batches - process more to ensure we get results from all queues
        for i in range(0, len(issue_keys), batch_size):
            # Process more batches to get results from all queues
            # Only exit if we have significantly more results than needed
            if len(result) >= limit * 2:  # Process 2x to get diversity from all queues
                break
                
            batch_keys = issue_keys[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[fetch_issue(key) for key in batch_keys],
                return_exceptions=True
            )
            
            for result_item in batch_results:
                # Don't exit early - we need to process more to get results from all queues
                # The final filtering will happen after all processing
                    
                # Handle exceptions from gather
                if isinstance(result_item, Exception):
                    continue
                key, issue_full = result_item
                if issue_full is None:
                    continue
                
                checklist_items = self._extract_checklist_items(issue_full)
                if not checklist_items:
                    continue

                # Optimize checklist item processing
                matched_items = []
                login_lower = login  # Cache lowercase login
                for ci in checklist_items:
                    # Early skip if checked and we only want unchecked
                    if only_unchecked and ci.get("checked") is True:
                        continue
                    
                    # Fast path: check assignee login
                    ass = ci.get("assignee")
                    if not ass or (ass.get("login") or "").lower() != login_lower:
                        continue
                    
                    # Build item dict efficiently
                    matched_items.append(
                        {
                            "id": str(ci.get("id", "")),
                            "text": (ci.get("text") or ci.get("textHtml") or "").strip(),
                            "checked": bool(ci.get("checked", False)),
                        }
                    )

                if matched_items:
                    # Use O(1) dict lookup instead of O(n) linear search
                    orig_issue = issues_by_key.get(key, {})
                    # Extract updated date from issue (prioritize full issue data)
                    updated_at = issue_full.get("updatedAt") or issue_full.get("updated") or orig_issue.get("updatedAt") or orig_issue.get("updated")
                    result.append(
                        {
                            "key": key,
                            "summary": orig_issue.get("summary") or issue_full.get("summary") or "",
                            "url": f"https://tracker.yandex.ru/{key}",
                            "updatedAt": updated_at,
                            "items": matched_items,
                        }
                    )
        
        # Limit results to requested amount, but keep diversity from all queues
        if len(result) > limit:
            # Try to keep results from different queues
            result_by_queue: dict[str, list[dict]] = {}
            for r in result:
                queue = r["key"].split("-")[0] if "-" in r["key"] else "UNKNOWN"
                if queue not in result_by_queue:
                    result_by_queue[queue] = []
                result_by_queue[queue].append(r)
            
            # Take results from each queue proportionally
            final_result = []
            per_queue = max(1, limit // max(len(result_by_queue), 1))
            for queue_results in result_by_queue.values():
                final_result.extend(queue_results[:per_queue])
                if len(final_result) >= limit:
                    break
            
            # Fill remaining slots if needed
            if len(final_result) < limit:
                for r in result:
                    if r not in final_result:
                        final_result.append(r)
                        if len(final_result) >= limit:
                            break
            
            result = final_result[:limit]

        return {
            "http_status": 200,
            "body": {
                "status_code": 200,
                "issues": result,
                "query": query,
                "settings": {"queues": queues, "days": days},
                "login": login,
            },
        }

    async def checklist_item_check(self, tg_id: int, issue_key: str, item_id: str, checked: bool) -> dict:
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        # 1) получить issue, достать checklistItems
        sti, issue_full = await self.tracker.get_issue(access, issue_key)
        if sti != 200 or not isinstance(issue_full, dict):
            return {"http_status": 200, "body": {"status_code": sti, "response": issue_full}}

        items = self._extract_checklist_items(issue_full)
        if not items:
            return {"http_status": 404, "body": {"error": "No checklistItems in issue"}}

        # 2) обновить checked у нужного item
        found = False
        new_items = []
        for ci in items:
            if str(ci.get("id")) == str(item_id):
                ci2 = dict(ci)
                ci2["checked"] = bool(checked)
                new_items.append(ci2)
                found = True
            else:
                new_items.append(ci)

        if not found:
            return {"http_status": 404, "body": {"error": "Checklist item not found in issue", "item_id": item_id}}

        # 3) PATCH issue (возможный формат)
        stp, resp = await self.tracker.patch_issue(access, issue_key, {"checklistItems": new_items})
        return {"http_status": 200, "body": {"status_code": stp, "response": resp}}

    async def get_issue_full(self, tg_id: int, issue_key: str) -> dict:
        """Get full issue data with changelog and comments for summary generation"""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err
        
        st, issue_data = await self.tracker.get_issue_with_changelog(access, issue_key)
        if st != 200:
            return {"http_status": st, "body": issue_data}
        
        return {"http_status": 200, "body": issue_data}

    async def issue_summary(self, tg_id: int, issue_key: str) -> dict:
        """Generate AI summary for issue"""
        # Получаем данные задачи
        issue_result = await self.get_issue_full(tg_id, issue_key)
        if issue_result["http_status"] != 200:
            return issue_result
        
        issue_data = issue_result["body"]
        
        # Генерируем summary с помощью ИИ
        try:
            from ai_service import generate_summary
            summary_text = await generate_summary(issue_data)
        except ImportError as e:
            return {
                "http_status": 500,
                "body": {"error": f"AI service module not found: {str(e)}"}
            }
        except Exception as e:
            return {
                "http_status": 500,
                "body": {"error": f"AI service error: {str(e)}"}
            }
        
        if not summary_text:
            return {
                "http_status": 500,
                "body": {"error": "Не удалось сгенерировать резюме. Проверьте настройки GPTunneL API."}
            }
        
        return {
            "http_status": 200,
            "body": {
                "issue_key": issue_key,
                "summary": summary_text,
                "issue_url": f"https://tracker.yandex.ru/{issue_key}"
            }
        }


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
    # Optimize HTTP client for low-resource servers:
    # - Connection pooling with limits
    # - Keep-alive connections
    # - Reduced timeout for faster failures
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20, keepalive_expiry=30.0)
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=5.0)
    _http = httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
    )

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

    return JSONResponse({"ok": True, "message": "Connected. Return to Telegram and run /me", "tg": tg_id}, status_code=200)


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


@app.get("/tg/settings")
async def tg_settings(tg: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.settings_get(tg)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/queues")
async def tg_settings_queues(tg: int, queues: str):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.settings_set_queues(tg, queues)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/days")
async def tg_settings_days(tg: int, days: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.settings_set_days(tg, days)
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/limit")
async def tg_settings_limit(tg: int, limit: int):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.settings_set_limit(tg, limit)
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


@app.get("/tracker/issue/{issue_key}/summary")
async def issue_summary_endpoint(tg: int, issue_key: str):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.issue_summary(tg, issue_key)
    return JSONResponse(result["body"], status_code=result["http_status"])
