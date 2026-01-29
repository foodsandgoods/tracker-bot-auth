"""
FastAPI service for Yandex Tracker OAuth and API proxy.
Optimized for low-resource environments (1GB RAM).
"""
import asyncio
import logging
import secrets
import time
from collections import defaultdict
from typing import Any, Optional
from urllib.parse import urlencode

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse

from config import settings
from db import TokenStorage, queues_list
from http_client import get_client, close_client, get_timeout, with_retry, safe_json
from metrics import metrics, Timer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Reduce noise from libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)


# =============================================================================
# Rate Limiting
# =============================================================================
class RateLimiter:
    """Simple in-memory rate limiter with sliding window."""
    
    __slots__ = ('_requests', '_window', '_max_requests', '_lock')
    
    def __init__(self, max_requests: int = 30, window_seconds: int = 60):
        self._requests: dict[int, list[float]] = defaultdict(list)
        self._window = window_seconds
        self._max_requests = max_requests
        self._lock = asyncio.Lock()
    
    async def is_allowed(self, tg_id: int) -> bool:
        """Check if request is allowed for given tg_id."""
        async with self._lock:
            now = time.time()
            cutoff = now - self._window
            
            # Clean old requests
            self._requests[tg_id] = [t for t in self._requests[tg_id] if t > cutoff]
            
            if len(self._requests[tg_id]) >= self._max_requests:
                metrics.inc("rate_limit.exceeded")
                return False
            
            self._requests[tg_id].append(now)
            return True
    
    async def cleanup(self) -> int:
        """Remove old entries. Returns count removed."""
        async with self._lock:
            now = time.time()
            cutoff = now - self._window * 2
            
            to_remove = [k for k, v in self._requests.items() 
                        if not v or max(v) < cutoff]
            for k in to_remove:
                del self._requests[k]
            return len(to_remove)


_rate_limiter = RateLimiter(max_requests=30, window_seconds=60)


# =============================================================================
# Settings Cache
# =============================================================================
class SettingsCache:
    """TTL cache for user settings to reduce DB queries."""
    
    __slots__ = ('_cache', '_ttl')
    
    def __init__(self, ttl: int = 300):
        self._cache: dict[int, tuple[dict, float]] = {}
        self._ttl = ttl
    
    def get(self, tg_id: int) -> Optional[dict]:
        if tg_id in self._cache:
            data, ts = self._cache[tg_id]
            if time.time() - ts < self._ttl:
                metrics.inc("settings_cache.hit")
                return data
            del self._cache[tg_id]
        metrics.inc("settings_cache.miss")
        return None
    
    def set(self, tg_id: int, data: dict) -> None:
        self._cache[tg_id] = (data, time.time())
        # Limit cache size
        if len(self._cache) > 500:
            oldest = min(self._cache.items(), key=lambda x: x[1][1])
            del self._cache[oldest[0]]
    
    def invalidate(self, tg_id: int) -> None:
        self._cache.pop(tg_id, None)


_settings_cache = SettingsCache(ttl=300)


# =============================================================================
# OAuth Client
# =============================================================================
class OAuthClient:
    """Yandex OAuth client."""
    
    def __init__(self):
        if not settings.oauth or not settings.tracker:
            raise ValueError("OAuth not configured")
        self._client_id = settings.oauth.client_id
        self._client_secret = settings.oauth.client_secret
        self._token_url = settings.tracker.token_url
        self._auth_url = settings.tracker.auth_url

    def build_authorize_url(self, tg_id: int) -> str:
        """Build OAuth authorization URL."""
        nonce = secrets.token_urlsafe(16)
        state = f"{tg_id}:{nonce}"
        redirect_uri = f"{settings.base_url}/oauth/callback"
        params = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": redirect_uri,
            "state": state,
        }
        return f"{self._auth_url}?{urlencode(params)}"

    @with_retry(max_attempts=2, base_delay=1.0)
    async def exchange_code(self, code: str) -> dict[str, Any]:
        """Exchange authorization code for tokens."""
        client = await get_client()
        redirect_uri = f"{settings.base_url}/oauth/callback"
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "redirect_uri": redirect_uri,
        }
        r = await client.post(self._token_url, data=data)
        payload = safe_json(r)
        
        if r.status_code != 200:
            raise RuntimeError(f"Token exchange failed: {r.status_code}")
        if "access_token" not in payload:
            raise RuntimeError("Token exchange response has no access_token")
        
        return payload

    @with_retry(max_attempts=2, base_delay=1.0)
    async def refresh(self, refresh_token: str) -> dict[str, Any]:
        """Refresh access token."""
        client = await get_client()
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }
        r = await client.post(self._token_url, data=data)
        payload = safe_json(r)
        
        if r.status_code != 200:
            raise RuntimeError(f"Token refresh failed: {r.status_code}")
        if "access_token" not in payload:
            raise RuntimeError("Refresh response has no access_token")
        
        return payload


# =============================================================================
# Tracker API Client
# =============================================================================
class TrackerClient:
    """Yandex Tracker API client with retry support."""
    
    def __init__(self):
        if not settings.tracker:
            raise ValueError("Tracker not configured")
        self._api_base = settings.tracker.api_base
        self._org_id = settings.tracker.org_id

    def _headers(self, access_token: str) -> dict[str, str]:
        """Build request headers."""
        return {
            "Authorization": f"OAuth {access_token}",
            "X-Org-Id": self._org_id,
        }

    @with_retry(max_attempts=3, base_delay=0.5)
    async def myself(self, access_token: str) -> tuple[int, Any]:
        """Get current user info."""
        client = await get_client()
        r = await client.get(
            f"{self._api_base}/myself",
            headers=self._headers(access_token)
        )
        return r.status_code, safe_json(r)

    @with_retry(max_attempts=3, base_delay=0.5)
    async def search_issues(
        self, access_token: str, query: str, limit: int = 50, order: str = ""
    ) -> tuple[int, Any]:
        """Search issues."""
        client = await get_client()
        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        params: dict = {"page": 1, "perPage": limit}
        if order:
            params["order"] = order
        body: dict = {"query": query}
        r = await client.post(
            f"{self._api_base}/issues/_search",
            headers=headers,
            params=params,
            json=body
        )
        return r.status_code, safe_json(r)

    @with_retry(max_attempts=3, base_delay=0.5)
    async def get_issue(self, access_token: str, issue_key: str) -> tuple[int, Any]:
        """Get single issue."""
        client = await get_client()
        r = await client.get(
            f"{self._api_base}/issues/{issue_key}",
            headers=self._headers(access_token)
        )
        return r.status_code, safe_json(r)

    async def get_issue_with_changelog(
        self, access_token: str, issue_key: str
    ) -> tuple[int, Any]:
        """Get issue with changelog and comments for summary generation."""
        st, issue_data = await self.get_issue(access_token, issue_key)
        if st != 200 or not isinstance(issue_data, dict):
            return st, issue_data

        client = await get_client()
        headers = self._headers(access_token)

        # Fetch comments
        try:
            r = await client.get(
                f"{self._api_base}/issues/{issue_key}/comments",
                headers=headers
            )
            if r.status_code == 200:
                comments = safe_json(r)
                if isinstance(comments, list):
                    issue_data["comments"] = comments
                elif isinstance(comments, dict) and "values" in comments:
                    issue_data["comments"] = comments.get("values", [])
        except Exception as e:
            logger.debug(f"Failed to fetch comments for {issue_key}: {e}")
            issue_data["comments"] = []

        # Fetch changelog
        try:
            r = await client.get(
                f"{self._api_base}/issues/{issue_key}/changelog",
                headers=headers
            )
            if r.status_code == 200:
                changelog = safe_json(r)
                if isinstance(changelog, list):
                    issue_data["changelog"] = changelog
                elif isinstance(changelog, dict) and "values" in changelog:
                    issue_data["changelog"] = changelog.get("values", [])
        except Exception as e:
            logger.debug(f"Failed to fetch changelog for {issue_key}: {e}")
            issue_data["changelog"] = []

        return 200, issue_data

    @with_retry(max_attempts=2, base_delay=0.5)
    async def patch_issue(
        self, access_token: str, issue_key: str, patch: dict
    ) -> tuple[int, Any]:
        """Update issue."""
        client = await get_client()
        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        r = await client.patch(
            f"{self._api_base}/issues/{issue_key}",
            headers=headers,
            json=patch
        )
        return r.status_code, safe_json(r)

    @with_retry(max_attempts=2, base_delay=0.5)
    async def add_comment(
        self, access_token: str, issue_key: str, text: str
    ) -> tuple[int, Any]:
        """Add comment to issue."""
        client = await get_client()
        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        r = await client.post(
            f"{self._api_base}/issues/{issue_key}/comments",
            headers=headers,
            json={"text": text}
        )
        return r.status_code, safe_json(r)

    @with_retry(max_attempts=2, base_delay=0.5)
    async def create_issue(
        self, access_token: str, issue_data: dict
    ) -> tuple[int, Any]:
        """Create new issue."""
        client = await get_client()
        headers = {**self._headers(access_token), "Content-Type": "application/json"}
        r = await client.post(
            f"{self._api_base}/issues",
            headers=headers,
            json=issue_data
        )
        return r.status_code, safe_json(r)


# =============================================================================
# Calendar Client (CalDAV)
# =============================================================================
class CalendarClient:
    """Yandex Calendar CalDAV client (личный caldav.yandex.ru или Яндекс 360 caldav.360.yandex.ru)."""
    
    def __init__(self, caldav_base: Optional[str] = None):
        self._caldav_base = (caldav_base or settings.caldav_base_url).rstrip("/")
    
    def _headers(self, access_token: str) -> dict[str, str]:
        """Build CalDAV request headers. Yandex CalDAV expects OAuth scheme (same as Tracker API)."""
        return {
            "Authorization": f"OAuth {access_token}",
            "Content-Type": "application/xml; charset=utf-8",
            "Depth": "1",
        }
    
    @with_retry(max_attempts=2, base_delay=1.0)
    async def get_events(self, access_token: str, email: str, start_date: str, end_date: str) -> tuple[int, Any]:
        """
        Get calendar events for date range using CalDAV REPORT.
        
        Args:
            access_token: OAuth token
            email: User email (login)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (inclusive)
        
        Returns:
            Tuple of (status_code, events_list)
        """
        client = await get_client()
        # CalDAV REPORT request body for date range query
        report_body = f"""<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
    <D:prop>
        <D:getetag/>
        <C:calendar-data/>
    </D:prop>
    <C:filter>
        <C:comp-filter name="VCALENDAR">
            <C:comp-filter name="VEVENT">
                <C:time-range start="{start_date}T00:00:00Z" end="{end_date}T23:59:59Z"/>
            </C:comp-filter>
        </C:comp-filter>
    </C:filter>
</C:calendar-query>"""

        # Yandex CalDAV may expect login or full email; try login first, then login@yandex.ru on 404
        identifiers_to_try: list[str] = [email]
        if "@" not in (email or ""):
            identifiers_to_try.append(f"{email}@yandex.ru")

        last_status = 0
        last_response_text = ""

        try:
            for cal_identifier in identifiers_to_try:
                calendar_url = f"{self._caldav_base}/calendars/{cal_identifier}/events-default"
                logger.info(f"[CALENDAR] Sending REPORT request to: {calendar_url}")
                r = await client.request(
                    "REPORT",
                    calendar_url,
                    headers=self._headers(access_token),
                    content=report_body.encode('utf-8'),
                    timeout=get_timeout(long=True)
                )
                last_status = r.status_code
                last_response_text = r.text[:500]
                logger.info(f"[CALENDAR] Response: status={r.status_code}, url={calendar_url}, body_length={len(r.text)}")

                if r.status_code == 200:
                    events = self._parse_icalendar(r.text)
                    logger.info(f"[CALENDAR] Retrieved events: identifier={cal_identifier}, date={start_date}, count={len(events)}")
                    return 200, events
                if r.status_code == 404 and cal_identifier != identifiers_to_try[-1]:
                    continue  # try next identifier
                if r.status_code == 401:
                    logger.warning("[CALENDAR] CalDAV 401 Unauthorized: identifier=%s", cal_identifier)
                    return 401, {"error": "Календарь: не авторизован (401). Выполните /connect заново.", "response": last_response_text}
                if r.status_code == 403:
                    logger.warning("[CALENDAR] CalDAV 403 Forbidden: identifier=%s", cal_identifier)
                    return 403, {"error": "Calendar access denied (403). Reconnect with calendar rights.", "response": last_response_text}
                if r.status_code == 404:
                    logger.warning("[CALENDAR] CalDAV 404 Not Found: tried %s", identifiers_to_try)
                    return 404, {"error": "Календарь не найден (404). Проверьте настройки календаря в Яндексе.", "response": last_response_text}
                logger.warning(f"[CALENDAR] Request failed: status={r.status_code}, identifier={cal_identifier}, response={last_response_text}")
                return r.status_code, {"error": f"Calendar API returned {r.status_code}", "response": last_response_text}

            # all identifiers tried, last was 404
            return 404, {"error": "Календарь не найден (404). Проверьте настройки календаря в Яндексе.", "response": last_response_text}
        except Exception as e:
            logger.error(f"[CALENDAR] Request exception: {type(e).__name__}: {e}", exc_info=True)
            return 503, {"error": f"Calendar request failed: {type(e).__name__}"}
    
    def _parse_icalendar(self, ical_data: str) -> list[dict]:
        """
        Parse iCalendar data and extract events.
        Returns list of event dicts with: summary, start, end, description, url
        """
        events = []
        if not ical_data or "BEGIN:VEVENT" not in ical_data:
            return events
        
        try:
            import re
            # Split by VEVENT blocks
            event_blocks = re.split(r'BEGIN:VEVENT', ical_data)
            
            for block in event_blocks[1:]:  # Skip first empty part
                if "END:VEVENT" not in block:
                    continue
                
                event = {}
                
                # Extract summary (title)
                summary_match = re.search(r'SUMMARY[;:]([^\r\n]+)', block)
                if summary_match:
                    event["summary"] = summary_match.group(1).strip()
                
                # Extract start time
                dtstart_match = re.search(r'DTSTART[;:]([^\r\n]+)', block)
                if dtstart_match:
                    dtstart = dtstart_match.group(1).strip()
                    event["start"] = self._parse_ical_date(dtstart)
                
                # Extract end time
                dtend_match = re.search(r'DTEND[;:]([^\r\n]+)', block)
                if dtend_match:
                    dtend = dtend_match.group(1).strip()
                    event["end"] = self._parse_ical_date(dtend)
                
                # Extract description
                desc_match = re.search(r'DESCRIPTION[;:]([^\r\n]+)', block)
                if desc_match:
                    event["description"] = desc_match.group(1).strip()
                
                # Extract URL
                url_match = re.search(r'URL[;:]([^\r\n]+)', block)
                if url_match:
                    event["url"] = url_match.group(1).strip()
                
                # Extract UID for calendar link
                uid_match = re.search(r'UID[;:]([^\r\n]+)', block)
                if uid_match:
                    uid = uid_match.group(1).strip()
                    # Build Yandex Calendar URL
                    event["calendar_url"] = f"https://calendar.yandex.ru/event/{uid}"
                
                if event.get("summary"):  # Only add if has title
                    events.append(event)
                    
        except Exception as e:
            logger.error(f"Failed to parse iCalendar: {e}")
        
        return events
    
    def _parse_ical_date(self, dt_str: str) -> str:
        """Parse iCalendar date format to readable string."""
        try:
            # Handle formats: 20240125T100000Z or 20240125
            dt_str = dt_str.replace("Z", "").replace("T", "")
            if len(dt_str) >= 8:
                year = dt_str[0:4]
                month = dt_str[4:6]
                day = dt_str[6:8]
                time_str = ""
                if len(dt_str) > 8:
                    hour = dt_str[9:11] if len(dt_str) > 9 else dt_str[8:10]
                    minute = dt_str[11:13] if len(dt_str) > 11 else dt_str[10:12]
                    time_str = f" {hour}:{minute}"
                return f"{year}-{month}-{day}{time_str}"
        except Exception:
            pass
        return dt_str


# =============================================================================
# Service Layer
# =============================================================================
class TrackerService:
    """Business logic for Tracker operations."""
    
    def __init__(self, storage: TokenStorage, oauth: OAuthClient, tracker: TrackerClient, calendar: CalendarClient):
        self.storage = storage
        self.oauth = oauth
        self.tracker = tracker
        self.calendar = calendar

    async def _get_valid_access_token(self, tg_id: int) -> tuple[Optional[str], Optional[dict]]:
        """Get valid access token, refreshing if needed."""
        tokens = await self.storage.get_tokens(tg_id)
        if not tokens or not tokens.get("access_token"):
            return None, {"http_status": 401, "body": {"error": "No token. Use /connect first."}}

        access = tokens["access_token"]
        
        try:
            st, me = await self.tracker.myself(access)
        except Exception as e:
            logger.warning(f"Token validation failed for tg_id={tg_id}: {type(e).__name__}")
            return None, {"http_status": 503, "body": {"error": "Tracker API unavailable"}}

        if st != 401:
            if st == 200 and isinstance(me, dict):
                login = me.get("login")
                uid = me.get("trackerUid") or me.get("passportUid") or me.get("uid")
                if login:
                    await self.storage.upsert_user(tg_id, login, str(uid) if uid else None)
            return access, None

        # Token expired, try refresh
        refresh_token = tokens.get("refresh_token")
        if not refresh_token:
            return None, {"http_status": 401, "body": {"error": "Expired token and no refresh_token. Reconnect."}}

        try:
            new_payload = await self.oauth.refresh(refresh_token)
        except Exception as e:
            logger.warning(f"Token refresh failed for tg_id={tg_id}: {type(e).__name__}")
            return None, {"http_status": 401, "body": {"error": "Token refresh failed. Reconnect."}}

        new_access = new_payload.get("access_token")
        new_refresh = new_payload.get("refresh_token") or refresh_token
        await self.storage.upsert_token(
            tg_id, new_access, new_refresh,
            new_payload.get("token_type"), new_payload.get("expires_in")
        )

        try:
            st2, me2 = await self.tracker.myself(new_access)
            if st2 == 200 and isinstance(me2, dict):
                login2 = me2.get("login")
                uid2 = me2.get("trackerUid") or me2.get("passportUid") or me2.get("uid")
                if login2:
                    await self.storage.upsert_user(tg_id, login2, str(uid2) if uid2 else None)
        except Exception:
            pass

        return new_access, None

    async def user_by_tg(self, tg_id: int) -> dict:
        """Get Tracker user by Telegram ID."""
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
        """Get /myself info for Telegram user."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err
        
        try:
            st, payload = await self.tracker.myself(access)  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Tracker API error: {type(e).__name__}"}}
        
        return {"http_status": 200, "body": {"status_code": st, "response": payload}}

    async def settings_get(self, tg_id: int) -> dict:
        """Get user settings with caching."""
        # Check cache first
        cached = _settings_cache.get(tg_id)
        if cached:
            return {"http_status": 200, "body": cached}
        
        s = await self.storage.get_settings(tg_id)
        body = {
            "queues": queues_list(s.get("queues_csv", "")),
            "days": s.get("days", 30),
            "limit": s.get("limit_results", 10),
            "reminder": s.get("reminder_hours", 0),
            "morning_report_enabled": s.get("morning_report_enabled", False),
            "morning_report_queue": s.get("morning_report_queue", ""),
            "morning_report_limit": s.get("morning_report_limit", 10),
            "evening_report_enabled": s.get("evening_report_enabled", False),
            "report_enabled": s.get("report_enabled", False),
            "report_queue": s.get("report_queue", ""),
            "report_period": s.get("report_period", "week"),
        }
        _settings_cache.set(tg_id, body)
        return {"http_status": 200, "body": body}

    async def settings_set_queues(self, tg_id: int, queues_csv: str) -> dict:
        await self.storage.set_queues(tg_id, queues_csv)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_days(self, tg_id: int, days: int) -> dict:
        await self.storage.set_days(tg_id, days)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_limit(self, tg_id: int, limit: int) -> dict:
        await self.storage.set_limit(tg_id, limit)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_reminder(self, tg_id: int, hours: int) -> dict:
        await self.storage.set_reminder(tg_id, hours)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_morning_enabled(self, tg_id: int, enabled: bool) -> dict:
        await self.storage.set_morning_report_enabled(tg_id, enabled)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_morning_queue(self, tg_id: int, queue: str) -> dict:
        await self.storage.set_morning_report_queue(tg_id, queue)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_morning_limit(self, tg_id: int, limit: int) -> dict:
        await self.storage.set_morning_report_limit(tg_id, limit)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_evening_enabled(self, tg_id: int, enabled: bool) -> dict:
        await self.storage.set_evening_report_enabled(tg_id, enabled)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_report_enabled(self, tg_id: int, enabled: bool) -> dict:
        await self.storage.set_report_enabled(tg_id, enabled)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_report_queue(self, tg_id: int, queue: str) -> dict:
        await self.storage.set_report_queue(tg_id, queue)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def settings_set_report_period(self, tg_id: int, period: str) -> dict:
        await self.storage.set_report_period(tg_id, period)
        _settings_cache.invalidate(tg_id)
        return await self.settings_get(tg_id)

    async def get_users_with_report(self) -> list[dict]:
        return await self.storage.get_users_with_report()

    async def get_users_with_morning_report(self) -> list[dict]:
        return await self.storage.get_users_with_morning_report()

    async def get_users_with_evening_report(self) -> list[dict]:
        return await self.storage.get_users_with_evening_report()

    async def morning_report(self, tg_id: int, queue: str = "", limit: int = 10, date_offset: int = 0) -> dict:
        """Get morning report: open issues in queue.
        
        date_offset: 0 = today, 1 = yesterday, etc.
        For historical dates, shows issues that were open on that date (updated before or on that date).
        """
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        # Use provided queue or get from settings
        if not queue:
            s = await self.storage.get_settings(tg_id)
            queue = s.get("morning_report_queue", "")
            limit = s.get("morning_report_limit", 10)

        if not queue:
            return {"http_status": 400, "body": {"error": "Очередь не указана"}}

        # For today: open issues (exclude closed status). For past dates: issues updated on or before that date
        if date_offset == 0:
            query = f'Queue: {queue} AND Status: !"Закрыт"'
        else:
            query = f'Queue: {queue} AND Status: !"Закрыт" AND Updated: <= now()-{date_offset}d'

        try:
            st, payload = await self.tracker.search_issues(access, query=query, limit=limit, order="-updated")
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Search failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": 200, "body": {"status_code": st, "response": payload}}

        issues_raw = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []

        issues = []
        for issue in issues_raw[:limit]:
            issues.append({
                "key": issue.get("key", ""),
                "summary": issue.get("summary", ""),
                "status": (issue.get("status") or {}).get("display", ""),
                "updatedAt": issue.get("updatedAt", ""),
                "url": f"https://tracker.yandex.ru/{issue.get('key', '')}"
            })

        return {
            "http_status": 200,
            "body": {
                "queue": queue,
                "count": len(issues),
                "issues": issues
            }
        }

    async def evening_report(self, tg_id: int, queue: str = "", date_offset: int = 0) -> dict:
        """Get evening report: issues closed on a specific day.
        
        date_offset: 0 = today, 1 = yesterday, etc.
        """
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        # Use provided queue or get from settings (same as morning)
        if not queue:
            s = await self.storage.get_settings(tg_id)
            queue = s.get("morning_report_queue", "")

        if not queue:
            return {"http_status": 400, "body": {"error": "Очередь не указана"}}

        # Closed on specific day - by status and updated date
        closed_statuses = '"Закрыт", "Завершен", "Решен", "Closed", "Done", "Resolved"'
        if date_offset == 0:
            query = f'Queue: {queue} AND Status: {closed_statuses} AND Updated: >= today()'
        else:
            # Closed on that specific day: >= start of day AND < start of next day
            query = f'Queue: {queue} AND Status: {closed_statuses} AND Updated: >= now()-{date_offset}d AND Updated: < now()-{date_offset - 1}d'

        try:
            st, payload = await self.tracker.search_issues(access, query=query, limit=50, order="-updated")
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Search failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": 200, "body": {"status_code": st, "response": payload}}

        issues_raw = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []

        issues = []
        for issue in issues_raw:
            issues.append({
                "key": issue.get("key", ""),
                "summary": issue.get("summary", ""),
                "status": (issue.get("status") or {}).get("display", ""),
                "updatedAt": issue.get("updatedAt", ""),
                "url": f"https://tracker.yandex.ru/{issue.get('key', '')}"
            })

        return {
            "http_status": 200,
            "body": {
                "queue": queue,
                "count": len(issues),
                "issues": issues
            }
        }

    async def queue_stats(self, tg_id: int, queue: str, period: str = "today") -> dict:
        """Get queue statistics for period."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        if not queue:
            return {"http_status": 400, "body": {"error": "Очередь не указана"}}

        # Build date filter based on period
        if period == "today":
            date_filter = "today()"
        elif period == "week":
            date_filter = "now()-7d"
        elif period == "month":
            date_filter = "now()-30d"
        else:
            # Custom date range: period = "2024-01-01,2024-01-31"
            if "," in period:
                start, end = period.split(",", 1)
                date_filter = None  # Will use custom filter
            else:
                date_filter = "now()-7d"  # default to week

        stats = {"queue": queue, "period": period, "created": 0, "in_progress": 0, "closed": 0}

        try:
            # Count created
            if date_filter:
                query = f"Queue: {queue} AND Created: >= {date_filter}"
            else:
                query = f'Queue: {queue} AND Created: >= "{start}" AND Created: <= "{end}"'
            st, payload = await self.tracker.search_issues(access, query=query, limit=200)
            if st == 200:
                issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
                stats["created"] = len(issues)

            # Count in progress (open)
            query = f"Queue: {queue} AND Resolution: empty()"
            st, payload = await self.tracker.search_issues(access, query=query, limit=200)
            if st == 200:
                issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
                stats["in_progress"] = len(issues)

            # Count closed in period
            if date_filter:
                query = f"Queue: {queue} AND Resolved: >= {date_filter}"
            else:
                query = f'Queue: {queue} AND Resolved: >= "{start}" AND Resolved: <= "{end}"'
            st, payload = await self.tracker.search_issues(access, query=query, limit=200)
            if st == 200:
                issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
                stats["closed"] = len(issues)

        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Stats failed: {type(e).__name__}"}}

        return {"http_status": 200, "body": stats}

    async def get_users_with_reminder(self) -> list[dict]:
        return await self.storage.get_users_with_reminder()

    def _build_candidate_query(self, queues: list[str], days: int) -> str:
        """Build search query for checklist candidates."""
        exclude_closed = (
            'Status: !"Закрыт" AND Status: !"Завершен" AND Status: !"Решен" '
            'AND Status: !"Closed" AND Status: !"Done" AND Status: !"Resolved"'
        )
        base = f"Updated: >= now()-{int(days)}d AND {exclude_closed}"
        if not queues:
            return base
        queue_conditions = [f"Queue: {x}" for x in queues]
        q = " OR ".join(queue_conditions)
        return f"({q}) AND {base}"

    @staticmethod
    def _extract_checklist_items(issue_payload: Any) -> list[dict]:
        if isinstance(issue_payload, dict) and isinstance(issue_payload.get("checklistItems"), list):
            return issue_payload["checklistItems"]
        return []

    async def checklist_assigned_issues(
        self, tg_id: int, only_unchecked: bool, limit: int = 10
    ) -> dict:
        """Get issues with checklist items assigned to user."""
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
        queues = queues_list(s.get("queues_csv", ""))
        days = int(s.get("days", 30))
        if limit == 10:
            limit = int(s.get("limit_results", 10))

        query = self._build_candidate_query(queues, days)
        search_limit = min(max(50, limit * 5), 150)

        try:
            st, payload = await self.tracker.search_issues(access, query=query, limit=search_limit)  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Search failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": 200, "body": {"status_code": st, "response": payload, "query": query}}

        issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []

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

        issues_by_key = {it.get("key"): it for it in issues if it.get("key")}
        issue_keys = list(issues_by_key.keys())[:min(len(issues_by_key), 120)]

        async def fetch_issue(key: str) -> tuple[str, Optional[dict]]:
            try:
                sti, issue_full = await self.tracker.get_issue(access, key)  # type: ignore
                if sti == 200 and isinstance(issue_full, dict):
                    return key, issue_full
            except Exception:
                pass
            return key, None

        batch_size = 20
        result: list[dict] = []

        for i in range(0, len(issue_keys), batch_size):
            if len(result) >= limit * 2:
                break

            batch_keys = issue_keys[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[fetch_issue(key) for key in batch_keys],
                return_exceptions=True
            )

            for result_item in batch_results:
                if isinstance(result_item, Exception):
                    continue
                key, issue_full = result_item
                if issue_full is None:
                    continue

                checklist_items = self._extract_checklist_items(issue_full)
                if not checklist_items:
                    continue

                matched_items = []
                all_items = []

                for ci in checklist_items:
                    ass = ci.get("assignee")
                    item_data = {
                        "id": str(ci.get("id", "")),
                        "text": (ci.get("text") or ci.get("textHtml") or "").strip(),
                        "checked": bool(ci.get("checked", False)),
                        "assignee": {
                            "display": ass.get("display") if ass else "",
                            "login": ass.get("login") if ass else ""
                        },
                        "is_mine": bool(ass and (ass.get("login") or "").lower() == login),
                    }
                    all_items.append(item_data)

                    if only_unchecked and ci.get("checked") is True:
                        continue

                    if ass and (ass.get("login") or "").lower() == login:
                        matched_items.append(item_data)

                if matched_items:
                    orig_issue = issues_by_key.get(key, {})
                    updated_at = (
                        issue_full.get("updatedAt") or issue_full.get("updated") or
                        orig_issue.get("updatedAt") or orig_issue.get("updated")
                    )
                    # Extract status
                    status_obj = issue_full.get("status") or orig_issue.get("status")
                    status_name = ""
                    if isinstance(status_obj, dict):
                        status_name = status_obj.get("display") or status_obj.get("name") or ""
                    elif isinstance(status_obj, str):
                        status_name = status_obj
                    
                    result.append({
                        "key": key,
                        "summary": orig_issue.get("summary") or issue_full.get("summary") or "",
                        "url": f"https://tracker.yandex.ru/{key}",
                        "updatedAt": updated_at,
                        "status": status_name,
                        "items": matched_items,
                        "all_items": all_items,
                    })

        # Distribute results across queues for diversity
        if len(result) > limit:
            result_by_queue: dict[str, list[dict]] = {}
            for r in result:
                queue = r["key"].split("-")[0] if "-" in r["key"] else "UNKNOWN"
                if queue not in result_by_queue:
                    result_by_queue[queue] = []
                result_by_queue[queue].append(r)

            final_result: list[dict] = []
            per_queue = max(1, limit // max(len(result_by_queue), 1))
            for queue_results in result_by_queue.values():
                final_result.extend(queue_results[:per_queue])
                if len(final_result) >= limit:
                    break

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

    async def checklist_item_check(
        self, tg_id: int, issue_key: str, item_id: str, checked: bool
    ) -> dict:
        """Check/uncheck a checklist item."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        try:
            sti, issue_full = await self.tracker.get_issue(access, issue_key)  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Get issue failed: {type(e).__name__}"}}

        if sti != 200 or not isinstance(issue_full, dict):
            return {"http_status": 200, "body": {"status_code": sti, "response": issue_full}}

        items = self._extract_checklist_items(issue_full)
        if not items:
            return {"http_status": 404, "body": {"error": "No checklistItems in issue"}}

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
            return {"http_status": 404, "body": {"error": "Checklist item not found", "item_id": item_id}}

        try:
            stp, resp = await self.tracker.patch_issue(access, issue_key, {"checklistItems": new_items})  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Patch failed: {type(e).__name__}"}}

        return {"http_status": 200, "body": {"status_code": stp, "response": resp}}

    async def get_issue_full(self, tg_id: int, issue_key: str) -> dict:
        """Get full issue data with changelog and comments."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        try:
            st, issue_data = await self.tracker.get_issue_with_changelog(access, issue_key)  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Get issue failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": st, "body": issue_data}

        return {"http_status": 200, "body": issue_data}

    async def search_issues(self, tg_id: int, query: str, limit: int = 10) -> dict:
        """Search issues using YQL query."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        try:
            st, payload = await self.tracker.search_issues(access, query=query, limit=limit, order="-updated")
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Search failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": st, "body": payload}

        issues_raw = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
        
        issues = []
        for issue in issues_raw[:limit]:
            issues.append({
                "key": issue.get("key", ""),
                "summary": issue.get("summary", ""),
                "status": issue.get("status", {}),
                "updatedAt": issue.get("updatedAt", ""),
                "url": f"https://tracker.yandex.ru/{issue.get('key', '')}"
            })

        return {
            "http_status": 200,
            "body": {
                "query": query,
                "count": len(issues),
                "issues": issues
            }
        }

    async def get_issue_checklist(self, tg_id: int, issue_key: str) -> dict:
        """Get checklist items for a specific issue."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        try:
            st, issue_data = await self.tracker.get_issue(access, issue_key)  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Get issue failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": st, "body": issue_data}

        items = self._extract_checklist_items(issue_data)
        return {
            "http_status": 200,
            "body": {
                "issue_key": issue_key,
                "checklist_items": items
            }
        }

    async def get_summons(self, tg_id: int, limit: int = 10) -> dict:
        """Get issues where user was mentioned. Optimized with batch comment fetching."""
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
        queues = queues_list(s.get("queues_csv", ""))
        days = int(s.get("days", 30))
        if limit == 10:
            limit = int(s.get("limit_results", 10))

        exclude_closed = (
            'Status: !"Закрыт" AND Status: !"Завершен" AND Status: !"Решен" '
            'AND Status: !"Closed" AND Status: !"Done" AND Status: !"Resolved"'
        )
        base_query = f"Updated: >= now()-{days}d AND {exclude_closed}"
        if queues:
            queue_conditions = [f"Queue: {x}" for x in queues]
            q = " OR ".join(queue_conditions)
            base_query = f"({q}) AND {base_query}"

        summon_query = f'"Нужен ответ пользователя": me() AND {base_query}'

        try:
            st, payload = await self.tracker.search_issues(access, query=summon_query, limit=min(limit * 3, 100))  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Search failed: {type(e).__name__}"}}

        issues = []
        if st == 200:
            raw_issues = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
            
            # Filter and prepare issues
            candidate_issues = []
            for issue in raw_issues:
                issue_key = issue.get("key")
                if issue_key and len(candidate_issues) < limit:
                    candidate_issues.append(issue)
            
            if not candidate_issues:
                return {
                    "http_status": 200,
                    "body": {
                        "status_code": 200,
                        "issues": [],
                        "settings": {"queues": queues, "days": days},
                        "login": login
                    }
                }
            
            # Batch fetch comments
            client = await get_client()
            headers = {
                "Authorization": f"OAuth {access}",
                "X-Org-Id": settings.tracker.org_id if settings.tracker else "",
            }
            api_base = settings.tracker.api_base if settings.tracker else ""
            
            async def check_responded(issue: dict) -> dict:
                """Check if user responded to issue."""
                issue_key = issue.get("key")
                has_responded = False
                
                try:
                    r = await client.get(
                        f"{api_base}/issues/{issue_key}/comments",
                        headers=headers
                    )
                    if r.status_code == 200:
                        comments = safe_json(r)
                        if isinstance(comments, list):
                            for comment in reversed(comments):
                                author = comment.get("createdBy", {})
                                if (author.get("login") or "").lower() == login:
                                    has_responded = True
                                    break
                except Exception:
                    pass
                
                # Extract status
                status_obj = issue.get("status")
                status_name = ""
                if isinstance(status_obj, dict):
                    status_name = status_obj.get("display") or status_obj.get("name") or ""
                elif isinstance(status_obj, str):
                    status_name = status_obj
                
                return {
                    "key": issue_key,
                    "summary": issue.get("summary") or "",
                    "url": f"https://tracker.yandex.ru/{issue_key}",
                    "updatedAt": issue.get("updatedAt") or issue.get("updated"),
                    "status": status_name,
                    "has_responded": has_responded
                }
            
            # Batch process in groups of 10
            batch_size = 10
            for i in range(0, len(candidate_issues), batch_size):
                batch = candidate_issues[i:i + batch_size]
                results = await asyncio.gather(
                    *[check_responded(issue) for issue in batch],
                    return_exceptions=True
                )
                for result in results:
                    if isinstance(result, dict):
                        issues.append(result)

        return {
            "http_status": 200,
            "body": {
                "status_code": 200,
                "issues": issues,
                "settings": {"queues": queues, "days": days},
                "login": login
            }
        }

    async def issue_summary(self, tg_id: int, issue_key: str, extended: bool = False) -> dict:
        """Generate AI summary for issue."""
        issue_result = await self.get_issue_full(tg_id, issue_key)
        if issue_result["http_status"] != 200:
            return issue_result

        issue_data = issue_result["body"]

        try:
            from ai_service import generate_summary
            summary_text, error_msg = await generate_summary(issue_data, extended=extended)
        except ImportError as e:
            return {"http_status": 500, "body": {"error": f"AI module not found: {e}"}}
        except Exception as e:
            logger.error(f"AI summary error for {issue_key}: {type(e).__name__}")
            return {"http_status": 500, "body": {"error": f"AI service error: {type(e).__name__}"}}

        if not summary_text:
            error_detail = error_msg or "Unknown error"
            return {"http_status": 500, "body": {"error": f"Summary generation failed: {error_detail}"}}

        return {
            "http_status": 200,
            "body": {
                "issue_key": issue_key,
                "summary": summary_text,
                "issue_url": f"https://tracker.yandex.ru/{issue_key}"
            }
        }

    async def ai_search(self, tg_id: int, user_query: str, limit: int = 10) -> dict:
        """Search issues using AI-generated query from natural language."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        s = await self.storage.get_settings(tg_id)
        queues = queues_list(s.get("queues_csv", ""))
        days = int(s.get("days", 30))
        if limit == 10:
            limit = int(s.get("limit_results", 10))

        try:
            from ai_service import generate_search_query
            tracker_query, error_msg = await generate_search_query(user_query, queues, days)
        except ImportError as e:
            return {"http_status": 500, "body": {"error": f"AI module not found: {e}"}}
        except Exception as e:
            logger.error(f"AI search error: {type(e).__name__}")
            return {"http_status": 500, "body": {"error": f"AI service error: {type(e).__name__}"}}

        if not tracker_query:
            return {"http_status": 500, "body": {"error": error_msg or "Failed to generate search query"}}

        # Check for special commands from AI
        tracker_query_upper = tracker_query.strip().upper()
        if tracker_query_upper == "CHECKLIST":
            return {"http_status": 200, "body": {"redirect": "checklist", "message": "Используйте /cl_my или /cl_my_open для чеклистов"}}
        if tracker_query_upper == "SUMMONS":
            return {"http_status": 200, "body": {"redirect": "summons", "message": "Используйте /mentions для призывов"}}

        # Execute the search
        try:
            st, payload = await self.tracker.search_issues(access, query=tracker_query, limit=limit)
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Search failed: {type(e).__name__}"}}

        if st != 200:
            return {"http_status": 200, "body": {"status_code": st, "response": payload, "query": tracker_query}}

        issues_raw = payload if isinstance(payload, list) else payload.get("issues") or payload.get("items") or []
        
        issues = []
        for issue in issues_raw[:limit]:
            issue_key = issue.get("key")
            if not issue_key:
                continue
            
            summary = issue.get("summary") or ""
            description = (issue.get("description") or "")[:150]
            if len(description) == 150:
                description += "..."
            
            status = ""
            if isinstance(issue.get("status"), dict):
                status = issue["status"].get("display", "")
            
            issues.append({
                "key": issue_key,
                "summary": summary,
                "description": description,
                "status": status,
                "url": f"https://tracker.yandex.ru/{issue_key}",
                "updatedAt": issue.get("updatedAt") or issue.get("updated"),
            })

        return {
            "http_status": 200,
            "body": {
                "status_code": 200,
                "issues": issues,
                "query": tracker_query,
                "user_query": user_query,
                "settings": {"queues": queues, "days": days},
            }
        }

    async def add_comment(self, tg_id: int, issue_key: str, text: str) -> dict:
        """Add comment to issue."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        try:
            st, resp = await self.tracker.add_comment(access, issue_key, text)  # type: ignore
        except Exception as e:
            return {"http_status": 503, "body": {"error": f"Add comment failed: {type(e).__name__}"}}

        if st not in (200, 201):
            return {"http_status": st, "body": resp}

        return {
            "http_status": 200,
            "body": {
                "status": "ok",
                "issue_key": issue_key,
                "comment_id": resp.get("id") if isinstance(resp, dict) else None
            }
        }
    
    async def get_calendar_events(self, tg_id: int, date: str) -> dict:
        """
        Get calendar events for a specific date.
        
        Args:
            tg_id: Telegram user ID
            date: Date in YYYY-MM-DD format
        
        Returns:
            Dict with http_status and body containing events list
        """
        logger.info(f"[CALENDAR] Getting events: tg_id={tg_id}, date={date}")
        
        # Get access token
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            logger.warning(f"[CALENDAR] No valid token for tg_id={tg_id}")
            return err
        
        # Get user email from /myself
        try:
            st, me = await self.tracker.myself(access)
            if st != 200 or not isinstance(me, dict):
                logger.error(f"[CALENDAR] Failed to get user info, status={st}")
                return {"http_status": 503, "body": {"error": "Failed to get user info"}}
            
            email = me.get("login")
            if not email:
                logger.error(f"[CALENDAR] No email in user info for tg_id={tg_id}")
                return {"http_status": 404, "body": {"error": "User email not found"}}
        except Exception as e:
            logger.error(f"[CALENDAR] Exception getting user info: {type(e).__name__}: {e}")
            return {"http_status": 503, "body": {"error": f"Failed to get user info: {type(e).__name__}"}}
        
        # Get events for the date (start and end are the same day)
        try:
            st, events = await self.calendar.get_events(access, email, date, date)
            if st == 200:
                event_count = len(events) if isinstance(events, list) else 0
                logger.info(f"[CALENDAR] Retrieved {event_count} events for {email} on {date}")
                return {"http_status": 200, "body": {"events": events, "date": date, "email": email}}
            else:
                err_body = events if isinstance(events, dict) else {"error": f"Calendar API error: {st}"}
                logger.warning(
                    f"[CALENDAR] API returned status {st} for tg_id={tg_id}, email={email}, date={date}, body={err_body.get('error', err_body)}"
                )
                return {"http_status": st, "body": err_body}
        except Exception as e:
            logger.error(f"[CALENDAR] Exception getting events: tg_id={tg_id}, {type(e).__name__}: {e}", exc_info=True)
            return {"http_status": 503, "body": {"error": f"Calendar request failed: {type(e).__name__}"}}

    async def create_issue(
        self, tg_id: int, queue: str, summary: str,
        description: str = "", assignee: str = "",
        pending_reply_from: str = "", followers: list = None
    ) -> dict:
        """Create new issue in Tracker."""
        access, err = await self._get_valid_access_token(tg_id)
        if err:
            return err

        issue_data = {
            "queue": queue.upper(),
            "summary": summary,
        }
        
        if description:
            issue_data["description"] = description
        if assignee:
            issue_data["assignee"] = assignee
        if pending_reply_from:
            issue_data["pendingReplyFrom"] = pending_reply_from
        if followers:
            issue_data["followers"] = followers

        try:
            st, resp = await self.tracker.create_issue(access, issue_data)  # type: ignore
        except Exception as e:
            logger.error(f"Create issue failed: {type(e).__name__}: {e}")
            return {"http_status": 503, "body": {"error": f"Create issue failed: {type(e).__name__}"}}

        if st not in (200, 201):
            error_msg = ""
            if isinstance(resp, dict):
                # Check errorMessages array first
                error_messages = resp.get("errorMessages", [])
                if isinstance(error_messages, list) and error_messages:
                    error_msg = "; ".join(str(e) for e in error_messages)
                # Check errors dict (field-level errors)
                errors_dict = resp.get("errors", {})
                if isinstance(errors_dict, dict) and errors_dict:
                    field_errors = [f"{k}: {v}" for k, v in errors_dict.items()]
                    if error_msg:
                        error_msg += "; " + "; ".join(field_errors)
                    else:
                        error_msg = "; ".join(field_errors)
                # Fallback to message
                if not error_msg:
                    error_msg = resp.get("message", str(resp))
            else:
                error_msg = str(resp)
            logger.warning(f"Create issue failed: st={st}, error={error_msg}")
            return {"http_status": st, "body": {"error": error_msg, "response": resp}}

        issue_key = resp.get("key", "") if isinstance(resp, dict) else ""
        return {
            "http_status": 201,
            "body": {
                "status": "ok",
                "issue_key": issue_key,
                "issue_url": f"https://tracker.yandex.ru/{issue_key}",
                "response": resp
            }
        }


# Global service instances (initialized in lifespan)
_storage: Optional[TokenStorage] = None
_oauth: Optional[OAuthClient] = None
_tracker: Optional[TrackerClient] = None
_service: Optional["TrackerService"] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown."""
    global _storage, _oauth, _tracker, _service

    # Startup
    logger.info("Starting tracker-bot-auth service...")

    if settings.database:
        _storage = TokenStorage(settings.database)
        await _storage.connect()
        await _storage.ensure_schema()

    if settings.is_configured and _storage:
        _oauth = OAuthClient()
        _tracker = TrackerClient()
        _calendar = CalendarClient()
        _service = TrackerService(_storage, _oauth, _tracker, _calendar)
        logger.info("Service layer initialized")
    else:
        logger.warning(f"Service not fully configured. Missing: {settings.missing_vars}")

    yield  # Application runs here

    # Shutdown
    if _storage:
        await _storage.close()

    await close_client()
    logger.info("Shutdown complete")


# =============================================================================
# FastAPI Application
# =============================================================================
app = FastAPI(title="Tracker Bot Auth", version="2.2.0", lifespan=lifespan)


def _check_config() -> Optional[JSONResponse]:
    """Check if service is configured."""
    if not settings.is_configured:
        return JSONResponse(
            {"error": "Service not configured", "missing": settings.missing_vars},
            status_code=500
        )
    return None


def _parse_state(state: str) -> int:
    """Parse Telegram ID from OAuth state."""
    tg_str, _nonce = state.split(":", 1)
    return int(tg_str)


async def _check_rate_limit(tg_id: int) -> Optional[JSONResponse]:
    """Check rate limit for user."""
    if not await _rate_limiter.is_allowed(tg_id):
        return JSONResponse(
            {"error": "Rate limit exceeded. Try again later."},
            status_code=429
        )
    return None




# =============================================================================
# Routes
# =============================================================================
@app.get("/")
async def root():
    return {"ok": True, "service": "tracker-bot-auth", "version": "2.2.0"}


@app.get("/ping")
async def ping():
    return "pong"


@app.get("/health")
async def health():
    """Health check endpoint for monitoring."""
    checks = {
        "service": "ok",
        "database": "unknown",
        "http_client": "unknown",
    }
    
    # Check database
    if _storage and _storage.is_connected:
        try:
            # Simple query to verify connection
            await _storage.get_settings(0)
            checks["database"] = "ok"
        except Exception as e:
            checks["database"] = f"error: {type(e).__name__}"
    else:
        checks["database"] = "not_connected"
    
    # Check HTTP client
    try:
        from http_client import _manager
        if _manager.is_active:
            checks["http_client"] = "ok"
        else:
            checks["http_client"] = "not_active"
    except Exception as e:
        checks["http_client"] = f"error: {type(e).__name__}"
    
    is_healthy = all(v == "ok" for v in checks.values())
    status_code = 200 if is_healthy else 503
    
    return JSONResponse({
        "status": "healthy" if is_healthy else "unhealthy",
        "checks": checks,
        "uptime_seconds": metrics.get_counter("uptime") or int(time.time() - metrics._start_time),
    }, status_code=status_code)


@app.get("/metrics")
async def get_metrics():
    """Get service metrics."""
    return JSONResponse(metrics.snapshot())


@app.get("/oauth/start")
async def oauth_start(tg: int = Query(..., ge=1)):
    err = _check_config()
    if err:
        return err
    return RedirectResponse(_oauth.build_authorize_url(tg), status_code=302)  # type: ignore


@app.get("/oauth/callback")
async def oauth_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None
):
    err = _check_config()
    if err:
        return err

    if error:
        return JSONResponse({"ok": False, "error": error, "state": state}, status_code=400)
    if not code or not state:
        return JSONResponse({"ok": False, "error": "Missing code/state"}, status_code=400)

    try:
        tg_id = _parse_state(state)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid state format"}, status_code=400)

    try:
        token_payload = await _oauth.exchange_code(code)  # type: ignore
    except Exception as e:
        logger.warning(f"Token exchange failed: {type(e).__name__}")
        return JSONResponse({"ok": False, "error": "Token exchange failed"}, status_code=400)

    access = token_payload.get("access_token")
    refresh = token_payload.get("refresh_token")
    token_type = token_payload.get("token_type")
    expires_in = token_payload.get("expires_in")

    await _storage.upsert_token(tg_id, access, refresh, token_type, expires_in)  # type: ignore

    try:
        st, me = await _tracker.myself(access)  # type: ignore
        if st == 200 and isinstance(me, dict):
            login = me.get("login")
            uid = me.get("trackerUid") or me.get("passportUid") or me.get("uid")
            if login:
                await _storage.upsert_user(tg_id, login, str(uid) if uid else None)  # type: ignore
    except Exception as e:
        logger.warning(f"Failed to get user info: {type(e).__name__}")

    return JSONResponse({"ok": True, "message": "Connected. Return to Telegram and run /me", "tg": tg_id})


@app.get("/tracker/me_by_tg")
async def tracker_me_by_tg(tg: int = Query(..., ge=1)):
    err = _check_config()
    if err:
        return err
    result = await _service.me_by_tg(tg)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/user_by_tg")
async def tracker_user_by_tg(tg: int = Query(..., ge=1)):
    err = _check_config()
    if err:
        return err
    result = await _service.user_by_tg(tg)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tg/settings")
async def tg_settings(tg: int = Query(..., ge=1)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_get(tg)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/queues")
async def tg_settings_queues(tg: int = Query(..., ge=1), queues: str = Query("")):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_queues(tg, queues)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/days")
async def tg_settings_days(tg: int = Query(..., ge=1), days: int = Query(..., ge=1, le=3650)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_days(tg, days)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/limit")
async def tg_settings_limit(tg: int = Query(..., ge=1), limit: int = Query(..., ge=1, le=100)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_limit(tg, limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/reminder")
async def tg_settings_reminder(tg: int = Query(..., ge=1), hours: int = Query(..., ge=0)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_reminder(tg, hours)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/morning_enabled")
async def tg_settings_morning_enabled(tg: int = Query(..., ge=1), enabled: bool = Query(...)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_morning_enabled(tg, enabled)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/morning_queue")
async def tg_settings_morning_queue(tg: int = Query(..., ge=1), queue: str = Query(...)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_morning_queue(tg, queue)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/morning_limit")
async def tg_settings_morning_limit(tg: int = Query(..., ge=1), limit: int = Query(..., ge=5, le=20)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_morning_limit(tg, limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/evening_enabled")
async def tg_settings_evening_enabled(tg: int = Query(..., ge=1), enabled: bool = Query(...)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_evening_enabled(tg, enabled)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/report_enabled")
async def tg_settings_report_enabled(tg: int = Query(..., ge=1), enabled: bool = Query(...)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_report_enabled(tg, enabled)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/report_queue")
async def tg_settings_report_queue(tg: int = Query(..., ge=1), queue: str = Query(...)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_report_queue(tg, queue)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tg/settings/report_period")
async def tg_settings_report_period(tg: int = Query(..., ge=1), period: str = Query(...)):
    err = _check_config()
    if err:
        return err
    result = await _service.settings_set_report_period(tg, period)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/morning_report")
async def tracker_morning_report(
    tg: int = Query(..., ge=1),
    queue: str = Query(""),
    limit: int = Query(10, ge=1, le=50),
    date_offset: int = Query(0, ge=0, le=30)
):
    """Get morning report: open issues in queue."""
    err = _check_config()
    if err:
        return err
    result = await _service.morning_report(tg, queue, limit, date_offset)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/evening_report")
async def tracker_evening_report(
    tg: int = Query(..., ge=1),
    queue: str = Query(""),
    date_offset: int = Query(0, ge=0, le=30)
):
    """Get evening report: issues closed on specific day."""
    err = _check_config()
    if err:
        return err
    result = await _service.evening_report(tg, queue, date_offset)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/queue_stats")
async def tracker_queue_stats(
    tg: int = Query(..., ge=1),
    queue: str = Query(..., min_length=1),
    period: str = Query("today"),
    date_from: str = Query(""),
    date_to: str = Query("")
):
    """Get queue statistics: created, in_progress, closed."""
    err = _check_config()
    if err:
        return err
    # For custom period, use comma-separated dates
    if period == "custom" and date_from and date_to:
        period = f"{date_from},{date_to}"
    result = await _service.queue_stats(tg, queue, period)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tg/users_with_reminder")
async def tg_users_with_reminder():
    err = _check_config()
    if err:
        return err
    users = await _service.get_users_with_reminder()  # type: ignore
    return JSONResponse({"users": users})


@app.get("/tg/users_with_morning_report")
async def tg_users_with_morning_report():
    err = _check_config()
    if err:
        return err
    users = await _service.get_users_with_morning_report()  # type: ignore
    return JSONResponse({"users": users})


@app.get("/tg/users_with_evening_report")
async def tg_users_with_evening_report():
    err = _check_config()
    if err:
        return err
    users = await _service.get_users_with_evening_report()  # type: ignore
    return JSONResponse({"users": users})


@app.get("/tg/users_with_report")
async def tg_users_with_report():
    err = _check_config()
    if err:
        return err
    users = await _service.get_users_with_report()  # type: ignore
    return JSONResponse({"users": users})


@app.get("/tracker/summons")
async def tracker_summons(tg: int = Query(..., ge=1), limit: int = Query(10, ge=1, le=100)):
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    metrics.inc("api.summons")
    with Timer("summons"):
        result = await _service.get_summons(tg, limit=limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tracker/issue/{issue_key}/comment")
async def tracker_add_comment(
    issue_key: str,
    tg: int = Query(..., ge=1),
    text: str = Query(..., min_length=1)
):
    err = _check_config()
    if err:
        return err
    result = await _service.add_comment(tg, issue_key, text)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tracker/issue/create")
async def tracker_create_issue(
    tg: int = Query(..., ge=1),
    queue: str = Query(..., min_length=1, max_length=20),
    summary: str = Query(..., min_length=1, max_length=500),
    description: str = Query(""),
    assignee: str = Query(""),
    pending_reply_from: str = Query(""),
    followers: str = Query("")  # comma-separated
):
    """Create new issue in Tracker."""
    err = _check_config()
    if err:
        return err
    followers_list = [f.strip() for f in followers.split(",") if f.strip()] if followers else []
    metrics.inc("api.create_issue")
    result = await _service.create_issue(  # type: ignore
        tg, queue, summary, description, assignee, pending_reply_from, followers_list
    )
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/checklist/assigned")
async def checklist_assigned(tg: int = Query(..., ge=1), limit: int = Query(10, ge=1, le=100)):
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    metrics.inc("api.checklist_assigned")
    with Timer("checklist_assigned"):
        result = await _service.checklist_assigned_issues(tg, only_unchecked=False, limit=limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/checklist/assigned_unchecked")
async def checklist_assigned_unchecked(tg: int = Query(..., ge=1), limit: int = Query(10, ge=1, le=100)):
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    metrics.inc("api.checklist_assigned_unchecked")
    with Timer("checklist_assigned_unchecked"):
        result = await _service.checklist_assigned_issues(tg, only_unchecked=True, limit=limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.post("/tracker/checklist/check")
async def checklist_check(
    tg: int = Query(..., ge=1),
    issue: str = Query(..., min_length=1),
    item: str = Query(..., min_length=1),
    checked: bool = Query(True)
):
    err = _check_config()
    if err:
        return err
    result = await _service.checklist_item_check(tg, issue_key=issue, item_id=item, checked=checked)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/issue/{issue_key}/checklist")
async def issue_checklist(tg: int = Query(..., ge=1), issue_key: str = ""):
    err = _check_config()
    if err:
        return err
    result = await _service.get_issue_checklist(tg, issue_key)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/issue/{issue_key}/summary")
async def issue_summary_endpoint(
    tg: int = Query(..., ge=1),
    issue_key: str = "",
    extended: str = Query("false")
):
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    is_extended = extended.lower() in ("true", "1", "yes")
    metrics.inc("api.summary_extended" if is_extended else "api.summary")
    with Timer("summary"):
        result = await _service.issue_summary(tg, issue_key, extended=is_extended)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/issue/{issue_key}")
async def get_issue_endpoint(
    tg: int = Query(..., ge=1),
    issue_key: str = ""
):
    """Get full issue data with comments."""
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    metrics.inc("api.get_issue")
    with Timer("get_issue"):
        result = await _service.get_issue_full(tg, issue_key)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/tracker/search")
async def search_endpoint(
    tg: int = Query(..., ge=1),
    query: str = Query(..., min_length=2, max_length=1000),
    limit: int = Query(10, ge=1, le=50)
):
    """Execute YQL search query."""
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    metrics.inc("api.search")
    with Timer("search"):
        result = await _service.search_issues(tg, query=query, limit=limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])


@app.get("/calendar/test")
async def calendar_test(
    tg: int = Query(..., ge=1)
):
    """Test calendar connection - check token and user info."""
    err = _check_config()
    if err:
        return err
    
    if not _service:
        return JSONResponse({"error": "Service not initialized"}, status_code=500)
    
    logger.info(f"[CALENDAR_TEST] Testing connection for tg_id={tg}")
    
    # Get access token
    access, err_dict = await _service._get_valid_access_token(tg)  # type: ignore
    if err_dict:
        logger.warning(f"[CALENDAR_TEST] No valid token: {err_dict}")
        return JSONResponse(err_dict["body"], status_code=err_dict["http_status"])
    
    logger.info(f"[CALENDAR_TEST] Token obtained: {access[:20]}...")
    
    # Get user info
    try:
        st, me = await _service.tracker.myself(access)  # type: ignore
        if st != 200 or not isinstance(me, dict):
            logger.error(f"[CALENDAR_TEST] Failed to get user info: status={st}")
            return JSONResponse({"error": f"Failed to get user info: {st}", "response": me}, status_code=503)
        
        email = me.get("login")
        if not email:
            logger.error(f"[CALENDAR_TEST] No email in user info")
            return JSONResponse({"error": "User email not found", "user_info": me}, status_code=404)
        
        logger.info(f"[CALENDAR_TEST] User email: {email}")
        
        # Calendar URL uses configured CalDAV base (personal or Yandex 360)
        base = settings.caldav_base_url.rstrip("/")
        calendar_url = f"{base}/calendars/{email}/events-default"
        logger.info(f"[CALENDAR_TEST] Success: tg_id={tg}, email={email}, calendar_url={calendar_url}")
        
        return JSONResponse({
            "status": "ok",
            "token_valid": True,
            "email": email,
            "calendar_url": calendar_url,
            "caldav_base": base,
            "scope_note": "Full calendar access (calendar:all) required for /calendar/events.",
            "user_info": {
                "id": me.get("id"),
                "login": me.get("login"),
                "display": me.get("display"),
            }
        }, status_code=200)
        
    except Exception as e:
        logger.error(f"[CALENDAR_TEST] Exception: {type(e).__name__}: {e}", exc_info=True)
        return JSONResponse({"error": f"Test failed: {type(e).__name__}", "message": str(e)}, status_code=503)


@app.get("/calendar/events")
async def calendar_events(
    tg: int = Query(..., ge=1),
    date: str = Query(..., description="Date in YYYY-MM-DD format")
):
    """Get calendar events for a specific date."""
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    
    # Validate date format
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
        return JSONResponse({"error": "Invalid date format. Use YYYY-MM-DD"}, status_code=400)
    
    metrics.inc("api.calendar_events")
    logger.info(f"[CALENDAR_EVENTS] Request: tg_id={tg}, date={date}")
    with Timer("calendar_events"):
        result = await _service.get_calendar_events(tg, date)  # type: ignore
    status = result["http_status"]
    body = result["body"]
    if status == 200 and isinstance(body, dict):
        events = body.get("events", [])
        logger.info(f"[CALENDAR_EVENTS] Success: tg_id={tg}, date={date}, events_count={len(events)}")
    else:
        logger.warning(f"[CALENDAR_EVENTS] Error: tg_id={tg}, date={date}, status={status}, error={body.get('error', body)}")
    return JSONResponse(body, status_code=status)


@app.get("/tracker/ai_search")
async def ai_search_endpoint(
    tg: int = Query(..., ge=1),
    q: str = Query(..., min_length=2, max_length=500),
    limit: int = Query(10, ge=1, le=50)
):
    """AI-powered search: convert natural language to Tracker query."""
    err = _check_config()
    if err:
        return err
    rate_err = await _check_rate_limit(tg)
    if rate_err:
        return rate_err
    metrics.inc("api.ai_search")
    with Timer("ai_search"):
        result = await _service.ai_search(tg, user_query=q, limit=limit)  # type: ignore
    return JSONResponse(result["body"], status_code=result["http_status"])
