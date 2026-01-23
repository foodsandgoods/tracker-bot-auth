"""
Database layer with connection pooling.
Optimized for low memory usage (1GB RAM target).
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from config import DatabaseConfig

logger = logging.getLogger(__name__)


def _normalize_queues_csv(s: str) -> str:
    """Normalize queue CSV: uppercase, deduplicate, clean."""
    raw = s.replace(";", ",").replace(" ", "")
    parts = [p for p in raw.split(",") if p]
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        p2 = p.upper()
        if p2 not in seen:
            out.append(p2)
            seen.add(p2)
    return ",".join(out)


def _queues_list(csv: str) -> list[str]:
    """Convert CSV to list."""
    if not csv:
        return []
    return [p for p in csv.split(",") if p]


class TokenStorage:
    """
    Database storage with connection pooling.
    
    Uses asyncpg pool with conservative settings for low-RAM environments.
    All methods acquire/release connections properly to avoid leaks.
    """
    
    __slots__ = ('_config', '_pool')
    
    def __init__(self, config: DatabaseConfig):
        self._config = config
        self._pool: Optional[asyncpg.Pool] = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None

    async def connect(self) -> None:
        """Initialize connection pool."""
        if self._pool is not None:
            return
        
        self._pool = await asyncpg.create_pool(
            self._config.url,
            min_size=self._config.min_pool_size,
            max_size=self._config.max_pool_size,
            max_inactive_connection_lifetime=self._config.max_inactive_lifetime,
            command_timeout=self._config.command_timeout,
        )
        logger.info(
            f"Database pool: {self._config.min_pool_size}-{self._config.max_pool_size} connections"
        )

    async def close(self) -> None:
        """Close connection pool gracefully."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database pool closed")

    async def _acquire(self) -> asyncpg.Connection:
        """Get connection from pool, initializing if needed."""
        if self._pool is None:
            await self.connect()
        return await self._pool.acquire()  # type: ignore

    async def ensure_schema(self) -> None:
        """Create tables if they don't exist."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS oauth_tokens (
                    tg_id BIGINT PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    refresh_token TEXT,
                    token_type TEXT,
                    expires_in BIGINT,
                    obtained_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tg_users (
                    tg_id BIGINT PRIMARY KEY,
                    tracker_login TEXT NOT NULL,
                    tracker_uid TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tg_settings (
                    tg_id BIGINT PRIMARY KEY,
                    queues_csv TEXT NOT NULL DEFAULT '',
                    days INT NOT NULL DEFAULT 30,
                    limit_results INT NOT NULL DEFAULT 10,
                    reminder_hours INT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            # Migration: add columns if missing
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS limit_results INT NOT NULL DEFAULT 10;
            """)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS reminder_hours INT NOT NULL DEFAULT 0;
            """)
            # New columns for morning/evening reports
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS morning_report_enabled BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS morning_report_queue TEXT NOT NULL DEFAULT '';
            """)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS morning_report_limit INT NOT NULL DEFAULT 10;
            """)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS evening_report_enabled BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            # Columns for итоговый отчёт (report)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS report_enabled BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS report_queue TEXT NOT NULL DEFAULT '';
            """)
            await conn.execute("""
                ALTER TABLE tg_settings
                ADD COLUMN IF NOT EXISTS report_period TEXT NOT NULL DEFAULT 'week';
            """)
            logger.info("Database schema ready")
        finally:
            await self._pool.release(conn)  # type: ignore

    # -------------------------------------------------------------------------
    # Token operations
    # -------------------------------------------------------------------------
    async def upsert_token(
        self,
        tg_id: int,
        access_token: str,
        refresh_token: Optional[str],
        token_type: Optional[str],
        expires_in: Optional[int],
    ) -> None:
        """Store or update OAuth tokens for user."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO oauth_tokens (tg_id, access_token, refresh_token, token_type, expires_in, obtained_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tg_id) DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_type = EXCLUDED.token_type,
                    expires_in = EXCLUDED.expires_in,
                    obtained_at = EXCLUDED.obtained_at;
            """, tg_id, access_token, refresh_token, token_type, expires_in, datetime.now(timezone.utc))
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_tokens(self, tg_id: int) -> Optional[dict]:
        """Get OAuth tokens for user."""
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(
                "SELECT access_token, refresh_token FROM oauth_tokens WHERE tg_id=$1",
                tg_id
            )
            return dict(row) if row else None
        finally:
            await self._pool.release(conn)  # type: ignore

    # -------------------------------------------------------------------------
    # User operations
    # -------------------------------------------------------------------------
    async def upsert_user(self, tg_id: int, tracker_login: str, tracker_uid: Optional[str]) -> None:
        """Store or update Tracker user info."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_users (tg_id, tracker_login, tracker_uid, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (tg_id) DO UPDATE SET
                    tracker_login = EXCLUDED.tracker_login,
                    tracker_uid = EXCLUDED.tracker_uid,
                    updated_at = NOW();
            """, tg_id, tracker_login, tracker_uid)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_user(self, tg_id: int) -> Optional[dict]:
        """Get Tracker user info."""
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(
                "SELECT tracker_login, tracker_uid FROM tg_users WHERE tg_id=$1",
                tg_id
            )
            return dict(row) if row else None
        finally:
            await self._pool.release(conn)  # type: ignore

    # -------------------------------------------------------------------------
    # Settings operations
    # -------------------------------------------------------------------------
    async def ensure_settings_row(self, tg_id: int) -> None:
        """Create settings row if not exists."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_settings (tg_id)
                VALUES ($1)
                ON CONFLICT (tg_id) DO NOTHING;
            """, tg_id)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_settings(self, tg_id: int) -> dict:
        """Get user settings."""
        await self.ensure_settings_row(tg_id)
        conn = await self._acquire()
        try:
            row = await conn.fetchrow(
                """SELECT queues_csv, days, limit_results, reminder_hours,
                          morning_report_enabled, morning_report_queue, morning_report_limit,
                          evening_report_enabled,
                          report_enabled, report_queue, report_period
                   FROM tg_settings WHERE tg_id=$1""",
                tg_id
            )
            if row:
                return dict(row)
            return {
                "queues_csv": "", "days": 30, "limit_results": 10, "reminder_hours": 0,
                "morning_report_enabled": False, "morning_report_queue": "", "morning_report_limit": 10,
                "evening_report_enabled": False,
                "report_enabled": False, "report_queue": "", "report_period": "week"
            }
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_queues(self, tg_id: int, queues_csv: str) -> None:
        """Set user queues filter."""
        q = _normalize_queues_csv(queues_csv)
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_settings (tg_id, queues_csv, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (tg_id) DO UPDATE SET
                    queues_csv = EXCLUDED.queues_csv,
                    updated_at = NOW();
            """, tg_id, q)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_days(self, tg_id: int, days: int) -> None:
        """Set search period in days."""
        days = max(1, min(int(days), 3650))
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_settings (tg_id, days, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (tg_id) DO UPDATE SET
                    days = EXCLUDED.days,
                    updated_at = NOW();
            """, tg_id, days)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_limit(self, tg_id: int, limit: int) -> None:
        """Set results limit."""
        limit = max(1, min(int(limit), 100))
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_settings (tg_id, limit_results, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (tg_id) DO UPDATE SET
                    limit_results = EXCLUDED.limit_results,
                    updated_at = NOW();
            """, tg_id, limit)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_reminder(self, tg_id: int, hours: int) -> None:
        """Set reminder interval."""
        hours = hours if hours in (0, 1, 3, 6) else 0
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_settings (tg_id, reminder_hours, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (tg_id) DO UPDATE SET
                    reminder_hours = EXCLUDED.reminder_hours,
                    updated_at = NOW();
            """, tg_id, hours)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_users_with_reminder(self) -> list[dict]:
        """Get all users with reminder enabled."""
        conn = await self._acquire()
        try:
            rows = await conn.fetch(
                "SELECT tg_id, reminder_hours FROM tg_settings WHERE reminder_hours > 0"
            )
            return [dict(r) for r in rows] if rows else []
        finally:
            await self._pool.release(conn)  # type: ignore

    # -------------------------------------------------------------------------
    # Morning/Evening report settings
    # -------------------------------------------------------------------------
    async def set_morning_report(
        self, tg_id: int, enabled: bool, queue: str = "", limit: int = 10
    ) -> None:
        """Set morning report settings."""
        limit = limit if limit in (5, 10, 20) else 10
        queue = queue.upper().strip()
        conn = await self._acquire()
        try:
            await conn.execute("""
                INSERT INTO tg_settings (tg_id, morning_report_enabled, morning_report_queue, morning_report_limit, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (tg_id) DO UPDATE SET
                    morning_report_enabled = EXCLUDED.morning_report_enabled,
                    morning_report_queue = EXCLUDED.morning_report_queue,
                    morning_report_limit = EXCLUDED.morning_report_limit,
                    updated_at = NOW();
            """, tg_id, enabled, queue, limit)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_morning_report_enabled(self, tg_id: int, enabled: bool) -> None:
        """Toggle morning report on/off."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET morning_report_enabled = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, enabled)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_morning_report_queue(self, tg_id: int, queue: str) -> None:
        """Set morning report queue."""
        queue = queue.upper().strip()
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET morning_report_queue = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, queue)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_morning_report_limit(self, tg_id: int, limit: int) -> None:
        """Set morning report limit."""
        limit = limit if limit in (5, 10, 20) else 10
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET morning_report_limit = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, limit)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_evening_report_enabled(self, tg_id: int, enabled: bool) -> None:
        """Toggle evening report on/off."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET evening_report_enabled = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, enabled)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_report_enabled(self, tg_id: int, enabled: bool) -> None:
        """Toggle итоговый отчёт on/off."""
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET report_enabled = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, enabled)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_report_queue(self, tg_id: int, queue: str) -> None:
        """Set итоговый отчёт queue."""
        queue = queue.upper().strip()
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET report_queue = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, queue)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def set_report_period(self, tg_id: int, period: str) -> None:
        """Set итоговый отчёт default period."""
        period = period if period in ("today", "week", "month") else "week"
        conn = await self._acquire()
        try:
            await conn.execute("""
                UPDATE tg_settings SET report_period = $2, updated_at = NOW()
                WHERE tg_id = $1;
            """, tg_id, period)
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_users_with_report(self) -> list[dict]:
        """Get all users with итоговый отчёт enabled."""
        conn = await self._acquire()
        try:
            rows = await conn.fetch(
                """SELECT tg_id, report_queue, report_period
                   FROM tg_settings 
                   WHERE report_enabled = TRUE AND report_queue != ''"""
            )
            return [dict(r) for r in rows] if rows else []
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_users_with_morning_report(self) -> list[dict]:
        """Get all users with morning report enabled."""
        conn = await self._acquire()
        try:
            rows = await conn.fetch(
                """SELECT tg_id, morning_report_queue, morning_report_limit 
                   FROM tg_settings 
                   WHERE morning_report_enabled = TRUE AND morning_report_queue != ''"""
            )
            return [dict(r) for r in rows] if rows else []
        finally:
            await self._pool.release(conn)  # type: ignore

    async def get_users_with_evening_report(self) -> list[dict]:
        """Get all users with evening report enabled."""
        conn = await self._acquire()
        try:
            rows = await conn.fetch(
                """SELECT tg_id, morning_report_queue as queue
                   FROM tg_settings 
                   WHERE evening_report_enabled = TRUE AND morning_report_queue != ''"""
            )
            return [dict(r) for r in rows] if rows else []
        finally:
            await self._pool.release(conn)  # type: ignore


# Helper to parse queues
def queues_list(csv: str) -> list[str]:
    """Public helper to convert CSV to list."""
    return _queues_list(csv)
