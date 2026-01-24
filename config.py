"""
Centralized configuration with validation.
All settings loaded from environment variables.
"""
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseConfig:
    """Database connection settings optimized for 1GB RAM."""
    url: str
    min_pool_size: int = 1
    max_pool_size: int = 3  # Conservative for low RAM
    max_inactive_lifetime: float = 120.0
    command_timeout: float = 30.0


@dataclass(frozen=True)
class HttpConfig:
    """HTTP client settings."""
    connect_timeout: float = 10.0
    read_timeout: float = 25.0
    write_timeout: float = 10.0
    pool_timeout: float = 5.0
    max_keepalive: int = 5
    max_connections: int = 10
    # AI requests need longer timeouts
    ai_read_timeout: float = 55.0


@dataclass(frozen=True)
class CacheConfig:
    """Cache settings for memory efficiency."""
    checklist_size: int = 25
    checklist_ttl: int = 1200  # 20 min
    summary_size: int = 15
    summary_ttl: int = 1800  # 30 min
    pending_state_ttl: int = 600  # 10 min


@dataclass(frozen=True)
class TrackerConfig:
    """Yandex Tracker API settings."""
    org_id: str
    auth_url: str = "https://oauth.yandex.ru/authorize"
    token_url: str = "https://oauth.yandex.ru/token"
    api_base: str = "https://api.tracker.yandex.net/v2"


@dataclass(frozen=True)
class OAuthConfig:
    """OAuth credentials."""
    client_id: str
    client_secret: str


@dataclass(frozen=True)
class BotConfig:
    """Telegram bot settings."""
    token: str
    keep_alive_interval: int = 300  # 5 min
    reminder_check_interval: int = 300  # 5 min


@dataclass(frozen=True)
class AIConfig:
    """AI service settings."""
    api_key: str
    api_url: str = "https://gptunnel.ru/v1/chat/completions"
    model: str = "gpt-4o-mini"
    max_tokens: int = 600
    temperature: float = 0.0  # No creativity, only facts
    max_retries: int = 2


@dataclass
class Settings:
    """Main settings container."""
    base_url: str
    port: int
    database: Optional[DatabaseConfig]
    http: HttpConfig
    cache: CacheConfig
    tracker: Optional[TrackerConfig]
    oauth: Optional[OAuthConfig]
    bot: Optional[BotConfig]
    ai: Optional[AIConfig]
    
    # Derived settings
    is_configured: bool = field(init=False)
    missing_vars: list = field(default_factory=list, init=False)
    
    def __post_init__(self):
        missing = []
        if not self.base_url:
            missing.append("BASE_URL")
        if not self.database:
            missing.append("DATABASE_URL")
        if not self.tracker:
            missing.append("YANDEX_ORG_ID")
        if not self.oauth:
            missing.append("YANDEX_CLIENT_ID/YANDEX_CLIENT_SECRET")
        
        self.missing_vars = missing
        self.is_configured = len(missing) == 0


def _get_env(key: str, default: str = "") -> str:
    """Get environment variable, stripping whitespace."""
    return (os.getenv(key) or default).strip()


def _get_env_int(key: str, default: int) -> int:
    """Get integer environment variable."""
    val = os.getenv(key)
    if val:
        try:
            return int(val)
        except ValueError:
            logger.warning(f"Invalid int for {key}: {val}, using default {default}")
    return default


def load_settings() -> Settings:
    """Load all settings from environment."""
    
    # Base URL
    base_url = _get_env("BASE_URL").rstrip("/")
    port = _get_env_int("PORT", 10000)
    
    # Database
    db_url = _get_env("DATABASE_URL")
    database = DatabaseConfig(url=db_url) if db_url else None
    
    # HTTP
    http = HttpConfig()
    
    # Cache
    cache = CacheConfig()
    
    # Tracker
    org_id = _get_env("YANDEX_ORG_ID")
    tracker = TrackerConfig(org_id=org_id) if org_id else None
    
    # OAuth
    client_id = _get_env("YANDEX_CLIENT_ID")
    client_secret = _get_env("YANDEX_CLIENT_SECRET")
    oauth = OAuthConfig(client_id=client_id, client_secret=client_secret) if client_id and client_secret else None
    
    # Bot
    bot_token = _get_env("BOT_TOKEN")
    bot = BotConfig(token=bot_token) if bot_token else None
    
    # AI
    ai_key = _get_env("GPTUNNEL_API_KEY")
    ai_model = _get_env("GPTUNNEL_MODEL", "gpt-4o-mini")
    ai = AIConfig(api_key=ai_key, model=ai_model) if ai_key else None
    
    settings = Settings(
        base_url=base_url,
        port=port,
        database=database,
        http=http,
        cache=cache,
        tracker=tracker,
        oauth=oauth,
        bot=bot,
        ai=ai,
    )
    
    if settings.missing_vars:
        logger.warning(f"Missing configuration: {', '.join(settings.missing_vars)}")
    
    return settings


# Global settings instance - loaded once at import
settings = load_settings()
