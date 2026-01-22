"""
Pytest configuration and fixtures.
"""
import os
import pytest
import asyncio

# Set test environment variables before imports
os.environ.setdefault("BASE_URL", "http://localhost:10000")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("YANDEX_CLIENT_ID", "test_client_id")
os.environ.setdefault("YANDEX_CLIENT_SECRET", "test_client_secret")
os.environ.setdefault("YANDEX_ORG_ID", "test_org_id")
os.environ.setdefault("BOT_TOKEN", "123456:ABC-DEF")
os.environ.setdefault("GPTUNNEL_API_KEY", "test_api_key")


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_settings():
    """Provide test settings."""
    from config import Settings, DatabaseConfig, HttpConfig, CacheConfig, TrackerConfig, OAuthConfig, BotConfig, AIConfig
    
    return Settings(
        base_url="http://localhost:10000",
        port=10000,
        database=DatabaseConfig(url="postgresql://test:test@localhost:5432/test"),
        http=HttpConfig(),
        cache=CacheConfig(),
        tracker=TrackerConfig(org_id="test_org"),
        oauth=OAuthConfig(client_id="test_id", client_secret="test_secret"),
        bot=BotConfig(token="123:ABC"),
        ai=AIConfig(api_key="test_key"),
    )
