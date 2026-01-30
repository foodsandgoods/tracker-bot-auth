"""
Tests for configuration module.
"""
import os
import pytest


class TestSettings:
    """Test Settings class."""
    
    def test_settings_loads_from_env(self):
        """Test that settings load from environment."""
        from config import settings
        
        assert settings.base_url == "http://localhost:10000"
        assert settings.port == 10000
    
    def test_settings_is_configured(self, mock_settings):
        """Test is_configured property."""
        assert mock_settings.is_configured is True
        assert len(mock_settings.missing_vars) == 0
    
    def test_settings_detects_missing_vars(self):
        """Test detection of missing configuration."""
        from config import Settings, HttpConfig, CacheConfig
        
        settings = Settings(
            base_url="",
            port=10000,
            database=None,
            http=HttpConfig(),
            cache=CacheConfig(),
            tracker=None,
            oauth=None,
            bot=None,
            ai=None,
        )
        
        assert settings.is_configured is False
        assert "BASE_URL" in settings.missing_vars
        assert "DATABASE_URL" in settings.missing_vars


class TestDatabaseConfig:
    """Test DatabaseConfig."""
    
    def test_default_values(self):
        """Test default pool configuration."""
        from config import DatabaseConfig
        
        config = DatabaseConfig(url="postgresql://localhost/test")
        
        assert config.min_pool_size == 1
        assert config.max_pool_size == 3
        assert config.command_timeout == 30.0


class TestHttpConfig:
    """Test HttpConfig."""
    
    def test_default_timeouts(self):
        """Test default timeout values."""
        from config import HttpConfig
        
        config = HttpConfig()
        
        assert config.connect_timeout == 10.0
        assert config.read_timeout == 25.0
        assert config.ai_read_timeout == 55.0
        assert config.max_connections == 10


class TestCacheConfig:
    """Test CacheConfig."""
    
    def test_default_sizes(self):
        """Test default cache sizes."""
        from config import CacheConfig
        
        config = CacheConfig()
        
        assert config.checklist_size == 25
        assert config.summary_size == 15
        assert config.checklist_ttl == 1200


class TestBotConfig:
    """Test BotConfig."""
    
    def test_default_admin_ids_empty(self):
        """Test default admin_ids is empty tuple."""
        from config import BotConfig
        
        config = BotConfig(token="test_token")
        
        assert config.admin_ids == ()
        assert config.keep_alive_interval == 300
    
    def test_admin_ids_set(self):
        """Test admin_ids can be set."""
        from config import BotConfig
        
        config = BotConfig(token="test_token", admin_ids=(123, 456))
        
        assert config.admin_ids == (123, 456)
        assert 123 in config.admin_ids
        assert 789 not in config.admin_ids
