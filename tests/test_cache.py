"""
Tests for cache utilities in bot_web.
"""
import time
import pytest


class TestTTLCache:
    """Test TTLCache class."""
    
    def test_set_and_get(self):
        """Test basic set and get operations."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=10, ttl=60)
        cache.set("key1", "value1")
        
        assert cache.get("key1") == "value1"
    
    def test_get_missing_key(self):
        """Test getting a missing key returns None."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=10, ttl=60)
        
        assert cache.get("missing") is None
    
    def test_ttl_expiration(self):
        """Test that entries expire after TTL."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=10, ttl=1)
        cache.set("key1", "value1")
        
        # Should work immediately
        assert cache.get("key1") == "value1"
        
        # Wait for expiration
        time.sleep(1.1)
        assert cache.get("key1") is None
    
    def test_maxsize_eviction(self):
        """Test that oldest entries are evicted when max size is reached."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=3, ttl=60)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        cache.set("key4", "value4")  # Should evict key1
        
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
        assert cache.get("key4") == "value4"
    
    def test_lru_behavior(self):
        """Test LRU eviction behavior."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=3, ttl=60)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        
        # Access key1 to make it recently used
        cache.get("key1")
        
        # Add new entry - should evict key2 (least recently used)
        cache.set("key4", "value4")
        
        assert cache.get("key1") == "value1"
        assert cache.get("key2") is None
    
    def test_pop(self):
        """Test pop removes and returns value."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=10, ttl=60)
        cache.set("key1", "value1")
        
        value = cache.pop("key1")
        
        assert value == "value1"
        assert cache.get("key1") is None
    
    def test_pop_missing_key(self):
        """Test pop returns default for missing key."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=10, ttl=60)
        
        value = cache.pop("missing", "default")
        
        assert value == "default"
    
    def test_cleanup_expired(self):
        """Test cleanup_expired removes expired entries."""
        from bot_web import TTLCache
        
        cache = TTLCache(maxsize=10, ttl=1)
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        time.sleep(1.1)
        
        removed = cache.cleanup_expired()
        
        assert removed == 2
        assert cache.get("key1") is None
        assert cache.get("key2") is None


class TestPendingState:
    """Test PendingState class."""
    
    def test_set_and_get(self):
        """Test basic set and get operations."""
        from bot_web import PendingState
        
        state = PendingState(max_age=60)
        state[123] = "value"
        
        assert state.get(123) == "value"
    
    def test_get_missing_key(self):
        """Test getting a missing key returns default."""
        from bot_web import PendingState
        
        state = PendingState(max_age=60)
        
        assert state.get(999) is None
        assert state.get(999, "default") == "default"
    
    def test_expiration(self):
        """Test that entries expire after max_age."""
        from bot_web import PendingState
        
        state = PendingState(max_age=1)
        state[123] = "value"
        
        assert state.get(123) == "value"
        
        time.sleep(1.1)
        assert state.get(123) is None
    
    def test_pop(self):
        """Test pop removes and returns value."""
        from bot_web import PendingState
        
        state = PendingState(max_age=60)
        state[123] = "value"
        
        value = state.pop(123)
        
        assert value == "value"
        assert state.get(123) is None
    
    def test_pop_expired(self):
        """Test pop returns default for expired entries."""
        from bot_web import PendingState
        
        state = PendingState(max_age=1)
        state[123] = "value"
        
        time.sleep(1.1)
        
        value = state.pop(123, "default")
        
        assert value == "default"
