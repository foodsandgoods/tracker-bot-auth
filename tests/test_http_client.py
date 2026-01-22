"""
Tests for HTTP client module.
"""
import pytest
import httpx
import respx
from httpx import Response


@pytest.mark.asyncio
class TestHttpClientManager:
    """Test HttpClientManager."""
    
    async def test_get_client_creates_client(self):
        """Test that get_client creates a new client."""
        from http_client import get_client, close_client
        
        client = await get_client()
        assert client is not None
        assert isinstance(client, httpx.AsyncClient)
        assert not client.is_closed
        
        await close_client()
    
    async def test_get_client_reuses_client(self):
        """Test that get_client returns same client instance."""
        from http_client import get_client, close_client
        
        client1 = await get_client()
        client2 = await get_client()
        
        assert client1 is client2
        
        await close_client()
    
    async def test_close_client(self):
        """Test that close_client closes the client."""
        from http_client import get_client, close_client, _manager
        
        client = await get_client()
        assert not client.is_closed
        
        await close_client()
        assert _manager._client is None
    
    def test_get_timeout(self):
        """Test timeout configuration."""
        from http_client import get_timeout
        
        normal = get_timeout(long=False)
        assert normal.read == 25.0
        
        long = get_timeout(long=True)
        assert long.read == 55.0


class TestSafeJson:
    """Test safe_json helper."""
    
    def test_parses_valid_json(self):
        """Test parsing valid JSON response."""
        from http_client import safe_json
        
        response = Response(200, json={"key": "value"})
        result = safe_json(response)
        
        assert result == {"key": "value"}
    
    def test_handles_invalid_json(self):
        """Test handling invalid JSON."""
        from http_client import safe_json
        
        response = Response(200, content=b"not json")
        result = safe_json(response)
        
        assert "raw" in result
        assert result["raw"] == "not json"


@pytest.mark.asyncio
class TestWithRetry:
    """Test retry decorator."""
    
    async def test_succeeds_on_first_try(self):
        """Test successful call on first attempt."""
        from http_client import with_retry
        
        call_count = 0
        
        @with_retry(max_attempts=3)
        async def test_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await test_func()
        
        assert result == "success"
        assert call_count == 1
    
    async def test_retries_on_timeout(self):
        """Test retry on timeout exception."""
        from http_client import with_retry
        
        call_count = 0
        
        @with_retry(max_attempts=3, base_delay=0.01)
        async def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.TimeoutException("timeout")
            return "success"
        
        result = await test_func()
        
        assert result == "success"
        assert call_count == 3
    
    async def test_raises_after_max_attempts(self):
        """Test that exception is raised after max attempts."""
        from http_client import with_retry
        
        @with_retry(max_attempts=2, base_delay=0.01)
        async def test_func():
            raise httpx.TimeoutException("timeout")
        
        with pytest.raises(httpx.TimeoutException):
            await test_func()
