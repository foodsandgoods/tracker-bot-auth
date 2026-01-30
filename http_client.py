"""
Shared HTTP client singleton.
Single AsyncClient instance for the entire application to minimize resource usage.
"""
import asyncio
import logging
import random
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

import httpx

from config import HttpConfig, settings

logger = logging.getLogger(__name__)

T = TypeVar("T")


class HttpClientManager:
    """
    Manages a single httpx.AsyncClient instance.
    
    Features:
    - Singleton pattern for resource efficiency
    - Configurable timeouts and limits
    - Automatic reconnection if closed
    - Graceful shutdown
    """
    
    __slots__ = ('_client', '_config', '_lock')
    
    def __init__(self, config: HttpConfig):
        self._client: Optional[httpx.AsyncClient] = None
        self._config = config
        self._lock = asyncio.Lock()
    
    def _create_timeout(self, long: bool = False) -> httpx.Timeout:
        """Create timeout config."""
        read_timeout = self._config.ai_read_timeout if long else self._config.read_timeout
        return httpx.Timeout(
            connect=self._config.connect_timeout,
            read=read_timeout,
            write=self._config.write_timeout,
            pool=self._config.pool_timeout,
        )
    
    def _create_limits(self) -> httpx.Limits:
        """Create connection limits."""
        return httpx.Limits(
            max_keepalive_connections=self._config.max_keepalive,
            max_connections=self._config.max_connections,
            keepalive_expiry=30.0,
        )
    
    async def get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            return self._client
        
        async with self._lock:
            # Double-check after acquiring lock
            if self._client is not None and not self._client.is_closed:
                return self._client
            
            self._client = httpx.AsyncClient(
                timeout=self._create_timeout(),
                limits=self._create_limits(),
                http2=False,  # HTTP/2 uses more memory
            )
            logger.info("HTTP client created")
            return self._client
    
    async def close(self) -> None:
        """Close the HTTP client."""
        async with self._lock:
            if self._client and not self._client.is_closed:
                await self._client.aclose()
                logger.info("HTTP client closed")
            self._client = None
    
    @property
    def is_active(self) -> bool:
        """Check if client exists and is open."""
        return self._client is not None and not self._client.is_closed
    
    def get_timeout(self, long: bool = False) -> httpx.Timeout:
        """Get timeout config for requests."""
        return self._create_timeout(long=long)


# Global instance
_manager = HttpClientManager(settings.http)


async def get_client() -> httpx.AsyncClient:
    """Get the shared HTTP client."""
    return await _manager.get_client()


async def close_client() -> None:
    """Close the shared HTTP client."""
    await _manager.close()


def get_timeout(long: bool = False) -> httpx.Timeout:
    """Get timeout for requests."""
    return _manager.get_timeout(long=long)


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
    jitter: bool = True,
    retryable: tuple = (httpx.TimeoutException, httpx.ConnectError),
) -> Callable:
    """
    Decorator for async functions with exponential backoff retry and jitter.
    
    Args:
        max_attempts: Maximum number of attempts
        base_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        jitter: If True, add random jitter (±50%) to prevent thundering herd
        retryable: Tuple of exception types to retry on
    
    The delay formula with jitter:
        delay = min(base_delay * 2^attempt, max_delay) * random(0.5, 1.5)
    
    This prevents all clients from retrying at exactly the same time
    after a service recovers from an outage.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Optional[Exception] = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except retryable as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        # Calculate base delay with exponential backoff
                        base = min(base_delay * (2 ** attempt), max_delay)
                        # Apply jitter: ±50% randomization to prevent thundering herd
                        if jitter:
                            delay = base * random.uniform(0.5, 1.5)
                        else:
                            delay = base
                        logger.warning(
                            f"Retry {attempt + 1}/{max_attempts} for {func.__name__}: "
                            f"{type(e).__name__}, delay={delay:.2f}s"
                        )
                        await asyncio.sleep(delay)
            
            raise last_exception  # type: ignore
        
        return wrapper
    return decorator


def safe_json(response: httpx.Response) -> Any:
    """Safely parse JSON from response."""
    try:
        return response.json()
    except Exception:
        return {"raw": response.text[:500] if response.text else ""}
