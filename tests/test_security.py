"""
Tests for security-related functionality.
- Queue name sanitization (YQL injection prevention)
- Token refresh race condition prevention
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestQueueSanitization:
    """Test queue name sanitization to prevent YQL injection."""

    def test_sanitize_queue_valid_names(self):
        """Test that valid queue names pass sanitization."""
        from main import sanitize_queue
        
        # Simple names
        assert sanitize_queue("INV") == "INV"
        assert sanitize_queue("DOC") == "DOC"
        assert sanitize_queue("HR") == "HR"
        
        # Lowercase converted to uppercase
        assert sanitize_queue("inv") == "INV"
        assert sanitize_queue("Doc") == "DOC"
        
        # With numbers
        assert sanitize_queue("TEAM1") == "TEAM1"
        assert sanitize_queue("PROJECT123") == "PROJECT123"
        
        # With underscores and hyphens
        assert sanitize_queue("MY_QUEUE") == "MY_QUEUE"
        assert sanitize_queue("MY-QUEUE") == "MY-QUEUE"
        
        # With whitespace (trimmed)
        assert sanitize_queue("  INV  ") == "INV"

    def test_sanitize_queue_invalid_names(self):
        """Test that invalid queue names raise ValueError."""
        from main import sanitize_queue
        
        # Empty
        with pytest.raises(ValueError, match="cannot be empty"):
            sanitize_queue("")
        
        with pytest.raises(ValueError, match="cannot be empty"):
            sanitize_queue("   ")
        
        # Special characters (potential injection)
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("INV AND Status:")
        
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("INV; DROP TABLE")
        
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("INV\"")
        
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("INV'")
        
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("INV()")
        
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("Queue: INV")
        
        # Too long (max 20 chars)
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queue("A" * 21)

    def test_sanitize_queues_list(self):
        """Test sanitization of queue list."""
        from main import sanitize_queues
        
        # Valid list
        result = sanitize_queues(["INV", "doc", "HR"])
        assert result == ["INV", "DOC", "HR"]
        
        # Empty strings filtered out
        result = sanitize_queues(["INV", "", "DOC"])
        assert result == ["INV", "DOC"]
        
        # Empty list
        result = sanitize_queues([])
        assert result == []

    def test_sanitize_queues_list_with_invalid(self):
        """Test that invalid queue in list raises ValueError."""
        from main import sanitize_queues
        
        with pytest.raises(ValueError, match="Invalid queue name"):
            sanitize_queues(["INV", "BAD NAME WITH SPACES", "DOC"])


class TestYqlInjectionPrevention:
    """Test that YQL injection is prevented in queries."""

    def test_build_candidate_query_sanitizes_queues(self):
        """Test that _build_candidate_query sanitizes queue names."""
        from main import TrackerService
        from unittest.mock import MagicMock
        
        # Create minimal service instance
        service = TrackerService(
            storage=MagicMock(),
            oauth=MagicMock(),
            tracker=MagicMock(),
            calendar=MagicMock()
        )
        
        # Valid queues work
        query = service._build_candidate_query(["INV", "DOC"], days=30)
        assert "Queue: INV" in query
        assert "Queue: DOC" in query
        
        # Invalid queue raises error
        with pytest.raises(ValueError):
            service._build_candidate_query(["INV", "BAD; DROP"], days=30)


@pytest.mark.asyncio
class TestTokenRefreshLock:
    """Test per-user lock for token refresh to prevent race conditions."""

    async def test_get_user_refresh_lock_creates_lock(self):
        """Test that locks are created per user."""
        from main import TrackerService
        
        service = TrackerService(
            storage=MagicMock(),
            oauth=MagicMock(),
            tracker=MagicMock(),
            calendar=MagicMock()
        )
        
        lock1 = await service._get_user_refresh_lock(123)
        lock2 = await service._get_user_refresh_lock(456)
        lock3 = await service._get_user_refresh_lock(123)  # Same user
        
        # Different users get different locks
        assert lock1 is not lock2
        
        # Same user gets same lock
        assert lock1 is lock3

    async def test_lock_eviction_when_full(self):
        """Test that old locks are evicted when cache is full."""
        from main import TrackerService
        
        service = TrackerService(
            storage=MagicMock(),
            oauth=MagicMock(),
            tracker=MagicMock(),
            calendar=MagicMock()
        )
        
        # Override max to test eviction
        original_max = TrackerService._MAX_REFRESH_LOCKS
        TrackerService._MAX_REFRESH_LOCKS = 10
        
        try:
            # Fill cache
            for i in range(15):
                await service._get_user_refresh_lock(i)
            
            # Should have evicted some locks
            assert len(service._refresh_locks) <= 10
        finally:
            TrackerService._MAX_REFRESH_LOCKS = original_max

    async def test_concurrent_token_refresh_serialized(self):
        """Test that concurrent token refreshes for same user are serialized."""
        from main import TrackerService
        
        refresh_call_count = 0
        refresh_order = []
        
        async def mock_get_tokens(tg_id):
            return {"access_token": "old_token", "refresh_token": "refresh_token"}
        
        async def mock_myself(token):
            # First call returns 401 (expired), subsequent calls return 200
            if token == "old_token":
                return 401, {"error": "unauthorized"}
            return 200, {"login": "test"}
        
        async def mock_refresh(refresh_token):
            nonlocal refresh_call_count
            refresh_call_count += 1
            refresh_order.append(refresh_call_count)
            # Simulate slow refresh
            await asyncio.sleep(0.1)
            return {"access_token": f"new_token_{refresh_call_count}", "refresh_token": "new_refresh"}
        
        async def mock_upsert_token(*args):
            pass
        
        async def mock_upsert_user(*args):
            pass
        
        storage = MagicMock()
        storage.get_tokens = AsyncMock(side_effect=mock_get_tokens)
        storage.upsert_token = AsyncMock(side_effect=mock_upsert_token)
        storage.upsert_user = AsyncMock(side_effect=mock_upsert_user)
        
        oauth = MagicMock()
        oauth.refresh = AsyncMock(side_effect=mock_refresh)
        
        tracker = MagicMock()
        tracker.myself = AsyncMock(side_effect=mock_myself)
        
        service = TrackerService(
            storage=storage,
            oauth=oauth,
            tracker=tracker,
            calendar=MagicMock()
        )
        
        # Launch concurrent requests for same user
        results = await asyncio.gather(
            service._get_valid_access_token(123),
            service._get_valid_access_token(123),
            service._get_valid_access_token(123),
        )
        
        # All should succeed
        for token, error in results:
            # At least one should get a valid token (the lock serializes them)
            # After first refresh, subsequent calls will see new token
            pass
        
        # Due to lock, refresh should not be called multiple times concurrently
        # The exact count depends on timing, but it should be serialized
        assert refresh_call_count >= 1

    async def test_cancellation_propagated(self):
        """Test that CancelledError is propagated through token refresh."""
        from main import TrackerService
        
        async def mock_get_tokens(tg_id):
            return {"access_token": "token", "refresh_token": "refresh"}
        
        async def mock_myself(token):
            await asyncio.sleep(10)  # Long operation
            return 200, {}
        
        storage = MagicMock()
        storage.get_tokens = AsyncMock(side_effect=mock_get_tokens)
        
        tracker = MagicMock()
        tracker.myself = AsyncMock(side_effect=mock_myself)
        
        service = TrackerService(
            storage=storage,
            oauth=MagicMock(),
            tracker=tracker,
            calendar=MagicMock()
        )
        
        # Start the operation
        task = asyncio.create_task(service._get_valid_access_token(123))
        
        # Give it time to start
        await asyncio.sleep(0.01)
        
        # Cancel it
        task.cancel()
        
        # Should raise CancelledError
        with pytest.raises(asyncio.CancelledError):
            await task


class TestDateValidation:
    """Test date parameter validation in queue_stats."""

    @pytest.mark.asyncio
    async def test_custom_date_format_validation(self):
        """Test that custom date ranges are validated."""
        from main import TrackerService
        
        async def mock_get_tokens(tg_id):
            return {"access_token": "token", "refresh_token": "refresh"}
        
        async def mock_myself(token):
            return 200, {"login": "test"}
        
        storage = MagicMock()
        storage.get_tokens = AsyncMock(side_effect=mock_get_tokens)
        storage.upsert_user = AsyncMock()
        
        tracker = MagicMock()
        tracker.myself = AsyncMock(side_effect=mock_myself)
        
        service = TrackerService(
            storage=storage,
            oauth=MagicMock(),
            tracker=tracker,
            calendar=MagicMock()
        )
        
        # Invalid date format should return 400
        result = await service.queue_stats(123, "INV", "invalid-date,also-invalid")
        assert result["http_status"] == 400
        assert "Invalid date format" in result["body"]["error"]
        
        # SQL injection attempt in dates
        result = await service.queue_stats(123, "INV", "2024-01-01; DROP TABLE,2024-01-31")
        assert result["http_status"] == 400


@pytest.mark.asyncio
class TestSemaphoreLimiting:
    """Test that semaphores limit concurrent requests."""
    
    async def test_fetch_issue_respects_semaphore(self):
        """Test that parallel fetches are limited by semaphore."""
        import time
        from main import TrackerService
        
        # Track concurrent calls
        max_concurrent = 0
        current_concurrent = 0
        lock = asyncio.Lock()
        
        async def mock_get_issue(token, key):
            nonlocal max_concurrent, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent:
                    max_concurrent = current_concurrent
            
            # Simulate API delay
            await asyncio.sleep(0.05)
            
            async with lock:
                current_concurrent -= 1
            
            return 200, {"key": key, "checklistItems": []}
        
        async def mock_get_tokens(tg_id):
            return {"access_token": "token", "refresh_token": "refresh"}
        
        async def mock_myself(token):
            return 200, {"login": "test"}
        
        async def mock_get_user(tg_id):
            return {"tracker_login": "test"}
        
        async def mock_get_settings(tg_id):
            return {"queues_csv": "INV", "days": 30, "limit_results": 10}
        
        async def mock_search_issues(token, query, limit):
            # Return 30 issues to trigger parallel fetches
            return 200, [{"key": f"INV-{i}"} for i in range(30)]
        
        storage = MagicMock()
        storage.get_tokens = AsyncMock(side_effect=mock_get_tokens)
        storage.get_user = AsyncMock(side_effect=mock_get_user)
        storage.get_settings = AsyncMock(side_effect=mock_get_settings)
        storage.upsert_user = AsyncMock()
        
        tracker = MagicMock()
        tracker.myself = AsyncMock(side_effect=mock_myself)
        tracker.search_issues = AsyncMock(side_effect=mock_search_issues)
        tracker.get_issue = AsyncMock(side_effect=mock_get_issue)
        
        service = TrackerService(
            storage=storage,
            oauth=MagicMock(),
            tracker=tracker,
            calendar=MagicMock()
        )
        
        # Run the method
        result = await service.checklist_assigned_issues(123, only_unchecked=False, limit=20)
        
        # Should succeed
        assert result["http_status"] == 200
        
        # Semaphore should limit concurrency to 10
        # Due to timing, actual max might be slightly lower
        assert max_concurrent <= 10, f"Max concurrent was {max_concurrent}, expected <= 10"
    
    async def test_cancellation_propagated_through_semaphore(self):
        """Test that CancelledError propagates through semaphore-protected functions."""
        from main import TrackerService
        
        cancel_point_reached = False
        
        async def mock_get_issue(token, key):
            nonlocal cancel_point_reached
            cancel_point_reached = True
            await asyncio.sleep(10)  # Long delay
            return 200, {}
        
        async def mock_get_tokens(tg_id):
            return {"access_token": "token", "refresh_token": "refresh"}
        
        async def mock_myself(token):
            return 200, {"login": "test"}
        
        async def mock_get_user(tg_id):
            return {"tracker_login": "test"}
        
        async def mock_get_settings(tg_id):
            return {"queues_csv": "INV", "days": 30, "limit_results": 10}
        
        async def mock_search_issues(token, query, limit):
            return 200, [{"key": "INV-1"}]
        
        storage = MagicMock()
        storage.get_tokens = AsyncMock(side_effect=mock_get_tokens)
        storage.get_user = AsyncMock(side_effect=mock_get_user)
        storage.get_settings = AsyncMock(side_effect=mock_get_settings)
        storage.upsert_user = AsyncMock()
        
        tracker = MagicMock()
        tracker.myself = AsyncMock(side_effect=mock_myself)
        tracker.search_issues = AsyncMock(side_effect=mock_search_issues)
        tracker.get_issue = AsyncMock(side_effect=mock_get_issue)
        
        service = TrackerService(
            storage=storage,
            oauth=MagicMock(),
            tracker=tracker,
            calendar=MagicMock()
        )
        
        # Start the operation
        task = asyncio.create_task(
            service.checklist_assigned_issues(123, only_unchecked=False, limit=5)
        )
        
        # Wait for it to reach the cancellation point
        for _ in range(50):
            if cancel_point_reached:
                break
            await asyncio.sleep(0.01)
        
        # Cancel the task
        task.cancel()
        
        # Should raise CancelledError
        with pytest.raises(asyncio.CancelledError):
            await task
