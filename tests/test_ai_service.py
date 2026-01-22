"""
Tests for AI service module.
"""
import pytest
import respx
from httpx import Response


class TestBuildPrompt:
    """Test prompt building."""
    
    def test_builds_basic_prompt(self):
        """Test building prompt with minimal data."""
        from ai_service import _build_prompt
        
        issue_data = {
            "key": "TEST-123",
            "summary": "Test issue",
            "description": "Test description",
        }
        
        prompt = _build_prompt(issue_data)
        
        assert "TEST-123" in prompt
        assert "Test issue" in prompt
        assert "Test description" in prompt
    
    def test_handles_missing_fields(self):
        """Test prompt building with missing fields."""
        from ai_service import _build_prompt
        
        issue_data = {"key": "TEST-1"}
        
        prompt = _build_prompt(issue_data)
        
        assert "TEST-1" in prompt
        assert "Нет описания" in prompt
    
    def test_includes_comments(self):
        """Test that comments are included."""
        from ai_service import _build_prompt
        
        issue_data = {
            "key": "TEST-1",
            "comments": [
                {"createdBy": {"display": "User1"}, "text": "Comment 1"},
                {"createdBy": {"display": "User2"}, "text": "Comment 2"},
            ]
        }
        
        prompt = _build_prompt(issue_data)
        
        assert "User1" in prompt
        assert "Comment 1" in prompt
    
    def test_includes_checklist(self):
        """Test that checklist items are included."""
        from ai_service import _build_prompt
        
        issue_data = {
            "key": "TEST-1",
            "checklistItems": [
                {"text": "Item 1", "checked": True},
                {"text": "Item 2", "checked": False},
            ]
        }
        
        prompt = _build_prompt(issue_data)
        
        assert "Item 1" in prompt
        assert "✅" in prompt
        assert "⬜" in prompt
    
    def test_truncates_long_description(self):
        """Test that long descriptions are truncated."""
        from ai_service import _build_prompt
        
        long_desc = "x" * 1000
        issue_data = {
            "key": "TEST-1",
            "description": long_desc,
        }
        
        prompt = _build_prompt(issue_data)
        
        # Should be truncated to 800 chars
        assert len([c for c in prompt if c == 'x']) <= 800


class TestExtractContent:
    """Test content extraction from API response."""
    
    def test_extracts_valid_content(self):
        """Test extracting content from valid response."""
        from ai_service import _extract_content
        
        data = {
            "choices": [
                {"message": {"content": "Summary text"}}
            ]
        }
        
        content = _extract_content(data)
        
        assert content == "Summary text"
    
    def test_handles_empty_choices(self):
        """Test handling empty choices."""
        from ai_service import _extract_content
        
        data = {"choices": []}
        
        content = _extract_content(data)
        
        assert content is None
    
    def test_handles_missing_message(self):
        """Test handling missing message."""
        from ai_service import _extract_content
        
        data = {"choices": [{}]}
        
        content = _extract_content(data)
        
        assert content is None
    
    def test_strips_whitespace(self):
        """Test that content whitespace is stripped."""
        from ai_service import _extract_content
        
        data = {
            "choices": [
                {"message": {"content": "  Summary text  \n"}}
            ]
        }
        
        content = _extract_content(data)
        
        assert content == "Summary text"


@pytest.mark.asyncio
class TestGenerateSummary:
    """Test summary generation."""
    
    @respx.mock
    async def test_returns_error_when_not_configured(self, monkeypatch):
        """Test error when AI not configured."""
        # Mock settings to have no AI config
        from config import Settings, HttpConfig, CacheConfig
        mock_settings = Settings(
            base_url="http://localhost",
            port=10000,
            database=None,
            http=HttpConfig(),
            cache=CacheConfig(),
            tracker=None,
            oauth=None,
            bot=None,
            ai=None,
        )
        
        import ai_service
        monkeypatch.setattr(ai_service, "settings", mock_settings)
        
        summary, error = await ai_service.generate_summary({"key": "TEST-1"})
        
        assert summary is None
        assert "not configured" in error.lower()
    
    @respx.mock
    async def test_successful_generation(self):
        """Test successful summary generation."""
        from ai_service import generate_summary
        from config import settings
        
        if not settings.ai:
            pytest.skip("AI not configured in test environment")
        
        # Mock the API response
        respx.post(settings.ai.api_url).mock(
            return_value=Response(
                200,
                json={
                    "choices": [
                        {"message": {"content": "Generated summary"}}
                    ]
                }
            )
        )
        
        summary, error = await generate_summary({"key": "TEST-1"})
        
        assert summary == "Generated summary"
        assert error is None
    
    @respx.mock
    async def test_truncates_long_summary(self):
        """Test that long summaries are truncated."""
        from ai_service import generate_summary
        from config import settings
        
        if not settings.ai:
            pytest.skip("AI not configured in test environment")
        
        long_text = "x" * 600
        
        respx.post(settings.ai.api_url).mock(
            return_value=Response(
                200,
                json={
                    "choices": [
                        {"message": {"content": long_text}}
                    ]
                }
            )
        )
        
        summary, error = await generate_summary({"key": "TEST-1"})
        
        assert len(summary) == 500
        assert summary.endswith("...")
    
    @respx.mock
    async def test_handles_api_error(self):
        """Test handling API errors."""
        from ai_service import generate_summary
        from config import settings
        
        if not settings.ai:
            pytest.skip("AI not configured in test environment")
        
        respx.post(settings.ai.api_url).mock(
            return_value=Response(500, json={"error": "Server error"})
        )
        
        summary, error = await generate_summary({"key": "TEST-1"})
        
        assert summary is None
        assert error is not None
