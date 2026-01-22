"""
AI service for generating issue summaries using GPTunnel API.
Uses shared HTTP client and includes retry logic.
"""
import asyncio
import logging
from typing import Optional, Tuple

from config import settings
from http_client import get_client, get_timeout

logger = logging.getLogger(__name__)


def _build_prompt(issue_data: dict) -> str:
    """Build structured prompt from issue data."""
    key = issue_data.get("key", "")
    summary = issue_data.get("summary", "")
    description = issue_data.get("description", "") or "Нет описания"
    
    status = "Не указан"
    if isinstance(issue_data.get("status"), dict):
        status = issue_data["status"].get("display", "Не указан")
    
    assignee = "Не назначен"
    if isinstance(issue_data.get("assignee"), dict):
        assignee = issue_data["assignee"].get("display", "Не назначен")
    
    # Last 5 comments
    comments = issue_data.get("comments", [])
    comments_text = ""
    if comments and isinstance(comments, list):
        comments_list = []
        for c in comments[-5:]:
            if isinstance(c, dict):
                author = "Неизвестно"
                if isinstance(c.get("createdBy"), dict):
                    author = c["createdBy"].get("display", "Неизвестно")
                text = (c.get("text") or "").strip()
                if text:
                    comments_list.append(f"  • {author}: {text[:150]}")
        if comments_list:
            comments_text = "\n".join(comments_list)
    
    # Checklist items
    checklist = issue_data.get("checklistItems", [])
    checklist_text = ""
    if checklist and isinstance(checklist, list):
        checklist_list = []
        for item in checklist[:5]:
            if isinstance(item, dict):
                checked = "✅" if item.get("checked", False) else "⬜"
                text = (item.get("text") or "").strip()
                if text:
                    checklist_list.append(f"  {checked} {text[:100]}")
        if checklist_list:
            checklist_text = "\n".join(checklist_list)
    
    # Limit description length
    desc_limited = description[:800] if len(description) > 800 else description
    
    return f"""Составь подробное резюме задачи из Yandex Tracker (максимум 500 символов).

Задача: {key} — {summary}
Статус: {status}
Исполнитель: {assignee}

Описание:
{desc_limited}

Чеклист:
{checklist_text if checklist_text else "Нет чеклиста"}

Последние комментарии:
{comments_text if comments_text else "Нет комментариев"}

Составь структурированное резюме в формате:
1. Цель задачи (1-2 предложения)
2. Текущий статус
3. Ключевые моменты из описания
4. Прогресс по чеклисту (если есть)
5. Последние действия/комментарии (если есть)

Резюме должно быть информативным (до 500 символов) и на русском языке."""


def _extract_content(data: dict) -> Optional[str]:
    """Extract content from API response."""
    if "choices" in data and isinstance(data["choices"], list) and len(data["choices"]) > 0:
        choice = data["choices"][0]
        if isinstance(choice, dict) and "message" in choice:
            message = choice["message"]
            if isinstance(message, dict):
                content = message.get("content", "")
                if content and isinstance(content, str):
                    return content.strip()
    return None


async def _make_request(
    client,
    url: str,
    headers: dict,
    payload: dict,
    timeout
) -> Tuple[int, dict]:
    """Make HTTP request to AI API."""
    try:
        r = await client.post(url, headers=headers, json=payload, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:500] if r.text else ""}
        return r.status_code, data
    except Exception as e:
        return 0, {"error": str(e)}


async def generate_summary(issue_data: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate summary for issue using GPTunnel API.
    
    Uses shared HTTP client and includes retry with exponential backoff.
    
    Args:
        issue_data: Issue data from Yandex Tracker
    
    Returns:
        Tuple of (summary_text, error_message)
    """
    if not settings.ai:
        return None, "AI service not configured (GPTUNNEL_API_KEY not set)"
    
    ai_config = settings.ai
    prompt = _build_prompt(issue_data)
    
    payload = {
        "model": ai_config.model,
        "messages": [
            {
                "role": "system",
                "content": "Ты помощник, который составляет краткие резюме задач. Отвечай только на русском языке, максимально кратко."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "useWalletBalance": True,
        "max_tokens": ai_config.max_tokens,
        "temperature": ai_config.temperature,
    }
    
    client = await get_client()
    timeout = get_timeout(long=True)
    
    # Try different auth methods
    auth_variants = [
        {"Authorization": ai_config.api_key, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {ai_config.api_key}", "Content-Type": "application/json"},
    ]
    
    last_error = None
    
    for attempt in range(ai_config.max_retries):
        for headers in auth_variants:
            try:
                status, data = await _make_request(
                    client, ai_config.api_url, headers, payload, timeout
                )
                
                if status == 0:
                    # Connection error
                    last_error = data.get("error", "Connection error")
                    continue
                
                if status == 200:
                    # Check for API error codes in response
                    if "code" in data and data.get("code") != 0:
                        code = data.get("code")
                        message = data.get("message", "Unknown error")
                        error_map = {
                            1: "Internal server error",
                            2: "Invalid token",
                            3: "Invalid request format",
                            5: "Insufficient balance",
                            6: "Service overloaded",
                        }
                        last_error = f"{error_map.get(code, f'Error {code}')}: {message}"
                        continue
                    
                    content = _extract_content(data)
                    if content:
                        # Truncate if too long
                        if len(content) > 500:
                            content = content[:497] + "..."
                        return content, None
                    
                    last_error = "API returned empty response"
                    continue
                
                if status == 401:
                    # Try next auth variant
                    last_error = "Authentication failed"
                    continue
                
                if status == 429:
                    # Rate limited - wait and retry
                    last_error = "Rate limited"
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                if status >= 500:
                    # Server error - retry
                    last_error = f"Server error: {status}"
                    await asyncio.sleep(1)
                    continue
                
                # Other error
                error_text = data.get("error") or data.get("raw", "")[:200]
                last_error = f"HTTP {status}: {error_text}"
                
            except asyncio.TimeoutError:
                last_error = "Timeout waiting for AI response"
                continue
            except Exception as e:
                last_error = f"Request error: {type(e).__name__}"
                logger.debug(f"AI request error: {e}")
                continue
        
        # Wait before retry
        if attempt < ai_config.max_retries - 1:
            await asyncio.sleep(1.5 ** attempt)
    
    return None, last_error or "Unknown error"
