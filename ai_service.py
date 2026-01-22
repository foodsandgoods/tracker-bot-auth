"""
AI service for generating issue summaries using GPTunnel API.
Uses shared HTTP client and includes retry logic with graceful degradation.
"""
import asyncio
import logging
from typing import Optional, Tuple

from config import settings
from http_client import get_client, get_timeout
from metrics import metrics

logger = logging.getLogger(__name__)

# Graceful degradation messages
FALLBACK_MESSAGES = {
    "not_configured": "⚠️ AI-сервис не настроен. Обратитесь к администратору.",
    "timeout": "⏱️ AI-сервис не отвечает. Попробуйте позже.",
    "rate_limit": "🚫 Превышен лимит запросов к AI. Попробуйте через минуту.",
    "auth_error": "🔐 Ошибка авторизации AI-сервиса. Обратитесь к администратору.",
    "server_error": "⚠️ AI-сервис временно недоступен. Попробуйте позже.",
    "unknown": "❌ Не удалось выполнить запрос. Попробуйте позже.",
}

# Search query generation prompt
SEARCH_PROMPT_TEMPLATE = """Преобразуй запрос пользователя в поисковый запрос для Yandex Tracker Query Language.

Синтаксис Tracker Query Language:
- Поиск по тексту: "текст" (в кавычках для точного совпадения)
- Очередь: Queue: KEY
- Статус: Status: "В работе", Status: "Открыт", Status: !Закрыт (! = не равно)
- Исполнитель: Assignee: me(), Assignee: login
- Автор: Author: me(), Author: login  
- Приоритет: Priority: critical, Priority: high
- Тип: Type: task, Type: bug
- Дата: Created: >= "2024-01-01", Updated: >= now()-7d
- Теги: Tags: "тег"
- Комбинации: AND, OR, скобки ()

Ограничения:
{constraints}

Запрос пользователя: {user_query}

Верни ТОЛЬКО поисковый запрос для Tracker, без объяснений. Если запрос слишком общий, добавь ограничение по дате обновления."""


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
    Returns user-friendly error messages for graceful degradation.
    
    Args:
        issue_data: Issue data from Yandex Tracker
    
    Returns:
        Tuple of (summary_text, error_message)
    """
    metrics.inc("ai.requests")
    
    if not settings.ai:
        metrics.inc("ai.not_configured")
        return None, FALLBACK_MESSAGES["not_configured"]
    
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
                        if code == 5:  # Insufficient balance
                            last_error = "server_error"
                        elif code == 6:  # Overloaded
                            last_error = "rate_limit"
                        else:
                            last_error = "server_error"
                        continue
                    
                    content = _extract_content(data)
                    if content:
                        # Truncate if too long
                        if len(content) > 500:
                            content = content[:497] + "..."
                        metrics.inc("ai.success")
                        return content, None
                    
                    last_error = "unknown"
                    continue
                
                if status == 401:
                    # Try next auth variant
                    last_error = "auth_error"
                    continue
                
                if status == 429:
                    # Rate limited - wait and retry
                    last_error = "rate_limit"
                    metrics.inc("ai.rate_limited")
                    await asyncio.sleep(2 ** attempt)
                    continue
                
                if status >= 500:
                    # Server error - retry
                    last_error = "server_error"
                    await asyncio.sleep(1)
                    continue
                
                # Other error
                last_error = "unknown"
                
            except asyncio.TimeoutError:
                last_error = "timeout"
                metrics.inc("ai.timeout")
                continue
            except Exception as e:
                last_error = "unknown"
                metrics.inc("ai.error")
                logger.debug(f"AI request error: {e}")
                continue
        
        # Wait before retry
        if attempt < ai_config.max_retries - 1:
            await asyncio.sleep(1.5 ** attempt)
    
    # Return user-friendly error message
    metrics.inc("ai.failed")
    error_key = last_error if last_error in FALLBACK_MESSAGES else "unknown"
    return None, FALLBACK_MESSAGES.get(error_key, FALLBACK_MESSAGES["unknown"])


async def generate_search_query(
    user_query: str,
    queues: list[str],
    days: int
) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate Tracker search query from natural language.
    
    Args:
        user_query: User's search request in natural language
        queues: List of queue keys to limit search
        days: Number of days to limit search period
    
    Returns:
        Tuple of (tracker_query, error_message)
    """
    metrics.inc("ai.search_requests")
    
    if not settings.ai:
        metrics.inc("ai.not_configured")
        return None, FALLBACK_MESSAGES["not_configured"]
    
    # Build constraints description
    constraints_parts = []
    if queues:
        constraints_parts.append(f"Искать только в очередях: {', '.join(queues)}")
    constraints_parts.append(f"Период: последние {days} дней (Updated: >= now()-{days}d)")
    constraints = "\n".join(constraints_parts) if constraints_parts else "Нет ограничений"
    
    prompt = SEARCH_PROMPT_TEMPLATE.format(
        constraints=constraints,
        user_query=user_query
    )
    
    ai_config = settings.ai
    
    payload = {
        "model": ai_config.model,
        "messages": [
            {
                "role": "system",
                "content": "Ты помощник для генерации поисковых запросов Yandex Tracker. Отвечай только поисковым запросом, без пояснений."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "useWalletBalance": True,
        "max_tokens": 200,
        "temperature": 0.3,  # Lower temperature for more deterministic output
    }
    
    client = await get_client()
    timeout = get_timeout(long=False)  # Shorter timeout for search query generation
    
    auth_variants = [
        {"Authorization": ai_config.api_key, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {ai_config.api_key}", "Content-Type": "application/json"},
    ]
    
    for headers in auth_variants:
        try:
            status, data = await _make_request(
                client, ai_config.api_url, headers, payload, timeout
            )
            
            if status == 200:
                content = _extract_content(data)
                if content:
                    # Clean up the query - remove quotes if wrapped
                    query = content.strip().strip('"\'`')
                    
                    # Add queue constraints if not present and queues specified
                    if queues and not any(f"Queue:" in query for _ in [1]):
                        queue_filter = " OR ".join([f"Queue: {q}" for q in queues])
                        query = f"({queue_filter}) AND ({query})"
                    
                    # Ensure date constraint is present
                    if "Updated:" not in query and "Created:" not in query:
                        query = f"({query}) AND Updated: >= now()-{days}d"
                    
                    metrics.inc("ai.search_success")
                    return query, None
            
            if status == 401:
                continue
                
        except Exception as e:
            logger.debug(f"AI search query error: {e}")
            continue
    
    metrics.inc("ai.search_failed")
    return None, FALLBACK_MESSAGES["unknown"]
