"""
AI service for generating issue summaries using GPTunnel API.
Optimized for shared httpx client to reduce memory usage.
"""
import logging
import os
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

GPTUNNEL_API_KEY = os.getenv("GPTUNNEL_API_KEY", "")
GPTUNNEL_API_URL = "https://gptunnel.ru/v1/chat/completions"
GPTUNNEL_MODEL = os.getenv("GPTUNNEL_MODEL", "gpt-4o-mini")

# Timeout for AI requests (longer than regular API calls)
AI_TIMEOUT = httpx.Timeout(connect=10.0, read=55.0, write=10.0, pool=5.0)


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


async def generate_summary(
    issue_data: dict,
    http_client: Optional[httpx.AsyncClient] = None
) -> tuple[Optional[str], Optional[str]]:
    """
    Generate summary for issue using GPTunnel API.
    
    Args:
        issue_data: Issue data from Yandex Tracker
        http_client: Shared httpx client (optional, creates new if not provided)
    
    Returns:
        Tuple of (summary_text, error_message)
    """
    if not GPTUNNEL_API_KEY:
        return None, "GPTUNNEL_API_KEY не установлен"
    
    prompt = _build_prompt(issue_data)
    
    headers = {
        "Authorization": GPTUNNEL_API_KEY,
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": GPTUNNEL_MODEL,
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
        "max_tokens": 600,
        "temperature": 0.7,
    }
    
    # Use provided client or create temporary one
    client_to_use = http_client
    created_client = False
    
    if client_to_use is None:
        client_to_use = httpx.AsyncClient(timeout=AI_TIMEOUT)
        created_client = True
    
    try:
        r = await client_to_use.post(
            GPTUNNEL_API_URL,
            headers=headers,
            json=payload,
            timeout=AI_TIMEOUT,
        )
        
        if r.status_code != 200:
            error_text = r.text[:300] if r.text else "No error text"
            
            # Try with Bearer auth on 401
            if r.status_code == 401:
                headers_alt = {
                    "Authorization": f"Bearer {GPTUNNEL_API_KEY}",
                    "Content-Type": "application/json",
                }
                try:
                    r2 = await client_to_use.post(
                        GPTUNNEL_API_URL,
                        headers=headers_alt,
                        json=payload,
                        timeout=AI_TIMEOUT,
                    )
                    if r2.status_code == 200:
                        data2 = r2.json()
                        content = _extract_content(data2)
                        if content:
                            if len(content) > 500:
                                content = content[:497] + "..."
                            return content, None
                except Exception as e:
                    logger.debug(f"Bearer auth fallback failed: {e}")
            
            return None, f"HTTP {r.status_code}: {error_text}"
        
        try:
            data = r.json()
        except Exception as e:
            return None, f"JSON parse error: {e}"
        
        # Check for API error codes
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
            error_msg = error_map.get(code, f"Error {code}")
            return None, f"{error_msg}: {message}"
        
        content = _extract_content(data)
        
        if not content:
            logger.warning(f"Empty AI response. Keys: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
            return None, "API returned empty response"
        
        # Truncate if too long
        if len(content) > 500:
            content = content[:497] + "..."
        
        return content, None
        
    except httpx.TimeoutException:
        return None, "Timeout waiting for AI response (55s)"
    except Exception as e:
        logger.error(f"AI request error: {e}")
        return None, f"Request error: {e}"
    finally:
        if created_client:
            await client_to_use.aclose()
