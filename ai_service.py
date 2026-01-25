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
SEARCH_PROMPT_TEMPLATE = """Преобразуй запрос пользователя в поисковый запрос Yandex Tracker Query Language (YQL).

## Синтаксис YQL:

### Основные фильтры:
- Очередь: Queue: DOC, Queue: INV, Queue: HR, Queue: BB, Queue: KOMDEP, Queue: FINANCE, Queue: BDEV
- Исполнитель: Assignee: me() (мои задачи), Assignee: empty() (без исполнителя)
- Автор: Author: me() (я создал)
- Следящий: Followers: me() (я в наблюдателях)
- Тип: Type: task, Type: bug, Type: story, Type: epic
- Приоритет: Priority: critical, Priority: blocker, Priority: high, Priority: normal, Priority: low

### Статусы (Status:):
Начальные: open, new, backlog, selectedForDev, newGoal, dutyQueue
В процессе: inProgress, testing, asPlanned, secondSupportLine, approvalbythoseresponsible, approved, transferredtothebank
На паузе: needInfo, tested, inReview, rc, readyForTest, needAcceptance, documentsPrepared, onHold, resultAcceptance, withRisks, blockedGoal, errorsinthebranch, documentsarerequired, requiresclarification
Завершены: resolved, closed, achieved
Отменены: cancelled, duplicate

### Даты и сроки:
- Updated: >= now()-7d (обновлено за N дней)
- Created: >= "2024-01-01" (создано после даты)
- Deadline: >= today() (срок сегодня или позже)
- Deadline: < today() (просроченные)

### Спринты:
- Sprint: "Название спринта"
- Sprint: notEmpty() (в каком-либо спринте)

### Операторы:
- AND, OR, скобки ()
- ! = не равно: Status: !closed, Assignee: !empty()
- Resolution: empty() (не решена)

### Сортировка:
- "Sort by: Updated DESC" (сначала недавние)
- "Sort by: Priority DESC" (сначала важные)
- "Sort by: Deadline ASC" (сначала срочные)

## ВАЖНО — ограничения YQL:
- НЕТ фильтра по чеклистам → для "мои чеклисты" ответь: CHECKLIST
- НЕТ фильтра по призывам/упоминаниям → для "меня призвали", "ждут ответа" ответь: SUMMONS
- Для "мои согласования" используй: (Assignee: me() OR Followers: me()) AND (Status: needAcceptance OR Status: resultAcceptance OR Status: approvalbythoseresponsible)

## Примеры преобразований:
- "мои задачи" → Assignee: me() AND Resolution: empty()
- "мои открытые" → Assignee: me() AND Status: !closed AND Status: !resolved
- "срочные баги" → Type: bug AND Priority: critical AND Resolution: empty()
- "просроченные" → Deadline: < today() AND Resolution: empty()
- "на согласовании" → Status: needAcceptance OR Status: resultAcceptance

Ограничения пользователя:
{constraints}

Запрос: {user_query}

Верни ТОЛЬКО YQL запрос (или CHECKLIST/SUMMONS если нужна спец.команда). Без пояснений."""


def _build_prompt(issue_data: dict, extended: bool = False) -> str:
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
    
    # Comments - more for extended
    comments = issue_data.get("comments", [])
    comments_text = ""
    comment_limit = 7 if extended else 5
    comment_len = 250 if extended else 150
    if comments and isinstance(comments, list):
        comments_list = []
        for c in comments[-comment_limit:]:
            if isinstance(c, dict):
                author = "Неизвестно"
                if isinstance(c.get("createdBy"), dict):
                    author = c["createdBy"].get("display", "Неизвестно")
                text = (c.get("text") or "").strip()
                if text:
                    comments_list.append(f"  • {author}: {text[:comment_len]}")
        if comments_list:
            comments_text = "\n".join(comments_list)
    
    # Checklist items - more for extended
    checklist = issue_data.get("checklistItems", [])
    checklist_text = ""
    checklist_limit = 10 if extended else 5
    checklist_len = 150 if extended else 100
    if checklist and isinstance(checklist, list):
        checklist_list = []
        for item in checklist[:checklist_limit]:
            if isinstance(item, dict):
                checked = "✅" if item.get("checked", False) else "⬜"
                text = (item.get("text") or "").strip()
                if text:
                    checklist_list.append(f"  {checked} {text[:checklist_len]}")
        if checklist_list:
            checklist_text = "\n".join(checklist_list)
    
    # Limit description length - more for extended
    desc_limit = 1200 if extended else 800
    desc_limited = description[:desc_limit] if len(description) > desc_limit else description
    
    # Different output limits
    max_chars = 800 if extended else 500
    
    return f"""Составь подробное резюме задачи из Yandex Tracker (максимум {max_chars} символов).

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

Резюме должно быть информативным (до {max_chars} символов) и на русском языке.
НЕ используй Markdown, звёздочки или другое форматирование — только plain text."""


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


async def generate_summary(issue_data: dict, extended: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """
    Generate summary for issue using GPTunnel API.
    
    Uses shared HTTP client and includes retry with exponential backoff.
    Returns user-friendly error messages for graceful degradation.
    
    Args:
        issue_data: Issue data from Yandex Tracker
        extended: If True, generate extended summary (800 chars vs 500)
    
    Returns:
        Tuple of (summary_text, error_message)
    """
    metrics.inc("ai.requests_extended" if extended else "ai.requests")
    
    if not settings.ai:
        metrics.inc("ai.not_configured")
        return None, FALLBACK_MESSAGES["not_configured"]
    
    ai_config = settings.ai
    prompt = _build_prompt(issue_data, extended=extended)
    
    payload = {
        "model": ai_config.model,
        "messages": [
            {
                "role": "system",
                "content": "Ты помощник, который составляет краткие резюме задач. Отвечай только на русском языке, максимально кратко. Не используй Markdown или другое форматирование."
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
                        # Truncate if too long (800 for extended, 500 for standard)
                        max_len = 800 if extended else 500
                        if len(content) > max_len:
                            content = content[:max_len - 3] + "..."
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


# Chat system prompt - короткий и фокусированный
CHAT_SYSTEM_PROMPT = """Ты — ассистент Яндекс Трекера. У тебя есть доступ к данным через функции.

ФУНКЦИИ (вызывай когда нужны данные):
• search_issues(query, limit) — поиск задач
• get_issue(issue_key) — детали задачи (INV-123, DOC-45)
• count_issues(query) — подсчёт задач

YQL ПРИМЕРЫ (для query параметра):
• "Queue: INV" — все задачи очереди INV
• "Queue: DOC AND Status: Open" — открытые задачи DOC
• "Queue: INV AND Status: !Closed" — незакрытые задачи INV
• "Queue: DOC AND Status: Closed AND Updated: >= now()-7d" — закрытые DOC за неделю
• "Queue: INV AND Status: Closed" — закрытые задачи INV
• "Updated: >= now()-7d" — изменённые за неделю
• "Assignee: me()" — мои задачи
• "Queue: DOC AND Status: Closed AND Updated: >= now()-30d" — закрытые DOC за месяц

ВАЖНО:
- Для "сколько задач" используй count_issues
- Для "покажи задачи" используй search_issues
- Для периода используй "Updated: >= now()-7d" (7 дней), "now()-30d" (30 дней)
- Статус "Closed" = закрытые, "!Closed" = незакрытые
- Если ошибка — попробуй упростить запрос (убери фильтры по дате или статусу)

ПРАВИЛА:
1. Нужны данные → вызови функцию. НЕ говори "нет доступа".
2. Отвечай ТОЛЬКО на основе полученных данных.
3. Кратко, по делу, без воды.
4. Числа и даты — из данных, не выдумывай.

ФОРМАТ ОТВЕТА:
- Для списка задач: нумерованный список с ключом, названием, статусом
- Для одной задачи: краткое summary (статус, что сделано, что дальше)
- Для подсчёта: число + краткий вывод"""


def _format_issue_context(issue_data: dict) -> str:
    """Format issue data as context for chat."""
    key = issue_data.get("key", "")
    summary = issue_data.get("summary", "")
    description = issue_data.get("description", "") or "Нет описания"
    
    status = "Не указан"
    if isinstance(issue_data.get("status"), dict):
        status = issue_data["status"].get("display", "Не указан")
    
    assignee = "Не назначен"
    if isinstance(issue_data.get("assignee"), dict):
        assignee = issue_data["assignee"].get("display", "Не назначен")
    
    priority = "Не указан"
    if isinstance(issue_data.get("priority"), dict):
        priority = issue_data["priority"].get("display", "Не указан")
    
    deadline = issue_data.get("deadline") or "Не указан"
    
    # Comments
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
                    comments_list.append(f"  • {author}: {text[:200]}")
        if comments_list:
            comments_text = "\n".join(comments_list)
    
    return f"""Задача: {key} — {summary}
Статус: {status}
Исполнитель: {assignee}
Приоритет: {priority}
Дедлайн: {deadline}

Описание:
{description[:1000]}

Последние комментарии:
{comments_text if comments_text else "Нет комментариев"}"""


# Tools definitions for function calling
TRACKER_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_issues",
            "description": "Поиск задач в Яндекс Трекере по YQL-запросу. Используй для поиска задач по очереди, статусу, исполнителю и т.д.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "YQL-запрос. Примеры: 'Queue: INV', 'Queue: DOC AND Status: Open', 'Assignee: me()'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Максимальное количество результатов (по умолчанию 10, максимум 50)",
                        "default": 10
                    }
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_issue",
            "description": "Получить полную информацию о задаче по её ключу (например INV-123, DOC-45)",
            "parameters": {
                "type": "object",
                "properties": {
                    "issue_key": {
                        "type": "string",
                        "description": "Ключ задачи, например INV-123 или DOC-45"
                    }
                },
                "required": ["issue_key"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "count_issues",
            "description": "Подсчитать количество задач по YQL-запросу",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "YQL-запрос для подсчёта. Примеры: 'Queue: INV AND Status: !Closed', 'Queue: DOC'"
                    }
                },
                "required": ["query"]
            }
        }
    }
]


async def chat_with_ai(
    user_message: str,
    history: list[dict],
    issue_context: str | None = None,
    tool_executor: callable = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Chat with AI assistant with function calling support.
    
    Args:
        user_message: User's message
        history: List of previous messages [{"role": "user/assistant", "content": "..."}]
        issue_context: Optional formatted issue data as context
        tool_executor: Async function to execute tools: async (name, args) -> result_dict
    
    Returns:
        Tuple of (response_text, error_message)
    """
    metrics.inc("ai.chat_requests")
    
    if not settings.ai:
        metrics.inc("ai.not_configured")
        return None, FALLBACK_MESSAGES["not_configured"]
    
    ai_config = settings.ai
    
    # Build messages list
    messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}]
    
    # Add issue context if provided
    if issue_context:
        messages.append({
            "role": "system",
            "content": f"Контекст задачи:\n\n{issue_context}"
        })
    
    # Add history
    messages.extend(history)
    
    # Add current message
    messages.append({"role": "user", "content": user_message})
    
    # Initial payload with tools
    payload = {
        "model": ai_config.model,
        "messages": messages,
        "useWalletBalance": True,
        "max_tokens": ai_config.max_tokens,
        "temperature": ai_config.temperature,
    }
    
    # Add tools if executor provided
    if tool_executor:
        payload["tools"] = TRACKER_TOOLS
        payload["tool_choice"] = "auto"
    
    client = await get_client()
    timeout = get_timeout(long=True)
    
    auth_variants = [
        {"Authorization": ai_config.api_key, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {ai_config.api_key}", "Content-Type": "application/json"},
    ]
    
    max_tool_rounds = 3  # Prevent infinite loops
    
    for headers in auth_variants:
        try:
            for round_num in range(max_tool_rounds + 1):
                status, data = await _make_request(
                    client, ai_config.api_url, headers, payload, timeout
                )
                
                if status != 200:
                    if status == 401:
                        break  # Try next auth variant
                    if status == 429:
                        metrics.inc("ai.rate_limited")
                        return None, FALLBACK_MESSAGES["rate_limit"]
                    if status >= 500:
                        return None, FALLBACK_MESSAGES["server_error"]
                    continue
                
                # Check for tool calls
                choices = data.get("choices", [])
                if not choices:
                    continue
                    
                message = choices[0].get("message", {})
                tool_calls = message.get("tool_calls", [])
                
                # If no tool calls or no executor, return content
                if not tool_calls or not tool_executor:
                    content = message.get("content", "")
                    if content:
                        metrics.inc("ai.chat_success")
                        return content, None
                    # Try extract from data
                    content = _extract_content(data)
                    if content:
                        metrics.inc("ai.chat_success")
                        return content, None
                    continue
                
                # Execute tool calls
                logger.info(f"AI requested {len(tool_calls)} tool calls")
                
                # Add assistant message with tool calls
                messages.append(message)
                
                # Execute each tool and add results
                for tool_call in tool_calls:
                    func_name = tool_call.get("function", {}).get("name", "")
                    func_args_str = tool_call.get("function", {}).get("arguments", "{}")
                    tool_id = tool_call.get("id", "")
                    
                    try:
                        import json
                        func_args = json.loads(func_args_str)
                    except:
                        func_args = {}
                    
                    logger.info(f"Executing tool: {func_name}({func_args})")
                    
                    try:
                        result = await tool_executor(func_name, func_args)
                        # Result is already formatted text
                        result_str = result if isinstance(result, str) else str(result)
                    except Exception as e:
                        logger.error(f"Tool execution error: {e}")
                        result_str = f"Ошибка выполнения: {e}"
                    
                    logger.info(f"Tool result preview: {result_str[:100]}...")
                    
                    # Add tool result message
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_id,
                        "content": result_str
                    })
                
                # Update payload for next round
                payload["messages"] = messages
                
            # If we exhausted rounds, try to get final response
            if status == 401:
                continue
                
        except asyncio.TimeoutError:
            metrics.inc("ai.timeout")
            return None, FALLBACK_MESSAGES["timeout"]
        except Exception as e:
            logger.debug(f"AI chat error: {e}")
            continue
    
    metrics.inc("ai.chat_failed")
    return None, FALLBACK_MESSAGES["unknown"]
