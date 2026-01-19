import os
from typing import Optional
import httpx

GPTUNNEL_API_KEY = os.getenv("GPTUNNEL_API_KEY", "")
GPTUNNEL_API_URL = "https://gptunnel.ru/v1/chat/completions"
GPTUNNEL_MODEL = os.getenv("GPTUNNEL_MODEL", "gpt-4o-mini")  # Используем проверенную модель


def _build_prompt(issue_data: dict) -> str:
    """Build structured prompt from issue data"""
    key = issue_data.get("key", "")
    summary = issue_data.get("summary", "")
    description = issue_data.get("description", "") or "Нет описания"
    status = issue_data.get("status", {}).get("display", "Не указан") if isinstance(issue_data.get("status"), dict) else "Не указан"
    assignee = issue_data.get("assignee", {}).get("display", "Не назначен") if isinstance(issue_data.get("assignee"), dict) else "Не назначен"
    
    # Собираем последние 5 комментариев
    comments = issue_data.get("comments", [])
    comments_text = ""
    if comments and isinstance(comments, list):
        last_comments = comments[-5:]  # Последние 5
        comments_list = []
        for c in last_comments:
            if isinstance(c, dict):
                author = c.get("createdBy", {}).get("display", "Неизвестно") if isinstance(c.get("createdBy"), dict) else "Неизвестно"
                text = c.get("text", "").strip()
                if text:
                    comments_list.append(f"  • {author}: {text[:150]}")
        if comments_list:
            comments_text = "\n".join(comments_list)
    
    # Собираем чеклист
    checklist = issue_data.get("checklistItems", [])
    checklist_text = ""
    if checklist and isinstance(checklist, list):
        checklist_list = []
        for item in checklist[:5]:  # Первые 5 пунктов
            if isinstance(item, dict):
                checked = "✅" if item.get("checked", False) else "⬜"
                text = item.get("text", "").strip()
                if text:
                    checklist_list.append(f"  {checked} {text[:100]}")
        if checklist_list:
            checklist_text = "\n".join(checklist_list)
    
    # Ограничиваем длину описания
    desc_limited = description[:800] if len(description) > 800 else description
    
    prompt = f"""Составь подробное резюме задачи из Yandex Tracker (максимум 400 символов).

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

Резюме должно быть информативным (до 400 символов) и на русском языке."""
    
    return prompt


async def generate_summary(issue_data: dict) -> tuple[Optional[str], Optional[str]]:
    """Generate summary for issue using GPTunneL API
    Returns: (summary_text, error_message)
    """
    if not GPTUNNEL_API_KEY:
        return None, "GPTUNNEL_API_KEY не установлен"
    
    prompt = _build_prompt(issue_data)
    
    # Согласно документации: Authorization: <API_KEY> (без Bearer)
    headers = {
        "Authorization": GPTUNNEL_API_KEY,
        "Content-Type": "application/json"
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
        "max_tokens": 500,  # Увеличено для резюме до 400 символов
        "temperature": 0.7
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(GPTUNNEL_API_URL, headers=headers, json=payload)
            
            # Проверяем статус код
            if r.status_code != 200:
                error_text = r.text[:500] if r.text else "Нет текста ошибки"
                try:
                    error_json = r.json()
                    error_detail = str(error_json)
                except:
                    error_detail = error_text
                
                # Если 401 - пробуем с Bearer
                if r.status_code == 401:
                    headers_alt = {
                        "Authorization": f"Bearer {GPTUNNEL_API_KEY}",
                        "Content-Type": "application/json"
                    }
                    try:
                        r2 = await client.post(GPTUNNEL_API_URL, headers=headers_alt, json=payload)
                        if r2.status_code == 200:
                            data2 = r2.json()
                            content = _extract_content(data2)
                            if content:
                                if len(content) > 200:
                                    content = content[:197] + "..."
                                return content.strip(), None
                    except Exception as e:
                        print(f"Bearer auth attempt failed: {e}")
                
                return None, f"HTTP {r.status_code}: {error_detail}"
            
            # Парсим JSON ответ
            try:
                data = r.json()
            except Exception as e:
                return None, f"Ошибка парсинга JSON: {str(e)}"
            
            # Проверяем поле code в ответе (согласно документации gptunnel.ru)
            if "code" in data and data.get("code") != 0:
                code = data.get("code")
                message = data.get("message", "Неизвестная ошибка")
                error_map = {
                    1: "Внутренняя ошибка сервера",
                    2: "Неверный токен",
                    3: "Неверный формат запроса",
                    5: "Недостаточно средств",
                    6: "Сервис перегружен"
                }
                error_msg = error_map.get(code, f"Ошибка {code}")
                return None, f"{error_msg}: {message}"
            
            # Извлекаем контент
            content = _extract_content(data)
            
            if not content:
                # Логируем для диагностики
                print(f"Empty content. Response structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                if "choices" in data and len(data["choices"]) > 0:
                    choice = data["choices"][0]
                    finish_reason = choice.get("finish_reason", "not set")
                    print(f"Finish reason: {finish_reason}")
                
                return None, "API вернул пустой ответ. Проверьте модель и баланс."
            
            # Обрезаем до 400 символов если превышает
            if len(content) > 400:
                content = content[:397] + "..."
            
            return content.strip(), None
            
    except httpx.TimeoutException:
        return None, "Превышено время ожидания ответа от API (60 сек)"
    except Exception as e:
        return None, f"Ошибка при запросе к API: {str(e)}"


def _extract_content(data: dict) -> Optional[str]:
    """Extract content from API response"""
    # Стандартный OpenAI формат
    if "choices" in data and isinstance(data["choices"], list) and len(data["choices"]) > 0:
        choice = data["choices"][0]
        if isinstance(choice, dict):
            if "message" in choice:
                message = choice["message"]
                if isinstance(message, dict):
                    content = message.get("content", "")
                    if content and isinstance(content, str):
                        return content.strip()
    
    return None
