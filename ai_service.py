import os
from typing import Optional
import httpx

GPTUNNEL_API_KEY = os.getenv("GPTUNNEL_API_KEY", "")
GPTUNNEL_API_URL = "https://gptunnel.ru/v1/chat/completions"
GPTUNNEL_MODEL = "gpt-5-nano"  # GPT 5 Nano


def _build_prompt(issue_data: dict) -> str:
    """Build structured prompt from issue data"""
    key = issue_data.get("key", "")
    summary = issue_data.get("summary", "")
    description = issue_data.get("description", "") or "Нет описания"
    status = issue_data.get("status", {}).get("display", "Не указан")
    assignee = issue_data.get("assignee", {}).get("display", "Не назначен")
    created = issue_data.get("createdAt", "")
    updated = issue_data.get("updatedAt", "")
    
    # Собираем последние 3 комментария
    comments = issue_data.get("comments", [])
    comments_text = ""
    if comments:
        last_comments = comments[-3:]  # Последние 3
        comments_list = []
        for c in last_comments:
            author = c.get("createdBy", {}).get("display", "Неизвестно")
            text = c.get("text", "").strip()
            if text:
                comments_list.append(f"  • {author}: {text[:200]}")
        if comments_list:
            comments_text = "\n".join(comments_list)
    
    # Собираем чеклист
    checklist = issue_data.get("checklistItems", [])
    checklist_text = ""
    if checklist:
        checklist_list = []
        for item in checklist:
            checked = "✅" if item.get("checked", False) else "⬜"
            text = item.get("text", "").strip()
            if text:
                checklist_list.append(f"  {checked} {text}")
        if checklist_list:
            checklist_text = "\n".join(checklist_list)
    
    # История изменений (changelog) - может быть недоступна
    changelog_text = ""
    changelog = issue_data.get("changelog")
    if changelog and isinstance(changelog, list):
        changelog_list = []
        for change in changelog[-5:]:  # Последние 5 изменений
            if isinstance(change, dict):
                field = change.get("field", "")
                from_val = change.get("from", "")
                to_val = change.get("to", "")
                author = change.get("updatedBy", {}).get("display", "Неизвестно")
                if field and to_val:
                    changelog_list.append(f"  • {field}: {from_val} → {to_val} ({author})")
        if changelog_list:
            changelog_text = "\n".join(changelog_list)
    
    prompt = f"""Составь структурированное резюме задачи из Yandex Tracker.

Данные задачи:
- Ключ: {key}
- Название: {summary}
- Статус: {status}
- Исполнитель: {assignee}
- Создана: {created}
- Обновлена: {updated}

Описание:
{description[:1500]}

Чеклист:
{checklist_text if checklist_text else "Нет чеклиста"}

Последние комментарии:
{comments_text if comments_text else "Нет комментариев"}

История изменений:
{changelog_text if changelog_text else "Нет истории изменений"}

Составь структурированное резюме задачи (максимум 200 символов) в следующем формате:
1. Цель задачи (одно предложение)
2. Текущий статус и ключевые моменты
3. Основные действия/проблемы

Резюме должно быть кратким, информативным и на русском языке."""
    
    return prompt


async def generate_summary(issue_data: dict) -> tuple[Optional[str], Optional[str]]:
    """Generate summary for issue using GPTunneL API
    Returns: (summary_text, error_message)
    """
    if not GPTUNNEL_API_KEY:
        return None, "GPTUNNEL_API_KEY не установлен"
    
    prompt = _build_prompt(issue_data)
    
    # Пробуем оба варианта авторизации (Bearer и просто ключ)
    headers = {
        "Authorization": f"Bearer {GPTUNNEL_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Если Bearer не работает, попробуем без Bearer
    # (некоторые API требуют просто ключ в заголовке)
    
    payload = {
        "model": GPTUNNEL_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "Ты помощник, который составляет краткие структурированные резюме задач из Yandex Tracker. Отвечай на русском языке, максимально кратко и информативно."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "useWalletBalance": True,
        "max_tokens": 150,  # Ограничиваем для краткости (200 символов ≈ 50-70 токенов, но берем с запасом)
        "temperature": 0.7
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(GPTUNNEL_API_URL, headers=headers, json=payload)
            
            if r.status_code != 200:
                error_text = r.text[:500] if r.text else "Нет текста ошибки"
                try:
                    error_json = r.json()
                    error_detail = str(error_json)
                except:
                    error_detail = error_text
                
                # Если 401 - проблема с авторизацией, пробуем без Bearer
                if r.status_code == 401:
                    headers_alt = {
                        "Authorization": GPTUNNEL_API_KEY,
                        "Content-Type": "application/json"
                    }
                    try:
                        r2 = await client.post(GPTUNNEL_API_URL, headers=headers_alt, json=payload)
                        if r2.status_code == 200:
                            data = r2.json()
                            if "choices" in data and len(data["choices"]) > 0:
                                content = data["choices"][0].get("message", {}).get("content", "")
                                if len(content) > 200:
                                    content = content[:197] + "..."
                                return content.strip(), None
                    except:
                        pass
                
                error_msg = f"HTTP {r.status_code}: {error_detail}"
                print(f"GPTunneL API error: {error_msg}")
                return None, error_msg
            
            try:
                data = r.json()
            except Exception as e:
                return None, f"Ошибка парсинга JSON ответа: {str(e)}"
            
            if "choices" in data and len(data["choices"]) > 0:
                content = data["choices"][0].get("message", {}).get("content", "")
                if not content:
                    return None, "API вернул пустой ответ"
                # Обрезаем до 200 символов если превышает
                if len(content) > 200:
                    content = content[:197] + "..."
                return content.strip(), None
            
            return None, f"Неожиданный формат ответа API: {data}"
    except httpx.TimeoutException:
        error_msg = "Превышено время ожидания ответа от GPTunneL API (30 сек)"
        print(f"GPTunneL API timeout")
        return None, error_msg
    except Exception as e:
        error_msg = f"Ошибка при запросе к GPTunneL API: {str(e)}"
        print(f"GPTunneL API error: {e}")
        return None, error_msg
