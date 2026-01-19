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
    
    # История изменений (changelog)
    changelog_text = ""
    changelog = issue_data.get("changelog", [])
    if changelog:
        changelog_list = []
        for change in changelog[-5:]:  # Последние 5 изменений
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


async def generate_summary(issue_data: dict) -> Optional[str]:
    """Generate summary for issue using GPTunneL API"""
    if not GPTUNNEL_API_KEY:
        return None
    
    prompt = _build_prompt(issue_data)
    
    headers = {
        "Authorization": f"Bearer {GPTUNNEL_API_KEY}",
        "Content-Type": "application/json"
    }
    
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
                print(f"GPTunneL API error: {r.status_code} - {r.text}")
                return None
            
            data = r.json()
            if "choices" in data and len(data["choices"]) > 0:
                content = data["choices"][0].get("message", {}).get("content", "")
                # Обрезаем до 200 символов если превышает
                if len(content) > 200:
                    content = content[:197] + "..."
                return content.strip()
            
            return None
    except httpx.TimeoutException:
        print("GPTunneL API timeout")
        return None
    except Exception as e:
        print(f"GPTunneL API error: {e}")
        return None
