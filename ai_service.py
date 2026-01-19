import os
from typing import Optional
import httpx

GPTUNNEL_API_KEY = os.getenv("GPTUNNEL_API_KEY", "")
GPTUNNEL_API_URL = "https://gptunnel.ru/v1/chat/completions"
# Попробуем разные варианты названия модели
GPTUNNEL_MODEL = os.getenv("GPTUNNEL_MODEL", "gpt-5-nano")  # GPT 5 Nano, можно переопределить через env


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


def _extract_content_from_response(data: dict) -> Optional[str]:
    """Extract content from API response in different formats"""
    content = None
    
    # Вариант 1: стандартный OpenAI формат - choices[0].message.content
    if "choices" in data and len(data["choices"]) > 0:
        choice = data["choices"][0]
        if isinstance(choice, dict):
            if "message" in choice:
                message = choice["message"]
                if isinstance(message, dict):
                    content = message.get("content", "")
                    if not content and "text" in message:
                        content = message.get("text", "")
            elif "text" in choice:
                content = choice.get("text", "")
            elif "delta" in choice:
                delta = choice["delta"]
                if isinstance(delta, dict) and "content" in delta:
                    content = delta.get("content", "")
            elif "content" in choice:
                content = choice.get("content", "")
    
    # Вариант 2: прямой ответ в корне
    if not content and "text" in data:
        content = data.get("text", "")
    
    # Вариант 3: ответ в поле result (YandexGPT формат)
    if not content and "result" in data:
        result = data["result"]
        if isinstance(result, dict):
            if "alternatives" in result and len(result["alternatives"]) > 0:
                content = result["alternatives"][0].get("message", {}).get("text", "")
            elif "text" in result:
                content = result.get("text", "")
        elif isinstance(result, str):
            content = result
    
    # Вариант 4: контент в корне ответа
    if not content and "content" in data:
        content = data.get("content", "")
    
    return content


async def generate_summary(issue_data: dict) -> tuple[Optional[str], Optional[str]]:
    """Generate summary for issue using GPTunneL API
    Returns: (summary_text, error_message)
    """
    if not GPTUNNEL_API_KEY:
        return None, "GPTUNNEL_API_KEY не установлен"
    
    prompt = _build_prompt(issue_data)
    
    # Согласно документации gptunnel.ru: Authorization: <API_KEY> (без Bearer)
    headers = {
        "Authorization": GPTUNNEL_API_KEY,
        "Content-Type": "application/json"
    }
    
    # Ограничиваем длину промпта, чтобы избежать проблем с токенами
    # Обрезаем описание и другие поля если промпт слишком длинный
    max_prompt_length = 3000  # Максимальная длина промпта
    if len(prompt) > max_prompt_length:
        # Обрезаем описание задачи
        desc_start = prompt.find("Описание:\n")
        if desc_start > 0:
            desc_end = prompt.find("\n\nЧеклист:", desc_start)
            if desc_end > 0:
                desc_section = prompt[desc_start:desc_end]
                if len(desc_section) > 500:
                    # Обрезаем описание до 500 символов
                    new_desc = prompt[:desc_start] + "Описание:\n" + prompt[desc_start+10:desc_start+510] + "...\n\nЧеклист:" + prompt[desc_end+2:]
                    prompt = new_desc
    
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
        "max_tokens": 200,  # Увеличиваем лимит токенов для ответа
        "temperature": 0.7
    }
    
    print(f"Request payload: model={GPTUNNEL_MODEL}, prompt_length={len(prompt)}, max_tokens={payload['max_tokens']}")
    
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
                
                # Если 401 - проблема с авторизацией, пробуем с Bearer (на случай если документация устарела)
                if r.status_code == 401:
                    headers_alt = {
                        "Authorization": f"Bearer {GPTUNNEL_API_KEY}",
                        "Content-Type": "application/json"
                    }
                    try:
                        r2 = await client.post(GPTUNNEL_API_URL, headers=headers_alt, json=payload)
                        if r2.status_code == 200:
                            data2 = r2.json()
                            print(f"GPTunneL API response (Bearer auth): {str(data2)[:1000]}")
                            # Используем ту же логику извлечения контента
                            content = _extract_content_from_response(data2)
                            if content:
                                if len(content) > 200:
                                    content = content[:197] + "..."
                                return content.strip(), None
                    except Exception as e:
                        print(f"Bearer auth attempt failed: {e}")
                        pass
                
                error_msg = f"HTTP {r.status_code}: {error_detail}"
                print(f"GPTunneL API error: {error_msg}")
                return None, error_msg
            
            try:
                data = r.json()
            except Exception as e:
                return None, f"Ошибка парсинга JSON ответа: {str(e)}"
            
            # Логируем полный ответ для отладки (первые 2000 символов)
            response_str = str(data)[:2000]
            print(f"GPTunneL API response: {response_str}")
            
            # Извлекаем контент используя универсальную функцию
            content = _extract_content_from_response(data)
            
            # Проверяем finish_reason для диагностики
            finish_reason = None
            if "choices" in data and len(data["choices"]) > 0:
                choice = data["choices"][0]
                if isinstance(choice, dict):
                    finish_reason = choice.get("finish_reason")
            
            if not content:
                # Логируем детальную структуру для диагностики
                choice_info = ""
                if "choices" in data and len(data["choices"]) > 0:
                    choice = data["choices"][0]
                    choice_info = f", choice keys: {list(choice.keys()) if isinstance(choice, dict) else 'not dict'}"
                    if isinstance(choice, dict):
                        choice_info += f", finish_reason: {choice.get('finish_reason', 'not set')}"
                        if "message" in choice:
                            msg = choice["message"]
                            choice_info += f", message keys: {list(msg.keys()) if isinstance(msg, dict) else 'not dict'}"
                            if isinstance(msg, dict):
                                choice_info += f", message content type: {type(msg.get('content', None))}"
                                choice_info += f", message content value: {repr(msg.get('content', ''))[:100]}"
                
                error_msg = f"API вернул пустой ответ"
                if finish_reason:
                    error_msg += f". Finish reason: {finish_reason}"
                    if finish_reason == "length":
                        error_msg += " (ответ обрезан из-за лимита токенов - попробуйте увеличить max_tokens)"
                    elif finish_reason == "content_filter":
                        error_msg += " (ответ отфильтрован модерацией)"
                    elif finish_reason == "stop":
                        error_msg += " (модель остановилась, но контент пустой - возможно модель недоступна или не поддерживает запрос)"
                    else:
                        error_msg += f" (неизвестная причина: {finish_reason})"
                else:
                    error_msg += " (finish_reason не указан)"
                
                # Проверяем, может быть модель недоступна
                model_in_response = data.get("model", "")
                if model_in_response and model_in_response != GPTUNNEL_MODEL:
                    error_msg += f". Модель в ответе: {model_in_response} (ожидалась: {GPTUNNEL_MODEL})"
                
                error_msg += f". Структура: {list(data.keys()) if isinstance(data, dict) else type(data)}{choice_info}"
                return None, error_msg
            
            # Обрезаем до 200 символов если превышает
            if len(content) > 200:
                content = content[:197] + "..."
            return content.strip(), None
    except httpx.TimeoutException:
        error_msg = "Превышено время ожидания ответа от GPTunneL API (30 сек)"
        print(f"GPTunneL API timeout")
        return None, error_msg
    except Exception as e:
        error_msg = f"Ошибка при запросе к GPTunneL API: {str(e)}"
        print(f"GPTunneL API error: {e}")
        return None, error_msg
