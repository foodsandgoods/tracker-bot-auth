# План реализации функции Summary с ИИ

## Обзор
Добавление функции `/summary ISSUE-KEY` для генерации резюме задачи с помощью ИИ.

## Шаг 1: Выбор ИИ-сервиса и настройка

### Варианты:
1. **OpenAI GPT** (рекомендуется) - `openai` библиотека
2. **YandexGPT** - если нужна интеграция с экосистемой Yandex
3. **Anthropic Claude** - альтернатива OpenAI

### Действия:
1. Выбрать сервис (рекомендую OpenAI GPT-4 или GPT-3.5-turbo)
2. Получить API ключ
3. Добавить в `.env` или переменные окружения:
   ```
   OPENAI_API_KEY=sk-...
   # или
   YANDEX_GPT_API_KEY=...
   ```

## Шаг 2: Установка зависимостей

Добавить в `requirements.txt`:
```
openai>=1.0.0
# или для YandexGPT
yandexcloud>=0.1.0
```

## Шаг 3: Добавить метод в TrackerService (main.py)

### 3.1. Добавить метод для получения полных данных задачи

В классе `TrackerService` добавить:

```python
async def get_issue_full(self, tg_id: int, issue_key: str) -> dict:
    """Get full issue data for summary generation"""
    access, err = await self._get_valid_access_token(tg_id)
    if err:
        return err
    
    st, issue_data = await self.tracker.get_issue(access, issue_key)
    if st != 200:
        return {"http_status": st, "body": issue_data}
    
    return {"http_status": 200, "body": issue_data}
```

## Шаг 4: Добавить функцию генерации summary с ИИ

### 4.1. Создать модуль для работы с ИИ (новый файл `ai_service.py`):

```python
import os
from typing import Optional
import httpx
from openai import AsyncOpenAI

# Или для YandexGPT:
# from yandexcloud import SDK

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

async def generate_summary(issue_data: dict) -> Optional[str]:
    """Generate summary for issue using AI"""
    if not OPENAI_API_KEY:
        return None
    
    # Формируем промпт из данных задачи
    prompt = _build_prompt(issue_data)
    
    # Вызываем OpenAI API
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    
    try:
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo",  # или "gpt-4" для лучшего качества
            messages=[
                {"role": "system", "content": "Ты помощник, который составляет краткие резюме задач из Yandex Tracker. Отвечай на русском языке."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=500
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"AI API error: {e}")
        return None

def _build_prompt(issue_data: dict) -> str:
    """Build prompt from issue data"""
    key = issue_data.get("key", "")
    summary = issue_data.get("summary", "")
    description = issue_data.get("description", "")
    status = issue_data.get("status", {}).get("display", "")
    assignee = issue_data.get("assignee", {}).get("display", "")
    created = issue_data.get("createdAt", "")
    updated = issue_data.get("updatedAt", "")
    
    # Собираем комментарии (если есть)
    comments = issue_data.get("comments", [])
    comments_text = "\n".join([c.get("text", "") for c in comments[:5]])  # Последние 5
    
    # Собираем чеклист (если есть)
    checklist = issue_data.get("checklistItems", [])
    checklist_text = "\n".join([
        f"- {'✅' if item.get('checked') else '⬜'} {item.get('text', '')}"
        for item in checklist[:10]  # Первые 10 пунктов
    ])
    
    prompt = f"""Составь краткое резюме задачи из Yandex Tracker:

Ключ задачи: {key}
Название: {summary}
Статус: {status}
Исполнитель: {assignee}
Создана: {created}
Обновлена: {updated}

Описание:
{description[:1000] if description else "Нет описания"}

Чеклист:
{checklist_text if checklist_text else "Нет чеклиста"}

Комментарии:
{comments_text if comments_text else "Нет комментариев"}

Составь краткое резюме (2-3 предложения) о том, что нужно сделать, текущий статус и основные моменты."""
    
    return prompt
```

### 4.2. Альтернатива для YandexGPT:

```python
from yandexcloud import SDK
import httpx

async def generate_summary_yandex(issue_data: dict) -> Optional[str]:
    """Generate summary using YandexGPT"""
    api_key = os.getenv("YANDEX_GPT_API_KEY", "")
    folder_id = os.getenv("YANDEX_FOLDER_ID", "")
    
    if not api_key or not folder_id:
        return None
    
    prompt = _build_prompt(issue_data)
    
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    headers = {
        "Authorization": f"Api-Key {api_key}",
        "Content-Type": "application/json"
    }
    data = {
        "modelUri": f"gpt://{folder_id}/yandexgpt/latest",
        "completionOptions": {
            "stream": False,
            "temperature": 0.7,
            "maxTokens": 500
        },
        "messages": [
            {
                "role": "system",
                "text": "Ты помощник, который составляет краткие резюме задач из Yandex Tracker. Отвечай на русском языке."
            },
            {
                "role": "user",
                "text": prompt
            }
        ]
    }
    
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(url, headers=headers, json=data, timeout=30.0)
            if r.status_code == 200:
                result = r.json()
                return result["result"]["alternatives"][0]["message"]["text"]
        except Exception as e:
            print(f"YandexGPT API error: {e}")
    
    return None
```

## Шаг 5: Добавить метод в TrackerService для summary

В `main.py` в класс `TrackerService`:

```python
async def issue_summary(self, tg_id: int, issue_key: str) -> dict:
    """Generate AI summary for issue"""
    # Получаем данные задачи
    issue_result = await self.get_issue_full(tg_id, issue_key)
    if issue_result["http_status"] != 200:
        return issue_result
    
    issue_data = issue_result["body"]
    
    # Генерируем summary с помощью ИИ
    from ai_service import generate_summary  # Импортируем функцию
    
    summary_text = await generate_summary(issue_data)
    
    if not summary_text:
        return {
            "http_status": 500,
            "body": {"error": "Не удалось сгенерировать резюме. Проверьте настройки ИИ API."}
        }
    
    return {
        "http_status": 200,
        "body": {
            "issue_key": issue_key,
            "summary": summary_text,
            "issue_url": f"https://tracker.yandex.ru/{issue_key}"
        }
    }
```

## Шаг 6: Добавить API endpoint в main.py

После существующих endpoints (около строки 863):

```python
@app.get("/tracker/issue/{issue_key}/summary")
async def issue_summary_endpoint(tg: int, issue_key: str):
    cfg_err = _require(settings)
    if cfg_err:
        return cfg_err
    assert _service is not None
    result = await _service.issue_summary(tg, issue_key)
    return JSONResponse(result["body"], status_code=result["http_status"])
```

## Шаг 7: Добавить команду в бот (bot_web.py)

### 7.1. Добавить обработчик команды:

После команды `done_cmd` (около строки 600):

```python
@router.message(Command("summary"))
@_require_base_url
async def summary_cmd(m: Message):
    """Generate AI summary for issue"""
    parts = (m.text or "").split()
    if len(parts) != 2:
        await m.answer("Использование: /summary ISSUE-KEY\nПример: /summary INV-123")
        return
    
    issue_key = parts[1].upper().strip()
    tg_id = m.from_user.id
    
    # Показываем индикатор загрузки
    loading_msg = await m.answer("🤖 Генерирую резюме...")
    
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_LONG, limits=HTTP_LIMITS) as client:
            r = await client.get(
                f"{BASE_URL}/tracker/issue/{issue_key}/summary",
                params={"tg": tg_id}
            )
        
        data = _parse_response(r)
        
        if r.status_code != 200:
            error_msg = data.get("error", "Неизвестная ошибка") if isinstance(data, dict) else str(data)[:200]
            await loading_msg.edit_text(f"❌ Ошибка {r.status_code}: {error_msg}")
            return
        
        summary = data.get("summary", "")
        issue_url = data.get("issue_url", "")
        
        if not summary:
            await loading_msg.edit_text("❌ Не удалось сгенерировать резюме")
            return
        
        # Форматируем ответ
        response_text = (
            f"📋 Резюме задачи {issue_key}:\n\n"
            f"{summary}\n\n"
            f"🔗 {issue_url}"
        )
        
        # Telegram ограничение на длину сообщения - разбиваем если нужно
        if len(response_text) > 4000:
            # Отправляем первую часть
            await loading_msg.edit_text(response_text[:4000])
            # Отправляем остальное
            await m.answer(response_text[4000:])
        else:
            await loading_msg.edit_text(response_text)
            
    except httpx.TimeoutException:
        await loading_msg.edit_text("⏱ Превышено время ожидания. Попробуйте позже.")
    except Exception as e:
        await loading_msg.edit_text(f"❌ Произошла ошибка: {str(e)[:300]}")
```

### 7.2. Добавить команду в меню бота:

В функции `setup_bot_commands` (около строки 650):

```python
commands = [
    BotCommand(command="menu", description="📋 Показать меню команд"),
    BotCommand(command="connect", description="🔗 Привязать аккаунт"),
    BotCommand(command="me", description="👤 Проверить доступ"),
    BotCommand(command="settings", description="⚙️ Настройки"),
    BotCommand(command="cl_my", description="✅ Мои задачи с чеклистами"),
    BotCommand(command="cl_my_open", description="⬜ Ожидают мое согласование"),
    BotCommand(command="done", description="✅ Отметить пункт по номеру"),
    BotCommand(command="summary", description="🤖 Резюме задачи (ИИ)"),  # НОВОЕ
]
```

### 7.3. Обновить меню `/menu`:

В функции `menu` (около строки 230):

```python
menu_text = (
    "📋 Меню команд:\n\n"
    "🔗 Подключение:\n"
    "/connect — привязать аккаунт\n"
    "/me — проверить доступ\n\n"
    "⚙️ Настройки:\n"
    "/settings — настройки очередей, периода и лимита\n\n"
    "✅ Чеклисты:\n"
    "/cl_my — задачи, где ты назначен исполнителем пункта чеклиста\n"
    "/cl_my_open — ожидают мое согласование\n"
    "/cl_done ISSUE-KEY ITEM_ID — отметить пункт чеклиста\n\n"
    "🤖 ИИ функции:\n"  # НОВОЕ
    "/summary ISSUE-KEY — составить резюме задачи"  # НОВОЕ
)
```

## Шаг 8: Оптимизация (опционально)

### 8.1. Кэширование summary:
- Сохранять сгенерированные summary в кэш на некоторое время
- Избегать повторных запросов к ИИ для одной задачи

### 8.2. Обработка больших задач:
- Если описание/комментарии слишком длинные, обрезать их
- Использовать более умную выборку важных комментариев

### 8.3. Обработка ошибок:
- Graceful fallback если ИИ недоступен
- Показывать частичное резюме если возможно

## Шаг 9: Тестирование

1. Установить API ключ
2. Протестировать `/summary INV-123` (замените на реальный ключ)
3. Проверить обработку ошибок (неверный ключ, нет доступа и т.д.)

## Шаг 10: Деплой

1. Добавить переменную окружения `OPENAI_API_KEY` (или `YANDEX_GPT_API_KEY`)
2. Установить зависимости: `pip install openai`
3. Перезапустить сервис

---

## Пример использования:

```
Пользователь: /summary INV-123

Бот: 🤖 Генерирую резюме...

Бот: 📋 Резюме задачи INV-123:

Задача связана с обновлением системы инвентаризации. 
Требуется проверить корректность работы модуля учета товаров 
и исправить ошибки в расчете остатков. Статус: В работе.

🔗 https://tracker.yandex.ru/INV-123
```
