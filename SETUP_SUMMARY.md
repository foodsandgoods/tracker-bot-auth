# Настройка функции Summary с GPTunneL

## Добавление API ключа

Ваш API ключ для gptunnel.ru: `shds-9HyJnCHZjHyawd5EUc7QkNiyXIP`

### Для локальной разработки:

1. Создайте файл `.env` в корне проекта (не коммитьте его в git!)
2. Добавьте строку:
   ```
   GPTUNNEL_API_KEY=shds-9HyJnCHZjHyawd5EUc7QkNiyXIP
   ```

### Для деплоя на Render:

1. Откройте панель Render
2. Перейдите в настройки вашего сервиса (tracker-bot-auth1 и tracker-bot-bot1)
3. В разделе "Environment Variables" добавьте:
   - Key: `GPTUNNEL_API_KEY`
   - Value: `shds-9HyJnCHZjHyawd5EUc7QkNiyXIP`
4. Сохраните и перезапустите сервисы

## Проверка работы

После добавления ключа:
1. Перезапустите сервисы
2. В Telegram боте выполните: `/summary INV-123` (замените на реальный ключ задачи)
3. Бот должен сгенерировать резюме задачи

## Важно!

⚠️ **НЕ коммитьте API ключ в git!** 
- Ключ уже добавлен в `render.yaml` как переменная окружения
- Для локальной разработки используйте `.env` файл (который должен быть в `.gitignore`)
