# Быстрый старт: Отправка Email

## Шаг 1: Настройте .env файл

```bash
# Включите отправку email
SEND_WELCOME_EMAIL=true

# Настройки SMTP (пример для Gmail)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=465
SMTP_LOGIN=your_email@gmail.com
SMTP_PASSWORD=your_16_char_app_password
SMTP_FROM_EMAIL=noreply@yourcompany.com
EMAIL_DOMAIN=yourcompany.ru
```

## Шаг 2: Получите пароль приложения

### Для Gmail:
1. Включите двухфакторную аутентификацию
2. Перейдите: https://myaccount.google.com/apppasswords
3. Создайте пароль для "Почта"
4. Используйте его в `SMTP_PASSWORD`

### Для Yandex:
1. Перейдите: https://passport.yandex.ru/profile
2. Безопасность → Пароли приложений
3. Создайте пароль для почтовой программы
4. Используйте его в `SMTP_PASSWORD`

## Шаг 3: Укажите personal_email в CSV

```csv
login;password;...;personal_email;department
ivanov;Pass123!;...;ivan@example.com;1
```

## Шаг 4: Запустите скрипт

```bash
python add_users.py
```

## Проверка

Проверьте логи в `add_users.log`:
- `INFO: Email успешно отправлен на адрес ...` - успех ✓
- `ERROR: Ошибка аутентификации SMTP` - проверьте логин/пароль ✗

## Кастомизация

Отредактируйте `email_template.html` для изменения дизайна письма.

---

Подробная документация: [EMAIL_SENDING_README.md](EMAIL_SENDING_README.md)


