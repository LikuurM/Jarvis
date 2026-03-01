"""
create_session.py — одноразовая авторизация юзер-аккаунта.

Запусти один раз: python create_session.py
Введи номер телефона и код из Telegram.
После этого файл sessions/user.session создастся и GroupMonitor заработает.
"""

import asyncio
import sys
from pathlib import Path

# Загружаем конфиг
sys.path.insert(0, str(Path(__file__).parent))
import config
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

async def main():
    print("\n" + "="*50)
    print("  JARVIS — Авторизация юзер-аккаунта")
    print("="*50)
    print(f"  API_ID:   {config.TELEGRAM_API_ID}")
    print(f"  Сессия:   {config.USER_SESSION_FILE}")
    print("="*50 + "\n")

    if not config.TELEGRAM_API_ID or not config.TELEGRAM_API_HASH:
        print("❌ Ошибка: TELEGRAM_API_ID или TELEGRAM_API_HASH не заполнены в .env")
        print("   Получи их на: https://my.telegram.org → API development tools")
        return

    client = TelegramClient(
        config.USER_SESSION_FILE,
        config.TELEGRAM_API_ID,
        config.TELEGRAM_API_HASH,
    )

    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✅ Уже авторизован как: {me.first_name} (@{me.username})")
        print("   Сессия активна, GroupMonitor будет работать.")
        await client.disconnect()
        return

    phone = config.TELEGRAM_PHONE or input("Введи номер телефона (с +7): ").strip()

    print(f"\nОтправляю код на {phone}...")
    await client.send_code_request(phone)

    code = input("Введи код из Telegram: ").strip()

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        password = input("Введи пароль двухфакторки: ").strip()
        await client.sign_in(password=password)

    me = await client.get_me()
    print(f"\n✅ Успешно! Авторизован как: {me.first_name} (@{me.username})")
    print(f"   Файл сессии: {config.USER_SESSION_FILE}")
    print("\n   Теперь запускай: python run.py")
    print("   GroupMonitor будет логировать все сообщения групп.\n")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
