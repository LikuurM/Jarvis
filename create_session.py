"""
Авторизация через QR-код в консоли.
Установи: pip install telethon qrcode
Запускай: python create_session.py
"""

import asyncio, sys, base64

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.functions.auth import ExportLoginTokenRequest, AcceptLoginTokenRequest
    from telethon.tl.types import auth as tl_auth
    from telethon.errors import SessionPasswordNeededError
except ImportError:
    print("❌ pip install telethon"); sys.exit(1)

try:
    import qrcode
except ImportError:
    print("❌ pip install qrcode"); sys.exit(1)

API_ID   = int(input("Введи API_ID: ").strip())
API_HASH = input("Введи API_HASH: ").strip()

def show_qr(data: str):
    qr = qrcode.QRCode(border=2)
    qr.add_data(data)
    qr.make(fit=True)
    qr.print_ascii(invert=True)

def save(session: str):
    print("\n" + "="*55)
    print(f"USER_SESSION_STRING={session}")
    print("="*55)
    with open("session_string.txt", "w", encoding="utf-8") as f:
        f.write(f"USER_SESSION_STRING={session}\n")
    print("✅ Сохранено в session_string.txt\n")

async def wait_for_auth(client, seconds=30):
    for _ in range(seconds):
        await asyncio.sleep(1)
        if await client.is_user_authorized():
            return True
    return False

async def main():
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✅ Уже авторизован: {me.first_name}")
        save(client.session.save())
        await client.disconnect()
        return

    while True:
        try:
            result = await client(ExportLoginTokenRequest(
                api_id=API_ID, api_hash=API_HASH, except_ids=[]
            ))

            if isinstance(result, tl_auth.LoginToken):
                token_b64 = base64.urlsafe_b64encode(result.token).decode()
                qr_url = f"tg://login?token={token_b64}"

                print("\nОткрой Telegram → Настройки → Устройства → Подключить устройство\n")
                show_qr(qr_url)
                print("Ожидаю сканирования...\n")

                authorized = await wait_for_auth(client, seconds=30)
                if authorized:
                    me = await client.get_me()
                    print(f"✅ Авторизован: {me.first_name}")
                    save(client.session.save())
                    break
                else:
                    print("QR истёк, генерирую новый...\n")
                    continue

            elif isinstance(result, (tl_auth.LoginTokenSuccess, tl_auth.LoginTokenMigrateTo)):
                if isinstance(result, tl_auth.LoginTokenMigrateTo):
                    await client._switch_dc(result.dc_id)
                    await client(AcceptLoginTokenRequest(result.token))
                me = await client.get_me()
                print(f"✅ Авторизован: {me.first_name}")
                save(client.session.save())
                break

        except SessionPasswordNeededError:
            print("\n🔐 Требуется пароль двухфакторной аутентификации")
            pwd = input("Введи пароль 2FA: ").strip()
            await client.sign_in(password=pwd)
            me = await client.get_me()
            print(f"✅ Авторизован: {me.first_name}")
            save(client.session.save())
            break

        except Exception as e:
            if await client.is_user_authorized():
                me = await client.get_me()
                print(f"✅ Авторизован: {me.first_name}")
                save(client.session.save())
                break
            print(f"Ошибка: {e}\nПовтор...\n")
            await asyncio.sleep(2)

    await client.disconnect()

asyncio.run(main())
