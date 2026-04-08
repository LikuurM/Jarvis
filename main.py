"""
JARVIS — финальная точка входа.
Telegram Bot + FastAPI iPhone + Планировщик + UserBot + 20 агентов.
"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import (LOG_LEVEL, LOG_FORMAT, TELEGRAM_BOT_TOKEN,
                    TELEGRAM_OWNER_ID, API_HOST, API_PORT)
from database.db import db

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/jarvis.log", encoding="utf-8"),
    ]
)

logger = logging.getLogger("jarvis.main")


def check_config():
    errors = []
    if not TELEGRAM_BOT_TOKEN:
        errors.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_OWNER_ID:
        errors.append("TELEGRAM_OWNER_ID")
    from config import GROQ_API_KEY
    if not GROQ_API_KEY:
        errors.append("GROQ_API_KEY")
    if errors:
        for e in errors:
            logger.error(f"  ✗ {e} не установлен")
        logger.error("→ Создайте .env на основе .env.example")
        sys.exit(1)
    logger.info("✓ Конфигурация OK")


async def start_api():
    try:
        import uvicorn
        from api.server import app
        config = uvicorn.Config(
            app, host=API_HOST, port=API_PORT,
            log_level="warning", access_log=False
        )
        server = uvicorn.Server(config)
        await server.serve()
    except ImportError:
        logger.warning("uvicorn не установлен, iPhone API недоступен")
    except Exception as e:
        logger.error(f"API ошибка: {e}")


async def main():
    logger.info("=" * 55)
    logger.info("  J.A.R.V.I.S. | 20 агентов | Groq + Telegram")
    logger.info("=" * 55)

    check_config()

    stats = db.get_stats()
    logger.info(f"✓ БД: {sum(stats.values())} записей")

    from bot.telegram_bot import bot, start_bot
    from scheduler.night_cycle import NightCycleScheduler
    from tools.alerts import AlertSystem

    alert_system = AlertSystem(bot=bot, owner_id=TELEGRAM_OWNER_ID)

    scheduler = NightCycleScheduler(bot=bot)
    scheduler.start()
    logger.info("✓ Планировщик запущен")

    # UserBot (опционально)
    userbot = None
    try:
        from agents.telegram_eye import TelegramEyeAgent
        from agents.digital_twin import AutoReplyAgent
        eye = TelegramEyeAgent(
            alert_system=alert_system,
            auto_reply=AutoReplyAgent()
        )
        if await eye.start():
            userbot = eye
            logger.info("✓ UserBot активен")
    except Exception as e:
        logger.debug(f"UserBot: {e}")

    # Стартовое сообщение
    try:
        await bot.send_message(
            TELEGRAM_OWNER_ID,
            f"✅ *ДЖАРВИС онлайн.*\n"
            f"БД: {sum(stats.values())} записей\n"
            f"UserBot: {'✓' if userbot else '✗'}\n"
            f"iPhone API: порт {API_PORT}\n"
            f"Готов, сэр.",
            parse_mode="Markdown"
        )
    except Exception:
        pass

    logger.info("✓ Запуск завершён")

    tasks = [
        asyncio.create_task(start_bot()),
        asyncio.create_task(start_api()),
    ]
    if userbot:
        tasks.append(asyncio.create_task(
            userbot.run_until_disconnected()
        ))

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Остановлен.")
    finally:
        scheduler.stop()
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
