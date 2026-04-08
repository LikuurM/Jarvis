"""
JARVIS TelegramEyeAgent — UserBot на Telethon.
Читает ВСЕ чаты, группы, каналы как пользователь.
"""
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger("jarvis.telegram_eye")


class TelegramEyeAgent:
    """
    UserBot ДЖАРВИСА на Telethon.
    Слушает все события в аккаунте пользователя.

    ВАЖНО: требует TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE
    """

    def __init__(self, alert_system=None, auto_reply=None):
        self.alert_system = alert_system
        self.auto_reply = auto_reply
        self.client = None
        self._running = False

    async def start(self) -> bool:
        """Запустить UserBot."""
        try:
            from telethon import TelegramClient, events
            from config import (TELEGRAM_API_ID, TELEGRAM_API_HASH,
                                TELEGRAM_PHONE, TELEGRAM_OWNER_ID)

            if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
                logger.info("TelegramEye: API_ID/HASH не настроены, пропускаем")
                return False

            self.client = TelegramClient(
                "data/userbot_session",
                TELEGRAM_API_ID,
                TELEGRAM_API_HASH
            )

            await self.client.start(phone=TELEGRAM_PHONE)
            me = await self.client.get_me()
            logger.info(f"TelegramEye запущен как @{me.username}")

            self._setup_handlers(events, TELEGRAM_OWNER_ID)
            self._running = True
            return True

        except ImportError:
            logger.warning("telethon не установлен")
            return False
        except Exception as e:
            logger.error(f"TelegramEye ошибка запуска: {e}")
            return False

    def _setup_handlers(self, events, owner_id: int):
        """Настроить обработчики событий."""
        from database.db import db
        from tools.alerts import classify_alert, Alert, AlertLevel

        @self.client.on(events.NewMessage())
        async def on_message(event):
            """Обработать новое сообщение."""
            try:
                msg = event.message
                if not msg or not msg.text:
                    return

                text = msg.text
                sender = await event.get_sender()
                chat = await event.get_chat()

                sender_id = str(getattr(sender, "id", "unknown"))
                sender_name = (
                    getattr(sender, "first_name", "") or ""
                ).strip() or "Unknown"

                chat_id = str(getattr(chat, "id", "unknown"))
                chat_title = getattr(chat, "title", "") or sender_name
                is_group = hasattr(chat, "title") and chat.title

                chat_type = "group" if is_group else "personal"

                # Оцениваем важность
                importance = _score_importance(text, event)

                # Сохраняем в БД если важное
                from database.db import db
                if importance >= 1:
                    db.save_message(
                        platform="telegram",
                        chat_type=chat_type,
                        chat_id=chat_id,
                        chat_title=chat_title,
                        sender_id=sender_id,
                        sender_name=sender_name,
                        content=text,
                        media_type="text",
                        importance_score=importance,
                        timestamp=datetime.now(timezone.utc)
                    )

                # Обновляем контакт
                db.upsert_contact(
                    platform="telegram",
                    platform_id=sender_id,
                    username=getattr(sender, "username", None),
                    display_name=sender_name
                )

                # Проверяем на угрозы если есть агент
                # (легковесная проверка)
                is_mention = (
                    f"@" in text and owner_id
                )

                # AutoReply если пользователь не в сети
                # (только для личных сообщений от знакомых)
                if (self.auto_reply and
                    chat_type == "personal" and
                    not event.out):  # не наше сообщение
                    asyncio.create_task(
                        self.auto_reply.process_incoming(
                            message=text,
                            sender_name=sender_name,
                            sender_id=sender_id,
                            chat_id=chat_id,
                            platform="telegram",
                            chat_type=chat_type,
                            bot=None,  # userbot не используется для ответов
                            alert_system=self.alert_system
                        )
                    )

            except Exception as e:
                logger.debug(f"TelegramEye handler ошибка: {e}")

    async def run_until_disconnected(self):
        """Запустить и держать соединение."""
        if self.client:
            await self.client.run_until_disconnected()

    async def get_chat_history(self, chat_id: str,
                                limit: int = 1000) -> list:
        """Получить историю чата."""
        if not self.client:
            return []

        try:
            messages = []
            async for msg in self.client.iter_messages(
                int(chat_id), limit=limit
            ):
                if msg.text:
                    messages.append({
                        "id": msg.id,
                        "date": msg.date,
                        "sender_id": str(msg.sender_id),
                        "text": msg.text,
                    })
            return messages
        except Exception as e:
            logger.error(f"Ошибка получения истории: {e}")
            return []

    async def stop(self):
        """Остановить UserBot."""
        if self.client:
            await self.client.disconnect()
        self._running = False


def _score_importance(text: str, event=None) -> float:
    """Быстрая оценка важности сообщения."""
    score = 2.0

    if len(text) > 200:
        score += 2
    elif len(text) > 50:
        score += 1

    if "?" in text:
        score += 1

    keywords = [
        "срочно", "важно", "помоги", "проблема",
        "urgent", "important", "jarvis", "джарвис"
    ]
    if any(k in text.lower() for k in keywords):
        score += 3

    if len(text) < 5:
        score -= 2

    return max(0, min(10, score))
