"""
JARVIS AlertSystem — система уведомлений с 5 уровнями важности.
Пишет сам когда нужно — даже ночью при критическом.
"""
import asyncio
import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

logger = logging.getLogger("jarvis.alerts")


class AlertLevel(IntEnum):
    LOW = 1           # утренний дайджест
    MEDIUM = 2        # при следующем появлении
    HIGH = 3          # в течение 15 минут
    URGENT = 4        # в течение 5 минут
    CRITICAL = 5      # немедленно, даже ночью


@dataclass
class Alert:
    level: AlertLevel
    message: str
    source: str           # telegram/vk/system
    sender: Optional[str] = None
    context: Optional[str] = None
    chat_id: Optional[str] = None
    url: Optional[str] = None


# Правила уровней важности
CRITICAL_PATTERNS = [
    "оскорбля", "угрожа", "взлом", "украл", "деньги срочно",
    "помоги", "авария", "emergency"
]

URGENT_PATTERNS = [
    "срочно", "urgent", "важно", "asap", "немедленно",
    "горит", "проблема"
]

HIGH_PATTERNS = [
    "проект", "работа", "договор", "встреча", "созвон",
    "оплата", "deadline"
]


def classify_alert(text: str, sender_trust: float = 5.0) -> AlertLevel:
    """Определить уровень важности по тексту и доверию отправителя."""
    text_lower = text.lower()

    # Критический: угрозы/деньги + высокий trust
    if any(p in text_lower for p in CRITICAL_PATTERNS) and sender_trust >= 7:
        return AlertLevel.CRITICAL

    # Срочный: срочные слова + хорошее доверие
    if any(p in text_lower for p in URGENT_PATTERNS) and sender_trust >= 5:
        return AlertLevel.URGENT

    # Высокий: рабочие темы
    if any(p in text_lower for p in HIGH_PATTERNS) and sender_trust >= 4:
        return AlertLevel.HIGH

    # Средний: от знакомых
    if sender_trust >= 5:
        return AlertLevel.MEDIUM

    return AlertLevel.LOW


class AlertSystem:
    """
    Система алертов ДЖАРВИСА.

    Агрегирует события из всех источников,
    классифицирует их и решает когда и как уведомить.
    """

    def __init__(self, bot=None, owner_id: int = 0):
        self.bot = bot
        self.owner_id = owner_id
        self._pending: list[Alert] = []    # очередь алертов
        self._processing = False

    async def add(self, alert: Alert):
        """Добавить алерт в систему."""
        self._pending.append(alert)
        logger.debug(f"Алерт [{alert.level.name}]: {alert.message[:50]}")

        # Критический — отправляем немедленно
        if alert.level == AlertLevel.CRITICAL:
            await self._send_alert(alert)
        elif alert.level >= AlertLevel.URGENT:
            # Срочный — через 5 минут (можно накопить несколько)
            asyncio.create_task(self._delayed_send(alert, delay=300))

    async def _delayed_send(self, alert: Alert, delay: int):
        """Отправить алерт с задержкой."""
        await asyncio.sleep(delay)
        await self._send_alert(alert)

    async def _send_alert(self, alert: Alert):
        """Отправить алерт пользователю."""
        if not self.bot or not self.owner_id:
            return

        from core.personality import personality
        formatted = personality.format_alert(
            level=int(alert.level),
            message=alert.message,
            sender=alert.sender,
            context=alert.context
        )

        try:
            await self.bot.send_message(
                self.owner_id,
                formatted.text,
                parse_mode="Markdown"
            )
            # Убираем из очереди
            if alert in self._pending:
                self._pending.remove(alert)
        except Exception as e:
            logger.error(f"Ошибка отправки алерта: {e}")

    async def send_digest(self):
        """Отправить дайджест накопленных алертов (утром)."""
        if not self._pending:
            return

        low_alerts = [a for a in self._pending
                      if a.level in (AlertLevel.LOW, AlertLevel.MEDIUM)]

        if not low_alerts:
            return

        lines = [f"📋 *Пропущенные уведомления ({len(low_alerts)}):*\n"]
        for alert in low_alerts[:10]:
            icon = "🟡" if alert.level == AlertLevel.MEDIUM else "ℹ️"
            lines.append(f"{icon} [{alert.source}] {alert.message[:100]}")

        if self.bot and self.owner_id:
            try:
                await self.bot.send_message(
                    self.owner_id,
                    "\n".join(lines),
                    parse_mode="Markdown"
                )
                # Очищаем отправленные
                self._pending = [a for a in self._pending
                                  if a.level not in (AlertLevel.LOW, AlertLevel.MEDIUM)]
            except Exception as e:
                logger.error(f"Ошибка отправки дайджеста: {e}")

    def create_telegram_alert(self, text: str, sender_name: str,
                               sender_trust: float, chat_id: str,
                               chat_title: str = "") -> Alert:
        """Создать алерт из Telegram сообщения."""
        level = classify_alert(text, sender_trust)

        message = f"В {'группе ' + chat_title if chat_title else 'личном чате'} "
        message += f"написал *{sender_name}*:\n_{text[:200]}_"

        return Alert(
            level=level,
            message=message,
            source="telegram",
            sender=sender_name,
            context=f"Чат: {chat_title or 'личный'}",
            chat_id=chat_id
        )
