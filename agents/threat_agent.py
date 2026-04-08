"""
JARVIS ThreatAssessmentAgent — мониторинг цифровых угроз.
Следит за подозрительной активностью, спамом, токсичностью.
"""
import logging
import re
from dataclasses import dataclass
from typing import Optional

from agents.base_agent import BaseAgent, AgentContext, AgentResult

logger = logging.getLogger("jarvis.threat")


@dataclass
class ThreatReport:
    threat_type: str        # spam/phishing/toxic/flood/suspicious
    severity: int           # 1-5
    source: str
    evidence: str
    recommendation: str


class ThreatAssessmentAgent(BaseAgent):
    """
    Агент оценки угроз.

    Мониторит:
    - Спам паттерны (новые аккаунты, повторяющиеся сообщения)
    - Фишинг (подозрительные ссылки)
    - Токсичные паттерны
    - Флуд
    - Аномальная активность
    """

    name = "ThreatAssessment"
    description = "Мониторинг цифровых угроз"

    # Паттерны угроз
    PHISHING_PATTERNS = [
        r"bit\.ly|tinyurl|t\.co/[a-z0-9]+",
        r"(бесплатно|free|FREE).{0,20}(клик|click|жми)",
        r"выиграл[и]?\s.{0,20}(приз|деньги|iphone)",
        r"подтвердите?\s.{0,20}(аккаунт|пароль|данные)",
        r"срочно.{0,30}(перевод|оплата|деньги)",
    ]

    SPAM_PATTERNS = [
        r"заработ[а-я]+\s.{0,10}(руб|рублей|\$|\d+к)",
        r"(партнёрск|реклам).{0,20}(предложени|ссылк)",
        r"\d{1,3}(\.\d{1,3}){3}",  # IP адреса
    ]

    TOXIC_WORDS = [
        "убью", "убьют", "взорву", "угрожаю",
        "найду тебя", "сообщу в полицию",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._message_counts: dict = {}  # sender_id → count за час

    async def run(self, context: AgentContext) -> AgentResult:
        """Полная проверка на угрозы."""
        threats = await self._scan_recent_messages()
        anomalies = self._check_anomalies()

        all_threats = threats + anomalies
        if not all_threats:
            return self.success("Угроз не обнаружено.", data=[])

        # Форматируем отчёт
        report_lines = [f"⚠️ Обнаружено угроз: {len(all_threats)}\n"]
        for t in all_threats[:5]:
            icon = "🔴" if t.severity >= 4 else "🟡"
            report_lines.append(
                f"{icon} *{t.threat_type}* (уровень {t.severity}/5)\n"
                f"   {t.evidence[:100]}\n"
                f"   Рекомендация: {t.recommendation}"
            )

        return self.success(
            "\n".join(report_lines),
            data=[t.__dict__ for t in all_threats]
        )

    async def assess_message(self, text: str, sender_id: str,
                              sender_name: str,
                              trust_score: float) -> Optional[ThreatReport]:
        """Оценить конкретное сообщение на угрозы."""

        # Фишинг
        for pattern in self.PHISHING_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return ThreatReport(
                    threat_type="phishing",
                    severity=4,
                    source=sender_name,
                    evidence=f"Паттерн фишинга в сообщении: {text[:80]}",
                    recommendation="Не переходить по ссылкам, заблокировать"
                )

        # Спам
        for pattern in self.SPAM_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                if trust_score < 4:
                    return ThreatReport(
                        threat_type="spam",
                        severity=2,
                        source=sender_name,
                        evidence=f"Спам-паттерн: {text[:80]}",
                        recommendation="Добавить в игнор-лист"
                    )

        # Токсичность
        for word in self.TOXIC_WORDS:
            if word in text.lower():
                return ThreatReport(
                    threat_type="toxic",
                    severity=5,
                    source=sender_name,
                    evidence=f"Угрозы в сообщении: {text[:80]}",
                    recommendation="Немедленно уведомить пользователя"
                )

        # Флуд (много сообщений от одного за час)
        self._message_counts[sender_id] = \
            self._message_counts.get(sender_id, 0) + 1

        if self._message_counts[sender_id] > 20 and trust_score < 5:
            return ThreatReport(
                threat_type="flood",
                severity=2,
                source=sender_name,
                evidence=f"Флуд: {self._message_counts[sender_id]} сообщений за час",
                recommendation="Временно игнорировать"
            )

        return None

    async def _scan_recent_messages(self) -> list[ThreatReport]:
        """Сканировать недавние сообщения на угрозы."""
        recent = self.db.execute(
            """SELECT content, sender_name, sender_id FROM messages
               WHERE timestamp > datetime('now', '-1 hour')
               AND importance_score >= 0
               ORDER BY timestamp DESC LIMIT 100"""
        )

        threats = []
        for row in recent:
            r = dict(row)
            threat = await self.assess_message(
                r.get("content", ""),
                r.get("sender_id", ""),
                r.get("sender_name", "?"),
                trust_score=3.0
            )
            if threat and threat.severity >= 3:
                threats.append(threat)

        return threats

    def _check_anomalies(self) -> list[ThreatReport]:
        """Проверить системные аномалии."""
        anomalies = []

        # Много неудачных попыток авторизации к API
        api_errors = self.db.execute_one(
            """SELECT COUNT(*) as cnt FROM agent_logs
               WHERE agent_name='API'
               AND success=0
               AND timestamp > datetime('now', '-1 hour')"""
        )

        if api_errors and dict(api_errors)["cnt"] > 20:
            anomalies.append(ThreatReport(
                threat_type="api_abuse",
                severity=3,
                source="API",
                evidence=f"Много ошибок API: {dict(api_errors)['cnt']} за час",
                recommendation="Проверить логи, возможно brute force"
            ))

        return anomalies

    def reset_message_counts(self):
        """Сбросить счётчики (каждый час)."""
        self._message_counts.clear()
