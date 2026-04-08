"""
JARVIS DataScientistAgent — анализ паттернов, статистика, инсайты.
Еженедельный отчёт о тебе: активность, темы, контакты.
"""
import json
import logging
from datetime import datetime, timedelta
from collections import Counter

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.data_scientist")


class DataScientistAgent(BaseAgent):
    """
    Аналитик данных ДЖАРВИСА.

    Анализирует:
    - Активность по времени (когда ты онлайн)
    - Топ контакты и темы
    - Эмоциональный фон переписки
    - Паттерны запросов к ДЖАРВИСУ
    - Аномалии и изменения
    """

    name = "DataScientist"
    description = "Анализ паттернов и статистика"

    system_prompt = """Ты — аналитик данных ДЖАРВИСА.
    Найди паттерны, тренды, аномалии.
    Давай практические инсайты, а не просто цифры.
    Отвечай на русском, кратко и по существу."""

    async def run(self, context: AgentContext) -> AgentResult:
        """Полный аналитический цикл."""
        results = {}

        # Анализ активности
        activity = self._analyze_activity()
        results["activity"] = activity

        # Топ контакты
        top_contacts = self._get_top_contacts()
        results["top_contacts"] = top_contacts

        # Топ темы (из базы знаний)
        top_topics = self._get_top_topics()
        results["top_topics"] = top_topics

        # Статистика агентов
        agent_stats = self._get_agent_stats()
        results["agent_stats"] = agent_stats

        # Генерируем нарратив через LLM
        narrative = await self._generate_narrative(results, context)
        results["narrative"] = narrative

        return self.success(narrative, data=results)

    def _analyze_activity(self) -> dict:
        """Анализ активности по часам и дням."""
        rows = self.db.execute(
            """SELECT strftime('%H', timestamp) as hour,
               COUNT(*) as count
               FROM messages
               WHERE timestamp > datetime('now', '-30 days')
               GROUP BY hour
               ORDER BY hour"""
        )

        hour_data = {str(i).zfill(2): 0 for i in range(24)}
        for row in rows:
            r = dict(row)
            hour_data[r["hour"]] = r["count"]

        peak_hour = max(hour_data, key=hour_data.get)
        total = sum(hour_data.values())

        return {
            "total_messages_30d": total,
            "peak_hour": f"{peak_hour}:00",
            "hourly": hour_data,
            "avg_per_day": round(total / 30, 1)
        }

    def _get_top_contacts(self, limit: int = 10) -> list:
        """Топ контакты по активности."""
        rows = self.db.execute(
            """SELECT display_name, username, trust_score,
               interaction_count, last_interaction
               FROM contacts
               ORDER BY interaction_count DESC
               LIMIT ?""",
            (limit,)
        )
        return [dict(r) for r in rows]

    def _get_top_topics(self, limit: int = 10) -> list:
        """Топ темы из базы знаний."""
        rows = self.db.execute(
            """SELECT category, COUNT(*) as count
               FROM knowledge
               WHERE category IS NOT NULL
               GROUP BY category
               ORDER BY count DESC
               LIMIT ?""",
            (limit,)
        )
        return [dict(r) for r in rows]

    def _get_agent_stats(self) -> dict:
        """Статистика работы агентов."""
        rows = self.db.execute(
            """SELECT agent_name,
               COUNT(*) as calls,
               SUM(tokens_used) as total_tokens,
               AVG(duration_ms) as avg_ms,
               SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) as errors
               FROM agent_logs
               WHERE timestamp > datetime('now', '-7 days')
               GROUP BY agent_name
               ORDER BY calls DESC"""
        )
        return [dict(r) for r in rows]

    async def _generate_narrative(self, data: dict,
                                   context: AgentContext) -> str:
        """Сгенерировать аналитический нарратив через LLM."""
        activity = data.get("activity", {})
        contacts = data.get("top_contacts", [])[:3]
        topics = data.get("top_topics", [])[:5]

        contact_names = [
            c.get("display_name") or c.get("username") or "?"
            for c in contacts
        ]
        topic_names = [t.get("category", "?") for t in topics]

        summary_data = {
            "сообщений_за_30д": activity.get("total_messages_30d", 0),
            "пик_активности": activity.get("peak_hour", "?"),
            "топ_контакты": contact_names,
            "топ_темы": topic_names,
        }

        response = await self.ask_llm(
            f"Дай краткий аналитический отчёт (3-4 предложения) "
            f"на основе этих данных о пользователе:\n"
            f"{json.dumps(summary_data, ensure_ascii=False, indent=2)}\n\n"
            f"Найди интересные паттерны и дай практический вывод.",
            context,
            task_type=TaskType.ANALYSIS,
            max_tokens=400
        )

        if response.success:
            return response.content

        # Fallback
        return (
            f"За 30 дней: {activity.get('total_messages_30d', 0)} сообщений. "
            f"Пик активности: {activity.get('peak_hour', '?')}. "
            f"Топ контакты: {', '.join(contact_names[:3])}. "
            f"Главные темы: {', '.join(topic_names[:3])}."
        )

    async def weekly_report(self, context: AgentContext) -> str:
        """Еженедельный отчёт для пользователя."""
        result = await self.run(context)

        data = result.data or {}
        activity = data.get("activity", {})
        agent_stats = data.get("agent_stats", [])

        # Считаем токены за неделю
        total_tokens = sum(
            s.get("total_tokens", 0) or 0
            for s in agent_stats
        )

        # Считаем ошибки
        total_errors = sum(
            s.get("errors", 0) or 0
            for s in agent_stats
        )

        db_stats = self.db.get_stats()

        report = (
            f"📊 *Еженедельный отчёт ДЖАРВИСА*\n\n"
            f"*Активность:*\n"
            f"  Сообщений за 30 дней: {activity.get('total_messages_30d', 0)}\n"
            f"  Пик активности: {activity.get('peak_hour', '?')}\n\n"
            f"*База знаний:*\n"
            f"  Знаний: {db_stats.get('knowledge', 0)}\n"
            f"  Сущностей в графе: {db_stats.get('entities', 0)}\n"
            f"  Файлов обработано: {db_stats.get('uploaded_files', 0)}\n\n"
            f"*Работа агентов (7 дней):*\n"
            f"  Всего токенов: {total_tokens:,}\n"
            f"  Ошибок: {total_errors}\n\n"
            f"*Анализ:*\n{result.content}"
        )

        return report

    def detect_anomalies(self) -> list[str]:
        """Обнаружить аномалии в паттернах."""
        anomalies = []

        # Резкое падение активности
        recent = self.db.execute_one(
            """SELECT COUNT(*) as cnt FROM messages
               WHERE timestamp > datetime('now', '-3 days')"""
        )
        older = self.db.execute_one(
            """SELECT COUNT(*) as cnt FROM messages
               WHERE timestamp BETWEEN
               datetime('now', '-10 days') AND datetime('now', '-3 days')"""
        )

        if recent and older:
            r_cnt = dict(recent)["cnt"]
            o_cnt = dict(older)["cnt"]
            if o_cnt > 0 and r_cnt < o_cnt * 0.3:
                anomalies.append(
                    "Активность упала на 70%+ за последние 3 дня"
                )

        # Много ошибок агентов
        errors = self.db.execute_one(
            """SELECT COUNT(*) as cnt FROM agent_logs
               WHERE success=0 AND timestamp > datetime('now', '-1 day')"""
        )
        if errors and dict(errors)["cnt"] > 10:
            anomalies.append(
                f"Много ошибок агентов за сутки: {dict(errors)['cnt']}"
            )

        return anomalies
