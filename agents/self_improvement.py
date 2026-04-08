"""
JARVIS SelfImprovementAgent — обучение на ошибках, эволюция промптов.
Анализирует взаимодействия, обновляет стиль, улучшает агентов.
"""
import json
import logging
from datetime import datetime, timedelta

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.self_improvement")


class SelfImprovementAgent(BaseAgent):
    """
    Агент самосовершенствования ДЖАРВИСА.

    Ночью анализирует:
    - Ошибки и сбои агентов
    - Паттерны запросов пользователя
    - Качество ответов (на основе реакций)
    - Стиль общения (для Digital Twin)

    И обновляет:
    - StyleProfile пользователя
    - Веса importance scoring
    - Предпочтения пользователя
    """

    name = "SelfImprovement"
    description = "Самообучение и оптимизация ДЖАРВИСА"

    system_prompt = """Ты — аналитик производительности ДЖАРВИСА.
    Анализируй паттерны и предлагай улучшения.
    Будь конкретен и практичен."""

    async def run(self, context: AgentContext) -> AgentResult:
        """Ночной цикл самосовершенствования."""
        results = {}

        # 1. Анализ ошибок за день
        error_count = await self._analyze_errors()
        results["errors_analyzed"] = error_count

        # 2. Обновление StyleProfile
        profile_updated = await self._update_style_profile(context)
        results["style_updated"] = profile_updated

        # 3. Анализ паттернов запросов
        patterns = await self._analyze_query_patterns(context)
        results["patterns_found"] = len(patterns)

        # 4. Обновление trust scores
        contacts_updated = await self._update_trust_scores()
        results["contacts_updated"] = contacts_updated

        # 5. Формируем отчёт
        report = await self._generate_self_report(results, context)

        logger.info(f"SelfImprovement завершён: {results}")
        return self.success(report, data=results)

    async def _analyze_errors(self) -> int:
        """Проанализировать ошибки агентов за последние 24 часа."""
        errors = self.db.execute(
            """SELECT agent_name, COUNT(*) as cnt, GROUP_CONCAT(error_message) as errors
               FROM agent_logs
               WHERE success=0
               AND timestamp > datetime('now', '-1 day')
               GROUP BY agent_name"""
        )

        error_count = 0
        for row in errors:
            r = dict(row)
            agent = r["agent_name"]
            count = r["cnt"]
            error_count += count

            # Если агент часто падает — понижаем его приоритет
            if count > 5:
                logger.warning(f"Агент {agent} упал {count} раз за сутки")
                self.db.execute_write(
                    """INSERT OR REPLACE INTO preferences
                       (category, key, value, confidence, source)
                       VALUES ('agents', ?, ?, 0.9, 'self_improvement')""",
                    (f"{agent}_error_count", str(count))
                )

        return error_count

    async def _update_style_profile(self, context: AgentContext) -> bool:
        """Обновить StyleProfile на основе последних сообщений."""
        # Берём последние 200 входящих сообщений
        messages = self.db.execute(
            """SELECT content FROM messages
               WHERE platform='telegram'
               AND sender_id = (
                   SELECT platform_id FROM contacts
                   WHERE trust_score >= 9
                   ORDER BY trust_score DESC LIMIT 1
               )
               AND timestamp > datetime('now', '-7 days')
               ORDER BY timestamp DESC LIMIT 200"""
        )

        if not messages:
            return False

        texts = [dict(m)["content"] for m in messages if dict(m)["content"]]
        if not texts:
            return False

        # Считаем статистику
        import re
        total = len(texts)
        avg_length = sum(len(t) for t in texts) / total

        # Эмодзи
        emoji_pattern = re.compile(
            "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F9FF\u2600-\u26FF]+",
            flags=re.UNICODE
        )
        emoji_count = sum(
            1 for t in texts if emoji_pattern.search(t)
        )
        emoji_freq = emoji_count / total

        # Знаки препинания
        punct_count = sum(
            1 for t in texts
            if t.endswith(".") or t.endswith("!")
        )
        formal_level = punct_count / total

        # Часто используемые слова-паразиты
        all_words = " ".join(texts[:50]).lower().split()
        filler_candidates = ["короче", "ну", "типа", "вообще",
                              "блин", "ладно", "окей", "ок"]
        fillers = [w for w in filler_candidates
                   if all_words.count(w) > 2]

        # Обновляем style_profile
        existing = self.db.execute_one("SELECT id FROM style_profile LIMIT 1")

        if existing:
            self.db.execute_write(
                """UPDATE style_profile SET
                   avg_message_length=?,
                   emoji_frequency=?,
                   formality_level=?,
                   filler_words=?,
                   samples_count=?,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (avg_length, emoji_freq, formal_level,
                 json.dumps(fillers), total, dict(existing)["id"])
            )
        else:
            self.db.execute_write(
                """INSERT INTO style_profile
                   (avg_message_length, emoji_frequency, formality_level,
                    filler_words, samples_count)
                   VALUES (?,?,?,?,?)""",
                (avg_length, emoji_freq, formal_level,
                 json.dumps(fillers), total)
            )

        logger.info(
            f"StyleProfile обновлён: avg_len={avg_length:.0f}, "
            f"emoji={emoji_freq:.2f}, formal={formal_level:.2f}"
        )
        return True

    async def _analyze_query_patterns(self, context: AgentContext) -> list:
        """Найти паттерны в запросах пользователя."""
        # Берём последние взаимодействия
        episodes = self.db.execute(
            """SELECT summary, timestamp FROM episodic_memory
               WHERE event_type='conversation'
               ORDER BY timestamp DESC LIMIT 100"""
        )

        if not episodes:
            return []

        summaries = [dict(e)["summary"] for e in episodes[:20]]
        combined = "\n".join(summaries[:10])

        response = await self.ask_llm(
            f"Найди 3-5 паттернов в запросах пользователя. "
            f"Что он спрашивает чаще всего? Когда? О чём?\n"
            f"Верни JSON: [{{\"pattern\": \"...\", \"frequency\": \"часто/иногда\"}}]\n\n"
            f"Данные:\n{combined}",
            context,
            task_type=TaskType.ANALYSIS,
            max_tokens=400,
            temperature=0.3
        )

        patterns = []
        if response.success:
            try:
                content = response.content.strip()
                if "```" in content:
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                patterns = json.loads(content)

                # Сохраняем паттерны в предпочтения
                for p in patterns:
                    self.db.execute_write(
                        """INSERT OR REPLACE INTO preferences
                           (category, key, value, confidence, source)
                           VALUES ('query_patterns', ?, ?, 0.7, 'self_improvement')""",
                        (p.get("pattern", "")[:50], p.get("frequency", ""))
                    )
            except Exception:
                pass

        return patterns

    async def _update_trust_scores(self) -> int:
        """Пересчитать trust scores всех контактов."""
        contacts = self.db.execute(
            "SELECT id, interaction_count, sentiment_avg FROM contacts"
        )

        updated = 0
        for contact in contacts:
            c = dict(contact)

            # Простая формула: базовый балл + активность + сентимент
            base = 3.0
            activity_bonus = min(2.0, c["interaction_count"] / 50)
            sentiment_bonus = (c["sentiment_avg"] - 0.5) * 2  # -1 до +1

            new_score = min(10.0, max(0.0, base + activity_bonus + sentiment_bonus))

            self.db.execute_write(
                """UPDATE contacts SET trust_score=?, updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (new_score, c["id"])
            )
            updated += 1

        return updated

    async def _generate_self_report(self, results: dict,
                                     context: AgentContext) -> str:
        """Генерировать ночной отчёт о состоянии системы."""
        db_stats = self.db.get_stats()

        lines = [
            "📊 *Ночной самоотчёт ДЖАРВИСА*\n",
            f"Ошибок проанализировано: {results.get('errors_analyzed', 0)}",
            f"StyleProfile обновлён: {'✓' if results.get('style_updated') else '✗'}",
            f"Паттернов найдено: {results.get('patterns_found', 0)}",
            f"Контактов обновлено: {results.get('contacts_updated', 0)}",
            "",
            "📈 *База знаний:*",
        ]

        for table, count in db_stats.items():
            lines.append(f"  • {table}: {count}")

        return "\n".join(lines)
