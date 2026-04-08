"""
JARVIS FactCheckerAgent — верификатор фактов.
Проверяет любое утверждение по 3+ источникам.
"""
import logging
from dataclasses import dataclass
from typing import Optional

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.fact_checker")


@dataclass
class FactCheckResult:
    claim: str
    confidence: float        # 0-1
    verdict: str             # confirmed/disputed/unknown
    sources: list[str]
    explanation: str
    is_disputed: bool = False


class FactCheckerAgent(BaseAgent):
    """
    Агент верификации фактов.

    Алгоритм:
    1. Классифицировать утверждение (факт/мнение/прогноз)
    2. Найти 3+ независимых источника
    3. Сравнить → выдать confidence score
    4. Обновить БД с результатом
    """

    name = "FactChecker"
    description = "Верификация фактов по множеству источников"

    system_prompt = """Ты — скептик и верификатор ДЖАРВИСА.
    Твоя задача: проверить истинность утверждений.
    Различай: факт (проверяемо) / мнение / прогноз.
    Оценивай источники: Wikipedia > научные > новости > блоги.
    Отвечай структурированно."""

    async def run(self, context: AgentContext) -> AgentResult:
        """Проверить факты в запросе."""
        claim = context.original_query
        result = await self.check_claim(claim, context)

        response_text = (
            f"**Проверка факта:** _{claim}_\n\n"
            f"**Вердикт:** {result.verdict}\n"
            f"**Уверенность:** {int(result.confidence * 100)}%\n\n"
            f"{result.explanation}"
        )

        if result.is_disputed:
            response_text += "\n\n⚠️ _Информация спорная — разные источники расходятся._"

        return self.success(response_text, data=result.__dict__)

    async def check_claim(self, claim: str,
                           context: AgentContext) -> FactCheckResult:
        """Проверить конкретное утверждение."""

        # Шаг 1: Классифицируем
        classification = await self._classify_claim(claim, context)

        if classification == "opinion":
            return FactCheckResult(
                claim=claim,
                confidence=0.5,
                verdict="мнение",
                sources=[],
                explanation="Это субъективное мнение, не поддающееся объективной проверке."
            )

        if classification == "prediction":
            return FactCheckResult(
                claim=claim,
                confidence=0.3,
                verdict="прогноз",
                sources=[],
                explanation="Это прогноз о будущем — не может быть проверен сейчас."
            )

        # Шаг 2: Ищем источники
        from agents.web_agent import DeepWebAgent
        web_agent = DeepWebAgent()

        sources_data = []
        # Ищем с разных углов
        search_queries = [
            claim,
            f"fact check {claim}",
            f"is it true that {claim}"
        ]

        for query in search_queries[:2]:
            results = await web_agent.search(query, num_results=3)
            sources_data.extend(results)

        # Также ищем в нашей БД
        db_results = self.db.search_knowledge(claim, limit=3)
        for r in db_results:
            r_dict = dict(r)
            sources_data.append({
                "content": r_dict.get("content", ""),
                "url": r_dict.get("source_url", "internal_db"),
                "confidence": r_dict.get("confidence_score", 0.5)
            })

        if not sources_data:
            return FactCheckResult(
                claim=claim,
                confidence=0.1,
                verdict="не проверено",
                sources=[],
                explanation="Не удалось найти источники для проверки."
            )

        # Шаг 3: LLM анализирует источники
        sources_text = "\n".join([
            f"- {s.get('content', s.get('snippet', ''))[:200]}"
            for s in sources_data[:5]
        ])

        analysis = await self.ask_llm(
            f"""Проверь это утверждение: "{claim}"

На основе этих источников:
{sources_text}

Ответь в формате:
ВЕРДИКТ: [подтверждено/опровергнуто/спорно/неизвестно]
УВЕРЕННОСТЬ: [0-100]%
ОБЪЯСНЕНИЕ: [краткое объяснение на русском]""",
            context,
            task_type=TaskType.CRITICISM,
            max_tokens=400,
            temperature=0.2
        )

        # Парсим ответ
        confidence = 0.5
        verdict = "неизвестно"
        explanation = analysis.content if analysis.success else "Анализ недоступен"
        is_disputed = False

        if analysis.success:
            content = analysis.content.lower()

            if "подтверждено" in content:
                verdict = "подтверждено ✓"
                confidence = 0.8
            elif "опровергнуто" in content:
                verdict = "опровергнуто ✗"
                confidence = 0.75
            elif "спорно" in content:
                verdict = "спорно ⚡"
                confidence = 0.5
                is_disputed = True

            # Извлекаем процент уверенности
            import re
            pct_match = re.search(r"уверенность[:\s]+(\d+)", content)
            if pct_match:
                confidence = int(pct_match.group(1)) / 100

        source_urls = [s.get("url", "") for s in sources_data[:3] if s.get("url")]

        # Шаг 4: Обновляем БД
        self._update_knowledge_confidence(claim, confidence, is_disputed)

        return FactCheckResult(
            claim=claim,
            confidence=confidence,
            verdict=verdict,
            sources=source_urls,
            explanation=explanation,
            is_disputed=is_disputed
        )

    async def _classify_claim(self, claim: str,
                               context: AgentContext) -> str:
        """Определить тип утверждения: fact/opinion/prediction."""
        response = await self.ask_llm(
            f"Классифицируй одним словом: '{claim}'\n"
            f"Варианты: fact (проверяемый факт) / opinion (мнение) / prediction (прогноз)\n"
            f"Ответ одним словом:",
            context,
            task_type=TaskType.SIMPLE,
            max_tokens=10,
            temperature=0.1
        )

        if response.success:
            content = response.content.lower().strip()
            if "opinion" in content or "мнени" in content:
                return "opinion"
            if "prediction" in content or "прогноз" in content:
                return "prediction"
        return "fact"

    def _update_knowledge_confidence(self, claim: str,
                                      confidence: float,
                                      is_disputed: bool):
        """Обновить confidence в БД для похожих записей."""
        try:
            results = self.db.search_knowledge(claim[:50], limit=3)
            for r in results:
                r_dict = dict(r)
                self.db.execute_write(
                    """UPDATE knowledge
                       SET confidence_score=?, is_disputed=?, updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (confidence, 1 if is_disputed else 0, r_dict["id"])
                )
        except Exception as e:
            logger.debug(f"Не удалось обновить confidence: {e}")

    async def verify_knowledge_batch(self) -> int:
        """
        Ночная переверификация устаревших записей.
        Проверяет записи с confidence < 0.7.
        """
        low_confidence = self.db.execute(
            """SELECT id, content FROM knowledge
               WHERE confidence_score < 0.7
               AND updated_at < datetime('now', '-7 days')
               LIMIT 10"""
        )

        verified = 0
        ctx = AgentContext(original_query="batch_verify")

        for row in low_confidence:
            row_dict = dict(row)
            claim = row_dict["content"][:200]
            try:
                result = await self.check_claim(claim, ctx)
                self.db.execute_write(
                    """UPDATE knowledge
                       SET confidence_score=?, is_disputed=?,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE id=?""",
                    (result.confidence, 1 if result.is_disputed else 0,
                     row_dict["id"])
                )
                verified += 1
            except Exception as e:
                logger.debug(f"Ошибка верификации: {e}")

        logger.info(f"Переверифицировано {verified} записей")
        return verified
