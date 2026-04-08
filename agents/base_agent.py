"""
JARVIS Base Agent — базовый класс для всех агентов.
Все 20 агентов наследуют от него.
"""
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.model_router import ModelRouter, TaskType, LLMResponse
from core.personality import Personality, ResponseTone
from database.db import Database


logger = logging.getLogger("jarvis.agent")


@dataclass
class AgentContext:
    """Контекст запроса — передаётся между агентами."""
    original_query: str                         # исходный запрос (Intent Anchor)
    user_id: int = 0
    chat_id: str = ""
    platform: str = "telegram"
    conversation_history: list = field(default_factory=list)
    memory_context: list = field(default_factory=list)   # из MemoryAgent
    web_context: list = field(default_factory=list)       # из WebAgent
    step_number: int = 0
    max_steps: int = 10
    metadata: dict = field(default_factory=dict)

    def check_intent_drift(self, current_action: str) -> bool:
        """
        Защита от агентского смещения.
        Проверяем что текущее действие соответствует исходному запросу.
        """
        # Простая проверка — в продакшне можно усилить через LLM
        if self.step_number > self.max_steps:
            logger.warning(f"Превышен лимит шагов ({self.max_steps})")
            return False
        return True


@dataclass
class AgentResult:
    """Результат работы агента."""
    success: bool
    content: str = ""
    data: Any = None
    error: str = None
    tokens_used: int = 0
    duration_ms: int = 0
    agent_name: str = ""
    model_used: str = ""


class BaseAgent(ABC):
    """
    Базовый агент ДЖАРВИСА.

    Каждый агент:
    1. Получает AgentContext
    2. Выполняет свою специализированную задачу
    3. Логирует в agent_logs
    4. Возвращает AgentResult
    """

    # Имя агента (переопределить в подклассе)
    name: str = "BaseAgent"
    description: str = "Базовый агент"

    # Промпт персональности (переопределить)
    system_prompt: str = """Ты — ДЖАРВИС, ИИ-ассистент Тони Старка.
    Отвечай кратко, точно, с британским достоинством.
    Обращайся "Сэр". Избегай лишних слов."""

    def __init__(self, router: ModelRouter = None, db: Database = None,
                 personality: Personality = None):
        from core.model_router import router as global_router
        from database.db import db as global_db
        from core.personality import personality as global_personality

        self.router = router or global_router
        self.db = db or global_db
        self.personality = personality or global_personality
        self.logger = logging.getLogger(f"jarvis.{self.name.lower()}")

    @abstractmethod
    async def run(self, context: AgentContext) -> AgentResult:
        """Основная логика агента — переопределить."""
        pass

    async def ask_llm(self, message: str, context: AgentContext,
                       task_type: TaskType = None,
                       max_tokens: int = 1024,
                       temperature: float = 0.7) -> LLMResponse:
        """Вызов LLM с логированием."""
        start = time.time()

        response = await self.router.ask(
            message=message,
            system_prompt=self.system_prompt,
            task_type=task_type,
            conversation_history=context.conversation_history[-10:],
            max_tokens=max_tokens,
            temperature=temperature,
        )

        duration = int((time.time() - start) * 1000)

        # Логируем в БД
        self.db.log_agent(
            agent_name=self.name,
            action="llm_call",
            model=response.model,
            tokens=response.tokens_used,
            duration_ms=duration,
            success=response.success,
            error=response.error,
            input_summary=message[:200] if message else None,
            output_summary=response.content[:200] if response.content else None,
        )

        return response

    def log_action(self, action: str, success: bool = True,
                    error: str = None, input_s: str = None,
                    output_s: str = None):
        """Залогировать действие агента."""
        self.db.log_agent(
            agent_name=self.name,
            action=action,
            success=success,
            error=error,
            input_summary=input_s,
            output_summary=output_s,
        )

    def success(self, content: str, data: Any = None,
                tokens: int = 0, duration: int = 0,
                model: str = "") -> AgentResult:
        return AgentResult(
            success=True,
            content=content,
            data=data,
            tokens_used=tokens,
            duration_ms=duration,
            agent_name=self.name,
            model_used=model,
        )

    def failure(self, error: str) -> AgentResult:
        self.logger.error(f"{self.name} ошибка: {error}")
        return AgentResult(
            success=False,
            error=error,
            agent_name=self.name,
        )
