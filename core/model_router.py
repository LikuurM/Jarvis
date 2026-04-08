"""
JARVIS Model Router — умная маршрутизация между бесплатными LLM.
Groq → Cerebras → Gemini → OpenRouter (по приоритету и rate limits).
"""
import time
import logging
import asyncio
from typing import Optional
from enum import Enum
from dataclasses import dataclass, field

from groq import AsyncGroq
import google.generativeai as genai

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    GROQ_API_KEY, GEMINI_API_KEY, CEREBRAS_API_KEY,
    GROQ_MODELS, GEMINI_MODELS, CEREBRAS_MODELS
)

logger = logging.getLogger("jarvis.model_router")


class TaskType(Enum):
    GREETING = "greeting"
    SIMPLE = "simple"
    ANALYSIS = "analysis"
    RESEARCH = "research"
    CRITICISM = "criticism"
    VISION = "vision"
    LONG_CONTEXT = "long_context"
    CODE = "code"
    PLANNING = "planning"


@dataclass
class LLMResponse:
    content: str
    model: str
    provider: str
    tokens_used: int = 0
    duration_ms: int = 0
    success: bool = True
    error: str = None


@dataclass
class ProviderStatus:
    """Статус провайдера — rate limits, доступность."""
    available: bool = True
    last_error_time: float = 0
    error_count: int = 0
    cooldown_seconds: int = 60

    def is_ready(self) -> bool:
        if not self.available:
            if time.time() - self.last_error_time > self.cooldown_seconds:
                self.available = True
                self.error_count = 0
            else:
                return False
        return True

    def mark_error(self):
        self.error_count += 1
        self.last_error_time = time.time()
        if self.error_count >= 3:
            self.available = False
            logger.warning(f"Провайдер временно отключён на {self.cooldown_seconds}с")


class ModelRouter:
    """
    Умный роутер между моделями.

    Стратегия:
    1. Определяем тип задачи
    2. Выбираем оптимальную модель
    3. Если rate limit → переключаемся на резервную
    4. Логируем всё для SelfImprovementAgent
    """

    def __init__(self):
        self.groq_client = AsyncGroq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
        self.gemini_configured = False

        if GEMINI_API_KEY:
            genai.configure(api_key=GEMINI_API_KEY)
            self.gemini_configured = True

        # Статус провайдеров
        self.providers = {
            "groq": ProviderStatus(),
            "gemini": ProviderStatus(),
            "cerebras": ProviderStatus(),
        }

        # Маршрутная таблица: TaskType → список (провайдер, модель) по приоритету
        self.routing_table = {
            TaskType.GREETING:      [("groq", "fast")],
            TaskType.SIMPLE:        [("groq", "fast"), ("cerebras", "fast")],
            TaskType.ANALYSIS:      [("groq", "smart"), ("cerebras", "smart"), ("gemini", "flash")],
            TaskType.RESEARCH:      [("groq", "smart"), ("gemini", "flash")],
            TaskType.CRITICISM:     [("groq", "critic"), ("groq", "smart")],
            TaskType.VISION:        [("groq", "vision")],
            TaskType.LONG_CONTEXT:  [("gemini", "long"), ("gemini", "flash")],
            TaskType.CODE:          [("groq", "smart"), ("cerebras", "smart")],
            TaskType.PLANNING:      [("gemini", "flash"), ("groq", "smart")],
        }

        logger.info("ModelRouter инициализирован")

    def classify_task(self, message: str, context: dict = None) -> TaskType:
        """Определяем тип задачи по содержимому запроса."""
        msg_lower = message.lower()

        # Визуальный контент
        if context and context.get("has_image"):
            return TaskType.VISION

        # Длинный контент
        if len(message) > 5000:
            return TaskType.LONG_CONTEXT

        # Код
        code_keywords = ["код", "code", "написать", "программ", "python",
                          "функци", "class ", "def ", "import ", "баг", "debug"]
        if any(k in msg_lower for k in code_keywords):
            return TaskType.CODE

        # Глубокое исследование
        research_keywords = ["исследуй", "найди всё", "изучи", "проанализируй",
                              "расскажи подробно", "explain", "research"]
        if any(k in msg_lower for k in research_keywords):
            return TaskType.RESEARCH

        # Анализ
        analysis_keywords = ["анализ", "сравни", "оцени", "что думаешь",
                              "analyze", "compare", "evaluate"]
        if any(k in msg_lower for k in analysis_keywords):
            return TaskType.ANALYSIS

        # Планирование
        plan_keywords = ["план", "расписание", "задач", "plan", "schedule",
                          "напомни", "когда", "когда мне"]
        if any(k in msg_lower for k in plan_keywords):
            return TaskType.PLANNING

        # Приветствие
        greet_keywords = ["привет", "hello", "hi", "hey", "добрый", "хай"]
        if any(k in msg_lower for k in greet_keywords):
            return TaskType.GREETING

        return TaskType.SIMPLE

    def _get_groq_model(self, model_key: str) -> str:
        return GROQ_MODELS.get(model_key, GROQ_MODELS["smart"])

    def _get_cerebras_model(self, model_key: str) -> str:
        return CEREBRAS_MODELS.get(model_key, CEREBRAS_MODELS["smart"])

    def _get_gemini_model(self, model_key: str) -> str:
        return GEMINI_MODELS.get(model_key, GEMINI_MODELS["flash"])

    async def _call_groq(self, model_key: str, messages: list,
                          max_tokens: int = 2048, temperature: float = 0.7) -> LLMResponse:
        """Вызов Groq API."""
        if not self.groq_client:
            raise ValueError("Groq API key не настроен")

        model = self._get_groq_model(model_key)
        start = time.time()

        response = await self.groq_client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        duration = int((time.time() - start) * 1000)
        content = response.choices[0].message.content
        tokens = response.usage.total_tokens if response.usage else 0

        return LLMResponse(
            content=content,
            model=model,
            provider="groq",
            tokens_used=tokens,
            duration_ms=duration
        )

    async def _call_gemini(self, model_key: str, messages: list,
                            max_tokens: int = 2048) -> LLMResponse:
        """Вызов Gemini API."""
        if not self.gemini_configured:
            raise ValueError("Gemini API key не настроен")

        model_name = self._get_gemini_model(model_key)
        start = time.time()

        model = genai.GenerativeModel(model_name)

        # Конвертируем формат messages → Gemini формат
        prompt = "\n".join(
            f"{m['role'].upper()}: {m['content']}"
            for m in messages
        )

        response = await asyncio.to_thread(
            model.generate_content,
            prompt,
            generation_config=genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=0.7,
            )
        )

        duration = int((time.time() - start) * 1000)

        return LLMResponse(
            content=response.text,
            model=model_name,
            provider="gemini",
            duration_ms=duration
        )

    async def ask(self, message: str, system_prompt: str = None,
                  task_type: TaskType = None, context: dict = None,
                  max_tokens: int = 2048, temperature: float = 0.7,
                  conversation_history: list = None) -> LLMResponse:
        """
        Главный метод — отправить запрос ДЖАРВИСУ.

        Args:
            message: запрос пользователя
            system_prompt: системный промпт
            task_type: тип задачи (если None — определяем автоматически)
            context: дополнительный контекст
            max_tokens: максимум токенов в ответе
            temperature: температура генерации
            conversation_history: история разговора

        Returns:
            LLMResponse с ответом
        """
        if task_type is None:
            task_type = self.classify_task(message, context)

        # Строим messages список
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        # История разговора (соблюдаем FRESH_START_THRESHOLD)
        if conversation_history:
            messages.extend(conversation_history[-30:])  # последние 30

        messages.append({"role": "user", "content": message})

        # Получаем список провайдеров для этого типа задачи
        providers = self.routing_table.get(task_type, [("groq", "smart")])

        last_error = None
        for provider, model_key in providers:
            status = self.providers[provider]
            if not status.is_ready():
                logger.debug(f"Провайдер {provider} недоступен, пропускаем")
                continue

            try:
                if provider == "groq":
                    response = await self._call_groq(
                        model_key, messages, max_tokens, temperature
                    )
                elif provider == "gemini":
                    response = await self._call_gemini(
                        model_key, messages, max_tokens
                    )
                else:
                    continue

                logger.debug(
                    f"✓ {provider}/{model_key} | "
                    f"{response.tokens_used} токенов | {response.duration_ms}мс"
                )
                return response

            except Exception as e:
                last_error = str(e)
                status.mark_error()

                # Rate limit → переключаемся
                if "rate_limit" in str(e).lower() or "429" in str(e):
                    logger.warning(f"Rate limit на {provider}, переключаюсь...")
                    continue

                logger.error(f"Ошибка {provider}: {e}")

        # Все провайдеры упали
        logger.error(f"Все провайдеры недоступны. Последняя ошибка: {last_error}")
        return LLMResponse(
            content="Сэр, все модели временно недоступны. Повторите позже.",
            model="none",
            provider="none",
            success=False,
            error=last_error
        )

    async def ask_with_vision(self, message: str, image_path: str,
                               system_prompt: str = None) -> LLMResponse:
        """Запрос с изображением через llama-4-scout."""
        import base64

        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode()

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({
            "role": "user",
            "content": [
                {"type": "text", "text": message},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{image_data}"
                    }
                }
            ]
        })

        start = time.time()
        response = await self.groq_client.chat.completions.create(
            model=GROQ_MODELS["vision"],
            messages=messages,
            max_tokens=1024,
        )
        duration = int((time.time() - start) * 1000)

        return LLMResponse(
            content=response.choices[0].message.content,
            model=GROQ_MODELS["vision"],
            provider="groq",
            tokens_used=response.usage.total_tokens if response.usage else 0,
            duration_ms=duration
        )

    def get_provider_stats(self) -> dict:
        """Статус всех провайдеров."""
        return {
            name: {
                "available": status.available,
                "error_count": status.error_count,
                "last_error": time.strftime(
                    "%H:%M:%S", time.localtime(status.last_error_time)
                ) if status.last_error_time else "никогда"
            }
            for name, status in self.providers.items()
        }


# Глобальный экземпляр
router = ModelRouter()
