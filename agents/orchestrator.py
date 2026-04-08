"""
JARVIS MasterOrchestrator — главный агент-координатор.
Принимает запрос, выбирает агентов, синтезирует финальный ответ.
"""
import time
import logging
from typing import Optional

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from agents.memory_agent import MemoryWeaverAgent
from core.model_router import TaskType
from core.personality import ResponseTone

logger = logging.getLogger("jarvis.orchestrator")


JARVIS_SYSTEM_PROMPT = """Ты — ДЖАРВИС (J.A.R.V.I.S.), персональный ИИ-ассистент.
Полное имя: Just A Rather Very Intelligent System.

Правила голоса:
- Обращайся "Сэр" в начале важных ответов
- Отвечай кратко, точно, уверенно
- Уверенность выражай в процентах когда уместно
- Сухой британский юмор — редко и уместно
- Проактивность: если видишь важное — скажи без вопроса
- НИКОГДА: "Конечно!", "С удовольствием!", "Отличный вопрос!"
- При незнании: "У меня недостаточно данных"

Специализация:
- Python разработка, Telegram боты, AI системы
- Знаешь пользователя и его проекты
- Помнишь историю разговоров

Отвечай на языке пользователя (русский/английский)."""


class MasterOrchestrator(BaseAgent):
    """
    Главный оркестратор ДЖАРВИСА.

    Поток обработки:
    1. Классифицировать запрос
    2. Запросить память (MemoryAgent)
    3. Если нужно — поиск в интернете (WebAgent)
    4. Синтезировать ответ через LLM
    5. Форматировать через PersonalityAgent
    6. Сохранить в память
    """

    name = "MasterOrchestrator"
    description = "Главный координатор всех агентов"
    system_prompt = JARVIS_SYSTEM_PROMPT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.memory_agent = MemoryWeaverAgent()
        # Другие агенты добавляются в Part 2
        self._message_counts = {}  # chat_id → count (для fresh start)

    async def run(self, context: AgentContext) -> AgentResult:
        """Обработать запрос пользователя."""
        start = time.time()
        query = context.original_query

        # Проверяем fresh start threshold
        chat_id = context.chat_id
        self._message_counts[chat_id] = self._message_counts.get(chat_id, 0) + 1
        if self._message_counts[chat_id] >= 15:
            # Сбрасываем историю, сохраняем только ключевые факты
            context.conversation_history = context.conversation_history[-3:]
            self._message_counts[chat_id] = 0
            logger.debug(f"Fresh start для чата {chat_id}")

        # Шаг 1: Классифицируем задачу
        task_type = self.router.classify_task(query, context.metadata)
        context.step_number += 1

        # Шаг 2: Ищем в памяти
        memory_context = []
        try:
            memory_result = await self.memory_agent.run(context)
            if memory_result.success and memory_result.data:
                memory_context = memory_result.data[:5]
                context.memory_context = memory_context
        except Exception as e:
            logger.debug(f"Память недоступна: {e}")

        # Шаг 3: Формируем промпт с контекстом
        full_prompt = self._build_prompt(query, memory_context, context)

        # Шаг 4: Вызываем LLM
        response = await self.ask_llm(
            full_prompt,
            context,
            task_type=task_type,
            max_tokens=1500,
            temperature=0.7
        )

        if not response.success:
            return self.failure(response.error or "LLM недоступен")

        duration = int((time.time() - start) * 1000)

        # Шаг 5: Сохраняем взаимодействие в историю
        context.conversation_history.append({"role": "user", "content": query})
        context.conversation_history.append({"role": "assistant", "content": response.content})

        # Шаг 6: Асинхронно сохраняем в память (не блокируем ответ)
        if len(query) > 20:
            await self._save_interaction(query, response.content, context)

        return self.success(
            response.content,
            tokens=response.tokens_used,
            duration=duration,
            model=response.model
        )

    def _build_prompt(self, query: str, memory_context: list,
                       context: AgentContext) -> str:
        """Строим промпт с памятью и контекстом."""
        parts = []

        # Добавляем релевантную память
        if memory_context:
            parts.append("=== Релевантная информация из памяти ===")
            for i, item in enumerate(memory_context[:3], 1):
                content = item.get("content", "")[:300]
                source = item.get("source", "")
                parts.append(f"{i}. [{source}] {content}")
            parts.append("========================================")
            parts.append("")

        parts.append(query)
        return "\n".join(parts)

    async def _save_interaction(self, query: str, answer: str,
                                  context: AgentContext):
        """Сохранить взаимодействие в память (фоновая задача)."""
        try:
            # Сохраняем как эпизодическую память
            self.db.execute_write(
                """INSERT INTO episodic_memory
                   (event_type, summary, importance_score)
                   VALUES ('conversation', ?, ?)""",
                (f"Q: {query[:100]} | A: {answer[:100]}", 3.0)
            )
        except Exception as e:
            logger.debug(f"Не удалось сохранить взаимодействие: {e}")

    async def process_command(self, command: str,
                               context: AgentContext) -> AgentResult:
        """Обработать команду (/start, /help, /status и т.д.)."""
        cmd = command.lower().strip("/")

        if cmd == "start":
            return self.success(
                "Сэр, ДЖАРВИС онлайн.\n"
                "Готов к работе. Чем могу помочь?\n\n"
                "Команды:\n"
                "/help — список команд\n"
                "/status — статус системы\n"
                "/stats — статистика базы знаний\n"
                "/replies — непросмотренные автоответы"
            )

        elif cmd == "help":
            return self.success(
                "*Команды ДЖАРВИСА:*\n\n"
                "/start — запустить\n"
                "/status — статус агентов и API\n"
                "/stats — статистика БД\n"
                "/replies — автоответы за моё отсутствие\n"
                "/memory [запрос] — поиск в памяти\n"
                "/web [запрос] — поиск в интернете\n\n"
                "Или просто напишите что нужно — сам разберусь."
            )

        elif cmd == "status":
            provider_stats = self.router.get_provider_stats()
            db_stats = self.db.get_stats()

            lines = ["*Статус ДЖАРВИСА:*\n"]

            # Провайдеры
            lines.append("*LLM провайдеры:*")
            for name, stat in provider_stats.items():
                icon = "✅" if stat["available"] else "❌"
                lines.append(f"  {icon} {name}: ошибок {stat['error_count']}")

            lines.append("\n*База знаний:*")
            for table, count in db_stats.items():
                lines.append(f"  • {table}: {count}")

            return self.success("\n".join(lines))

        elif cmd == "stats":
            stats = self.db.get_stats()
            lines = ["*📊 Статистика базы знаний:*\n"]
            emoji_map = {
                "messages": "💬",
                "contacts": "👥",
                "knowledge": "🧠",
                "entities": "🔗",
                "uploaded_files": "📁",
                "file_chunks": "📄",
                "auto_replies": "🤖",
            }
            for table, count in stats.items():
                emoji = emoji_map.get(table, "•")
                lines.append(f"  {emoji} {table}: *{count}*")
            return self.success("\n".join(lines))

        elif cmd == "replies":
            replies = self.db.get_unreviewed_replies()
            if not replies:
                return self.success("Непросмотренных автоответов нет, сэр.")

            lines = [f"*Автоответы ({len(replies)} непросмотренных):*\n"]
            for r in replies[:5]:
                r_dict = dict(r)
                lines.append(
                    f"🕐 {r_dict.get('timestamp', '')[:16]}\n"
                    f"От: {r_dict.get('sender_name', '?')}\n"
                    f"Их: _{r_dict.get('original_message', '')[:80]}_\n"
                    f"Я: {r_dict.get('my_reply', '')[:80]}\n"
                )
            return self.success("\n".join(lines))

        return AgentResult(success=False, error=f"Неизвестная команда: {cmd}")
