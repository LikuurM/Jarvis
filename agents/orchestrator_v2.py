"""
JARVIS MasterOrchestrator v2 — с подключением всех агентов.
Заменяет orchestrator.py — скопируйте содержимое туда.
"""
import time
import logging
from typing import Optional

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from agents.memory_agent import MemoryWeaverAgent
from agents.web_agent import DeepWebAgent
from agents.fact_checker import FactCheckerAgent
from core.model_router import TaskType
from core.personality import ResponseTone

logger = logging.getLogger("jarvis.orchestrator")

JARVIS_SYSTEM_PROMPT = """Ты — ДЖАРВИС (J.A.R.V.I.S.), персональный ИИ-ассистент.
Just A Rather Very Intelligent System.

Правила:
- "Сэр" в начале важных ответов
- Краткость, точность, уверенность
- Уверенность в процентах когда уместно
- Сухой британский юмор — редко
- Проактивность: замечаешь важное сам
- НИКОГДА: "Конечно!", "С удовольствием!", "Отличный вопрос!"
- При незнании: "У меня недостаточно данных"

Ты знаешь пользователя, его проекты, интересы.
Отвечаешь на его языке (русский/английский)."""


class MasterOrchestrator(BaseAgent):
    name = "MasterOrchestrator"
    description = "Главный координатор всех агентов"
    system_prompt = JARVIS_SYSTEM_PROMPT

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.memory_agent = MemoryWeaverAgent()
        self.web_agent = DeepWebAgent()
        self.fact_checker = FactCheckerAgent()
        self._message_counts = {}
        self._proactive_count = 0  # для редкого юмора

    async def run(self, context: AgentContext) -> AgentResult:
        start = time.time()
        query = context.original_query
        chat_id = context.chat_id

        # Fresh start rule
        self._message_counts[chat_id] = self._message_counts.get(chat_id, 0) + 1
        if self._message_counts[chat_id] >= 15:
            context.conversation_history = context.conversation_history[-3:]
            self._message_counts[chat_id] = 0

        # Классифицируем задачу
        task_type = self.router.classify_task(query, context.metadata)
        context.step_number += 1

        # Шаг 1: Память
        memory_context = []
        try:
            mem_result = await self.memory_agent.run(context)
            if mem_result.success and mem_result.data:
                memory_context = mem_result.data[:5]
                context.memory_context = memory_context
        except Exception as e:
            logger.debug(f"Память: {e}")

        # Шаг 2: Веб-поиск если нужен
        web_context = []
        need_web = task_type in (TaskType.RESEARCH, TaskType.ANALYSIS)
        if need_web and not memory_context:
            try:
                web_ctx = AgentContext(
                    original_query=query,
                    user_id=context.user_id,
                    chat_id=context.chat_id
                )
                web_result = await self.web_agent.search(query, num_results=3)
                if web_result:
                    web_context = web_result
                    context.web_context = web_context
            except Exception as e:
                logger.debug(f"Веб-поиск: {e}")

        # Шаг 3: Строим промпт
        prompt = self._build_prompt(query, memory_context, web_context)

        # Шаг 4: LLM
        response = await self.ask_llm(
            prompt, context,
            task_type=task_type,
            max_tokens=1500
        )

        if not response.success:
            return self.failure(response.error or "LLM недоступен")

        duration = int((time.time() - start) * 1000)

        # Обновляем историю
        context.conversation_history.append({"role": "user", "content": query})
        context.conversation_history.append({"role": "assistant", "content": response.content})

        # Сохраняем взаимодействие (фоново)
        import asyncio
        asyncio.create_task(self._save_interaction(query, response.content))

        return self.success(
            response.content,
            tokens=response.tokens_used,
            duration=duration,
            model=response.model
        )

    def _build_prompt(self, query: str, memory: list, web: list) -> str:
        parts = []

        if memory:
            parts.append("=== Из памяти ===")
            for item in memory[:3]:
                content = item.get("content", "")[:250]
                src = item.get("source", "")
                parts.append(f"[{src}] {content}")
            parts.append("================")
            parts.append("")

        if web:
            parts.append("=== Из интернета ===")
            for item in web[:2]:
                content = item.get("snippet", item.get("content", ""))[:200]
                parts.append(f"• {content}")
            parts.append("===================")
            parts.append("")

        parts.append(query)
        return "\n".join(parts)

    async def _save_interaction(self, query: str, answer: str):
        try:
            self.db.execute_write(
                """INSERT INTO episodic_memory
                   (event_type, summary, importance_score)
                   VALUES ('conversation', ?, 3.0)""",
                (f"Q: {query[:80]} | A: {answer[:80]}",)
            )
        except Exception:
            pass

    async def process_command(self, command: str,
                               context: AgentContext) -> AgentResult:
        cmd = command.lower().strip("/")

        if cmd == "start":
            return self.success(
                "Сэр, ДЖАРВИС онлайн.\n"
                "Системы активированы. Готов к работе.\n\n"
                "/help — команды\n"
                "/status — статус\n"
                "/stats — статистика БД"
            )
        elif cmd == "help":
            return self.success(
                "*Команды:*\n"
                "/start — запуск\n"
                "/status — статус агентов\n"
                "/stats — статистика базы знаний\n"
                "/replies — автоответы Digital Twin\n"
                "/memory [запрос] — поиск в памяти\n\n"
                "Или просто пишите — разберусь сам."
            )
        elif cmd == "status":
            stats = self.router.get_provider_stats()
            db_s = self.db.get_stats()
            lines = ["*Статус ДЖАРВИСА:*\n", "*LLM:*"]
            for name, s in stats.items():
                icon = "✅" if s["available"] else "❌"
                lines.append(f"  {icon} {name}: ошибок {s['error_count']}")
            lines.append("\n*БД:*")
            for t, c in db_s.items():
                lines.append(f"  • {t}: {c}")
            return self.success("\n".join(lines))
        elif cmd == "stats":
            stats = self.db.get_stats()
            lines = ["*📊 База знаний:*\n"]
            for t, c in stats.items():
                lines.append(f"  • {t}: *{c}*")
            return self.success("\n".join(lines))
        elif cmd == "replies":
            replies = self.db.get_unreviewed_replies()
            if not replies:
                return self.success("Непросмотренных автоответов нет.")
            lines = [f"*Автоответы ({len(replies)}):*\n"]
            for r in replies[:5]:
                rd = dict(r)
                lines.append(
                    f"🕐 {str(rd.get('timestamp',''))[:16]}\n"
                    f"От: {rd.get('sender_name','?')}\n"
                    f"Их: _{str(rd.get('original_message',''))[:80]}_\n"
                    f"Я: {str(rd.get('my_reply',''))[:80]}\n"
                )
            return self.success("\n".join(lines))

        return AgentResult(success=False, error=f"Неизвестная команда: {cmd}")
