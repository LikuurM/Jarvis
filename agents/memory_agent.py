"""
JARVIS MemoryWeaverAgent — управление памятью, RAG поиск.
Ищет в SQLite FTS5 + ChromaDB (векторный) + граф знаний.
"""
import json
import logging
from typing import Optional

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.memory")


class MemoryWeaverAgent(BaseAgent):
    """
    Агент памяти ДЖАРВИСА.

    Умеет:
    1. Полнотекстовый поиск (FTS5)
    2. Семантический поиск (ChromaDB)
    3. Поиск по графу знаний
    4. Сохранение новых знаний
    5. Консолидация памяти (ночью)
    """

    name = "MemoryWeaver"
    description = "Управление памятью и поиск по базе знаний"

    system_prompt = """Ты — MemoryWeaver, агент памяти ДЖАРВИСА.
    Твоя задача: найти наиболее релевантную информацию из базы знаний
    для ответа на вопрос пользователя.
    Извлекай только факты, подтверждённые источниками.
    Отвечай кратко на русском языке."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._chroma_client = None
        self._chroma_collection = None
        self._embedder = None

    def _get_chroma(self):
        """Ленивая инициализация ChromaDB."""
        if self._chroma_client is None:
            try:
                import chromadb
                from chromadb.config import Settings
                import sys
                from pathlib import Path
                sys.path.insert(0, str(Path(__file__).parent.parent))
                from config import CHROMA_PATH

                self._chroma_client = chromadb.PersistentClient(
                    path=str(CHROMA_PATH),
                    settings=Settings(anonymized_telemetry=False)
                )
                self._chroma_collection = self._chroma_client.get_or_create_collection(
                    name="jarvis_knowledge",
                    metadata={"hnsw:space": "cosine"}
                )
                logger.info("ChromaDB инициализирован")
            except ImportError:
                logger.warning("ChromaDB не установлен, используем только FTS5")
        return self._chroma_collection

    def _get_embedder(self):
        """Ленивая инициализация sentence-transformers."""
        if self._embedder is None:
            try:
                from sentence_transformers import SentenceTransformer
                import sys
                from pathlib import Path
                sys.path.insert(0, str(Path(__file__).parent.parent))
                from config import EMBEDDING_MODEL

                self._embedder = SentenceTransformer(EMBEDDING_MODEL)
                logger.info(f"Embedder загружен: {EMBEDDING_MODEL}")
            except ImportError:
                logger.warning("sentence-transformers не установлен")
        return self._embedder

    async def run(self, context: AgentContext) -> AgentResult:
        """Поиск релевантной памяти для запроса."""
        query = context.original_query

        results = []

        # 1. FTS5 поиск по сообщениям
        fts_messages = self.db.search_messages(query, limit=5)
        for msg in fts_messages:
            results.append({
                "source": "messages",
                "content": dict(msg).get("content", ""),
                "sender": dict(msg).get("sender_name", ""),
                "timestamp": dict(msg).get("timestamp", ""),
                "score": 0.7,
            })

        # 2. FTS5 поиск по базе знаний
        fts_knowledge = self.db.search_knowledge(query, limit=5)
        for k in fts_knowledge:
            results.append({
                "source": "knowledge",
                "content": dict(k).get("summary") or dict(k).get("content", ""),
                "source_url": dict(k).get("source_url", ""),
                "confidence": dict(k).get("confidence_score", 0.5),
                "score": 0.8,
            })

        # 3. Семантический поиск (если ChromaDB доступен)
        collection = self._get_chroma()
        embedder = self._get_embedder()

        if collection and embedder:
            try:
                query_embedding = embedder.encode(query).tolist()
                chroma_results = collection.query(
                    query_embeddings=[query_embedding],
                    n_results=5,
                    include=["documents", "metadatas", "distances"]
                )

                if chroma_results and chroma_results["documents"]:
                    for doc, meta, dist in zip(
                        chroma_results["documents"][0],
                        chroma_results["metadatas"][0],
                        chroma_results["distances"][0]
                    ):
                        similarity = 1 - dist
                        if similarity > 0.5:
                            results.append({
                                "source": "vector",
                                "content": doc,
                                "metadata": meta,
                                "score": similarity,
                            })
            except Exception as e:
                logger.debug(f"ChromaDB поиск не удался: {e}")

        # 4. Сортируем по релевантности
        results.sort(key=lambda x: x.get("score", 0), reverse=True)
        results = results[:10]  # топ-10

        if not results:
            return self.success(
                "В базе знаний нет релевантной информации.",
                data=[]
            )

        return self.success(
            f"Найдено {len(results)} релевантных фрагментов",
            data=results
        )

    async def save_to_memory(self, content: str, source_type: str = "conversation",
                              source_url: str = None, category: str = None,
                              tags: list = None, confidence: float = 0.7) -> int:
        """Сохранить новое знание."""
        # Генерируем резюме через LLM если контент длинный
        summary = None
        if len(content) > 500:
            from agents.base_agent import AgentContext
            ctx = AgentContext(original_query=f"Сожми в 1-2 предложения:\n{content[:1000]}")
            response = await self.ask_llm(
                f"Кратко резюмируй в 1-2 предложения:\n{content[:1000]}",
                ctx,
                task_type=TaskType.SIMPLE,
                max_tokens=150
            )
            summary = response.content if response.success else content[:200]

        # Сохраняем в SQLite
        knowledge_id = self.db.save_knowledge(
            content=content,
            summary=summary,
            source_url=source_url,
            source_type=source_type,
            category=category,
            tags=tags,
            confidence=confidence,
        )

        # Сохраняем вектор в ChromaDB
        collection = self._get_chroma()
        embedder = self._get_embedder()

        if collection and embedder:
            try:
                text_for_embed = summary or content[:500]
                embedding = embedder.encode(text_for_embed).tolist()

                collection.add(
                    ids=[f"k_{knowledge_id}"],
                    embeddings=[embedding],
                    documents=[text_for_embed],
                    metadatas=[{
                        "source_type": source_type,
                        "category": category or "",
                        "knowledge_id": knowledge_id,
                    }]
                )

                # Обновляем embedding_id в SQLite
                self.db.execute_write(
                    "UPDATE knowledge SET embedding_id=? WHERE id=?",
                    (f"k_{knowledge_id}", knowledge_id)
                )
            except Exception as e:
                logger.debug(f"Не удалось сохранить вектор: {e}")

        return knowledge_id

    async def consolidate_daily(self):
        """
        Ночная консолидация памяти.
        Сжимает эпизодическую память, удаляет дубли.
        """
        logger.info("Начинаю консолидацию памяти...")

        # Находим сообщения за день с importance < 3
        low_importance = self.db.execute(
            """SELECT chat_id, COUNT(*) as cnt,
               GROUP_CONCAT(content, ' | ') as combined
               FROM messages
               WHERE importance_score < 3
               AND date(timestamp) = date('now', '-1 day')
               GROUP BY chat_id"""
        )

        compressed = 0
        for row in low_importance:
            row_dict = dict(row)
            # Сохраняем сжатую версию как эпизодическую память
            self.db.execute_write(
                """INSERT INTO episodic_memory
                   (event_type, summary, topics, importance_score, timestamp)
                   VALUES ('daily_summary', ?, '[]', 3.0, date('now', '-1 day'))""",
                (f"Активность в чате {row_dict['chat_id']}: "
                 f"{row_dict['cnt']} сообщений",)
            )
            compressed += 1

        logger.info(f"Консолидировано {compressed} записей")
        return compressed

    async def extract_entities_from_text(self, text: str,
                                          context: AgentContext) -> list:
        """Извлечь сущности из текста для графа знаний."""
        response = await self.ask_llm(
            f"""Извлеки из текста именованные сущности.
            Верни JSON массив: [{{"name": "...", "type": "person/place/org/concept", "description": "..."}}]
            Только JSON, без пояснений.

            Текст: {text[:2000]}""",
            context,
            task_type=TaskType.ANALYSIS,
            max_tokens=512,
            temperature=0.1
        )

        if not response.success:
            return []

        try:
            # Очищаем от markdown
            content = response.content.strip()
            if "```" in content:
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]

            entities = json.loads(content)
            return entities if isinstance(entities, list) else []
        except json.JSONDecodeError:
            return []
