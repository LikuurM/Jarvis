"""
knowledge_db.py — База знаний Джарвиса (knowledge.db)
Вторая БД: категории, документы, статьи загруженные владельцем.
Джарвис ищет здесь перед обычным ответом.
"""
import sqlite3
import asyncio
import concurrent.futures
import logging
import os
from pathlib import Path

log = logging.getLogger("jarvis.knowledge_db")

DEFAULT_PATH = os.path.join(os.getenv("DATA_DIR", "/app/data"), "knowledge.db")


class KnowledgeDB:
    def __init__(self, db_path: str = DEFAULT_PATH):
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._init()

    def _init(self):
        if not Path(self.db_path).exists():
            log.info(f"📚 knowledge.db не найден — база знаний пуста ({self.db_path})")
            self._conn = None
            return
        try:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            log.info(f"✅ База знаний загружена: {self.db_path}")
        except Exception as e:
            log.error(f"❌ Ошибка загрузки knowledge.db: {e}")
            self._conn = None

    def reload(self, new_path: str = None):
        """Перезагружает БД (после получения нового файла от владельца)."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        if new_path:
            self.db_path = new_path
        self._init()

    def stats(self) -> dict:
        if not self._conn:
            return {"docs": 0, "categories": 0, "chars": 0}
        try:
            docs = self._conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            cats = self._conn.execute(
                "SELECT COUNT(DISTINCT category) FROM documents WHERE category != ''"
            ).fetchone()[0]
            chars = self._conn.execute(
                "SELECT COALESCE(SUM(LENGTH(content)),0) FROM documents"
            ).fetchone()[0]
            return {"docs": docs, "categories": cats, "chars": chars}
        except Exception:
            return {"docs": 0, "categories": 0, "chars": 0}

    def search(self, query: str, limit: int = 3) -> str:
        """Ищет по базе знаний. Возвращает текстовые фрагменты или ''."""
        if not self._conn or not query:
            return ""
        try:
            # Сначала FTS5
            results = []
            try:
                rows = self._conn.execute(
                    """SELECT title, category, snippet(documents_fts,2,'>','<','…',30) AS snip
                       FROM documents_fts
                       WHERE documents_fts MATCH ?
                       ORDER BY rank LIMIT ?""",
                    (query, limit)
                ).fetchall()
                results = list(rows)
            except Exception:
                pass

            # Фоллбэк — LIKE поиск
            if not results:
                q_like = f"%{query}%"
                rows = self._conn.execute(
                    """SELECT title, category,
                              SUBSTR(content, MAX(1, INSTR(LOWER(content), LOWER(?)) - 100), 300) AS snip
                       FROM documents
                       WHERE LOWER(content) LIKE LOWER(?) OR LOWER(title) LIKE LOWER(?)
                       LIMIT ?""",
                    (query, q_like, q_like, limit)
                ).fetchall()
                results = list(rows)

            if not results:
                return ""

            parts = []
            for row in results:
                title   = row["title"] or "Документ"
                cat     = row["category"] or ""
                snippet = (row["snip"] or "")[:400]
                meta    = f" [{cat}]" if cat else ""
                parts.append(f"[{title}{meta}]\n{snippet}")

            return "\n\n---\n\n".join(parts)

        except Exception as e:
            log.debug(f"knowledge_db.search: {e}")
            return ""

    async def search_async(self, query: str, limit: int = 3) -> str:
        """Асинхронная обёртка для search()."""
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            return await loop.run_in_executor(ex, self.search, query, limit)

    def list_categories(self) -> list[dict]:
        if not self._conn:
            return []
        try:
            rows = self._conn.execute(
                """SELECT category, COUNT(*) as cnt
                   FROM documents
                   WHERE category != ''
                   GROUP BY category
                   ORDER BY cnt DESC"""
            ).fetchall()
            return [{"name": r["category"], "count": r["cnt"]} for r in rows]
        except Exception:
            return []

    def list_docs(self, limit: int = 50, category: str = "") -> list[dict]:
        if not self._conn:
            return []
        try:
            if category:
                rows = self._conn.execute(
                    "SELECT id, title, category, file_name, added_date FROM documents WHERE category=? LIMIT ?",
                    (category, limit)
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id, title, category, file_name, added_date FROM documents LIMIT ?",
                    (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
