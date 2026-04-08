"""
JARVIS FileHandler — обработка всех форматов файлов.
Поддерживает 30+ форматов: PDF, DOCX, CSV, Telegram export, ZIP и т.д.
"""
import json
import os
import zipfile
import logging
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Optional, Callable

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import TEMP_DIR, SUPPORTED_EXTENSIONS
from database.db import db
from agents.base_agent import AgentContext

logger = logging.getLogger("jarvis.file_handler")


class FileHandler:
    """
    Обрабатывает загруженные пользователем файлы.

    Поток:
    1. Определить тип файла
    2. Спросить пользователя "что это?"
    3. Распарсить соответствующим парсером
    4. Разбить на чанки
    5. Извлечь сущности
    6. Сохранить в БД
    7. Удалить исходный файл
    """

    def __init__(self, progress_callback: Callable = None):
        """
        Args:
            progress_callback: функция(file_id, stage, percent)
                для отправки прогресса пользователю
        """
        self.progress_callback = progress_callback

    def detect_file_type(self, filename: str) -> str:
        """Определить тип файла по расширению."""
        name = filename.lower()
        ext = Path(name).suffix

        # Специальный случай: Telegram экспорт
        if name == "result.json":
            return "telegram_export"

        for file_type, extensions in SUPPORTED_EXTENSIONS.items():
            if ext in extensions:
                return file_type

        return "unknown"

    async def process(self, file_path: Path, user_description: str,
                       file_id: int, from_llm_context: AgentContext = None) -> dict:
        """
        Главный метод обработки файла.

        Returns:
            dict с результатами обработки
        """
        filename = file_path.name
        file_type = self.detect_file_type(filename)
        size = file_path.stat().st_size

        logger.info(f"Обрабатываю: {filename} ({file_type}, {size/1024:.1f}KB)")

        # Обновляем статус
        db.update_file_status(file_id, "analyzing", progress=5,
                               user_description=user_description)

        try:
            if file_type == "telegram_export":
                result = await self._process_telegram_export(file_path, file_id)

            elif file_type == "archive":
                result = await self._process_archive(file_path, file_id,
                                                       user_description)

            elif file_type == "document":
                result = await self._process_document(file_path, file_id)

            elif file_type == "spreadsheet":
                result = await self._process_spreadsheet(file_path, file_id)

            elif file_type == "text":
                result = await self._process_text(file_path, file_id)

            elif file_type == "image":
                result = await self._process_image(file_path, file_id,
                                                     from_llm_context)

            elif file_type == "audio":
                result = await self._process_audio(file_path, file_id)

            elif file_type == "code":
                result = await self._process_code(file_path, file_id)

            elif file_type == "ebook":
                result = await self._process_ebook(file_path, file_id)

            elif file_type == "database":
                result = await self._process_database(file_path, file_id)

            else:
                # Пробуем как текст
                result = await self._process_text(file_path, file_id)

            # Завершаем
            db.update_file_status(
                file_id, "done", progress=100,
                records_extracted=result.get("records", 0),
                knowledge_chunks=result.get("chunks", 0),
                entities_found=result.get("entities", 0),
                summary=result.get("summary", ""),
                raw_deleted=1
            )

            # Удаляем исходный файл
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Файл удалён: {file_path}")

            return result

        except Exception as e:
            logger.error(f"Ошибка обработки {filename}: {e}")
            db.update_file_status(file_id, "failed", error_message=str(e))
            raise

    # ── Telegram Export ───────────────────────────────────────────────────────

    async def _process_telegram_export(self, file_path: Path,
                                        file_id: int) -> dict:
        """Обработать экспорт чата из Telegram."""
        await self._notify(file_id, "Разбираю Telegram экспорт...", 10)

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        chat_name = data.get("name", "Неизвестный чат")
        chat_type = data.get("type", "unknown")
        messages = data.get("messages", [])

        total = len(messages)
        logger.info(f"Telegram экспорт: {chat_name}, {total} сообщений")

        await self._notify(file_id, f"Обрабатываю {total} сообщений...", 20)

        # Определяем участников
        participants = set()
        chunks_saved = 0
        records = 0
        dates = []

        # Обрабатываем батчами по 100
        batch_size = 100
        for batch_start in range(0, total, batch_size):
            batch = messages[batch_start:batch_start + batch_size]

            for msg in batch:
                text = self._extract_tg_message_text(msg)
                if not text or len(text.strip()) < 3:
                    continue

                sender = msg.get("from", "Unknown")
                participants.add(sender)

                date_str = msg.get("date", "")
                msg_date = None
                if date_str:
                    try:
                        msg_date = datetime.fromisoformat(date_str)
                        dates.append(msg_date)
                    except Exception:
                        pass

                # Оцениваем важность
                importance = self._score_message_importance(text, msg)

                if importance >= 3:
                    chunk_id = db.save_file_chunk(
                        file_id=file_id,
                        content=text,
                        chunk_index=records,
                        chunk_type="message",
                        sender_name=sender,
                        message_date=msg_date,
                        importance=importance
                    )
                    chunks_saved += 1

                records += 1

            # Прогресс
            progress = 20 + int((batch_start / total) * 60)
            await self._notify(
                file_id,
                f"Обработано {batch_start + len(batch)}/{total}...",
                progress
            )
            await asyncio.sleep(0)  # даём event loop дышать

        await self._notify(file_id, "Анализирую темы...", 85)

        # Обновляем участников в БД
        participants_list = list(participants)[:20]

        # Определяем период
        period_start = min(dates).isoformat() if dates else None
        period_end = max(dates).isoformat() if dates else None

        if period_start:
            db.update_file_status(
                file_id, "analyzing", progress=90,
                source_period_start=period_start,
                source_period_end=period_end,
                participants=json.dumps(participants_list)
            )

        summary = (
            f"Экспорт чата «{chat_name}» ({chat_type}). "
            f"{records} сообщений, {len(participants)} участников. "
            f"Период: {period_start[:10] if period_start else '?'} — "
            f"{period_end[:10] if period_end else '?'}"
        )

        return {
            "records": records,
            "chunks": chunks_saved,
            "entities": len(participants),
            "summary": summary,
        }

    def _extract_tg_message_text(self, msg: dict) -> str:
        """Извлечь текст из Telegram сообщения (может быть списком)."""
        text = msg.get("text", "")

        if isinstance(text, list):
            # Telegram хранит форматированный текст как список
            parts = []
            for part in text:
                if isinstance(part, str):
                    parts.append(part)
                elif isinstance(part, dict):
                    parts.append(part.get("text", ""))
            text = " ".join(parts)

        return text.strip() if text else ""

    def _score_message_importance(self, text: str, msg: dict) -> float:
        """Оценить важность сообщения (0-10)."""
        score = 3.0  # базовая

        # Длинное сообщение важнее
        if len(text) > 200:
            score += 2
        elif len(text) > 50:
            score += 1

        # Вопросы важнее
        if "?" in text:
            score += 1

        # Медиа с подписью
        if msg.get("photo") or msg.get("file"):
            score += 0.5

        # Пересланное сообщение
        if msg.get("forwarded_from"):
            score += 0.5

        # Очень короткие флуд сообщения
        if len(text) < 10:
            score -= 2

        return max(0, min(10, score))

    # ── PDF ───────────────────────────────────────────────────────────────────

    async def _process_document(self, file_path: Path, file_id: int) -> dict:
        """Обработать документ: PDF, DOCX, DOC."""
        ext = file_path.suffix.lower()
        await self._notify(file_id, f"Извлекаю текст из {ext}...", 15)

        text_chunks = []

        if ext == ".pdf":
            text_chunks = self._extract_pdf(file_path)
        elif ext in [".docx", ".doc"]:
            text_chunks = self._extract_docx(file_path)
        elif ext == ".rtf":
            text_chunks = self._extract_text_file(file_path)
        else:
            text_chunks = self._extract_text_file(file_path)

        await self._notify(file_id, "Сохраняю в базу знаний...", 70)

        chunks_saved = 0
        for i, chunk in enumerate(text_chunks):
            if len(chunk.strip()) < 20:
                continue
            db.save_file_chunk(
                file_id=file_id,
                content=chunk,
                chunk_index=i,
                chunk_type="text",
                importance=5.0
            )
            chunks_saved += 1

        summary = (
            f"Документ обработан. "
            f"Извлечено {len(text_chunks)} фрагментов текста."
        )

        return {"records": len(text_chunks), "chunks": chunks_saved,
                "entities": 0, "summary": summary}

    def _extract_pdf(self, path: Path) -> list[str]:
        """Извлечь текст из PDF по страницам."""
        try:
            import pdfplumber
            chunks = []
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        chunks.append(text)
            return chunks
        except ImportError:
            logger.warning("pdfplumber не установлен")
            return [f"[PDF файл, требуется pdfplumber]"]

    def _extract_docx(self, path: Path) -> list[str]:
        """Извлечь текст из DOCX."""
        try:
            from docx import Document
            doc = Document(path)
            chunks = []
            current_chunk = []
            for para in doc.paragraphs:
                if para.text.strip():
                    current_chunk.append(para.text)
                    # Разбиваем на чанки по ~500 символов
                    if sum(len(p) for p in current_chunk) > 500:
                        chunks.append("\n".join(current_chunk))
                        current_chunk = []
            if current_chunk:
                chunks.append("\n".join(current_chunk))
            return chunks
        except ImportError:
            logger.warning("python-docx не установлен")
            return [f"[DOCX файл, требуется python-docx]"]

    # ── CSV / Excel / JSON ────────────────────────────────────────────────────

    async def _process_spreadsheet(self, file_path: Path, file_id: int) -> dict:
        """Обработать таблицы и датасеты."""
        ext = file_path.suffix.lower()
        await self._notify(file_id, "Анализирую данные...", 20)

        try:
            import pandas as pd

            if ext == ".csv":
                df = pd.read_csv(file_path, encoding="utf-8", errors="replace")
            elif ext in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path)
            elif ext == ".json":
                df = pd.read_json(file_path)
            elif ext == ".parquet":
                df = pd.read_parquet(file_path)
            else:
                df = pd.read_csv(file_path, sep=None, engine="python",
                                  encoding="utf-8", errors="replace")

            rows, cols = df.shape
            await self._notify(file_id,
                                f"Таблица: {rows} строк, {cols} столбцов. Профилирую...",
                                50)

            # Базовая статистика
            stats = {
                "rows": rows,
                "columns": cols,
                "column_names": list(df.columns[:20]),
                "dtypes": {str(k): str(v) for k, v in df.dtypes.items()},
                "null_counts": df.isnull().sum().to_dict(),
            }

            # Числовая статистика
            numeric_stats = df.describe().to_dict() if not df.empty else {}

            # Сохраняем инсайты
            insight_text = (
                f"Датасет: {rows} строк, {cols} столбцов. "
                f"Колонки: {', '.join(list(df.columns[:10]))}. "
            )
            if numeric_stats:
                insight_text += f"Числовые данные: {list(numeric_stats.keys())[:5]}"

            db.save_file_chunk(
                file_id=file_id,
                content=insight_text,
                chunk_index=0,
                chunk_type="insight",
                importance=7.0
            )

            # Первые несколько строк для примера
            sample = df.head(5).to_string(max_cols=10)
            db.save_file_chunk(
                file_id=file_id,
                content=f"Пример данных:\n{sample}",
                chunk_index=1,
                chunk_type="sample",
                importance=5.0
            )

            summary = f"Таблица {rows}×{cols}. Колонки: {', '.join(list(df.columns[:5]))}"

            return {"records": rows, "chunks": 2, "entities": 0, "summary": summary}

        except Exception as e:
            logger.error(f"Ошибка обработки таблицы: {e}")
            return await self._process_text(file_path, file_id)

    # ── Текстовые файлы ───────────────────────────────────────────────────────

    async def _process_text(self, file_path: Path, file_id: int) -> dict:
        """Обработать текстовый файл."""
        await self._notify(file_id, "Читаю текст...", 20)

        chunks = self._extract_text_file(file_path)

        for i, chunk in enumerate(chunks):
            if len(chunk.strip()) > 20:
                db.save_file_chunk(
                    file_id=file_id,
                    content=chunk,
                    chunk_index=i,
                    chunk_type="text",
                    importance=5.0
                )

        summary = f"Текстовый файл. {len(chunks)} фрагментов."
        return {"records": len(chunks), "chunks": len(chunks),
                "entities": 0, "summary": summary}

    def _extract_text_file(self, path: Path,
                             chunk_size: int = 1000) -> list[str]:
        """Прочитать текстовый файл и разбить на чанки."""
        for encoding in ["utf-8", "cp1251", "latin-1"]:
            try:
                with open(path, encoding=encoding) as f:
                    content = f.read()

                # Разбиваем на чанки по chunk_size символов
                chunks = []
                while content:
                    chunk = content[:chunk_size]
                    # Стараемся резать по концу предложения
                    if len(content) > chunk_size:
                        last_period = max(
                            chunk.rfind("."),
                            chunk.rfind("!"),
                            chunk.rfind("?"),
                            chunk.rfind("\n")
                        )
                        if last_period > chunk_size // 2:
                            chunk = content[:last_period + 1]
                    chunks.append(chunk.strip())
                    content = content[len(chunk):]

                return [c for c in chunks if c]
            except UnicodeDecodeError:
                continue
        return [f"[Не удалось декодировать файл]"]

    # ── Изображения ───────────────────────────────────────────────────────────

    async def _process_image(self, file_path: Path, file_id: int,
                               context: AgentContext = None) -> dict:
        """Анализировать изображение через Vision."""
        await self._notify(file_id, "Анализирую изображение...", 30)

        description = f"[Изображение: {file_path.name}]"

        try:
            from core.model_router import router
            response = await router.ask_with_vision(
                "Опиши подробно что на этом изображении. Если есть текст — перепиши его полностью.",
                str(file_path)
            )
            if response.success:
                description = response.content
        except Exception as e:
            logger.debug(f"Vision анализ не удался: {e}")

        db.save_file_chunk(
            file_id=file_id,
            content=description,
            chunk_index=0,
            chunk_type="image_description",
            importance=6.0
        )

        return {"records": 1, "chunks": 1, "entities": 0,
                "summary": description[:200]}

    # ── Аудио ─────────────────────────────────────────────────────────────────

    async def _process_audio(self, file_path: Path, file_id: int) -> dict:
        """Транскрибировать аудио через faster-whisper."""
        await self._notify(file_id, "Транскрибирую аудио...", 20)

        transcript = f"[Аудио файл: {file_path.name}]"

        try:
            from faster_whisper import WhisperModel
            model = WhisperModel("tiny", device="cpu", compute_type="int8")
            segments, info = model.transcribe(str(file_path), language="ru")
            transcript = " ".join(seg.text for seg in segments)
        except ImportError:
            logger.warning("faster-whisper не установлен")
        except Exception as e:
            logger.error(f"Ошибка транскрипции: {e}")

        db.save_file_chunk(
            file_id=file_id,
            content=transcript,
            chunk_index=0,
            chunk_type="audio_transcript",
            importance=6.0
        )

        return {"records": 1, "chunks": 1, "entities": 0,
                "summary": f"Аудио транскрипция: {transcript[:200]}"}

    # ── Код ───────────────────────────────────────────────────────────────────

    async def _process_code(self, file_path: Path, file_id: int) -> dict:
        """Обработать файл с кодом."""
        chunks = self._extract_text_file(file_path)

        for i, chunk in enumerate(chunks):
            db.save_file_chunk(
                file_id=file_id,
                content=chunk,
                chunk_index=i,
                chunk_type="code",
                importance=6.0
            )

        summary = f"Файл кода {file_path.suffix}. {len(chunks)} блоков."
        return {"records": len(chunks), "chunks": len(chunks),
                "entities": 0, "summary": summary}

    # ── Epub / FB2 ────────────────────────────────────────────────────────────

    async def _process_ebook(self, file_path: Path, file_id: int) -> dict:
        """Обработать электронную книгу."""
        ext = file_path.suffix.lower()

        if ext == ".epub":
            try:
                import ebooklib
                from ebooklib import epub
                from bs4 import BeautifulSoup

                book = epub.read_epub(str(file_path))
                texts = []
                for item in book.get_items():
                    if item.get_type() == ebooklib.ITEM_DOCUMENT:
                        soup = BeautifulSoup(item.get_content(), "html.parser")
                        texts.append(soup.get_text())

                content = "\n".join(texts)
                chunks = [content[i:i+1000] for i in range(0, len(content), 1000)]

                for i, chunk in enumerate(chunks[:200]):  # максимум 200 чанков
                    if chunk.strip():
                        db.save_file_chunk(
                            file_id=file_id,
                            content=chunk,
                            chunk_index=i,
                            chunk_type="book_text",
                            importance=4.0
                        )

                return {"records": len(chunks), "chunks": min(len(chunks), 200),
                        "entities": 0, "summary": f"Книга EPUB, {len(chunks)} страниц"}
            except ImportError:
                pass

        return await self._process_text(file_path, file_id)

    # ── SQLite БД ─────────────────────────────────────────────────────────────

    async def _process_database(self, file_path: Path, file_id: int) -> dict:
        """Извлечь структуру из SQLite файла."""
        import sqlite3

        conn = sqlite3.connect(str(file_path))
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]

        description = f"SQLite база данных. Таблицы: {', '.join(tables)}.\n"

        for table in tables[:10]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                cursor.execute(f"PRAGMA table_info({table})")
                cols = [r[1] for r in cursor.fetchall()]
                description += f"\n{table}: {count} строк, колонки: {', '.join(cols[:10])}"
            except Exception:
                pass

        conn.close()

        db.save_file_chunk(
            file_id=file_id,
            content=description,
            chunk_index=0,
            chunk_type="db_schema",
            importance=7.0
        )

        return {"records": len(tables), "chunks": 1, "entities": 0,
                "summary": description[:300]}

    # ── Архивы ────────────────────────────────────────────────────────────────

    async def _process_archive(self, file_path: Path, file_id: int,
                                 user_description: str) -> dict:
        """Распаковать архив и обработать каждый файл."""
        await self._notify(file_id, "Распаковываю архив...", 10)

        extract_dir = TEMP_DIR / f"archive_{file_id}"
        extract_dir.mkdir(exist_ok=True)

        try:
            if file_path.suffix.lower() == ".zip":
                with zipfile.ZipFile(file_path, "r") as z:
                    z.extractall(extract_dir)
            else:
                import tarfile
                with tarfile.open(file_path) as t:
                    t.extractall(extract_dir)
        except Exception as e:
            return {"records": 0, "chunks": 0, "entities": 0,
                    "summary": f"Ошибка распаковки: {e}"}

        # Находим все файлы
        all_files = list(extract_dir.rglob("*"))
        total_records = 0
        total_chunks = 0

        for sub_file in all_files:
            if not sub_file.is_file():
                continue
            if sub_file.stat().st_size > 100 * 1024 * 1024:  # > 100MB пропускаем
                continue

            try:
                result = await self.process(sub_file, user_description, file_id)
                total_records += result.get("records", 0)
                total_chunks += result.get("chunks", 0)
            except Exception as e:
                logger.debug(f"Не удалось обработать {sub_file}: {e}")

        # Чистим временную папку
        import shutil
        shutil.rmtree(extract_dir, ignore_errors=True)

        return {
            "records": total_records,
            "chunks": total_chunks,
            "entities": 0,
            "summary": f"Архив с {len(all_files)} файлами. Обработано {total_chunks} чанков."
        }

    # ── Утилиты ───────────────────────────────────────────────────────────────

    async def _notify(self, file_id: int, stage: str, percent: int):
        """Обновить прогресс."""
        db.update_file_status(file_id, "analyzing", progress=percent)
        if self.progress_callback:
            try:
                await self.progress_callback(file_id, stage, percent)
            except Exception:
                pass
