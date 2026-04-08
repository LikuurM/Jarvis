"""
JARVIS KnowledgeMinerAgent — автономный добытчик знаний.
Kaggle датасеты + arXiv + Papers With Code + Wikipedia.
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import httpx

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.knowledge_miner")


class KnowledgeMinerAgent(BaseAgent):
    """
    Агент добычи знаний.

    Каждое воскресенье (ночной цикл):
    1. Анализирует темы из истории разговоров
    2. Ищет релевантные датасеты на Kaggle
    3. Скачивает топ-3, профилирует, извлекает инсайты
    4. Удаляет сырые данные — сохраняет только инсайты
    5. Скачивает свежие статьи с arXiv
    6. Обогащает Wikipedia статьи
    """

    name = "KnowledgeMiner"
    description = "Автономный добытчик знаний из Kaggle, arXiv, Wikipedia"

    system_prompt = """Ты — аналитик данных ДЖАРВИСА.
    Извлекай ключевые инсайты из датасетов и научных статей.
    Отвечай структурированно на русском языке.
    Выдели: главные факты, тренды, практическую ценность."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from config import (KAGGLE_USERNAME, KAGGLE_KEY,
                            KAGGLE_MAX_SIZE_MB, KAGGLE_MIN_USABILITY,
                            TEMP_DIR)
        self.kaggle_user = KAGGLE_USERNAME
        self.kaggle_key = KAGGLE_KEY
        self.max_size_mb = KAGGLE_MAX_SIZE_MB
        self.min_usability = KAGGLE_MIN_USABILITY
        self.temp_dir = TEMP_DIR

    async def run(self, context: AgentContext) -> AgentResult:
        """Полный цикл добычи знаний."""
        results = {
            "kaggle": 0,
            "arxiv": 0,
            "wikipedia": 0,
        }

        # Определяем темы интересов из истории
        topics = await self._get_interest_topics(context)
        logger.info(f"Темы интересов: {topics}")

        # Kaggle (воскресенье)
        if datetime.now().weekday() == 6:  # воскресенье
            for topic in topics[:3]:
                count = await self.mine_kaggle(topic, context)
                results["kaggle"] += count

        # arXiv (ежедневно)
        for topic in topics[:2]:
            count = await self.mine_arxiv(topic)
            results["arxiv"] += count

        # Wikipedia для новых терминов
        entities = self.db.execute(
            """SELECT name FROM entities
               WHERE description IS NULL
               ORDER BY created_at DESC LIMIT 10"""
        )
        for entity in entities:
            await self.mine_wikipedia(dict(entity)["name"])
            results["wikipedia"] += 1

        summary = (
            f"Добыто знаний: Kaggle={results['kaggle']}, "
            f"arXiv={results['arxiv']}, Wikipedia={results['wikipedia']}"
        )
        return self.success(summary, data=results)

    # ── Темы интересов ────────────────────────────────────────────────────────

    async def _get_interest_topics(self, context: AgentContext) -> list[str]:
        """Определить темы интересов из истории разговоров."""
        # Берём последние 100 сообщений из эпизодической памяти
        episodes = self.db.execute(
            """SELECT summary FROM episodic_memory
               ORDER BY timestamp DESC LIMIT 50"""
        )

        if not episodes:
            return ["python", "machine learning", "telegram bots"]

        text = " ".join([dict(e)["summary"] for e in episodes])[:2000]

        response = await self.ask_llm(
            f"Из этих записей извлеки 5 главных тем интересов. "
            f"Верни JSON массив строк: [\"тема1\", \"тема2\", ...]\n\n{text}",
            context,
            task_type=TaskType.ANALYSIS,
            max_tokens=200,
            temperature=0.3
        )

        if response.success:
            try:
                content = response.content.strip()
                if "```" in content:
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                topics = json.loads(content)
                return [str(t) for t in topics[:5]]
            except Exception:
                pass

        return ["python", "machine learning", "telegram", "AI", "данные"]

    # ── Kaggle ────────────────────────────────────────────────────────────────

    async def mine_kaggle(self, topic: str,
                           context: AgentContext) -> int:
        """Скачать и проанализировать датасеты Kaggle по теме."""
        if not self.kaggle_user or not self.kaggle_key:
            logger.debug("Kaggle credentials не настроены")
            return 0

        try:
            # Настраиваем kaggle API
            os.environ["KAGGLE_USERNAME"] = self.kaggle_user
            os.environ["KAGGLE_KEY"] = self.kaggle_key

            import kaggle
            from kaggle.api.kaggle_api_extended import KaggleApiClient

            # Ищем датасеты
            datasets = kaggle.api.dataset_list(
                search=topic,
                sort_by="usability",
                max_size=self.max_size_mb * 1024 * 1024
            )

            saved = 0
            for dataset in datasets[:3]:
                usability = getattr(dataset, "usabilityRating", 0)
                if usability < self.min_usability:
                    continue

                # Проверяем не скачивали ли уже
                existing = self.db.execute_one(
                    "SELECT id FROM kaggle_datasets WHERE dataset_id=?",
                    (dataset.ref,)
                )
                if existing:
                    continue

                # Скачиваем
                count = await self._download_and_analyze_kaggle(
                    dataset, topic, context
                )
                saved += count

            return saved

        except ImportError:
            logger.debug("kaggle не установлен")
        except Exception as e:
            logger.error(f"Kaggle ошибка: {e}")
        return 0

    async def _download_and_analyze_kaggle(self, dataset, topic: str,
                                            context: AgentContext) -> int:
        """Скачать датасет, извлечь инсайты, удалить файл."""
        import kaggle
        dataset_dir = self.temp_dir / f"kaggle_{dataset.ref.replace('/', '_')}"
        dataset_dir.mkdir(exist_ok=True)

        try:
            # Скачиваем
            logger.info(f"Скачиваю датасет: {dataset.ref}")
            await asyncio.to_thread(
                kaggle.api.dataset_download_files,
                dataset.ref,
                path=str(dataset_dir),
                unzip=True
            )

            # Ищем CSV файлы
            csv_files = list(dataset_dir.glob("**/*.csv"))
            if not csv_files:
                return 0

            # Анализируем первый CSV
            insights = await self._analyze_csv(
                csv_files[0], str(dataset.title), context
            )

            if insights:
                # Сохраняем в БД
                self.db.execute_write(
                    """INSERT OR IGNORE INTO kaggle_datasets
                       (dataset_id, title, description, download_date,
                        key_insights, usability_score, topic_query, raw_deleted)
                       VALUES (?,?,?,CURRENT_TIMESTAMP,?,?,?,1)""",
                    (dataset.ref, str(dataset.title),
                     str(getattr(dataset, "subtitle", "")),
                     insights,
                     float(getattr(dataset, "usabilityRating", 0)),
                     topic)
                )

                # Сохраняем в базу знаний
                self.db.save_knowledge(
                    content=insights,
                    summary=f"Kaggle: {dataset.title}",
                    source_url=f"https://kaggle.com/datasets/{dataset.ref}",
                    source_type="kaggle",
                    category=topic,
                    confidence=0.75
                )

                logger.info(f"Датасет {dataset.ref} проанализирован")
                return 1

        except Exception as e:
            logger.error(f"Ошибка анализа датасета: {e}")
        finally:
            # ВСЕГДА удаляем сырые данные
            import shutil
            shutil.rmtree(dataset_dir, ignore_errors=True)

        return 0

    async def _analyze_csv(self, csv_path: Path, title: str,
                            context: AgentContext) -> str:
        """Проанализировать CSV файл и извлечь инсайты."""
        try:
            import pandas as pd

            # Читаем (ограничиваем 10k строк для скорости)
            df = pd.read_csv(
                csv_path,
                nrows=10000,
                encoding="utf-8",
                on_bad_lines="skip"
            )

            rows, cols = df.shape
            col_names = list(df.columns[:15])

            # Базовая статистика
            stats_text = f"Датасет '{title}': {rows} строк, {cols} столбцов.\n"
            stats_text += f"Колонки: {', '.join(col_names)}\n"

            # Числовая статистика
            numeric = df.select_dtypes(include="number")
            if not numeric.empty:
                desc = numeric.describe()
                stats_text += f"\nЧисловые колонки:\n{desc.to_string()[:500]}\n"

            # Пример данных
            sample = df.head(3).to_string(max_cols=8)
            stats_text += f"\nПример:\n{sample[:500]}"

            # LLM извлекает инсайты
            response = await self.ask_llm(
                f"Проанализируй этот датасет и извлеки:\n"
                f"1. Главные инсайты (3-5 пунктов)\n"
                f"2. Практическая ценность\n"
                f"3. Интересные тренды\n\n{stats_text}",
                context,
                task_type=TaskType.ANALYSIS,
                max_tokens=600
            )

            return response.content if response.success else stats_text[:500]

        except Exception as e:
            logger.debug(f"Ошибка анализа CSV: {e}")
            return ""

    # ── arXiv ─────────────────────────────────────────────────────────────────

    async def mine_arxiv(self, topic: str) -> int:
        """Получить свежие статьи с arXiv."""
        try:
            client = httpx.AsyncClient(timeout=10)
            query = topic.replace(" ", "+")

            resp = await client.get(
                f"https://export.arxiv.org/api/query"
                f"?search_query=all:{query}"
                f"&start=0&max_results=5"
                f"&sortBy=submittedDate&sortOrder=descending"
            )

            if resp.status_code != 200:
                return 0

            # Парсим XML ответ
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            saved = 0
            for entry in root.findall("atom:entry", ns):
                title_el = entry.find("atom:title", ns)
                summary_el = entry.find("atom:summary", ns)
                link_el = entry.find("atom:id", ns)

                if not title_el:
                    continue

                title = title_el.text.strip().replace("\n", " ")
                summary = summary_el.text.strip()[:600] if summary_el else ""
                url = link_el.text.strip() if link_el else ""

                self.db.save_knowledge(
                    content=f"{title}. {summary}",
                    summary=title,
                    source_url=url,
                    source_type="arxiv",
                    category=topic,
                    confidence=0.8
                )
                saved += 1

            await client.aclose()
            logger.info(f"arXiv: сохранено {saved} статей по теме '{topic}'")
            return saved

        except Exception as e:
            logger.debug(f"arXiv ошибка: {e}")
        return 0

    # ── Wikipedia ─────────────────────────────────────────────────────────────

    async def mine_wikipedia(self, term: str) -> bool:
        """Получить статью Wikipedia для термина/сущности."""
        try:
            client = httpx.AsyncClient(timeout=8)

            # Wikipedia API
            resp = await client.get(
                "https://ru.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "titles": term,
                    "prop": "extracts",
                    "exintro": True,
                    "explaintext": True,
                    "format": "json",
                    "redirects": 1
                }
            )

            if resp.status_code != 200:
                await client.aclose()
                return False

            data = resp.json()
            pages = data.get("query", {}).get("pages", {})

            for page_id, page in pages.items():
                if page_id == "-1":
                    break

                extract = page.get("extract", "")[:800]
                if len(extract) < 50:
                    break

                # Сохраняем в БД знаний
                self.db.save_knowledge(
                    content=extract,
                    summary=f"Wikipedia: {term}",
                    source_url=f"https://ru.wikipedia.org/wiki/{term.replace(' ', '_')}",
                    source_type="wikipedia",
                    category="encyclopedia",
                    confidence=0.85
                )

                # Обновляем описание сущности в графе
                self.db.execute_write(
                    """UPDATE entities SET description=?, updated_at=CURRENT_TIMESTAMP
                       WHERE name=? AND description IS NULL""",
                    (extract[:300], term)
                )

                await client.aclose()
                return True

            await client.aclose()

        except Exception as e:
            logger.debug(f"Wikipedia ошибка для '{term}': {e}")
        return False
