"""
JARVIS DeepWebAgent — многоуровневый веб-поиск.
SearXNG (self-hosted) → Tavily → DuckDuckGo → Playwright.
"""
import asyncio
import logging
import time
from typing import Optional
from urllib.parse import quote_plus

import httpx

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.web_agent")


class DeepWebAgent(BaseAgent):
    """
    Агент веб-поиска ДЖАРВИСА.

    Уровни поиска:
    1. SearXNG (self-hosted, без лимитов) — основной
    2. DuckDuckGo (без API ключа) — резервный
    3. Tavily (1000/мес бесплатно) — для сложных запросов
    4. Playwright — если нужна страница полностью
    """

    name = "DeepWebAgent"
    description = "Поиск информации в интернете"

    system_prompt = """Ты — веб-исследователь ДЖАРВИСА.
    Анализируй найденные материалы и извлекай ключевые факты.
    Отвечай на русском. Указывай источники."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from config import SEARXNG_URL, TAVILY_API_KEY
        self.searxng_url = SEARXNG_URL
        self.tavily_key = TAVILY_API_KEY
        self._http_client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(
                timeout=15.0,
                headers={"User-Agent": "Mozilla/5.0 (compatible; JarvisBot/1.0)"},
                follow_redirects=True
            )
        return self._http_client

    async def run(self, context: AgentContext) -> AgentResult:
        """Выполнить поиск по запросу."""
        query = context.original_query
        results = await self.search(query, num_results=5)

        if not results:
            return self.success("Ничего не найдено в интернете.", data=[])

        # Синтезируем ответ через LLM
        sources_text = "\n".join([
            f"[{i+1}] {r.get('title','')}: {r.get('snippet','')}"
            for i, r in enumerate(results[:5])
        ])

        response = await self.ask_llm(
            f"На основе этих результатов поиска ответь на вопрос: {query}\n\n{sources_text}",
            context,
            task_type=TaskType.RESEARCH,
            max_tokens=800
        )

        # Сохраняем найденное в базу знаний
        for r in results:
            if r.get("snippet") and len(r["snippet"]) > 50:
                self.db.save_knowledge(
                    content=r["snippet"],
                    summary=r.get("title"),
                    source_url=r.get("url"),
                    source_type="web",
                    confidence=0.6
                )

        return self.success(
            response.content if response.success else sources_text,
            data=results,
            tokens=response.tokens_used,
            model=response.model
        )

    async def search(self, query: str, num_results: int = 5) -> list[dict]:
        """Поиск через доступные провайдеры."""
        # Пробуем SearXNG
        results = await self._search_searxng(query, num_results)
        if results:
            return results

        # Резерв: DuckDuckGo
        results = await self._search_ddg(query, num_results)
        if results:
            return results

        # Резерв: Tavily
        if self.tavily_key:
            results = await self._search_tavily(query, num_results)

        return results

    async def _search_searxng(self, query: str, num: int) -> list[dict]:
        """Поиск через self-hosted SearXNG."""
        try:
            client = self._get_client()
            resp = await client.get(
                f"{self.searxng_url}/search",
                params={
                    "q": query,
                    "format": "json",
                    "language": "ru",
                    "engines": "google,bing,duckduckgo,wikipedia",
                    "num_results": num
                }
            )
            if resp.status_code == 200:
                data = resp.json()
                return [
                    {
                        "title": r.get("title", ""),
                        "url": r.get("url", ""),
                        "snippet": r.get("content", ""),
                        "source": "searxng"
                    }
                    for r in data.get("results", [])[:num]
                ]
        except Exception as e:
            logger.debug(f"SearXNG недоступен: {e}")
        return []

    async def _search_ddg(self, query: str, num: int) -> list[dict]:
        """Поиск через DuckDuckGo (без API)."""
        try:
            from duckduckgo_search import DDGS
            with DDGS() as ddgs:
                results = list(ddgs.text(
                    query,
                    region="ru-ru",
                    max_results=num
                ))
            return [
                {
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "snippet": r.get("body", ""),
                    "source": "duckduckgo"
                }
                for r in results
            ]
        except Exception as e:
            logger.debug(f"DDG поиск не удался: {e}")
        return []

    async def _search_tavily(self, query: str, num: int) -> list[dict]:
        """Поиск через Tavily (лучший для LLM)."""
        try:
            client = self._get_client()
            resp = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": self.tavily_key,
                    "query": query,
                    "max_results": num,
                    "include_answer": True,
                    "search_depth": "basic"
                }
            )
            if resp.status_code == 200:
                data = resp.json()
                return [
                    {
                        "title": r.get("title", ""),
                        "url": r.get("url", ""),
                        "snippet": r.get("content", ""),
                        "source": "tavily"
                    }
                    for r in data.get("results", [])[:num]
                ]
        except Exception as e:
            logger.debug(f"Tavily не удался: {e}")
        return []

    async def fetch_page(self, url: str) -> Optional[str]:
        """
        Скачать и очистить страницу.
        trafilatura убирает рекламу/навигацию → чистый текст.
        """
        try:
            import trafilatura
            client = self._get_client()
            resp = await client.get(url, timeout=10)
            if resp.status_code == 200:
                text = trafilatura.extract(
                    resp.text,
                    include_comments=False,
                    include_tables=True,
                    no_fallback=False
                )
                return text
        except Exception as e:
            logger.debug(f"Не удалось скачать {url}: {e}")
        return None

    async def deep_research(self, topic: str, context: AgentContext,
                             iterations: int = 3) -> str:
        """
        Глубокое исследование темы.
        Итеративный поиск: результат 1 → новые запросы → результат 2...
        """
        all_facts = []
        queries = [topic]

        for i in range(iterations):
            query = queries[i] if i < len(queries) else topic
            results = await self.search(query, num_results=3)

            for r in results:
                if r.get("snippet"):
                    all_facts.append(r["snippet"])

            # Генерируем следующие запросы на основе найденного
            if i < iterations - 1 and all_facts:
                next_query_response = await self.ask_llm(
                    f"Тема: {topic}\nНайдено: {all_facts[-1][:300]}\n"
                    f"Напиши один уточняющий поисковый запрос (только запрос, без пояснений):",
                    context,
                    task_type=TaskType.SIMPLE,
                    max_tokens=50
                )
                if next_query_response.success:
                    queries.append(next_query_response.content.strip())

        # Синтезируем финальный ответ
        combined = "\n".join(all_facts[:10])
        synthesis = await self.ask_llm(
            f"Тема: {topic}\n\nДанные:\n{combined}\n\n"
            f"Напиши подробный аналитический ответ на русском языке.",
            context,
            task_type=TaskType.ANALYSIS,
            max_tokens=1500
        )

        return synthesis.content if synthesis.success else combined
