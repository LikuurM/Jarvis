"""
JARVIS SocialRadarAgent — мониторинг всех соцсетей.
VK + Twitter/Nitter + Reddit + YouTube + GitHub + RSS.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType

logger = logging.getLogger("jarvis.social_radar")

# Зеркала Nitter для X/Twitter (ротация при недоступности)
NITTER_MIRRORS = [
    "https://nitter.net",
    "https://nitter.cz",
    "https://nitter.it",
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
]


class SocialRadarAgent(BaseAgent):
    """
    Агент мониторинга социальных сетей.

    Сканирует:
    - VK API (личные + группы + лента)
    - X/Twitter через Nitter (без API ключа)
    - Reddit через PRAW
    - YouTube RSS фиды
    - GitHub Trending
    - RSS агрегатор (Habr, новости)
    """

    name = "SocialRadarAgent"
    description = "Мониторинг соцсетей и новостей"

    system_prompt = """Ты — аналитик соцсетей ДЖАРВИСА.
    Извлекай важные события, тренды, упоминания.
    Кратко и по существу на русском языке."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._nitter_index = 0
        self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=10.0,
                headers={"User-Agent": "Mozilla/5.0"},
                follow_redirects=True
            )
        return self._client

    async def run(self, context: AgentContext) -> AgentResult:
        """Полный скан всех источников."""
        results = {}

        # Параллельный скан
        tasks = [
            ("vk", self.scan_vk()),
            ("twitter", self.scan_twitter()),
            ("reddit", self.scan_reddit()),
            ("github", self.scan_github_trending()),
            ("rss", self.scan_rss()),
        ]

        for name, coro in tasks:
            try:
                data = await asyncio.wait_for(coro, timeout=15)
                results[name] = data
            except asyncio.TimeoutError:
                logger.debug(f"{name}: таймаут")
                results[name] = []
            except Exception as e:
                logger.debug(f"{name}: ошибка {e}")
                results[name] = []

        # Сохраняем найденное в БД
        total_saved = 0
        for source, items in results.items():
            for item in items:
                if item.get("content") and len(item["content"]) > 30:
                    self.db.save_knowledge(
                        content=item["content"],
                        summary=item.get("title"),
                        source_url=item.get("url"),
                        source_type=source,
                        category=item.get("category", "social"),
                        confidence=0.6
                    )
                    total_saved += 1

        summary = f"Сканирование завершено. Сохранено: {total_saved} записей."
        return self.success(summary, data=results)

    # ── VK ────────────────────────────────────────────────────────────────────

    async def scan_vk(self) -> list[dict]:
        """Сканировать VK через VK API."""
        try:
            import vk_api
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent.parent))

            import os
            vk_token = os.getenv("VK_TOKEN", "")
            if not vk_token:
                return []

            vk_session = vk_api.VkApi(token=vk_token)
            vk = vk_session.get_api()

            # Получаем новостную ленту
            news = vk.newsfeed.get(count=20, filters="post")
            items = []

            for item in news.get("items", []):
                text = item.get("text", "")
                if len(text) < 20:
                    continue

                items.append({
                    "title": f"VK пост",
                    "content": text[:500],
                    "url": f"https://vk.com/wall{item.get('source_id')}_{item.get('post_id')}",
                    "category": "vk_feed",
                    "timestamp": item.get("date")
                })

            logger.info(f"VK: найдено {len(items)} постов")
            return items[:10]

        except ImportError:
            logger.debug("vk-api не установлен")
        except Exception as e:
            logger.debug(f"VK ошибка: {e}")
        return []

    # ── Twitter/X через Nitter ────────────────────────────────────────────────

    async def scan_twitter(self, accounts: list[str] = None,
                            keywords: list[str] = None) -> list[dict]:
        """Сканировать Twitter через Nitter (без API)."""
        try:
            import feedparser

            # Загружаем интересные аккаунты из предпочтений
            prefs = self.db.execute(
                "SELECT value FROM preferences WHERE category='twitter' AND key='accounts'"
            )
            if prefs and not accounts:
                import json
                accounts = json.loads(dict(prefs[0])["value"]) if prefs else []

            accounts = accounts or []  # если нет настроек
            items = []

            for account in accounts[:10]:
                mirror = self._get_nitter_mirror()
                rss_url = f"{mirror}/{account}/rss"

                try:
                    client = self._get_client()
                    resp = await client.get(rss_url, timeout=8)
                    if resp.status_code == 200:
                        feed = feedparser.parse(resp.text)
                        for entry in feed.entries[:5]:
                            items.append({
                                "title": f"@{account}",
                                "content": entry.get("summary", "")[:400],
                                "url": entry.get("link", ""),
                                "category": "twitter",
                            })
                except Exception:
                    self._rotate_nitter()
                    continue

                await asyncio.sleep(0.5)  # вежливость

            return items

        except ImportError:
            logger.debug("feedparser не установлен")
        return []

    def _get_nitter_mirror(self) -> str:
        return NITTER_MIRRORS[self._nitter_index % len(NITTER_MIRRORS)]

    def _rotate_nitter(self):
        self._nitter_index += 1

    # ── Reddit ────────────────────────────────────────────────────────────────

    async def scan_reddit(self, subreddits: list[str] = None) -> list[dict]:
        """Сканировать Reddit через PRAW."""
        try:
            import praw
            import sys, os
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent.parent))

            client_id = os.getenv("REDDIT_CLIENT_ID", "")
            client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")

            if not client_id:
                # Используем публичный JSON API без авторизации
                return await self._scan_reddit_public(subreddits)

            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent="JarvisBot/1.0"
            )

            # Загружаем subreddits из настроек
            if not subreddits:
                subreddits = ["Python", "MachineLearning", "artificial",
                               "programming", "selfhosted"]

            items = []
            for sub_name in subreddits[:5]:
                sub = reddit.subreddit(sub_name)
                for post in sub.hot(limit=5):
                    if post.score < 10:
                        continue
                    items.append({
                        "title": post.title,
                        "content": f"{post.title}. {post.selftext[:300] if post.selftext else ''}",
                        "url": f"https://reddit.com{post.permalink}",
                        "category": "reddit",
                    })

            return items

        except ImportError:
            return await self._scan_reddit_public(subreddits)
        except Exception as e:
            logger.debug(f"Reddit ошибка: {e}")
        return []

    async def _scan_reddit_public(self, subreddits: list[str] = None) -> list[dict]:
        """Reddit без API ключа через JSON."""
        subreddits = subreddits or ["Python", "MachineLearning"]
        items = []
        client = self._get_client()

        for sub in subreddits[:3]:
            try:
                resp = await client.get(
                    f"https://www.reddit.com/r/{sub}/hot.json?limit=5",
                    headers={"User-Agent": "JarvisBot/1.0"}
                )
                if resp.status_code == 200:
                    posts = resp.json().get("data", {}).get("children", [])
                    for p in posts:
                        d = p.get("data", {})
                        items.append({
                            "title": d.get("title", ""),
                            "content": d.get("title", "") + " " + d.get("selftext", "")[:200],
                            "url": f"https://reddit.com{d.get('permalink', '')}",
                            "category": "reddit",
                        })
            except Exception:
                pass
            await asyncio.sleep(1)

        return items

    # ── GitHub Trending ───────────────────────────────────────────────────────

    async def scan_github_trending(self, language: str = "python") -> list[dict]:
        """Скан GitHub Trending через scraping."""
        try:
            from bs4 import BeautifulSoup
            client = self._get_client()

            url = f"https://github.com/trending/{language}?since=daily"
            resp = await client.get(url)

            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")
            repos = soup.select("article.Box-row")

            items = []
            for repo in repos[:10]:
                name_tag = repo.select_one("h2 a")
                desc_tag = repo.select_one("p")
                stars_tag = repo.select_one("a[href*='stargazers']")

                if not name_tag:
                    continue

                repo_path = name_tag.get("href", "").strip("/")
                desc = desc_tag.text.strip() if desc_tag else ""
                stars = stars_tag.text.strip() if stars_tag else "?"

                items.append({
                    "title": repo_path,
                    "content": f"GitHub Trending: {repo_path}. {desc} ⭐{stars}",
                    "url": f"https://github.com/{repo_path}",
                    "category": "github_trending",
                })

            logger.info(f"GitHub: найдено {len(items)} trending репозиториев")
            return items

        except Exception as e:
            logger.debug(f"GitHub trending ошибка: {e}")
        return []

    # ── RSS фиды ─────────────────────────────────────────────────────────────

    async def scan_rss(self, feeds: list[str] = None) -> list[dict]:
        """Сканировать RSS фиды."""
        try:
            import feedparser

            default_feeds = [
                "https://habr.com/ru/rss/hubs/artificial_intelligence/articles/",
                "https://habr.com/ru/rss/hubs/python/articles/",
                "https://news.ycombinator.com/rss",
                "https://www.reddit.com/r/MachineLearning/.rss",
            ]

            feeds = feeds or default_feeds
            items = []
            client = self._get_client()

            for feed_url in feeds[:5]:
                try:
                    resp = await client.get(feed_url, timeout=8)
                    if resp.status_code == 200:
                        feed = feedparser.parse(resp.text)
                        for entry in feed.entries[:3]:
                            title = entry.get("title", "")
                            summary = entry.get("summary", "")[:400]
                            link = entry.get("link", "")

                            items.append({
                                "title": title,
                                "content": f"{title}. {summary}",
                                "url": link,
                                "category": "rss",
                            })
                except Exception:
                    continue
                await asyncio.sleep(0.3)

            return items

        except ImportError:
            logger.debug("feedparser не установлен")
        return []

    # ── YouTube ───────────────────────────────────────────────────────────────

    async def scan_youtube_channels(self, channel_ids: list[str] = None) -> list[dict]:
        """Новые видео с YouTube каналов через RSS (без API)."""
        try:
            import feedparser
            channel_ids = channel_ids or []
            items = []
            client = self._get_client()

            for ch_id in channel_ids[:5]:
                rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={ch_id}"
                try:
                    resp = await client.get(rss_url, timeout=8)
                    if resp.status_code == 200:
                        feed = feedparser.parse(resp.text)
                        for entry in feed.entries[:3]:
                            items.append({
                                "title": entry.get("title", ""),
                                "content": f"YouTube: {entry.get('title','')}",
                                "url": entry.get("link", ""),
                                "category": "youtube",
                            })
                except Exception:
                    continue

            return items
        except ImportError:
            pass
        return []
