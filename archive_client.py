"""
archive_client.py — HTTP-клиент Джарвиса для работы с Архивом знаний.
Поддерживает двуязычный поиск: запрос переводится через Groq и ищется на обоих языках.
"""
import os
import logging
import asyncio
import concurrent.futures
import httpx

log = logging.getLogger("jarvis.archive_client")


def _url() -> str:
    return os.getenv("ARCHIVE_API_URL", "http://172.19.0.105:8765").rstrip("/")

def _key() -> str:
    return os.getenv("ARCHIVE_API_KEY", "ArchiveJarvisHost")

def _headers() -> dict:
    return {"X-API-Key": _key(), "Content-Type": "application/json"}

def _groq_key() -> str:
    return os.getenv("GROQ_API_KEY", "")


# ─────────────────────────────────────────────────────────────────
# Определение языка и перевод
# ─────────────────────────────────────────────────────────────────

def _is_latin(text: str) -> bool:
    latin = sum(1 for c in text if c.isascii() and c.isalpha())
    total = sum(1 for c in text if c.isalpha())
    return total > 0 and latin / total > 0.6


async def _translate_query(query: str) -> str | None:
    """
    Переводит поисковый запрос через Groq.
    Русский -> Английский, Английский -> Русский.
    """
    gk = _groq_key()
    if not gk:
        return None
    try:
        target = "Russian" if _is_latin(query) else "English"

        def _call():
            from groq import Groq
            client = Groq(api_key=gk)
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{
                    "role": "user",
                    "content": (
                        f"Translate this search query to {target}. "
                        f"Return ONLY the translated text, no quotes, no explanation:\n{query}"
                    )
                }],
                max_completion_tokens=60,
                temperature=0,
            )
            return (resp.choices[0].message.content or "").strip()

        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            translated = await loop.run_in_executor(ex, _call)

        if not translated or translated.lower() == query.lower():
            return None
        log.debug(f"📖 Архив перевод: «{query}» → «{translated}»")
        return translated

    except Exception as e:
        log.debug(f"translate_query: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
# Поиск
# ─────────────────────────────────────────────────────────────────

async def _search_one(cl: httpx.AsyncClient, query: str, limit: int) -> list:
    try:
        r = await cl.post(
            f"{_url()}/search",
            json={"query": query, "limit": limit},
        )
        if r.status_code == 200:
            return r.json().get("results", [])
    except Exception as e:
        log.debug(f"_search_one: {e}")
    return []


async def archive_search(query: str, limit: int = 3) -> str:
    """
    Двуязычный поиск в Архиве:
    1. Ищет оригинальным запросом
    2. Переводит запрос через Groq
    3. Ищет переведённым запросом
    4. Объединяет результаты без дублей
    """
    try:
        async with httpx.AsyncClient(
            timeout=10, verify=False, headers=_headers()
        ) as cl:
            # Запускаем поиск и перевод параллельно
            results_orig, translated = await asyncio.gather(
                _search_one(cl, query, limit),
                _translate_query(query),
                return_exceptions=True,
            )

            if isinstance(results_orig, Exception):
                results_orig = []
            if isinstance(translated, Exception):
                translated = None

            results_trans = []
            if translated:
                results_trans = await _search_one(cl, translated, limit)

            # Дедупликация по (id, page_num)
            seen, merged = set(), []
            for res in list(results_orig) + list(results_trans):
                key = (res.get("id"), res.get("page_num"))
                if key not in seen:
                    seen.add(key)
                    merged.append(res)

            if not merged:
                return ""

            parts = []
            for res in merged[:limit * 2]:
                title   = res.get("title", "Документ")
                snippet = (res.get("snippet") or "")[:500]
                cat     = res.get("category", "")
                lang    = res.get("language", "")
                meta    = " | ".join(filter(None, [cat, lang]))
                meta_s  = f" [{meta}]" if meta else ""
                parts.append(f"[{title}{meta_s}]\n{snippet}")

            return "\n\n---\n\n".join(parts)

    except Exception as e:
        log.debug(f"archive_search: {e}")
        return ""


async def archive_health() -> bool:
    try:
        async with httpx.AsyncClient(timeout=4, verify=False) as cl:
            r = await cl.get(f"{_url()}/health")
            return r.status_code == 200
    except Exception:
        return False


async def archive_stats() -> dict:
    try:
        async with httpx.AsyncClient(
            timeout=5, verify=False, headers=_headers()
        ) as cl:
            r = await cl.get(f"{_url()}/stats")
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        log.debug(f"archive_stats: {e}")
    return {}
