"""
archive_client.py — HTTP-клиент Джарвиса для работы с Архивом знаний
Использует переменные окружения:
  ARCHIVE_API_URL  — URL Архива (например http://localhost:8765)
  ARCHIVE_API_KEY  — ключ доступа (совпадает с ARCHIVE_API_KEY в .env Архива)
"""
import os
import logging
import httpx

log = logging.getLogger("jarvis.archive_client")

def _url() -> str:
    return os.getenv("ARCHIVE_API_URL", "http://localhost:8765").rstrip("/")

def _key() -> str:
    return os.getenv("ARCHIVE_API_KEY", "ArchiveJarvisHost")

def _headers() -> dict:
    return {"X-API-Key": _key(), "Content-Type": "application/json"}


async def archive_search(query: str, limit: int = 3) -> str:
    """
    Поиск в Архиве знаний.
    Возвращает текстовые фрагменты или '' если ничего не найдено / Архив недоступен.
    """
    try:
        async with httpx.AsyncClient(
            timeout=8, verify=False, headers=_headers()
        ) as cl:
            r = await cl.post(
                f"{_url()}/search",
                json={"query": query, "limit": limit},
            )
            if r.status_code != 200:
                return ""
            results = r.json().get("results", [])
            if not results:
                return ""
            parts = []
            for res in results[:limit]:
                title   = res.get("title", "Документ")
                snippet = (res.get("snippet") or "")[:500]
                cat     = res.get("category", "")
                cat_str = f" [{cat}]" if cat else ""
                parts.append(f"[{title}{cat_str}]\n{snippet}")
            return "\n\n---\n\n".join(parts)
    except Exception as e:
        log.debug(f"archive_search error: {e}")
        return ""


async def archive_health() -> bool:
    """Проверяет доступность Архива (GET /health, без авторизации)."""
    try:
        async with httpx.AsyncClient(timeout=4, verify=False) as cl:
            r = await cl.get(f"{_url()}/health")
            return r.status_code == 200
    except Exception:
        return False


async def archive_stats() -> dict:
    """Статистика Архива (GET /stats)."""
    try:
        async with httpx.AsyncClient(
            timeout=5, verify=False, headers=_headers()
        ) as cl:
            r = await cl.get(f"{_url()}/stats")
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        log.debug(f"archive_stats error: {e}")
    return {}
