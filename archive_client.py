"""
archive_client.py — Telegram Bridge версия.
Джарвис общается с Архивом через общую Telegram группу.

Настройка:
  ARCHIVE_BRIDGE_CHAT  — ID группы-моста (например -1001234567890)
  ARCHIVE_BOT_ID       — ID Archive бота (узнать у @userinfobot)

Протокол:
  Джарвис отправляет:  REQ:abc123 SEARCH:запрос LIMIT:3
  Архив отвечает:      RESP:abc123 [{"title":...,"snippet":...}]
  
  Джарвис отправляет:  REQ:abc123 PING
  Архив отвечает:      RESP:abc123 OK
  
  Джарвис отправляет:  REQ:abc123 STATS
  Архив отвечает:      RESP:abc123 {"docs":5,"pages":120,...}
"""
import asyncio
import json
import uuid
import os
import logging

log = logging.getLogger("jarvis.archive_bridge")

# ─────────────────────────────────────────────────────────────────
# Конфиг
# ─────────────────────────────────────────────────────────────────

def _bridge_chat() -> int:
    """ID группы-моста."""
    return int(os.getenv("ARCHIVE_BRIDGE_CHAT", "0"))

def _archive_bot_id() -> int:
    """ID Archive бота — чтобы Джарвис слушал только его ответы."""
    return int(os.getenv("ARCHIVE_BOT_ID", "0"))

TIMEOUT = 12  # секунд ждать ответ

# ─────────────────────────────────────────────────────────────────
# Состояние: ожидающие запросы
# ─────────────────────────────────────────────────────────────────

_pending: dict[str, asyncio.Future] = {}
_client = None   # Telethon клиент (регистрируется через register_client)


def register_client(client):
    """Регистрирует Telethon клиент Джарвиса для отправки сообщений."""
    global _client
    _client = client


def handle_incoming(sender_id: int, text: str) -> bool:
    """
    Вызывается из обработчика сообщений Джарвиса.
    Если сообщение — ответ от Archive бота, разрешает ожидающий Future.
    Возвращает True если сообщение было обработано.
    """
    archive_id = _archive_bot_id()
    # Принимаем от Archive бота или от любого если ARCHIVE_BOT_ID не задан
    if archive_id and sender_id != archive_id:
        return False

    if not text or not text.startswith("RESP:"):
        return False

    try:
        # Формат: RESP:reqid данные
        rest    = text[5:]   # убираем "RESP:"
        sp      = rest.find(" ")
        req_id  = rest[:sp] if sp != -1 else rest
        payload = rest[sp+1:] if sp != -1 else ""

        fut = _pending.get(req_id)
        if fut and not fut.done():
            fut.set_result(payload)
            log.debug(f"📬 Archive ответил на {req_id}")
            return True
    except Exception as e:
        log.debug(f"handle_incoming: {e}")

    return False


# ─────────────────────────────────────────────────────────────────
# Отправка запроса и ожидание ответа
# ─────────────────────────────────────────────────────────────────

async def _ask(command: str) -> str:
    """Отправляет команду в группу-мост, ждёт ответа."""
    chat = _bridge_chat()
    if not chat or _client is None:
        return ""

    req_id = uuid.uuid4().hex[:10]
    loop   = asyncio.get_event_loop()
    fut    = loop.create_future()
    _pending[req_id] = fut

    try:
        await _client.send_message(chat, f"REQ:{req_id} {command}")
        result = await asyncio.wait_for(fut, timeout=TIMEOUT)
        return result
    except asyncio.TimeoutError:
        log.debug(f"⏱ Archive timeout ({command[:30]})")
        return ""
    except Exception as e:
        log.debug(f"_ask error: {e}")
        return ""
    finally:
        _pending.pop(req_id, None)


# ─────────────────────────────────────────────────────────────────
# Публичный API (такой же как HTTP версия)
# ─────────────────────────────────────────────────────────────────

async def archive_search(query: str, limit: int = 3) -> str:
    """Поиск в архиве через Telegram мост."""
    raw = await _ask(f"SEARCH:{query} LIMIT:{limit}")
    if not raw:
        return ""
    try:
        items = json.loads(raw)
        if not items:
            return ""
        parts = []
        for r in items[:limit * 2]:
            title   = r.get("title", "Документ")
            snippet = (r.get("snippet") or "")[:400]
            cat     = r.get("category", "")
            meta    = f" [{cat}]" if cat else ""
            parts.append(f"[{title}{meta}]\n{snippet}")
        return "\n\n---\n\n".join(parts)
    except Exception as e:
        log.debug(f"archive_search parse: {e}")
        return ""


async def archive_health() -> bool:
    """Проверяет доступность Archive бота."""
    result = await _ask("PING")
    return result.strip() == "OK"


async def archive_stats() -> dict:
    """Статистика архива."""
    raw = await _ask("STATS")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}
