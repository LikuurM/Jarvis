"""
JARVIS ULTIMATE 2026 — config.py
Загружает все настройки из .env
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BASE_DIR = Path(__file__).parent

# ── Папки проекта ─────────────────────────────────────────────
DIRS = {
    "prompts":      BASE_DIR / "prompts",
    "phrases":      BASE_DIR / "phrases" / "iron_man",
    "knowledge":    BASE_DIR / "knowledge",
    "sessions":     BASE_DIR / "sessions",
    "logs":         BASE_DIR / "logs",
    "chroma":       BASE_DIR / "chroma_db",
    "chat_history": BASE_DIR / "chat_history",
}

for d in DIRS.values():
    d.mkdir(parents=True, exist_ok=True)

# ── Файлы ────────────────────────────────────────────────────
SYSTEM_PROMPT_FILE = DIRS["prompts"] / "system_prompt.txt"
QA_RESPONSES_FILE  = BASE_DIR / "qa_responses.json"
LOG_FILE           = DIRS["logs"] / "jarvis.log"
SESSION_FILE       = str(DIRS["sessions"] / "telegram.session")   # legacy
BOT_SESSION_FILE   = str(DIRS["sessions"] / "bot.session")         # бот-режим
USER_SESSION_FILE  = str(DIRS["sessions"] / "user.session")        # юзер-режим
CHROMA_PERSIST_DIR = str(DIRS["chroma"])

# ── Telegram ─────────────────────────────────────────────────
TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE    = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8216466403:AAHG92R2yfE-XxWC3w0YfEmPtn7nC4w_HzU")

# Владелец бота — только он может отправлять файлы в группы
# Узнать свой ID: напиши @userinfobot в Telegram
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

# ID группы куда отправлять файлы по умолчанию (если не указана другая)
# Узнать ID группы: добавь @userinfobot в группу, он напишет ID
DEFAULT_GROUP_ID = int(os.getenv("DEFAULT_GROUP_ID", "0"))

# ── OpenRouter (основная нейросеть) ──────────────────────────
OPENROUTER_API_KEY  = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_API_BASE = os.getenv("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL    = os.getenv("OPENROUTER_MODEL", "arcee-ai/trinity-large-preview:free")

# ── Дополнительные LLM (по умолчанию ОТКЛЮЧЕНЫ — используется только OpenRouter)
# Раскомментируй LLM_MODELS в .env только если хочешь добавить ещё нейросети

LLM_API_KEYS: dict = {}   # доп. ключи не нужны — всё через OpenRouter
LLM_MODELS: list[str] = []  # стандартные модели отключены
DEFAULT_LLM: str = "openrouter"  # всегда OpenRouter

# ── Поиск ────────────────────────────────────────────────────
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# ── GitHub ────────────────────────────────────────────────────
GITHUB_REPO_PATH = os.getenv("GITHUB_REPO_PATH", str(BASE_DIR))
GITHUB_REMOTE    = os.getenv("GITHUB_REMOTE", "origin")
GITHUB_BRANCH    = os.getenv("GITHUB_BRANCH", "main")

# ── Память разговора ─────────────────────────────────────────
MAX_HISTORY = 30  # последних 30 пар сообщений на пользователя

# ── Активационные префиксы ───────────────────────────────────
ACTIVATION_PREFIXES = ("джарвис,", "jarvis,")

# (apply_api_keys_to_env удалён — все запросы идут через OpenRouter)

# ── Кастомные нейросети из .env (CUSTOM_LLM_1..5) ────────────
def _load_custom_llms() -> list[dict]:
    """Формат: ИМЯ|URL|API_КЛЮЧ|МОДЕЛЬ"""
    custom = []
    for i in range(1, 6):
        raw = os.getenv(f"CUSTOM_LLM_{i}", "").strip()
        if not raw:
            continue
        parts = raw.split("|")
        if len(parts) < 4:
            logger.warning(f"CUSTOM_LLM_{i}: неверный формат (нужно ИМЯ|URL|КЛЮЧ|МОДЕЛЬ)")
            continue
        name, url, key, model = [p.strip() for p in parts[:4]]
        if not all([name, url, model]):
            continue
        custom.append({"slot": i, "name": name, "url": url, "key": key, "model": model})
        logger.info(f"Custom LLM [{i}]: {name} → {url}")
    return custom

CUSTOM_LLMS: list[dict] = _load_custom_llms()
