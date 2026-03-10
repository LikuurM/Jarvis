"""
JARVIS ULTIMATE 2026 — config.py  v2.0
Загружает все настройки из .env

Структура папок:
  data/json/   — JSON файлы (qa_responses, etc.)
  data/txt/    — текстовые файлы (system_prompt, phrases)
  database/    — SQLite БД и бэкапы
  sessions/    — Telegram сессии
  logs/        — логи
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BASE_DIR = Path(__file__).parent

# ── Структура папок ──────────────────────────────────────────
DIR_DATABASE = BASE_DIR / "database"
DIR_DATA     = BASE_DIR / "data"
DIR_JSON     = BASE_DIR / "data" / "json"
DIR_TXT      = BASE_DIR / "data" / "txt"
DIR_SESSIONS = BASE_DIR / "sessions"
DIR_LOGS     = BASE_DIR / "logs"
DIR_KNOWLEDGE= BASE_DIR / "knowledge"

for _d in [DIR_DATABASE, DIR_DATA, DIR_JSON, DIR_TXT,
           DIR_SESSIONS, DIR_LOGS, DIR_KNOWLEDGE]:
    _d.mkdir(parents=True, exist_ok=True)

# Обратная совместимость
DIRS = {
    "data":     DIR_DATA,
    "sessions": DIR_SESSIONS,
    "logs":     DIR_LOGS,
}
KNOWLEDGE_DIR = DIR_KNOWLEDGE

# ── Файлы ────────────────────────────────────────────────────
# TXT файлы
SYSTEM_PROMPT_FILE = DIR_TXT / "system_prompt.txt"
PHRASES_FILE       = DIR_TXT / "phrases.txt"
# Fallback на старое расположение
if not SYSTEM_PROMPT_FILE.exists() and (DIR_DATA / "system_prompt.txt").exists():
    SYSTEM_PROMPT_FILE = DIR_DATA / "system_prompt.txt"
if not PHRASES_FILE.exists() and (DIR_DATA / "phrases.txt").exists():
    PHRASES_FILE = DIR_DATA / "phrases.txt"

# JSON файлы
QA_RESPONSES_FILE = DIR_JSON / "qa_responses.json"
if not QA_RESPONSES_FILE.exists() and (DIR_DATA / "qa_responses.json").exists():
    QA_RESPONSES_FILE = DIR_DATA / "qa_responses.json"
if not QA_RESPONSES_FILE.exists() and (BASE_DIR / "qa_responses.json").exists():
    QA_RESPONSES_FILE = BASE_DIR / "qa_responses.json"

# БД — только в папке database/
DB_FILE          = DIR_DATABASE / "Jarvis.db"
DB_BACKUP_DIR    = DIR_DATABASE   # бэкапы рядом с БД

# Логи
LOG_FILE         = DIR_LOGS / "jarvis.log"

# Сессии
SESSION_FILE      = str(DIR_SESSIONS / "telegram.session")
BOT_SESSION_FILE  = str(DIR_SESSIONS / "bot.session")
USER_SESSION_FILE = str(DIR_SESSIONS / "user.session")
USER_SESSION_STRING = os.getenv("USER_SESSION_STRING", "")
CHROMA_PERSIST_DIR  = str(BASE_DIR / "chroma_db")

# ── Telegram ─────────────────────────────────────────────────
TELEGRAM_API_ID    = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH  = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE     = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

OWNER_ID         = int(os.getenv("OWNER_ID", "0"))
DEFAULT_GROUP_ID = int(os.getenv("DEFAULT_GROUP_ID", "0"))


# ── API ключи ────────────────────────────────────────────────
TAVILY_API_KEY  = os.getenv("TAVILY_API_KEY", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

# ── Groq API ─────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# ── Яндекс.Диск ──────────────────────────────────────────────
YANDEX_DISK_TOKEN  = os.getenv("YANDEX_DISK_TOKEN", "")
YANDEX_DISK_FOLDER = os.getenv("YANDEX_DISK_FOLDER", "Jarvis_Backup")

# ── Параметры ────────────────────────────────────────────────
MAX_HISTORY = 30
ACTIVATION_PREFIXES = ("джарвис,", "jarvis,")

# ── Кастомные LLM ────────────────────────────────────────────
def _load_custom_llms() -> list[dict]:
    custom = []
    for i in range(1, 6):
        raw = os.getenv(f"CUSTOM_LLM_{i}", "").strip()
        if not raw:
            continue
        parts = raw.split("|")
        if len(parts) < 4:
            logger.warning(f"CUSTOM_LLM_{i}: неверный формат (ИМЯ|URL|КЛЮЧ|МОДЕЛЬ)")
            continue
        name, url, key, model = [p.strip() for p in parts[:4]]
        if not all([name, url, model]):
            continue
        custom.append({"slot": i, "name": name, "url": url, "key": key, "model": model})
    return custom

CUSTOM_LLMS: list[dict] = _load_custom_llms()
