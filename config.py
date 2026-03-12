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

# ── Google Drive (бэкапы через Service Account) ──────────────
GOOGLE_DRIVE_FOLDER    = os.getenv("GOOGLE_DRIVE_FOLDER", "Jarvis_Backup")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "1X3uVPe79HdDWhmyP0vcT-U1ziXS0UEFP")

# Service Account JSON — вшит напрямую, не требует .env
# Файл: nimble-factor-454016-q3-e260a622ff3a.json
_SA_JSON_PATH = BASE_DIR / "nimble-factor-454016-q3-e260a622ff3a.json"

def _load_service_account() -> dict:
    # Сначала ищем JSON файл рядом с кодом
    if _SA_JSON_PATH.exists():
        import json as _j
        return _j.loads(_SA_JSON_PATH.read_text())
    # Fallback — вшитые данные
    return {
        "type": "service_account",
        "project_id": "nimble-factor-454016-q3",
        "private_key_id": "e260a622ff3aa8974ed8a966decbf17fb90deea0",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC6XeYe47uG8clO\nB6pWO7hq7duFub98lwOG78pI5c2BFx7waedmKlV9bvpIW9akAZ8O4weQG4s6Jf3P\n2YPZAh/N9T4PgypTKudny8u9TY3J99jyRSSNUNm63hh1/dr19WxKyW9nW1z0nNYQ\nzNDXHNQv7P7BD2M0UuhWjXIKBpYoyBFd3W773ugQ/9+dlnA4qiRdMlFS3KYb6I5g\nTS0i2hQNUNiXMa0oHwObjGRKyA+F4mzBHayAKc31HAyQ4Sa6zxdzf3hQl1d/i9Ko\nFD8jnZGVN0zfpDlDlHg+Z2pj878uwbu3c1Cuje1pRiIs/56l6vJCjM5LclNljkye\nuktO2O2xAgMBAAECggEAAKxUfPYvu5upMW3nxRq/V0hsxi1+FS+DkT9f7hDsNZs3\nKY97CP5FJx2Oq/087vYqwoZIoYsDe3OQrdshOUoN13dwsnJsyRoCEk4TMyfK1gql\nMpNF6pT65wVIPTnBkakXHVMm1JXMglLLXnwOBNY6bqWpjpMTfNMQD+P5A8GzVXwJ\nfXB7onoU5ypMpnpfQfTXKxPxDE7xUF2ZwOo74iafCqL0HhATVedXCrSK2pDKP1sf\nBc0pT1Tif2Fj2fuKKnPeNLKGLIQVfWt1ADd8gMuGWbCaLmJZQW8GNdLMaTk5B2vT\nwVmOqGuu2KS6Z3C7YnbpboaSrcMKakTgQi5xTOHl2QKBgQDsnw4OQQvBApNMF/Vd\nkKleKVJ2AgDsvTP4+0qUs/7QsOpBN3oe9j5gpaHFoOQCcpuvPmaLCaNbq6DQrMsX\nti9fyK0IjL9n3+gsYsVxTPrJji/MmeVbBZuSg4nv9nkzzcBHeZpDAjsms3dGprp8\nX4AnnsJCyq7/NCPqOxmcI0XUbQKBgQDJoThRNmpStSq/gDKhtiLDxHzjow64QWs3\npztJ2mTwLAqx6VTViDPYkBw98ETcjZ7jcdBskgLvr+s2UjW9juQY3aQiVsKZ8iBH\n4LAiyZGAXEYQPQBxu6gsuo4o4CV/sBsoWYHPJ2suLM5D8PHfUF7EhOez7193EJj5\n9+1tCAiL1QKBgFB/7DA9QNpxHduNaxRh2r0GId+z3syrsbrLAxyD//TPu2JklU9j\nRuVqdBbgHbIXe4+rEwfKR6EwSo2zu4RdntBfXT7DY3rbWgl/sNxS7B8WGHzq/nRw\n+/Rke9D/cUyGexvV6v1RDP6ln7aRknAtrVPNVxmSCucXvgA6CwRrm54hAoGBALxS\nC0K3G0lSkrG4MBIgBopoi+klU8s+tsCNPm/1Pk+gIwEWmLiz9RCxUN98+SQyVhPD\nKtMs8PcjjQH4eN8qhdq/sNytwiZ9Ii9gKcLkFzUXeg0SnMadai8Us1B0QjHnrwXZ\ny8dK3u2KxcBpW8+ixlCwfaTuz3BqnSbjrOsoFKtdAoGAMVdTqSY/qxClavpk3tLQ\nZR/tyugBFFrzjZYq4p4UnQ2NKj+FdFpKXpDmZ98fYN2CxnhmecZBSd3F8zje53jl\n2tboM0XEp/eDdflryrXMmYRM6r19mfnnFfBXlid7GegurX0yfmvavWtv3Z/kMZx9\nUoRQtg0+zAH5fw3E6tXraj0=\n-----END PRIVATE KEY-----\n",
        "client_email": "jarvis@nimble-factor-454016-q3.iam.gserviceaccount.com",
        "client_id": "101602965823632304530",
        "token_uri": "https://oauth2.googleapis.com/token",
    }

GOOGLE_SERVICE_ACCOUNT: dict = _load_service_account()

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
