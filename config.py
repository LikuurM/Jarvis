"""
JARVIS Configuration — все настройки в одном месте.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─── Пути ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = BASE_DIR / "uploads"
TEMP_DIR = UPLOADS_DIR / "temp"
LOGS_DIR = BASE_DIR / "logs"
BACKUPS_DIR = BASE_DIR / "backups"

for d in [DATA_DIR, UPLOADS_DIR, TEMP_DIR, LOGS_DIR, BACKUPS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ─── Telegram ─────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "0"))
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE", "")

# ─── LLM ключи (все бесплатные уровни) ───────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")

# ─── Модели ───────────────────────────────────────────────────────────────────
GROQ_MODELS = {
    "fast": "llama-3.1-8b-instant",
    "smart": "llama-3.3-70b-versatile",
    "vision": "llama-4-scout-17b-16e-instruct",
    "critic": "mixtral-8x7b-32768",
}
CEREBRAS_MODELS = {
    "fast": "llama3.1-8b",
    "smart": "llama-3.3-70b",
}
GEMINI_MODELS = {
    "flash": "gemini-2.0-flash",
    "long": "gemini-1.5-flash",
}

# Роутинг: тип задачи → (провайдер, модель)
MODEL_ROUTING = {
    "greeting": ("groq", "fast"),
    "simple": ("groq", "fast"),
    "analysis": ("groq", "smart"),
    "research": ("groq", "smart"),
    "criticism": ("groq", "critic"),
    "vision": ("groq", "vision"),
    "long_context": ("gemini", "long"),
    "code": ("groq", "smart"),
    "planning": ("gemini", "flash"),
}

# ─── База данных ──────────────────────────────────────────────────────────────
DB_PATH = DATA_DIR / "jarvis.db"
CHROMA_PATH = DATA_DIR / "chroma"

# ─── Embeddings (локально, бесплатно) ────────────────────────────────────────
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# ─── Поиск ────────────────────────────────────────────────────────────────────
SEARXNG_URL = os.getenv("SEARXNG_URL", "http://localhost:8080")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# ─── Kaggle ───────────────────────────────────────────────────────────────────
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME", "")
KAGGLE_KEY = os.getenv("KAGGLE_KEY", "")
KAGGLE_MAX_SIZE_MB = 500
KAGGLE_MIN_USABILITY = 7.0

# ─── API (iPhone) ────────────────────────────────────────────────────────────
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN", "change_this_secret")

# ─── TTS голос ДЖАРВИСА ───────────────────────────────────────────────────────
TTS_VOICE_EN = "en-GB-RyanNeural"
TTS_VOICE_RU = "ru-RU-DmitryNeural"

# ─── Поведение системы ────────────────────────────────────────────────────────
IMPORTANCE_FULL_SAVE = 4       # > 4 → полное сохранение + embedding
IMPORTANCE_STAT_ONLY = 1       # 1-4 → только статистика
AUTOREPLY_MIN_TRUST = 5.0
AUTOREPLY_MIN_CONFIDENCE = 0.85
FRESH_START_THRESHOLD = 15     # сообщений до "свежего старта"
NIGHT_CYCLE_HOUR = 3

# Сколько дней хранить разные типы памяти
MEMORY_DAYS = {
    "episodic_full": 7,
    "episodic_compressed": 30,
    "semantic": 3650,
    "web_cache": 7,
}

# ─── Файлы ────────────────────────────────────────────────────────────────────
SUPPORTED_EXTENSIONS = {
    "text": [".txt", ".md", ".rst", ".log"],
    "document": [".pdf", ".docx", ".doc", ".rtf", ".odt"],
    "spreadsheet": [".csv", ".xlsx", ".xls", ".parquet", ".json", ".xml", ".yaml"],
    "archive": [".zip", ".tar", ".gz", ".rar", ".7z"],
    "code": [".py", ".js", ".ts", ".go", ".rs", ".java", ".cpp", ".html", ".css"],
    "image": [".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"],
    "audio": [".mp3", ".wav", ".ogg", ".m4a", ".opus"],
    "video": [".mp4", ".avi", ".mov", ".mkv"],
    "ebook": [".epub", ".fb2"],
    "presentation": [".pptx", ".ppt"],
    "database": [".db", ".sqlite", ".sqlite3"],
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
