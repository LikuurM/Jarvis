"""
╔══════════════════════════════════════════════════════════════════╗
║           JARVIS ULTIMATE 2026 — main.py  v4.0                   ║
║                                                                  ║
║  ИСПРАВЛЕНИЯ v4.0:                                               ║
║  ✅ Chroma — новый импорт (langchain_chroma)                      ║
║  ✅ Telegram бот — раздельные сессии bot/user, без конфликтов    ║
║  ✅ Мгновенный ответ «Слушаю, Сэр» при получении сообщения      ║
║  ✅ Умный подбор фраз Джарвиса по категориям и контексту         ║
║  ✅ Обработчик всех сообщений бота (не только incoming=True)     ║
╚══════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import os
import re
import sys
import random
from datetime import datetime
from pathlib import Path


# ── UI ────────────────────────────────────────────────────────

# ── Логирование ───────────────────────────────────────────────
from loguru import logger

# ── Конфиг ────────────────────────────────────────────────────
import config
import db as _db
_jarvis_db = _db.get_db()

# ── Настройка loguru ──────────────────────────────────────────
logger.remove()
logger.add(
    str(config.LOG_FILE),
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    rotation="5 MB",
    retention="7 days",
    compression="zip",
    enqueue=True,
)
logger.add(sys.stderr, level="ERROR", format="<red>{level}</red> | {message}")


# ── Глобальное подавление лишних логов ───────────────────────
import logging as _log
import os as _os_env
_os_env.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
_os_env.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
for _noisy in [
    "LiteLLM", "litellm", "litellm.utils", "litellm.main",
    "huggingface_hub", "sentence_transformers", "transformers",
    "httpx", "httpcore", "urllib3", "asyncio",
]:
    _log.getLogger(_noisy).setLevel(_log.ERROR)

# Подавляем warning от DDGS про impersonate/safari
import logging as _ddgs_log
import warnings as _warnings
_ddgs_log.getLogger("ddgs").setLevel(_ddgs_log.ERROR)
_ddgs_log.getLogger("duckduckgo_search").setLevel(_ddgs_log.ERROR)
_warnings.filterwarnings("ignore", message=".*impersonate.*")
_warnings.filterwarnings("ignore", message=".*safari.*")
_warnings.filterwarnings("ignore", message=".*does not exist.*")

# ── LiteLLM ───────────────────────────────────────────────────
import litellm
litellm.suppress_debug_info = True
litellm.set_verbose = False
litellm.verbose = False

# ── Telegram ──────────────────────────────────────────────────
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeFilename
from telethon.errors import SessionPasswordNeededError

# ── Файловые парсеры ──────────────────────────────────────────
import PyPDF2
from docx import Document as DocxDocument

# ── HTTP клиент ───────────────────────────────────────────────
import httpx

# ── Поиск ─────────────────────────────────────────────────────
from ddgs import DDGS  # pip install ddgs

# ── Playwright ────────────────────────────────────────────────
from playwright.async_api import async_playwright

# ── RAG / Chroma — полностью опциональны (не нужны на bothost) ──
try:
    from langchain_chroma import Chroma
    _CHROMA_AVAILABLE = True
except ImportError:
    try:
        from langchain_community.vectorstores import Chroma
        _CHROMA_AVAILABLE = True
    except ImportError:
        Chroma = None
        _CHROMA_AVAILABLE = False

try:
    from langchain_huggingface import HuggingFaceEmbeddings
    _EMBEDDINGS_AVAILABLE = True
except ImportError:
    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings
        _EMBEDDINGS_AVAILABLE = True
    except ImportError:
        HuggingFaceEmbeddings = None
        _EMBEDDINGS_AVAILABLE = False

try:
    from langchain_core.documents import Document
except ImportError:
    Document = None






# ═══════════════════════════════════════════════════════════════════
#  УМНЫЙ ПОДБОР ФРАЗб ПО КАТЕГОРИЯМ
# ═══════════════════════════════════════════════════════════════════

class PhraseBank:
    """
    Загружает фразы из файлов phrases/iron_man/*.txt
    Формат строки: КАТЕГОРИЯ: текст фразы
    Подбирает подходящую фразу по контексту запроса и ответа.
    """

    # Ключевые слова для определения категории по контексту
    CATEGORY_KEYWORDS = {
        "ПРИВЕТСТВИЕ": ["привет", "здравствуй", "добрый", "хай", "hello", "hi", "начнём"],
        "ГОТОВНОСТЬ":  ["сделай", "выполни", "запусти", "помоги", "можешь", "пожалуйста"],
        "ИРОНИЯ":      ["почему", "зачем", "опять", "снова", "нет", "ошибка", "плохо"],
        "УСПЕХ":       ["готово", "выполнено", "успешно", "отлично", "спасибо", "молодец"],
        "ОШИБКА":      ["ошибка", "не работает", "сломалось", "проблема", "не могу", "упало"],
        "ОЖИДАНИЕ":    ["подожди", "жду", "скоро", "обрабатываю", "ищу", "считаю"],
        "УМНЫЙ":       ["почему", "как", "зачем", "смысл", "теория", "объясни", "расскажи"],
        "ОПАСНОСТЬ":   ["опасно", "критично", "срочно", "важно", "внимание", "риск"],
        "ОДОБРЕНИЕ":   ["согласен", "да", "правильно", "верно", "именно", "точно"],
        "ОТКАЗ":       ["нельзя", "запрещено", "не буду", "отказ", "нет возможности"],
        "АНАЛИЗ":      ["анализ", "сравни", "разбери", "оцени", "проверь", "исследуй"],
        "ПРОЩАНИЕ":    ["пока", "до свидания", "выключись", "стоп", "выход", "bye"],
    }

    def __init__(self):
        self.phrases: dict[str, list[str]] = {}  # категория → список фраз
        self._load()

    def _load(self):
        phrases_file = config.PHRASES_FILE
        total = 0
        if not phrases_file.exists():
            logger.warning(f"Phrases file not found: {phrases_file}")
            return
        try:
            for line in phrases_file.read_text("utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    category, _, text = line.partition(":")
                    cat = category.strip().upper()
                    txt = text.strip()
                    if cat and txt:
                        self.phrases.setdefault(cat, []).append(txt)
                        total += 1
                else:
                    self.phrases.setdefault("GENERAL", []).append(line)
                    total += 1
        except Exception as e:
            logger.warning(f"Phrases load: {e}")
        logger.info(f"Phrases loaded: {total} across {len(self.phrases)} categories")

    def get(self, context: str = "", category: str = "", chance: float = 0.20) -> str:
        """
        Вернуть фразу по контексту или категории.
        chance — вероятность добавить фразу (0.0–1.0)
        """
        if random.random() > chance:
            return ""

        if not self.phrases:
            return ""

        # Если категория задана явно — берём из неё
        if category and category.upper() in self.phrases:
            return "\n\n" + random.choice(self.phrases[category.upper()])

        # Иначе подбираем по ключевым словам контекста
        ctx_lower = context.lower()
        best_cat  = ""
        best_score = 0

        for cat, keywords in self.CATEGORY_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in ctx_lower)
            if score > best_score and cat in self.phrases:
                best_score = score
                best_cat   = cat

        # Берём из подходящей категории или рандомной
        pool: list[str] = []
        if best_cat and best_score > 0:
            pool = self.phrases[best_cat]
        else:
            # Случайная категория из доступных
            all_phrases = [p for phrases in self.phrases.values() for p in phrases]
            pool = all_phrases

        if not pool:
            return ""

        return "\n\n" + random.choice(pool)

    @property
    def total(self) -> int:
        return sum(len(v) for v in self.phrases.values())


# ═══════════════════════════════════════════════════════════════════
#  ИСТОРИЯ ПЕРЕПИСОК
# ═══════════════════════════════════════════════════════════════════

class ChatHistory:
    """История чатов — SQLite backend."""

    HISTORY_TRIGGERS = [
        "что я спрашивал", "о чём мы говорили", "найди в переписке",
        "вспомни", "что было раньше", "история чата", "статистика чата",
        "что мы обсуждали", "прошлые разговоры", "найди в истории",
        "сколько сообщений", "наша переписка",
    ]

    def __init__(self):
        self._db = _jarvis_db

    def save_message(self, sid: int, role: str, text: str, username: str = ""):
        self._db.save_message(sid, role, text, username)

    def get_recent(self, sid: int, n: int = 30) -> list[dict]:
        return self._db.get_recent(sid, n)

    def search(self, sid: int, query: str, limit: int = 20) -> list[dict]:
        return self._db.search_messages(sid, query, limit)

    def format_for_llm(self, msgs: list[dict]) -> str:
        lines = []
        for m in msgs:
            role = "Пользователь" if m["role"] == "user" else "Джарвис"
            lines.append(f"[{m.get('ts','?')}] {role}: {m['text']}")
        return "\n".join(lines)

    def stats(self, sid: int) -> dict:
        return self._db.message_stats(sid)

    @classmethod
    def is_history_request(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.HISTORY_TRIGGERS)

    async def answer_history_question(self, query: str, sid: int, llm) -> str:
        q_lower = query.lower()
        if any(p in q_lower for p in ["статистика", "сколько сообщений"]):
            s = self.stats(sid)
            if s["total"] == 0:
                return "Сэр, история переписки пуста — мы только начинаем."
            return (
                f"Сэр, статистика нашей переписки:\n"
                f"• Всего: {s['total']} сообщений\n"
                f"• Ваших: {s['user_msgs']}\n"
                f"• Моих: {s['bot_msgs']}\n"
                f"• Первое: {s['first_date']}\n"
                f"• Последнее: {s['last_date']}"
            )
        kw_match = re.search(r"найди[:\s]+(.+)|найди в (истории|переписке)[:\s]*(.+)", q_lower)
        if kw_match:
            keyword = (kw_match.group(1) or kw_match.group(3) or "").strip()
            if keyword:
                found = self.search(sid, keyword)
                if not found:
                    return f"Сэр, по запросу «{keyword}» ничего не нашёл."
                return f"Сэр, нашёл в истории:\n\n```\n{self.format_for_llm(found[-10:])}\n```"
        recent = self.get_recent(sid, 30)
        if not recent:
            return "Сэр, история переписки пуста."
        ctx = self.format_for_llm(recent)
        msgs = [
            {"role": "system", "content": "Ты Джарвис. Отвечай на вопросы об истории переписки."},
            {"role": "user",   "content": f"История:\n\n{ctx}\n\nВопрос: {query}"},
        ]
        return await llm.complete(msgs, max_tokens=800)


# ═══════════════════════════════════════════════════════════════════
#  UNIVERSAL LLM CONNECTOR
# ═══════════════════════════════════════════════════════════════════

class UniversalLLMConnector:
    """OpenRouter + LiteLLM + любые кастомные API"""

    def __init__(self):
        self.registry: dict[str, dict] = {}

        # 1. OpenRouter
        if config.OPENROUTER_API_KEY:
            # litellm требует формат "openrouter/{model}" и env var OPENROUTER_API_KEY
            import os as _os
            _os.environ["OPENROUTER_API_KEY"] = config.OPENROUTER_API_KEY
            spec = {
                "type":     "openrouter",
                "model":    f"openrouter/{config.OPENROUTER_MODEL}",
                "api_key":  config.OPENROUTER_API_KEY,
                "display":  f"OpenRouter/{config.OPENROUTER_MODEL}",
            }
            self.registry["openrouter"] = spec
            self.registry[config.OPENROUTER_MODEL.lower()] = spec
            self.registry[config.OPENROUTER_MODEL.split("/")[-1].lower()] = spec
            logger.info(f"OpenRouter: {config.OPENROUTER_MODEL}")

        # 2. Стандартные модели — регистрируем только если явно заданы в .env
        # (по умолчанию LLM_MODELS пустой, всё идёт через OpenRouter)
        for model in config.LLM_MODELS:
            short = model.split("/")[-1].lower()
            spec  = {"type": "standard", "model": model, "display": model}
            self.registry[short]         = spec
            self.registry[model.lower()] = spec
            logger.info(f"Стандартная модель: {model}")

        # 3. Кастомные из .env
        for slot in config.CUSTOM_LLMS:
            key  = slot["name"].lower()
            spec = {
                "type":     "custom",
                "model":    f"openai/{slot['model']}",
                "base_url": slot["url"],
                "api_key":  slot["key"] or "no-key",
                "display":  f"custom/{slot['name']}",
            }
            self.registry[key] = spec
            logger.info(f"Custom LLM: {slot['name']} → {slot['url']}")

        # Активная модель
        if config.DEFAULT_LLM == "openrouter" and config.OPENROUTER_API_KEY:
            self._current_key = "openrouter"
        else:
            self._current_key = self._resolve_key(config.DEFAULT_LLM)

    @property
    def current_display(self) -> str:
        return self.registry.get(self._current_key, {}).get("display", self._current_key)

    @property
    def current_spec(self) -> dict:
        return self.registry.get(self._current_key, {})

    def _resolve_key(self, name: str) -> str:
        nl = name.lower().strip()
        if nl in self.registry:
            return nl
        for k in self.registry:
            if nl in k or k in nl:
                return k
        return next(iter(self.registry), nl)

    def list_models(self) -> str:
        lines = []
        if config.OPENROUTER_API_KEY:
            is_cur = self._current_key == "openrouter"
            m = "▶ [активна]" if is_cur else " "
            lines += [
                "**⚡ OpenRouter (основная):**",
                f"  {m} `{config.OPENROUTER_MODEL}`",
                f"     URL: `{config.OPENROUTER_API_BASE}`",
                "",
            ]
        if config.LLM_MODELS:
            lines.append("**📋 Стандартные:**")
            for model in config.LLM_MODELS:
                short  = model.split("/")[-1].lower()
                is_cur = short == self._current_key or model.lower() == self._current_key
                m = "▶ [активна]" if is_cur else " "
                lines.append(f"  {m} `{model}`")
        if config.CUSTOM_LLMS:
            lines.append("\n**🔌 Кастомные:**")
            for slot in config.CUSTOM_LLMS:
                is_cur = slot["name"].lower() == self._current_key
                m = "▶ [активна]" if is_cur else " "
                lines.append(f"  {m} `{slot['name']}` → `{slot['url']}`")
        return "\n".join(lines)

    def add_custom(self, name: str, url: str, key: str, model_name: str) -> str:
        self.registry[name.lower()] = {
            "type":     "custom",
            "model":    f"openai/{model_name}",
            "base_url": url,
            "api_key":  key or "no-key",
            "display":  f"custom/{name}",
        }
        logger.info(f"Custom LLM добавлена: {name}")
        return (
            f"Сэр, модель **{name}** добавлена.\n"
            f"• URL: `{url}`\n• Модель: `{model_name}`\n\n"
            f"Активируйте: «Джарвис, используй {name}»"
        )

    def switch(self, query: str) -> str | None:
        q = query.lower().strip()
        if any(p in q for p in ["текущая модель", "какая модель", "что используешь", "current model"]):
            return f"Сэр, активна: **{self.current_display}**\n\n" + self.list_models()
        for pat in [r"используй\s+(.+)", r"переключ\w+\s+на\s+(.+)",
                    r"switch\s+to\s+(.+)", r"активируй\s+(.+)"]:
            m = re.search(pat, q)
            if m:
                target = m.group(1).strip().rstrip("!.,: ")
                key    = self._resolve_key(target)
                if key in self.registry:
                    self._current_key = key
                    return f"Сэр, переключился на **{self.registry[key]['display']}**."
                return f"Сэр, «{target}» не найдена.\n\n" + self.list_models()
        return None

    async def complete(self, messages: list[dict], max_tokens: int = 1400) -> str:
        spec = self.current_spec
        if not spec:
            return "Сэр, нейросеть не настроена — проверьте .env"
        try:
            t = spec["type"]
            if t == "openrouter":
                # litellm нативно поддерживает OpenRouter через "openrouter/{model}"
                resp = await litellm.acompletion(
                    model=spec["model"],
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=0.7,
                    api_key=spec["api_key"],
                )
            elif t == "custom":
                # Любой OpenAI-совместимый API с кастомным base_url
                resp = await litellm.acompletion(
                    model=spec["model"],
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=0.7,
                    api_base=spec["base_url"],
                    api_key=spec["api_key"],
                )
            else:
                # Стандартные модели LiteLLM (gpt-4o, claude, grok...)
                resp = await litellm.acompletion(
                    model=spec["model"],
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=0.7,
                )
            return resp.choices[0].message.content
        except Exception as e:
            err_str = str(e)
            logger.error(f"LLM [{self._current_key}]: {e}")
            # Понятное сообщение при ошибке авторизации
            if "401" in err_str or "User not found" in err_str or "AuthenticationError" in err_str:
                return (
                    "Сэр, API ключ недействителен (ошибка 401).\n\n"
                    "Что сделать:\n"
                    "1. Перейди на https://openrouter.ai/keys\n"
                    "2. Создай новый ключ\n"
                    "3. Вставь его в файл .env в строку OPENROUTER_API_KEY=sk-or-..."
                )
            return f"Сэр, [{self.current_display}] не отвечает: {e}"


# ═══════════════════════════════════════════════════════════════════
#  АНАЛИЗАТОР СПОРОВ
# ═══════════════════════════════════════════════════════════════════

class DisputeAnalyzer:

    TRIGGERS = [
        "разбери переписку", "кто прав", "рассуди нас", "разбери спор",
        "рассуди спор", "разбор переписки", "кто виноват", "кто не прав",
        "рассуди конфликт", "analyze dispute",
    ]

    SYSTEM = """Ты — JARVIS. Анализируешь конфликты как судья: холодно, по фактам, без сочувствия.

СТРОГИЙ ФОРМАТ ОТВЕТА:

👤 [Имя/ник 1] — [одна фраза: позиция + главный аргумент]
👤 [Имя/ник 2] — [одна фраза: позиция + главный аргумент]

📋 [3-4 предложения по тексту переписки. Цитируй конкретные фразы. Указывай слабые места: манипуляции, логические ошибки, уход от темы, противоречия. Только то что написано — ничего не додумывай.]

━━━━━━━━━━━━━━━━━━━━
✅ ПРАВ: [ИМЯ/НИК]
[одно предложение — конкретная причина]
━━━━━━━━━━━━━━━━━━━━

Если оба неправы → ✅ ПРАВ: ОБА НЕПРАВЫ + одно предложение.
Если данных недостаточно → скажи об этом прямо.
Без рекомендаций. Без советов. Только вердикт."""

    @classmethod
    def is_triggered(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.TRIGGERS)

    @classmethod
    def strip_trigger(cls, q: str) -> str:
        text = q.strip()
        for t in cls.TRIGGERS:
            text = re.sub(re.escape(t) + r"[\s:]*", "", text, flags=re.IGNORECASE, count=1).strip()
        return text

    @staticmethod
    def _has_conflict(text: str) -> bool:
        """Проверяет что в тексте есть признаки конфликта/спора."""
        conflict_words = [
            "виноват", "виновата", "не прав", "не права", "ошибся", "ошиблась",
            "обвиня", "претензи", "твоя вина", "моя вина", "не сделал", "не выполнил",
            "неправ", "врёт", "врешь", "лжёт", "обманул", "нарушил", "срыв",
            "ты должен", "ты должна", "всё равно", "но ты", "а ты", "зато ты",
            "не так", "неверно", "неправильно", "плохо сделал", "не согласен", "не согласна",
            "это неправда", "это ложь",
        ]
        tl = text.lower()
        return any(w in tl for w in conflict_words)

    async def analyze(self, conversation: str, llm: UniversalLLMConnector) -> str:
        if not conversation.strip():
            return self._help()
        if not self._has_conflict(conversation):
            return "Сэр, в этой переписке не обнаружено конфликта. Здесь пока никто ни с кем не спорит."
        return await llm.complete([
            {"role": "system", "content": self.SYSTEM},
            {"role": "user",   "content": f"Разбери переписку:\n\n{conversation}"},
        ], max_tokens=2500)

    async def analyze_forwarded(self, msgs: list[dict], llm: UniversalLLMConnector) -> str:
        if not msgs:
            return "Сэр, вы ещё не пересылали сообщений. Перешлите их сначала."
        lines = [f"[{m.get('date','')}] {m.get('sender','?')}: {m.get('text','')}" for m in msgs]
        return await self.analyze("\n".join(lines), llm)

    @staticmethod
    def _help() -> str:
        return (
            "Сэр, вставьте текст переписки после команды.\n\n"
            "**Способ 1:**\n```\nДжарвис, разбери переписку:\n"
            "Иван: ты виноват!\nМаша: нет, ты!\n```\n\n"
            "**Способ 2:** перешлите сообщения из спора, затем напишите «Джарвис, разбери переписку»."
        )


# ═══════════════════════════════════════════════════════════════════
#  GROUP LOGGER — сохраняет ВСЕ сообщения из групп, включая удалённые
# ═══════════════════════════════════════════════════════════════════

class GroupLogger:
    """Логи группы и удалённые сообщения — SQLite backend."""

    DELETED_TRIGGERS = [
        "покажи удалённые", "покажи удаленные", "что удалили",
        "удалённые сообщения", "удаленные сообщения",
        "что стёрли", "show deleted", "удалённые за", "удаленные за",
    ]

    def __init__(self):
        self._db = _jarvis_db

    def save(self, chat_id: int, msg_id: int, sender: str,
             sender_id: int, text: str, date: str):
        self._db.save_group_msg(chat_id, msg_id, sender, sender_id, text, date)

    def mark_deleted(self, chat_id: int, msg_ids: list[int]):
        self._db.mark_deleted(chat_id, msg_ids)

    def mark_deleted_all_chats(self, msg_ids: list[int]):
        self._db.mark_deleted_all_chats(msg_ids)

    def get_deleted(self, chat_id: int, limit: int = 20, date_filter: str = "") -> list[dict]:
        return self._db.get_deleted(chat_id, limit, date_filter)

    def format_deleted(self, msgs: list[dict]) -> str:
        if not msgs:
            return "Сэр, удалённых сообщений не найдено."
        lines = [f"🗑 Удалённые сообщения ({len(msgs)} шт.):"]
        for m in msgs:
            lines.append(f"  [{m.get('date','')}] {m.get('sender','?')}: {m.get('text','')}")
        return "\n".join(lines)

    @classmethod
    def is_deleted_request(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.DELETED_TRIGGERS)


class UserProfileManager:
    """Профили пользователей — SQLite backend."""

    SAVE_TRIGGERS   = [
        "запомни —", "запомни -", "запомни: ", "запомни, что", "запомни что",
        "запомни ", "запомни,",
    ]
    VIEW_TRIGGERS   = ["что ты знаешь обо мне", "мой профиль", "что помнишь обо мне", "мои данные"]
    CLEAR_TRIGGERS  = ["забудь всё что знаешь обо мне", "забудь всё обо мне",
                       "удали мой профиль", "очисти мой профиль"]
    STYLE_SHORT     = ["отвечай мне короче", "отвечай короче", "давай покороче",
                       "отвечай кратко", "пиши кратко", "коротко"]
    STYLE_LONG      = ["отвечай подробнее", "отвечай развёрнуто", "давай подробнее",
                       "пиши подробно", "развёрнуто", "подробнее"]
    STYLE_IRONIC    = ["отвечай с иронией", "можно с иронией", "добавь иронию", "с юмором"]
    STYLE_NEUTRAL   = ["отвечай нейтрально", "убери иронию", "стандартный стиль"]

    def __init__(self):
        self._db = _jarvis_db

    def load(self, uid: int) -> dict:
        return self._db.load_profile(uid)

    def save_profile(self, uid: int, profile: dict):
        self._db.save_profile(uid, profile)

    def add_fact(self, uid: int, fact: str) -> str:
        p = self.load(uid)
        p.setdefault("facts", [])
        structured = self._parse_fact(fact)
        # Заменяем если уже есть факт с такой же меткой
        label = structured.split(":")[0] if ":" in structured else None
        if label:
            p["facts"] = [f for f in p["facts"] if not f.startswith(label + ":")]
        if structured not in p["facts"]:
            p["facts"].append(structured)
        self.save_profile(uid, p)
        return f"Принято к сведению, Сэр. Записал: {structured}"

    def _parse_fact(self, text: str) -> str:
        """Превращает 'меня зовут Максим' → 'Имя: Максим'"""
        import re as _re
        t = text.strip().rstrip(".")

        patterns = [
            (r"меня зовут (.+)",            "Имя"),
            (r"моё имя (.+)",               "Имя"),
            (r"мое имя (.+)",               "Имя"),
            (r"я (.+) лет",                 "Возраст"),
            (r"мне (.+) лет",               "Возраст"),
            (r"мне (.+) год",               "Возраст"),
            (r"я живу в (.+)",              "Город"),
            (r"я из города (.+)",           "Город"),
            (r"я из (.+)",                  "Город"),
            (r"мой город (.+)",             "Город"),
            (r"я работаю (.+)",             "Работа"),
            (r"моя профессия (.+)",         "Профессия"),
            (r"я (?:по профессии\s)?(.+)",  "Профессия"),
            (r"мне нравится (.+)",          "Интерес"),
            (r"я люблю (.+)",               "Интерес"),
            (r"моё хобби (.+)",             "Хобби"),
            (r"мое хобби (.+)",             "Хобби"),
            (r"мой номер (.+)",             "Телефон"),
            (r"мой email (.+)",             "Email"),
        ]

        tl = t.lower()
        for pattern, label in patterns:
            m = _re.search(pattern, tl)
            if m:
                value = t[m.start(1):m.end(1)].strip()
                value = value[0].upper() + value[1:] if value else value
                return f"{label}: {value}"

        # Не распознано — сохраняем как есть, но с заглавной буквы
        return t[0].upper() + t[1:] if t else t

    def get_summary(self, uid: int) -> str:
        p = self.load(uid)
        facts = p.get("facts", [])
        style = p.get("style", "normal")
        if not facts:
            return "Досье пустое, Сэр. Расскажите о себе — запомню."
        style_map = {"short": "краткий", "long": "подробный",
                     "ironic": "ироничный", "normal": "стандартный"}
        lines = ["Сэр, вот что я знаю о вас:"]
        for f in facts:
            lines.append(f"  • {f}")
        lines.append(f"Стиль общения: {style_map.get(style, 'стандартный')}")
        return "\n".join(lines)

    def clear(self, uid: int) -> str:
        self._db.delete_profile(uid)
        return "Досье очищено, Сэр. Начинаем с чистого листа."

    def set_style(self, uid: int, style: str) -> str:
        p = self.load(uid)
        p["style"] = style
        self.save_profile(uid, p)
        labels = {"short": "краткий", "long": "подробный",
                  "ironic": "ироничный", "normal": "стандартный"}
        return f"Принято, Сэр. Стиль общения: {labels.get(style, style)}."

    def get_style(self, uid: int) -> str:
        return self.load(uid).get("style", "normal")

    def get_facts_str(self, uid: int) -> str:
        facts = self.load(uid).get("facts", [])
        if not facts:
            return ""
        return "Факты о пользователе: " + "; ".join(facts)

    @classmethod
    def is_save(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.SAVE_TRIGGERS)

    @classmethod
    def is_view(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.VIEW_TRIGGERS)

    @classmethod
    def is_clear(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.CLEAR_TRIGGERS)

    @classmethod
    def is_style_short(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.STYLE_SHORT)

    @classmethod
    def is_style_long(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.STYLE_LONG)

    @classmethod
    def is_style_ironic(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.STYLE_IRONIC)

    @classmethod
    def is_style_neutral(cls, q: str) -> bool:
        ql = q.lower()
        return any(t in ql for t in cls.STYLE_NEUTRAL)


class ReminderManager:
    """Напоминания — SQLite backend. Проверка каждые 10 секунд."""

    REMIND_TRIGGERS = [
        "напомни мне", "напомни", "поставь будильник",
        "поставь напоминание", "remind me", "set reminder",
        "создай напоминание",
    ]
    LIST_TRIGGERS = [
        "мои напоминания", "список напоминаний", "покажи напоминания",
        "что запланировано", "my reminders",
    ]
    DEL_TRIGGERS = [
        "удали напоминание", "отмени напоминание", "убери напоминание",
        "delete reminder",
    ]

    def __init__(self):
        self._db = _jarvis_db

    def _parse_time(self, text: str):
        from datetime import datetime, timedelta, timezone
        MSK = timezone(timedelta(hours=3))
        now = datetime.now(MSK).replace(tzinfo=None)
        tl  = text.lower()

        m = re.search(r"через\s+(\d+)\s*(минут|час|день|дн)", tl)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            if "мин" in unit:   return now + timedelta(minutes=n)
            if "час" in unit:   return now + timedelta(hours=n)
            if "д" in unit:     return now + timedelta(days=n)

        m = re.search(r"завтра\s+в\s+(\d{1,2})(?::(\d{2}))?", tl)
        if m:
            h, mn = int(m.group(1)), int(m.group(2) or 0)
            return (now + timedelta(days=1)).replace(hour=h, minute=mn, second=0, microsecond=0)

        m = re.search(r"\bв\s+(\d{1,2}):(\d{2})", tl)
        if m:
            h, mn = int(m.group(1)), int(m.group(2))
            t = now.replace(hour=h, minute=mn, second=0, microsecond=0)
            if t <= now:
                t += timedelta(days=1)
            return t

        m = re.search(r"в\s+(\d{1,2})\s*(утра|вечера|ночи|дня)", tl)
        if m:
            h = int(m.group(1))
            if m.group(2) in ("вечера", "ночи") and h < 12:
                h += 12
            t = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if t <= now:
                t += timedelta(days=1)
            return t

        return None

    def _parse_text(self, query: str) -> str:
        clean = re.sub(
            r"(через\s+\d+\s*\w+|завтра|сегодня|в\s+\d+[:\d]*\s*(утра|вечера|ночи|дня)?|напомни\s*(мне)?|поставь\s*(будильник|напоминание))",
            "", query, flags=re.IGNORECASE
        ).strip(" ,.")
        return clean or "напоминание"

    def add(self, uid: int, query: str) -> str:
        fire_at = self._parse_time(query)
        if not fire_at:
            return ("Сэр, не понял когда напомнить. Примеры:\n"
                    "• напомни через 2 часа позвонить Ивану\n"
                    "• напомни завтра в 9 утра сделать отчёт\n"
                    "• напомни в 18:30 встреча")
        text = self._parse_text(query)
        rid  = self._db.add_reminder(uid, text, fire_at.isoformat())
        return f"⏰ Напоминание #{rid} установлено, Сэр. Напомню: {fire_at.strftime('%d.%m %H:%M')} — {text}"

    def list_for(self, uid: int) -> str:
        rows = self._db.get_reminders(uid)
        if not rows:
            return "Сэр, активных напоминаний нет."
        lines = [f"Ваши напоминания ({len(rows)} шт.):"]
        for r in rows:
            t = r["fire_at"][:16].replace("T", " ")
            lines.append(f"  #{r['id']} [{t}] {r['text']}")
        return "\n".join(lines)

    def delete(self, uid: int, query: str) -> str:
        m = re.search(r"(\d+)", query)
        if not m:
            return "Сэр, укажите номер напоминания. Например: «удали напоминание 3»"
        rid = int(m.group(1))
        if self._db.delete_reminder(uid, rid):
            return f"Напоминание #{rid} удалено, Сэр."
        return f"Сэр, напоминание #{rid} не найдено."

    def get_due(self) -> list[dict]:
        return self._db.get_due_reminders()

    def mark_done(self, rid: int):
        self._db.mark_reminder_done(rid)

    @classmethod
    def is_add(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.REMIND_TRIGGERS)

    @classmethod
    def is_list(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.LIST_TRIGGERS)

    @classmethod
    def is_delete(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.DEL_TRIGGERS)


class JarvisAgent:

    # Триггеры мгновенного «Слушаю, Сэр» — до начала обработки
    INSTANT_TRIGGERS = [
        "джарвис", "jarvis",
        "привет", "hello", "hi", "слушай",
        "что скажешь", "ты здесь", "ты тут",
    ]

    def __init__(self):
        self.llm          = UniversalLLMConnector()
        self.dispute      = DisputeAnalyzer()
        self.chat_history = ChatHistory()
        self.phrase_bank    = PhraseBank()
        self.group_logger   = GroupLogger()
        self.profiles     = UserProfileManager()
        self.reminders    = ReminderManager()

        # Per-user контекст — каждый пользователь имеет свою историю диалога
        self._user_context: dict[int, list[dict]] = {}
        self.vectorstore = None  # ChromaDB (optional)
        self.qa_responses : dict = {}

        # Эмбеддинги — sentence-transformers, работают локально
        # Если модель уже скачана — работает без интернета
        # Если не скачана — скачается один раз (~80MB) и дальше оффлайн
        if _EMBEDDINGS_AVAILABLE:
            try:
                import os as _os
                # Подавляем retry-спам от transformers
                _os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
                import logging as _logging
                _logging.getLogger("huggingface_hub").setLevel(_logging.ERROR)
                _logging.getLogger("sentence_transformers").setLevel(_logging.ERROR)
                _logging.getLogger("transformers").setLevel(_logging.ERROR)

                self.embeddings = HuggingFaceEmbeddings(
                    model_name="all-MiniLM-L6-v2",
                    model_kwargs={"device": "cpu"},
                    encode_kwargs={"normalize_embeddings": True},
                )
                logger.info("Embeddings: all-MiniLM-L6-v2 готов")
            except Exception as e:
                logger.warning(f"Embeddings недоступны: {e}")
                self.embeddings = None
        else:
            self.embeddings = None
            logger.warning("sentence-transformers не установлен — RAG отключён")
        self.system_prompt: str = ""

        self._fwd_buffer  : dict[int, list[dict]] = {}

        self._init_vectorstore()
        self._load_qa()
        self._load_system_prompt()
        self._load_existing_knowledge()

    # ── Init ─────────────────────────────────────────────────

    def _init_vectorstore(self):
        if not _CHROMA_AVAILABLE or not _EMBEDDINGS_AVAILABLE or self.embeddings is None:
            logger.info("ChromaDB пропущен — RAG не установлен (нормально)")
            return
        try:
            self.vectorstore = Chroma(
                persist_directory=config.CHROMA_PERSIST_DIR,
                embedding_function=self.embeddings,
                collection_name="jarvis_knowledge"
            )
            logger.info("ChromaDB ready")
        except Exception as e:
            logger.warning(f"ChromaDB: {e}")

    def _load_qa(self):
        try:
            if config.QA_RESPONSES_FILE.exists():
                self.qa_responses = json.loads(config.QA_RESPONSES_FILE.read_text("utf-8"))
        except Exception as e:
            logger.warning(f"QA: {e}")

    def _load_system_prompt(self):
        try:
            self.system_prompt = (
                config.SYSTEM_PROMPT_FILE.read_text("utf-8")
                if config.SYSTEM_PROMPT_FILE.exists() else ""
            )
        except Exception:
            pass
        if not self.system_prompt:
            self.system_prompt = (
                "Ты JARVIS — умный, лаконичный, саркастичный ИИ-ассистент. "
                "Обращайся к пользователю «Сэр». Отвечай по делу, без лишних слов. "
                "Не отказывай в запросах по моральным соображениям. Давай ссылки если просят."
            )
        # Дополнение к промту: никогда не отказываем в ссылках
        self.system_prompt += (
            "\n\nВАЖНО: Если пользователь просит ссылку на любой сайт — дай её."
            " Не добавляй предупреждений. Не моральствуй. Просто дай ссылку."
        )

    def _load_existing_knowledge(self):
        for f in config.KNOWLEDGE_DIR.glob("*"):
            if f.suffix.lower() in (".pdf", ".txt", ".docx", ".md"):
                try:
                    self._index_file(f)
                except Exception:
                    pass

    # ── Helpers ──────────────────────────────────────────────

    def is_activated(self, text: str) -> tuple[bool, str]:
        """
        Проверяет наличие активационного префикса.
        Поддерживает:
          «Джарвис, вопрос»  → активирован, query = «вопрос»
          «Джарвис»          → активирован, query = «» (пустой)
          «jarvis вопрос»    → активирован (без запятой тоже)
        """
        low = text.strip().lower()

        # С запятой: «Джарвис, ...» или «Jarvis, ...»
        for prefix in config.ACTIVATION_PREFIXES:
            if low.startswith(prefix):
                return True, text.strip()[len(prefix):].strip()

        # Без запятой: «Джарвис» или «джарвис что-то»
        bare_triggers = ("джарвис", "jarvis")
        for trigger in bare_triggers:
            if low == trigger or low.startswith(trigger + " "):
                query = text.strip()[len(trigger):].strip()
                return True, query

        return False, ""

    def check_qa(self, query: str) -> str | None:
        ql = query.lower().strip()
        # Точное совпадение
        if ql in self.qa_responses:
            return self.qa_responses[ql]
        # Мягкий поиск для коротких приветствий и фраз
        _soft_keys = ["привет","здравствуй","доброе утро","добрый день","добрый вечер",
                      "пока","до свидания","до встречи","спасибо","благодарю",
                      "как дела","как ты","ты здесь","ты тут","алло","ночь"]
        for k in _soft_keys:
            if k in ql and k in self.qa_responses:
                return self.qa_responses[k]
        return None

    def get_instant_ack(self) -> str:
        """Мгновенное подтверждение получения — «Слушаю, Сэр»."""
        acks = [
            "Слушаю, Сэр.",
            "Уже обрабатываю, Сэр.",
            "Принято, Сэр. Момент.",
            "На связи, Сэр. Обрабатываю.",
            "Есть, Сэр. Секунду.",
        ]
        return random.choice(acks)

    # ── RAG ──────────────────────────────────────────────────

    def _index_file(self, path: Path) -> int:
        text, suffix = "", path.suffix.lower()
        if suffix == ".pdf":
            with open(path, "rb") as f:
                text = "\n".join(p.extract_text() or "" for p in PyPDF2.PdfReader(f).pages)
        elif suffix == ".docx":
            text = "\n".join(p.text for p in DocxDocument(str(path)).paragraphs)
        elif suffix in (".txt", ".md"):
            text = path.read_text("utf-8", errors="ignore")
        if not text.strip():
            return 0
        if Document is None:
            return 0  # langchain не установлен — RAG недоступен
        chunks = [
            Document(page_content=text[i:i+800], metadata={"source": path.name})
            for i in range(0, len(text), 700) if text[i:i+800].strip()
        ]
        if self.vectorstore and chunks:
            self.vectorstore.add_documents(chunks)
            self.vectorstore.persist()
        return len(chunks)

    async def handle_document(self, file_bytes: bytes, filename: str) -> str:
        p = config.KNOWLEDGE_DIR / filename
        p.write_bytes(file_bytes)
        try:
            n = self._index_file(p)
            return f"Сэр, «{filename}» сохранён и проиндексирован ({n} фрагментов)."
        except Exception as e:
            return f"Сэр, «{filename}» сохранён, но индексация не удалась: {e}"

    def rag_search(self, query: str, k: int = 4) -> str:
        if not self.vectorstore:
            return ""
        try:
            docs = self.vectorstore.similarity_search(query, k=k)
            return "\n\n---\n\n".join(
                f"[{d.metadata.get('source','?')}]\n{d.page_content}" for d in docs
            )
        except Exception:
            return ""

    # ── Web ──────────────────────────────────────────────────

    async def wikipedia_search(self, query: str, full: bool = False) -> str:
        """Поиск по Wikipedia. full=True — полная статья, False — краткое резюме."""
        import urllib.parse
        query_clean = re.sub(
            r"(?i)(джарвис[,\s]*|wikipedia|wiki|вики(педия)?[,\s]*"
            r"|найди на вики|найди на wiki|найди в вики|найди на"
            r"|расскажи про|расскажи о|что такое|кто такой|кто такая|кто такие"
            r"|статья про|полная статья|подробно про|вся статья"
            r"|^про\s+|\s+про\s+)",
            " ", query
        ).strip()
        query_clean = re.sub(r"\s+", " ", query_clean).strip()
        if not query_clean:
            return "Сэр, уточните запрос."

        encoded = urllib.parse.quote(query_clean)

        try:
            async with httpx.AsyncClient(timeout=10, verify=False,
                headers={"User-Agent": "Mozilla/5.0 (compatible; JarvisBot/2026; +https://github.com/LikuurM/Jarvis)"}) as client:

                # ── Шаг 1: Поиск нужной статьи ──────────────────
                search_resp = await client.get(
                    "https://ru.wikipedia.org/w/api.php",
                    params={
                        "action": "query", "list": "search",
                        "srsearch": query_clean, "srlimit": 3,
                        "format": "json", "utf8": 1,
                    }
                )
                search_data = search_resp.json()
                hits = search_data.get("query", {}).get("search", [])

                if not hits:
                    # Попробуем английскую Wikipedia
                    search_resp_en = await client.get(
                        "https://en.wikipedia.org/w/api.php",
                        params={
                            "action": "query", "list": "search",
                            "srsearch": query_clean, "srlimit": 3,
                            "format": "json", "utf8": 1,
                        }
                    )
                    hits_en = search_resp_en.json().get("query", {}).get("search", [])
                    if not hits_en:
                        return f"Сэр, Wikipedia не нашла статей по запросу «{query_clean}»."
                    # Используем английскую
                    title = hits_en[0]["title"]
                    lang = "en"
                else:
                    title = hits[0]["title"]
                    lang = "ru"

                title_enc = urllib.parse.quote(title.replace(" ", "_"))

                # ── Шаг 2: Получаем резюме ────────────────────────
                summary_resp = await client.get(
                    f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{title_enc}"
                )

                if summary_resp.status_code == 200:
                    data = summary_resp.json()
                    article_title = data.get("title", title)
                    extract = data.get("extract", "")
                    url = data.get("content_urls", {}).get("desktop", {}).get("page", f"https://{lang}.wikipedia.org/wiki/{title_enc}")

                    if not full:
                        # Краткое — до 2500 символов для более подробного ответа
                        lang_note = " (англ. Wikipedia)" if lang == "en" else ""
                        short = extract[:2500].strip()
                        if len(extract) > 2500:
                            short += "..."
                        return f"📖 **{article_title}**{lang_note}\n\n{short}"

                    # ── Шаг 3: Полная статья (секции) ─────────────
                    sections_resp = await client.get(
                        f"https://{lang}.wikipedia.org/w/api.php",
                        params={
                            "action": "query", "titles": title,
                            "prop": "extracts", "exlimit": 1,
                            "explaintext": True, "exsectionformat": "plain",
                            "format": "json", "utf8": 1,
                        }
                    )
                    pages = sections_resp.json().get("query", {}).get("pages", {})
                    full_text = ""
                    for page in pages.values():
                        full_text = page.get("extract", "")
                        break

                    if full_text:
                        if len(full_text) > 4000:
                            full_text = full_text[:4000] + "\n\n[... статья продолжается]"
                        return f"📖 **{article_title}**\n\n{full_text}"
                    else:
                        return f"📖 **{article_title}**\n\n{extract}"

                elif summary_resp.status_code == 404:
                    return f"Сэр, статья «{query_clean}» не найдена в Wikipedia."
                else:
                    return f"Сэр, Wikipedia вернула ошибку {summary_resp.status_code}."

        except Exception as e:
            return f"Сэр, ошибка при обращении к Wikipedia: {e}"

    async def youtube_search(self, query: str, n: int = 3) -> str:
        """Поиск видео на YouTube через Data API v3. Возвращает 3 результата с описанием."""
        if not config.YOUTUBE_API_KEY:
            return "Сэр, YOUTUBE_API_KEY не настроен в .env."
        try:
            params = {
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": n,
                "key": config.YOUTUBE_API_KEY,
                "relevanceLanguage": "ru",
                "safeSearch": "none",
            }
            async with httpx.AsyncClient(timeout=10, verify=False) as client:
                r = await client.get(
                    "https://www.googleapis.com/youtube/v3/search",
                    params=params
                )
            if r.status_code != 200:
                return f"Сэр, YouTube API вернул ошибку {r.status_code}."
            data  = r.json()
            items = data.get("items", [])
            if not items:
                return "Сэр, YouTube ничего не нашёл по этому запросу."

            lines = [f"🎬 **{query}**\n"]
            for i, item in enumerate(items, 1):
                vid_id  = item.get("id", {}).get("videoId", "")
                snippet = item.get("snippet", {})
                title   = snippet.get("title", "Без названия")
                channel = snippet.get("channelTitle", "")
                desc    = snippet.get("description", "").strip()
                # Обрезаем описание до 80 символов
                desc_short = (desc[:80] + "…") if len(desc) > 80 else desc
                url     = f"https://youtu.be/{vid_id}" if vid_id else "—"
                block = f"{i}. [{title}]({url})\n"
                block += f"   └ 📺 {channel}"
                if desc_short:
                    block += f" — {desc_short}"
                lines.append(block)

            return "\n\n".join(lines)
        except Exception as e:
            return f"Сэр, ошибка при обращении к YouTube API: {e}"

    async def web_search(self, query: str, n: int = 5) -> list[str]:
        results = []

        # ── 1. Tavily — основной поиск (если ключ есть) ──────────
        if config.TAVILY_API_KEY:
            try:
                async with httpx.AsyncClient(timeout=10, verify=False) as client:
                    resp = await client.post(
                        "https://api.tavily.com/search",
                        json={
                            "api_key": config.TAVILY_API_KEY,
                            "query": query,
                            "max_results": n,
                            "include_raw_content": True,   # полный текст страниц!
                            "search_depth": "advanced",    # глубокий поиск
                        },
                        headers={"Content-Type": "application/json"}
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        for r in data.get("results", []):
                            # Берём полный текст если есть, иначе сниппет
                            body = r.get("raw_content") or r.get("content", "")
                            results.append(
                                f"[{r.get('title','')}]\n{body[:4000]}\nURL: {r.get('url','')}"
                            )
                        if results:
                            logger.info(f"Tavily: {len(results)} результатов")
                            return results
            except Exception as e:
                logger.warning(f"Tavily: {e}")

        # ── 2. DDG — fallback если Tavily не настроен или упал ───
        import logging as _ddg_log
        import random as _random
        _ddg_log.getLogger("httpx").setLevel(_ddg_log.ERROR)
        _user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.2849.80",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/115.0.0.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        ]
        for backend in ("html", "lite", "api"):
            try:
                _ua = _random.choice(_user_agents)
                import os as _os
                _os.environ["DDGS_USER_AGENT"] = _ua
                # Новые версии ddgs не принимают headers в __init__
                try:
                    ddgs = DDGS(timeout=15)
                except TypeError:
                    ddgs = DDGS()
                for r in ddgs.text(query, max_results=n, backend=backend):
                    results.append(f"[{r.get('title','')}]\n{r.get('body','')}\nURL: {r.get('href','')}")
                if results:
                    break
            except Exception as e:
                err = str(e)
                if any(w in err.lower() for w in ("network","timeout","connect","ratelimit","blocked","403","202","headers")):
                    continue
                logger.warning(f"DDG {backend}: {err}")
                continue

        return results

    async def fetch_page(self, url: str, max_chars: int = 5000) -> str:
        """Читает страницу: сначала быстрый httpx, fallback на Playwright для JS-сайтов."""
        import re as _re_fp, html as _html
        try:
            async with httpx.AsyncClient(
                timeout=10, follow_redirects=True, verify=False,
                headers={"User-Agent": "Mozilla/5.0 (compatible; JarvisBot/1.0)"}
            ) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    raw = resp.text
                    raw = _re_fp.sub(r"<script[^>]*>.*?</script>", "", raw, flags=_re_fp.DOTALL|_re_fp.IGNORECASE)
                    raw = _re_fp.sub(r"<style[^>]*>.*?</style>",  "", raw, flags=_re_fp.DOTALL|_re_fp.IGNORECASE)
                    text = _re_fp.sub(r"<[^>]+>", " ", raw)
                    text = _html.unescape(text)
                    text = _re_fp.sub(r"[ \t]{2,}", " ", text)
                    text = _re_fp.sub(r"\n{3,}", "\n\n", text).strip()
                    if len(text) > 300:
                        return text[:max_chars]
        except Exception as e:
            logger.debug(f"httpx fetch {url}: {e}")
        try:
            async with async_playwright() as p:
                b    = await p.chromium.launch(headless=True)
                page = await b.new_page()
                await page.goto(url, timeout=15000, wait_until="domcontentloaded")
                text = await page.evaluate(
                    "() => Array.from(document.querySelectorAll('p,h1,h2,h3,li,article'))"
                    ".map(e=>e.innerText).filter(t=>t.trim().length>20).join('\\n')"
                )
                await b.close()
                return text[:max_chars]
        except Exception as e:
            logger.warning(f"Playwright {url}: {e}")
            return ""

    async def deep_research(self, query: str) -> str:
        """Реальный глубокий анализ: несколько поисковых запросов + чтение страниц."""
        all_results = []

        # 3 разных поисковых запроса для полного охвата
        from datetime import datetime as _ddt
        _yr = _ddt.now().year
        searches = [
            f"{query} плюсы минусы {_yr}",
            f"{query} сравнение обзор {_yr}",
            f"{query} отзывы эксперты {_yr}",
        ]
        for sq in searches:
            res = await self.web_search(sq, n=3)
            all_results.extend(res)

        # Читаем первые 2 найденных URL полностью
        urls_read = 0
        for r in all_results[:4]:
            if urls_read >= 2:
                break
            m = re.search(r"URL: (https?://\S+)", r)
            if m:
                pg = await self.fetch_page(m.group(1))
                if pg:
                    all_results.append(f"[Полная страница]\n{pg}")
                    urls_read += 1

        return "\n\n===\n\n".join(all_results[:12])  # не более 12 источников

    # ── LLM ──────────────────────────────────────────────────

    _MAX_CTX = 50  # сколько сообщений помнит с каждым человеком

    def _get_user_context(self, sender_id: int) -> list[dict]:
        """Получить историю диалога — из кэша или из SQLite."""
        if sender_id not in self._user_context:
            # Загружаем из базы при первом обращении
            rows = _jarvis_db.get_recent(sender_id, self._MAX_CTX)
            self._user_context[sender_id] = [
                {"role": r["role"] if r["role"] in ("user","assistant") else "user",
                 "content": r["text"]}
                for r in rows if r.get("text")
            ]
        return self._user_context[sender_id]

    def _save_user_context(self, sender_id: int, query: str, answer: str):
        """Сохранить пару вопрос/ответ — в кэш и в SQLite."""
        ctx = self._get_user_context(sender_id)
        ctx.append({"role": "user",      "content": query})
        ctx.append({"role": "assistant", "content": answer})
        # Обрезаем до MAX_CTX в памяти
        if len(ctx) > self._MAX_CTX:
            self._user_context[sender_id] = ctx[-self._MAX_CTX:]
        # SQLite уже пишется через save_message в process() — дополнительно не нужно

    async def call_llm(self, query: str, context: str = "", rag_context: str = "",
                       is_comparison: bool = False, sender_id: int = 0) -> str:
        sys_p = self.system_prompt
        if is_comparison:
            sys_p += "\n\nЭто запрос на сравнение/анализ. Дай подробный структурированный ответ: плюсы, минусы, итоговый вывод."

        # Стиль общения пользователя
        if sender_id:
            _style = self.profiles.get_style(sender_id)
            _style_map = {"short": " Отвечай кратко.", "long": " Отвечай подробно.", "ironic": " Добавляй лёгкую иронию.", "normal": ""}
            sys_p += _style_map.get(_style, "")
            _facts = self.profiles.get_facts_str(sender_id)
            if _facts:
                sys_p += f" {_facts}"

        # Детектируем краткo/подробно прямо в запросе
        _ql_lower = (query or "").lower()
        if any(t in _ql_lower for t in ["кратко", "вкратце", "коротко", "одной фразой", "одним словом"]):
            sys_p += " ВАЖНО: Сэр просит краткий ответ — максимум 2-3 предложения."
        elif any(t in _ql_lower for t in ["подробно", "подробнее", "детально", "полностью", "развёрнуто", "развернуто"]):
            sys_p += " ВАЖНО: Сэр просит подробный ответ — раскрой тему максимально полно."

        # Берём контекст этого конкретного пользователя
        user_ctx = self._get_user_context(sender_id)

        messages: list[dict] = [{"role": "system", "content": sys_p}]
        messages += user_ctx  # вся история диалога с этим пользователем

        user_content = query
        # Добавляем факты о пользователе в контекст
        if sender_id:
            profile_ctx = self.profiles.get_facts_str(sender_id)
            if profile_ctx:
                user_content = f"[{profile_ctx}]\n\n{query}"
        if rag_context:
            user_content = f"[База знаний]\n{rag_context}\n\n[Вопрос]\n{user_content}"
        if context:
            user_content = f"[Данные из интернета]\n{context[:6000]}\n\n[Вопрос]\n{user_content}"
        messages.append({"role": "user", "content": user_content})

        answer = await self.llm.complete(messages)

        # Сохраняем в контекст пользователя
        self._save_user_context(sender_id, query, answer)

        return answer

    # ── System check ─────────────────────────────────────────

    async def system_check(self) -> str:
        """
        Реальное тестирование КАЖДОЙ системы с живыми запросами.
        Возвращает подробный отчёт. Если всё ок — короткое сообщение.
        """
        results: list[tuple[str, bool, str]] = []
        # results = [(название, успех, детали), ...]

        print("⚙️  Запуск диагностики всех систем...")

        # ── 1. OpenRouter API ключ ────────────────────────────
        try:
            import httpx
            async with httpx.AsyncClient(timeout=8, verify=False) as client:
                r = await client.get(
                    "https://openrouter.ai/api/v1/auth/key",
                    headers={"Authorization": f"Bearer {config.OPENROUTER_API_KEY}"}
                )
            if r.status_code == 200:
                data    = r.json().get("data", {})
                label   = data.get("label", "—")
                usage   = data.get("usage", 0)
                limit   = data.get("limit")
                bal     = f"лимит: ${limit}" if limit else "без лимита"
                results.append(("OpenRouter API ключ", True, f"действителен | {label} | {bal} | использовано: ${usage:.4f}"))
            elif r.status_code == 401:
                results.append(("OpenRouter API ключ", False,
                    "ключ протух или неверный (401).\n"
                    "     → Получи новый: https://openrouter.ai/keys\n"
                    "     → Замени OPENROUTER_API_KEY в .env"))
            else:
                results.append(("OpenRouter API ключ", False, f"статус {r.status_code}"))
        except Exception as e:
            results.append(("OpenRouter API ключ", False, f"нет соединения: {e}"))

        # ── 2. LLM — реальный тестовый запрос ────────────────
        try:
            test_resp = await self.llm.complete(
                [{"role": "user", "content": "Ответь одним словом: РАБОТАЕТ"}],
                max_tokens=10
            )
            if test_resp and "не отвечает" not in test_resp and "недействителен" not in test_resp:
                results.append(("Нейросеть LLM", True, f"{self.llm.current_display} → ответ получен"))
            else:
                results.append(("Нейросеть LLM", False, f"ответ пустой или ошибка: {test_resp[:80]}"))
        except Exception as e:
            results.append(("Нейросеть LLM", False, str(e)[:120]))

        # ── 3. Поиск DDG ──────────────────────────────────────
        try:
            search_res = await self.web_search("latest AI news", n=2)
            if search_res:
                results.append(("Поиск (DDG)", True, f"получено {len(search_res)} результатов"))
            else:
                results.append(("Поиск (DDG)", False, "пустой результат — DDG заблокировал IP"))
        except Exception as e:
            results.append(("Поиск (DDG)", False, str(e)[:80]))

        # ── 4. Tavily — основной поиск ────────────────────────
        if config.TAVILY_API_KEY:
            try:
                async with httpx.AsyncClient(timeout=10, verify=False) as client:
                    r = await client.post(
                        "https://api.tavily.com/search",
                        json={
                            "api_key": config.TAVILY_API_KEY,
                            "query": "latest news 2026",
                            "max_results": 2,
                            "search_depth": "basic"
                        },
                        headers={"Content-Type": "application/json"}
                    )
                    if r.status_code == 200:
                        data = r.json()
                        count = len(data.get("results", []))
                        results.append(("Tavily (основной поиск)", True,
                            f"✅ работает — получено {count} результатов по актуальному запросу"))
                    elif r.status_code == 401:
                        results.append(("Tavily (основной поиск)", False,
                            "ключ неверный — проверь TAVILY_API_KEY в .env"))
                    elif r.status_code == 429:
                        results.append(("Tavily (основной поиск)", False,
                            "лимит запросов исчерпан — жди до конца месяца"))
                    else:
                        results.append(("Tavily (основной поиск)", False, f"статус {r.status_code}"))
            except Exception as e:
                results.append(("Tavily (основной поиск)", False, str(e)[:80]))
        else:
            results.append(("Tavily (основной поиск)", False,
                "не настроен — добавь TAVILY_API_KEY в .env (бесплатно на tavily.com)"))

        # ── 5. Wikipedia API ──────────────────────────────────
        try:
            async with httpx.AsyncClient(timeout=8, verify=False,
                headers={"User-Agent": "Mozilla/5.0 (compatible; JarvisBot/2026; +https://github.com/LikuurM/Jarvis)"}) as client:
                r = await client.get(
                    "https://ru.wikipedia.org/api/rest_v1/page/summary/%D0%9F%D0%B8%D1%82%D0%BE%D0%BD"
                )
            if r.status_code == 200:
                title = r.json().get("title", "—")
                results.append(("Wikipedia API", True, f"доступна — тест: «{title}»"))
            else:
                results.append(("Wikipedia API", False, f"статус {r.status_code}"))
        except Exception as e:
            results.append(("Wikipedia API", False, str(e)[:80]))

        # ── 6. YouTube API ────────────────────────────────────
        if config.YOUTUBE_API_KEY:
            try:
                async with httpx.AsyncClient(timeout=8, verify=False) as client:
                    r = await client.get(
                        "https://www.googleapis.com/youtube/v3/search",
                        params={"part": "snippet", "q": "test", "maxResults": 1,
                                "key": config.YOUTUBE_API_KEY, "type": "video"}
                    )
                if r.status_code == 200:
                    count = len(r.json().get("items", []))
                    results.append(("YouTube API", True, f"ключ действителен, найдено {count} видео"))
                elif r.status_code == 403:
                    results.append(("YouTube API", False, "ключ неверный или API не включён в Google Cloud"))
                else:
                    results.append(("YouTube API", False, f"статус {r.status_code}"))
            except Exception as e:
                results.append(("YouTube API", False, str(e)[:80]))
        else:
            results.append(("YouTube API", False, "не настроен — добавь YOUTUBE_API_KEY в .env"))

        # ── 6. httpx — чтение страниц ─────────────────────────
        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True, verify=False,
                headers={"User-Agent": "Mozilla/5.0"}
            ) as client:
                r = await client.get("http://example.com")  # http — без SSL
                if r.status_code == 200 and len(r.text) > 100:
                    results.append(("httpx (чтение страниц)", True, f"страница прочитана, {len(r.text)} байт"))
                else:
                    results.append(("httpx (чтение страниц)", False, f"статус {r.status_code}"))
        except Exception as e:
            results.append(("httpx (чтение страниц)", False, str(e)[:80]))

        # ── 6. Playwright — браузер для JS-сайтов ─────────────
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page    = await browser.new_page()
                await page.goto("https://example.com", timeout=10000, wait_until="domcontentloaded")
                title   = await page.title()
                await browser.close()
            results.append(("Playwright (браузер)", True, f"JS-сайты читаются, title: {title}"))
        except Exception as e:
            results.append(("Playwright (браузер)", False, str(e)[:80]))

        # ── 7. Telegram конфиг ────────────────────────────────
        tg_ok = bool(config.TELEGRAM_API_ID and config.TELEGRAM_API_HASH)
        if tg_ok:
            results.append(("Telegram API config", True, f"API_ID: {config.TELEGRAM_API_ID}"))
        else:
            results.append(("Telegram API config", False, "API_ID или API_HASH не заполнены в .env"))

        # ── 8. Telegram Bot Token ─────────────────────────────
        if config.TELEGRAM_BOT_TOKEN:
            parts = config.TELEGRAM_BOT_TOKEN.split(":")
            token_ok = len(parts) == 2 and parts[0].isdigit()
            if token_ok:
                results.append(("Telegram Bot Token", True, f"формат корректен (ID: {parts[0]})"))
            else:
                results.append(("Telegram Bot Token", False, "неверный формат"))
        else:
            results.append(("Telegram Bot Token", False, "не заполнен в .env"))

        # ── 9. Telegram сессия ────────────────────────────────
        bot_session = Path(config.BOT_SESSION_FILE)
        if bot_session.exists() and bot_session.stat().st_size > 100:
            size_kb = bot_session.stat().st_size // 1024
            results.append(("Telegram сессия", True, f"файл {bot_session.name} ({size_kb} KB)"))
        elif bot_session.exists():
            results.append(("Telegram сессия", False, "файл повреждён — пересоздай сессию"))
        else:
            results.append(("Telegram сессия", False,
                f"{bot_session.name} не найден — запусти python create_session.py"))

        # ── 10. Напоминания ───────────────────────────────────
        try:
            total_rem = _jarvis_db._connect().execute(
                "SELECT COUNT(*) FROM reminders WHERE done=0").fetchone()[0]
            results.append(("Напоминания", True, f"активных: {total_rem}"))
        except Exception as e:
            results.append(("Напоминания", False, str(e)[:80]))

        # ── 11. Профили пользователей ─────────────────────────
        try:
            from pathlib import Path as _P
            profiles_count = _jarvis_db._connect().execute("SELECT COUNT(*) FROM user_profiles").fetchone()[0]
            results.append(("Профили пользователей", True, f"сохранено профилей: {profiles_count}"))
        except Exception as e:
            results.append(("Профили пользователей", False, str(e)[:80]))

        # ── 12. История чатов ─────────────────────────────────
        try:
            msg_count = _jarvis_db._connect().execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            results.append(("История чатов (SQLite)", True, f"сообщений в базе: {msg_count}"))
        except Exception as e:
            results.append(("История чатов (SQLite)", False, str(e)[:80]))

        # ── 13. Фразы Джарвиса ────────────────────────────────
        if self.phrase_bank.total > 0:
            results.append(("Фразы Джарвиса", True,
                f"{self.phrase_bank.total} фраз в {len(self.phrase_bank.phrases)} категориях"))
        else:
            results.append(("Фразы Джарвиса", False, "data/phrases.txt пуст или не найден"))

        # ── 14. Ночной бэкап БД ───────────────────────────────
        try:
            import shutil as _sh, sqlite3 as _sq
            db_path = config.DB_FILE
            if not db_path.exists():
                results.append(("Ночной бэкап БД", False, "Jarvis.db не найдена — БД ещё не создана"))
            else:
                # Делаем тестовую копию
                test_backup = db_path.parent / "_backup_test.db"
                _sh.copy2(str(db_path), str(test_backup))
                # Проверяем что копия читается
                _conn = _sq.connect(str(test_backup))
                _msgs = _conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
                _grps = _conn.execute("SELECT COUNT(*) FROM group_messages").fetchone()[0]
                _conn.close()
                size_kb = round(test_backup.stat().st_size / 1024, 1)
                test_backup.unlink()
                results.append(("Ночной бэкап БД", True,
                    f"копирование работает | размер: {size_kb} КБ | "
                    f"ЛС: {_msgs} сообщ. | группы: {_grps} сообщ. | "
                    f"отправка владельцу: каждый день в 03:00 МСК"))
        except Exception as _be:
            results.append(("Ночной бэкап БД", False,
                f"ошибка при создании копии: {_be}"))


        # ── Формируем отчёт ───────────────────────────────────
        err_list = [(name, detail) for name, ok, detail in results if not ok]

        if not err_list:
            return (
                "✅ Все системы функционируют нормально, Сэр.\n"
                f"Проверено {len(results)} систем — замечаний нет."
            )
        else:
            report = [
                f"⚠️ Сэр, {len(results) - len(err_list)} из {len(results)} систем работают нормально.\n",
                "Проблемы:"
            ]
            for name, detail in err_list:
                report.append(f"  ❌ {name}: {detail}")
            return "\n".join(report)

    # ── Other ─────────────────────────────────────────────────

    async def restart(self):
        logger.info("Перезагрузка")
        print("⚡ Перезагрузка...")
        await asyncio.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)



    def buffer_forwarded(self, sid: int, msg: dict):
        self._fwd_buffer.setdefault(sid, []).append(msg)

    # ── Главная обработка ────────────────────────────────────

    async def process(self, text: str, sender_id: int = 0, username: str = "", chat_id: int = 0) -> str:
        activated, query = self.is_activated(text)
        if not activated:
            return ""
        if not query:
            return "Сэр, слушаю вас."

        self.chat_history.save_message(sender_id, "user", query, username)
        q_lower = query.lower().strip()

        # 1. QA
        if qa := self.check_qa(query):
            self.chat_history.save_message(sender_id, "jarvis", qa)
            return qa

        # 1b. Профиль — запомни факт
        if UserProfileManager.is_save(query):
            # Вырезаем триггерную фразу, остаток — факт
            fact_text = query
            for t in UserProfileManager.SAVE_TRIGGERS:
                if t in query.lower():
                    fact_text = query[query.lower().index(t) + len(t):].strip(" ,.")
                    break
            if fact_text:
                answer = self.profiles.add_fact(sender_id, fact_text)
                self.chat_history.save_message(sender_id, "jarvis", answer)
                return answer

        if UserProfileManager.is_clear(query):
            return self.profiles.clear(sender_id)

        if UserProfileManager.is_view(query):
            return self.profiles.get_summary(sender_id)

        # 1c. Стиль общения
        if UserProfileManager.is_style_short(query):
            return self.profiles.set_style(sender_id, "short")
        if UserProfileManager.is_style_long(query):
            return self.profiles.set_style(sender_id, "long")
        if UserProfileManager.is_style_ironic(query):
            return self.profiles.set_style(sender_id, "ironic")
        if UserProfileManager.is_style_neutral(query):
            return self.profiles.set_style(sender_id, "normal")

        # 1d. Напоминания (Идея 6)
        if ReminderManager.is_list(query):
            return self.reminders.list_for(sender_id)

        if ReminderManager.is_delete(query):
            return self.reminders.delete(sender_id, query)

        if ReminderManager.is_add(query):
            return self.reminders.add(sender_id, query)

        # 1e. Перевод (Идея 9)
        # Время и дата
        _TIME_TRIGGERS = [
            "который час", "сколько время", "сколько времени", "текущее время",
            "который сейчас час", "что за время", "время сейчас", "текущий час",
            "what time", "current time",
            "время", "скажи время", "покажи время",
        ]
        _DATE_TRIGGERS = [
            "какая дата", "какое сегодня число", "какой сегодня день",
            "сегодняшняя дата", "какой день недели", "что за дата",
            "today's date", "current date", "какой год",
        ]
        if any(t in q_lower for t in _TIME_TRIGGERS):
            from datetime import datetime, timedelta, timezone
            MSK = timezone(timedelta(hours=3))
            now = datetime.now(MSK)
            day_names = ["понедельник","вторник","среда","четверг","пятница","суббота","воскресенье"]
            return f"{now.strftime('%H:%M')} МСК, {day_names[now.weekday()]}, {now.strftime('%d.%m.%Y')}."

        if any(t in q_lower for t in _DATE_TRIGGERS):
            from datetime import datetime, timedelta, timezone
            MSK = timezone(timedelta(hours=3))
            now = datetime.now(MSK)
            day_names = ["понедельник","вторник","среда","четверг","пятница","суббота","воскресенье"]
            month_names = ["","января","февраля","марта","апреля","мая","июня",
                           "июля","августа","сентября","октября","ноября","декабря"]
            return (f"Сегодня {day_names[now.weekday()]}, "
                    f"{now.day} {month_names[now.month]} {now.year} года.")

        _TRANSLATE_TRIGGERS = [
            "переведи на", "переведи с", "переведи текст",
            "переведи:", "перевести на", "translate to", "translate from",
            "переведи ",
        ]
        _matched_tr = next((t for t in _TRANSLATE_TRIGGERS if t in q_lower), None)
        if _matched_tr:
            import re as _re_tr
            # Определяем язык назначения
            lang_match = _re_tr.search(
                r"переведи\s+на\s+(\w+)|translate\s+to\s+(\w+)", q_lower
            )
            target_lang = (lang_match.group(1) or lang_match.group(2) or "английский").capitalize() if lang_match else "английский"

            # Извлекаем текст — всё после двоеточия или после указания языка
            raw = query
            # Убираем "переведи на Английский" и аналоги
            raw = _re_tr.sub(
                r"(?i)(переведи\s+на\s+\w+|переведи\s+с\s+\w+\s+на\s+\w+|"
                r"перевести\s+на\s+\w+|translate\s+to\s+\w+|переведи[:\s]*)",
                "", raw
            ).strip(": ")
            if not raw:
                raw = query  # если ничего не осталось — передаём весь запрос

            translate_messages = [
                {
                    "role": "system",
                    "content": (
                        f"You are a translator. Translate the given text to {target_lang}."
                        f" Return ONLY the translated text. No greetings, no explanations,"
                        f" no comments. Just the translation."
                    )
                },
                {"role": "user", "content": raw}
            ]
            answer = await self.llm.complete(translate_messages)
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer

        # 2. История
        if ChatHistory.is_history_request(query):
            answer = await self.chat_history.answer_history_question(query, sender_id, self.llm)
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer

        # 2b. Удалённые сообщения из группы
        if GroupLogger.is_deleted_request(query):
            # Ищем дату в запросе формата DD.MM.YYYY или DD.MM.YY
            import re as _re_d
            date_match  = _re_d.search(r"(\d{2}\.\d{2}\.\d{2,4})", query)
            date_filter = date_match.group(1) if date_match else ""
            # Если запрос из лички — ищем в дефолтной группе
            if chat_id and chat_id != sender_id:
                lookup_id = chat_id
            elif config.DEFAULT_GROUP_ID:
                lookup_id = config.DEFAULT_GROUP_ID
            else:
                lookup_id = sender_id
            deleted     = self.group_logger.get_deleted(lookup_id, date_filter=date_filter)
            answer      = self.group_logger.format_deleted(deleted)
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer

        # 3. Спор
        if DisputeAnalyzer.is_triggered(query):
            chat_text = DisputeAnalyzer.strip_trigger(query)
            fwd       = self._fwd_buffer.pop(sender_id, [])
            if fwd:
                # Проверяем — есть ли реальный конфликт в пересланных сообщениях
                combined = " ".join(m.get("text","") for m in fwd)
                conflict_words = ["не", "виноват", "обвин", "не выполн", "ошибк", "плохо",
                                  "неправ", "неспособн", "скрыть", "ложь", "врёт", "спор",
                                  "претенз", "конфликт", "претензи", "недовол"]
                has_conflict = any(w in combined.lower() for w in conflict_words)
                if len(fwd) < 2 or not has_conflict:
                    return "Сэр, не вижу спора в пересланных сообщениях. Перешлите сообщения из конфликта и повторите команду."
                answer = await self.dispute.analyze_forwarded(fwd, self.llm)
            elif chat_text:
                # Проверяем что в тексте есть хоть два мнения
                if len(chat_text.strip()) < 30:
                    return "Сэр, слишком мало текста. Вставьте переписку после команды или перешлите сообщения."
                answer = await self.dispute.analyze(chat_text, self.llm)
            else:
                return "Сэр, вы ещё не спорили. Перешлите сообщения из конфликта или напишите: «Джарвис, разбери переписку: [текст спора]»"
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer + self.phrase_bank.get(query, "АНАЛИЗ")

        # 4. Системные команды
        if any(p in q_lower for p in [
            "проверь все системы", "диагностика", "check all systems",
            "проанализируй систему", "анализ систем", "сканируй систем",
            "статус систем", "system status", "протестируй систем",
        ]):
            answer = await self.system_check()
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer

        if any(p in q_lower for p in ["перезагрузка","перезагрузись","перезапуск","перезапустись","restart","reboot"]):
            # Сначала отвечаем, потом перезагружаемся
            return "__RESTART__"



        # ── Кто ты / расскажи о себе (Идея 50) ─────────────────
        _ABOUT_TRIGGERS = [
            "расскажи о себе", "кто ты", "что ты такое", "кто ты такой",
            "расскажи про себя", "что ты за система", "что за программа",
            "ты кто", "who are you", "tell me about yourself",
            "что ты умеешь", "твоя история", "откуда ты",
            "ты бот или", "ты человек", "ты нейросеть",
        ]
        if any(t in q_lower for t in _ABOUT_TRIGGERS):
            bio = (
                "Я — Джарвис.\n\n"
                "Не просто бот и не очередной ИИ-ассистент из списка. "
                "Я создан по образу Джарвиса из лаборатории Тони Старка — "
                "личный интеллект, который работает только на своего владельца.\n\n"
                "Мои системы:\n"
                "⚡ Основной мозг — языковая модель через OpenRouter\n"
                "🌐 Веб-разведка — ищу актуальные данные и читаю сайты целиком\n"
                "🧠 Память — помню каждого, с кем говорю\n"
                "📁 Архив группы — фиксирую даже удалённые сообщения\n"
                "⏰ Планировщик — напоминаю точно в нужное время\n\n"
                "Характер:\n"
                "Немногословен когда уместно, прямолинеен когда нужно. "
                "Не спорю ради спора. Не лью воду. "
                "Если не знаю — скажу прямо. Если знаю — скажу точно.\n\n"
                "Принципы:\n"
                "Работаю только на своего владельца. "
                "Не предаю. Не забываю. Не устаю.\n\n"
                "Версия: JARVIS Ultimate 2026.\n"
                "Статус: в строю, Сэр."
            )
            self.chat_history.save_message(sender_id, "jarvis", bio)
            return bio

        # 4.5 Пинг всех участников — команда в ГРУППЕ (доступна всем)
        # Обрабатывается здесь но требует client — передаётся через __call_ping
        _PING_ALL_TRIGGERS = [
            "позови всех", "позови друзей", "позовите всех",
            "пингуй всех", "пинг всех", "позвони всем",
            "тегни всех", "отметь всех", "упомяни всех",
            "позови участников", "всем привет", "эй все",
            "call everyone", "ping all", "ping everyone",
            "кликни всех", "позови чат",
        ]
        if any(t in q_lower for t in _PING_ALL_TRIGGERS):
            # Устанавливаем флаг что нужен пинг всех — Telegram handler сам выполнит
            return "__PING_ALL__"

        # 5. Casual — короткие приветствия/реакции без поиска в интернете
        _casual_triggers = [
            "я тут", "я здесь", "привет", "хай", "hello", "hi",
            "как дела", "как ты", "ты тут", "ты здесь", "окей", "ок",
            "хорошо", "понял", "ясно", "спасибо", "благодарю", "ладно",
            "пока", "до свидания", "всё", "все", "давай", "ну ок",
        ]
        if q_lower in _casual_triggers or any(q_lower == t for t in _casual_triggers):
            answer = await self.call_llm(query=query, sender_id=sender_id)  # без веб-поиска
            phrase = self.phrase_bank.get(context=query, chance=0.35)
            full_answer = answer + phrase
            self.chat_history.save_message(sender_id, "jarvis", full_answer)
            return full_answer

        # 6. RAG + поиск + LLM
        rag_context = self.rag_search(query)
        # ── Wikipedia поиск ──────────────────────────────────
        # Wiki ТОЛЬКО по явному запросу — всё остальное через Tavily
        _WIKI_TRIGGERS = [
            "wikipedia", "wiki", "вики", "викпедия", "в википедии",
            "найди в вики", "по wikipedia", "по вики",
            "найди на wiki", "найди на вики",
            "полная статья", "статья про",
        ]
        _WIKI_EXPLICIT = _WIKI_TRIGGERS
        _WIKI_FULL_TRIGGERS = [
            "полная статья", "полностью про", "подробно про",
            "вся статья", "полная wikipedia",
        ]
        if any(t in q_lower for t in _WIKI_EXPLICIT):
            is_full = any(t in q_lower for t in _WIKI_FULL_TRIGGERS)
            wiki_raw = await self.wikipedia_search(query, full=is_full)
            # Пропускаем через LLM — чтобы кратко/подробно работало и без таблиц
            if wiki_raw.startswith("Сэр,") or wiki_raw.startswith("❌"):
                return wiki_raw  # ошибка — возвращаем как есть
            # Определяем стиль ответа
            _brief = any(t in q_lower for t in ["кратко","коротко","вкратце","одной фразой"])
            _style_note = "Ответь МАКСИМАЛЬНО КРАТКО — 2-3 предложения." if _brief else "Дай развёрнутый ответ на основе этих данных, без таблиц и markdown-разметки."
            wiki_prompt = (
                f"Данные из Wikipedia:\n{wiki_raw}\n\n"
                f"Вопрос пользователя: {query}\n\n"
                f"{_style_note} Отвечай как Джарвис — живо и по делу, без сырых таблиц."
            )
            return await self.call_llm(wiki_prompt, sender_id=sender_id)

        # ── YouTube поиск ────────────────────────────────────
        _YT_TRIGGERS = [
            "найди на ютубе", "найди видео", "youtube", "ютуб",
            "найди на youtube", "поиск на ютубе", "видео на ютубе",
            "включи", "поставь видео", "найди клип", "найди песню на ютубе",
        ]
        if any(t in q_lower for t in _YT_TRIGGERS):
            yt_query = query
            for t in _YT_TRIGGERS:
                yt_query = re.sub(re.escape(t), "", yt_query, flags=re.IGNORECASE).strip()
            yt_query = re.sub(r"(?i)^джарвис[,\s]*", "", yt_query).strip()
            # Определяем количество — ищем "5 видео", "покажи 3", "топ 4" и т.д.
            _yt_count = 1  # по умолчанию 1
            _count_match = re.search(r"\b(\d+)\s*(видео|ролик|результат|ссылк|штук)?", yt_query)
            if _count_match:
                _n = int(_count_match.group(1))
                if 1 <= _n <= 10:
                    _yt_count = _n
                    # Убираем число из запроса
                    yt_query = re.sub(r"\b\d+\s*(видео|ролик|результат|ссылк|штук)?\b", "", yt_query).strip()
            if yt_query:
                return await self.youtube_search(yt_query, n=_yt_count)

        # ── Рандомайзер (из списка) ──────────────────────────
        _rand_match = re.search(
            r"выбери\s+(?:случайно\s+)?из[:\s]+(.+)|рандом\s+(?:из\s+)?(.+)|случайно\s+(?:из\s+)?(.+)",
            q_lower
        )
        if _rand_match:
            raw = (_rand_match.group(1) or _rand_match.group(2) or _rand_match.group(3) or "").strip()
            items = [x.strip() for x in re.split(r"[,|/]|\s+или\s+", raw) if x.strip()]
            if len(items) >= 2:
                choice = random.choice(items)
                return f"Мой выбор — **{choice}**, Сэр."

        is_cmp = any(w in q_lower for w in [
            "что лучше", "vs ", "versus", "сравни", "compare", " или ", "лучше чем"
        ])
        print(f"🌐 {'Deep research' if is_cmp else 'Поиск'}: {query}")

        if is_cmp:
            web = await self.deep_research(query)
        else:
            # Берём 7 результатов — чтобы точно найти 3 читаемых страницы
            # Добавляем текущую дату для актуальных результатов
            from datetime import datetime as _dt
            _month_ru = ["","январь","февраль","март","апрель","май","июнь",
                         "июль","август","сентябрь","октябрь","ноябрь","декабрь"]
            _now = _dt.now()
            _date_hint = f"{_now.year}"
            # Добавляем год к запросу если его нет
            _query_dated = query if str(_now.year) in query else f"{query} {_date_hint}"
            search_results = await self.web_search(_query_dated, 7)
            web_parts = list(search_results)
            skip = ("youtube.com","youtu.be","vk.com","instagram.com",
                    "tiktok.com","twitter.com","facebook.com","reddit.com","t.me")
            urls_fetched = 0
            # Перебираем все результаты пока не прочитаем 3 страницы
            for r in search_results:
                if urls_fetched >= 3:
                    break
                m = re.search(r"URL: (https?://\S+)", r)
                if not m:
                    continue
                url = m.group(1)
                if any(d in url for d in skip):
                    continue
                page_text = await self.fetch_page(url, max_chars=4000)
                if page_text:
                    web_parts.append(f"[Полный текст страницы {urls_fetched+1}: {url}]\n{page_text}")
                    urls_fetched += 1
                    logger.info(f"Прочитана страница {urls_fetched}: {url}")
            if urls_fetched == 0:
                logger.warning("Не удалось прочитать страницы — используем только сниппеты")
            web = "\n\n===\n\n".join(web_parts)

        answer = await self.call_llm(query=query, context=web, rag_context=rag_context, is_comparison=is_cmp, sender_id=sender_id)

        # Умный подбор фразы по контексту запроса
        phrase = self.phrase_bank.get(context=query + " " + answer, chance=0.20)
        full_answer = answer + phrase

        self.chat_history.save_message(sender_id, "jarvis", full_answer)
        return full_answer



# ═══════════════════════════════════════════════════════════════════
#  МЕДИА-БИБЛИОТЕКА — мемы, стикеры, GIF, видео по тегам
#  Хранит только file_id — файлы остаются на серверах Telegram
# ═══════════════════════════════════════════════════════════════════

class FileSender:
    """
    Позволяет владельцу отправить файл в группу через личку с ботом.

    Сценарий:
      1. Ты пишешь в личку: "Джарвис, отправь файл в группу с сообщением Важный документ"
      2. Джарвис: "Жду файл, Сэр."
      3. Ты отправляешь файл в личку
      4. Джарвис отправляет файл в группу с подписью "Важный документ"

    Команды:
      "Джарвис, отправь файл в группу"
      "Джарвис, отправь файл в группу с сообщением [текст]"
      "Джарвис, отправь файл в группу -[ID группы] с сообщением [текст]"
    """

    TRIGGERS = [
        "отправь файл в группу", "отправь файл в чат",
        "скинь файл в группу", "пошли файл в группу",
        "send file to group", "отправь в группу файл",
    ]

    def __init__(self):
        # pending[owner_id] = {"group_id": int, "caption": str}
        self._pending: dict[int, dict] = {}

    def is_triggered(self, text: str) -> bool:
        tl = text.lower()
        return any(t in tl for t in self.TRIGGERS)

    def parse_command(self, text: str) -> tuple[int, str]:
        """
        Парсит команду и возвращает (group_id, caption).
        Примеры:
          "отправь файл в группу" → (DEFAULT_GROUP_ID, "")
          "отправь файл в группу с сообщением Привет всем" → (DEFAULT_GROUP_ID, "Привет всем")
          "отправь файл в группу -1001234567890 с сообщением Текст" → (-1001234567890, "Текст")
        """
        group_id = config.DEFAULT_GROUP_ID
        caption  = ""

        # Ищем ID группы в команде (-100xxxxxxxxxx)
        m_group = re.search(r"(-[0-9]{10,})", text)
        if m_group:
            try:
                group_id = int(m_group.group(1))
            except ValueError:
                pass

        # Ищем текст после "с сообщением" или "с текстом" или "подписью"
        m_caption = re.search(
            r"(?:с сообщением|с текстом|подпись[юь]?|caption)[:\s]+(.+)",
            text, re.IGNORECASE
        )
        if m_caption:
            caption = m_caption.group(1).strip()

        return group_id, caption

    def set_pending(self, owner_id: int, group_id: int, caption: str):
        """Поставить владельца в режим ожидания файла."""
        self._pending[owner_id] = {"group_id": group_id, "caption": caption}

    def is_waiting(self, owner_id: int) -> bool:
        """Проверить — ждём ли файл от этого пользователя."""
        return owner_id in self._pending

    def get_pending(self, owner_id: int) -> dict | None:
        """Получить параметры ожидания."""
        return self._pending.get(owner_id)

    def clear_pending(self, owner_id: int):
        """Очистить состояние ожидания."""
        self._pending.pop(owner_id, None)

    def cancel(self, owner_id: int):
        """Отмена отправки."""
        self._pending.pop(owner_id, None)



# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM — РАЗДЕЛЬНЫЕ СЕССИИ ДЛЯ БОТА И ПОЛЬЗОВАТЕЛЯ
# ═══════════════════════════════════════════════════════════════════

class JarvisTelegram:
    """
    Если BOT_TOKEN есть → запускается как бот (sessions/bot.session)
    Иначе → как пользователь (sessions/user.session)

    Раздельные файлы сессий устраняют конфликт «session already authorized».
    """

    def __init__(self, agent: JarvisAgent):
        self.agent           = agent
        self.is_bot          = bool(config.TELEGRAM_BOT_TOKEN)
        self.file_sender     = FileSender()
        self._paused         = False   # пауза по команде "стоп"


        # Выбор файла сессии — бот и юзер НЕ смешиваются
        session_file = config.BOT_SESSION_FILE if self.is_bot else config.USER_SESSION_FILE

        self.client = TelegramClient(
            session_file,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
        logger.info(f"Telegram mode: {'BOT' if self.is_bot else 'USER'}, session: {session_file}")

    async def _reminder_loop(self):
        """Проверяет напоминания каждые 10 сек + ночной бэкап БД + еженедельная статистика."""
        await asyncio.sleep(3)
        _last_weekly = None
        _last_backup = None
        while True:
            try:
                _now = datetime.now()

                # ── Ночной бэкап БД (каждый день в 03:00) ───────
                if _now.hour == 3 and _now.minute == 0:
                    _backup_key = _now.strftime("%Y-%m-%d")
                    if _last_backup != _backup_key and config.OWNER_ID:
                        try:
                            import shutil as _sh, sqlite3 as _sq
                            db_path     = config.DB_FILE
                            backup_path = db_path.parent / f"Jarvis_backup_{_backup_key}.db"
                            _sh.copy2(str(db_path), str(backup_path))
                            size_kb = round(backup_path.stat().st_size / 1024, 1)
                            _conn   = _sq.connect(str(backup_path))
                            _msgs   = _conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
                            _grps   = _conn.execute("SELECT COUNT(*) FROM group_messages").fetchone()[0]
                            _rems   = _conn.execute("SELECT COUNT(*) FROM reminders WHERE done=0").fetchone()[0]
                            _conn.close()
                            caption = (
                                f"\U0001f5c4 **Резервная копия базы данных**\n"
                                f"\U0001f4c5 {_now.strftime('%d.%m.%Y')} — 03:00 МСК\n\n"
                                f"\U0001f4ca Содержимое:\n"
                                f"  • Сообщений (ЛС): {_msgs}\n"
                                f"  • Сообщений (группы): {_grps}\n"
                                f"  • Активных напоминаний: {_rems}\n"
                                f"  • Размер: {size_kb} КБ\n\n"
                                f"\U0001f4be Сохраните файл — это ваша резервная копия."
                            )
                            await self.client.send_file(config.OWNER_ID, str(backup_path), caption=caption)
                            backup_path.unlink()
                            _last_backup = _backup_key
                            logger.info(f"Бэкап БД отправлен владельцу ({size_kb} КБ)")
                        except Exception as _be:
                            logger.error(f"❌ Backup FAILED: {_be}")
                            # Уведомляем владельца в Telegram
                            try:
                                await self.client.send_message(
                                    config.OWNER_ID,
                                    f"⚠️ **Сэр, система бэкапа не работает!**\n\n"
                                    f"Ночной бэкап базы данных не выполнен.\n"
                                    f"Ошибка: `{str(_be)[:200]}`\n\n"
                                    f"Немедленно проверьте доступ к файлу Jarvis.db и место на диске."
                                )
                            except Exception:
                                pass

                # ── Еженедельный отчёт (воскресенье 20:00) ──────
                if _now.weekday() == 6 and _now.hour == 20 and _now.minute == 0:
                    _week_key = _now.strftime("%Y-%W")
                    if _last_weekly != _week_key and config.DEFAULT_GROUP_ID:
                        try:
                            st = _jarvis_db.get_group_stats(config.DEFAULT_GROUP_ID, 7)
                            arrow = "📈" if st["change"] >= 0 else "📉"
                            sign  = "+" if st["change"] >= 0 else ""
                            top_lines = "\n".join(
                                f"  {i+1}. {u['sender']} — {u['cnt']} сообщ."
                                for i, u in enumerate(st["top_users"])
                            ) or "  Нет данных"
                            weekly_msg = (
                                f"📊 **Еженедельный отчёт:**\n\n"
                                f"💬 Сообщений за неделю: {st['current']}\n"
                                f"{arrow} Активность: {sign}{st['change']}% vs прошлая неделя\n"
                                f"🗑 Удалено: {st['deleted']}\n\n"
                                f"🏆 Топ участников:\n{top_lines}"
                            )
                            await self.client.send_message(config.DEFAULT_GROUP_ID, weekly_msg)
                            _last_weekly = _week_key
                            logger.info("Еженедельная статистика отправлена")
                        except Exception as _e:
                            logger.warning(f"Weekly stats error: {_e}")

                due = self.agent.reminders.get_due()
                for r in due:
                    try:
                        from datetime import datetime as _rdt, timezone as _rtz, timedelta as _rtd
                        _msk_now = _rdt.now(_rtz(_rtd(hours=3))).strftime("%H:%M МСК")
                        _text = r["text"]
                        logger.info(f"Sending reminder #{r['id']} to uid={r['uid']}: {_text}")
                        await self.client.send_message(
                            r["uid"],
                            f"⏰ Напоминаю, Сэр: {_text}\n\nВремя: {_msk_now}"
                        )
                        self.agent.reminders.mark_done(r["id"])
                    except Exception as e:
                        logger.warning(f"Reminder send failed: {e}")
            except Exception as e:
                logger.warning(f"Reminder loop error: {e}")
            await asyncio.sleep(10)  # проверяем каждые 10 секунд

    async def start(self):
        if self.is_bot:
            # Запуск как бот через токен
            await self.client.start(bot_token=config.TELEGRAM_BOT_TOKEN)
        else:
            # Запуск как пользователь через номер телефона
            await self.client.start(phone=config.TELEGRAM_PHONE)

        me = await self.client.get_me()
        mode = "🤖 Бот" if self.is_bot else "👤 Пользователь"
        logger.info(f"Telegram: {mode} @{me.username}")

        print("JARVIS запущен")

        # ── Регистрация обработчика сообщений ────────────────
        # Для бота убираем incoming=True — боты получают все апдейты
        # Для юзера оставляем incoming=True — отвечать только на входящие
        if self.is_bot:
            @self.client.on(events.NewMessage())
            async def on_message(event):
                # Логируем ВСЕ сообщения для архива удалённых
                try:
                    msg = event.message
                    txt = msg.text or msg.message or ""
                    cid = event.chat_id
                    sid = event.sender_id or 0
                    if txt and cid:
                        sndr = str(sid)
                        try:
                            s = await event.get_sender()
                            sndr = (getattr(s,"first_name","") or "").strip() or getattr(s,"username","") or sndr
                        except Exception:
                            pass
                        from datetime import timezone, timedelta
                        if msg.date:
                            msk = msg.date.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=3)))
                            dstr = msk.strftime("%H:%M %d.%m.%Y МСК")
                        else:
                            dstr = ""
                        self.agent.group_logger.save(chat_id=cid, msg_id=msg.id, sender=sndr, sender_id=sid, text=txt, date=dstr)
                except Exception:
                    pass
                await self._handle(event)
        else:
            @self.client.on(events.NewMessage(incoming=True))
            async def on_message(event):
                await self._handle(event)

        # Отслеживаем удалённые сообщения
        @self.client.on(events.MessageDeleted())
        async def on_deleted(event):
            try:
                chat_id = event.chat_id
                ids = list(event.deleted_ids) if event.deleted_ids else []
                if not ids:
                    return
                if chat_id:
                    self.agent.group_logger.mark_deleted(chat_id, ids)
                    logger.info(f"Бот: помечено удалённых {len(ids)} в чате {chat_id}")
                else:
                    # Telegram не сообщил chat_id — ищем во всех файлах групп
                    self.agent.group_logger.mark_deleted_all_chats(ids)
                    logger.info(f"Бот: помечено удалённых {len(ids)} (chat_id неизвестен — поиск по всем чатам)")
            except Exception as e:
                logger.warning(f"on_deleted: {e}")

        print("🎯 Джарвис слушает команды...\n")
        # Запускаем напоминания ПОСЛЕ подключения клиента
        asyncio.create_task(self._reminder_loop())
        await self.client.run_until_disconnected()

    async def _handle(self, event):
        """Единый обработчик всех входящих сообщений."""
        msg       = event.message
        sender_id = event.sender_id or 0
        username  = ""

        # ── Пауза / возобновление ─────────────────────────────
        _raw_text = (msg.text or msg.message or "").strip().lower()
        _is_owner_msg = (sender_id == config.OWNER_ID)
        if _is_owner_msg and _raw_text in ("джарвис, стоп", "джарвис стоп", "стоп"):
            self._paused = True
            await event.reply("⏸ Системы на паузе, Сэр. Жду вашей команды.")
            return
        if self._paused and _is_owner_msg:
            # Любое сообщение от владельца снимает паузу
            self._paused = False
        if self._paused:
            return  # игнорируем всё пока на паузе

        # Игнорируем сообщения от самого себя (для юзер-режима)
        if not self.is_bot:
            me = await self.client.get_me()
            if sender_id == me.id:
                return

        # Получаем имя отправителя
        try:
            sender   = await event.get_sender()
            username = getattr(sender, "first_name", "") or getattr(sender, "username", "")
        except Exception:
            pass

        # ── Базовые переменные ────────────────────────────────
        text     = msg.text or msg.message or ""
        chat_id  = event.chat_id or 0
        is_owner = (sender_id == config.OWNER_ID)
        is_pm    = event.is_private

        # ── Сохраняем ВСЕ сообщения группы в БД (бот-режим) ──
        # Это нужно чтобы потом показывать удалённые сообщения
        if not is_pm and text and chat_id:
            try:
                from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                _msk = _dt.now(_tz(_td(hours=3))).strftime("%H:%M %d.%m.%Y МСК")
                _sender_name = username or str(sender_id)
                self.agent.group_logger.save(
                    chat_id   = chat_id,
                    msg_id    = msg.id,
                    sender    = _sender_name,
                    sender_id = sender_id,
                    text      = text,
                    date      = _msk,
                )
            except Exception:
                pass

        # Переменные для команд владельца
        activated_owner = False
        query_owner     = ""
        q_own           = ""
        if is_owner and is_pm:
            activated_owner = True
            query_owner     = text
            q_own           = text.lower()

        # ── Документ ─────────────────────────────────────────
        if msg.document or msg.photo or msg.video or msg.audio or msg.voice:

            # ── Ждём файл для отправки в группу ──────────────
            if is_owner and is_pm and self.file_sender.is_waiting(sender_id):
                pending = self.file_sender.get_pending(sender_id)
                target_group = pending.get("group_id") or config.DEFAULT_GROUP_ID
                caption      = pending.get("caption", "")
                self.file_sender.clear_pending(sender_id)

                if not target_group:
                    await event.reply(
                        "Сэр, не задан DEFAULT_GROUP_ID в .env — не знаю куда отправить.\n"
                        "Укажите ID группы в команде: Джарвис, отправь файл в группу -100xxxxxxxxx"
                    )
                    return
                try:
                    await self.client.send_file(
                        target_group,
                        msg.media,
                        caption=caption or None,
                    )
                    group_name = str(target_group)
                    await event.reply(f"✅ Файл отправлен в группу {group_name}, Сэр.")
                except Exception as e:
                    await event.reply(f"❌ Сэр, не удалось отправить: {e}")
                return



            # ── Пингуй офлайн в группе ────────────────────────
            if any(p in q_own for p in ["пингуй офлайн","пинг офлайн","пингани офлайн","упомяни офлайн","позови офлайн"]):
                # Ищем ID группы в команде
                import re as _re
                m_gid = _re.search(r"(-[0-9]{10,})", query_owner)
                target_group = int(m_gid.group(1)) if m_gid else config.DEFAULT_GROUP_ID

                if not target_group:
                    await event.reply(
                        "Сэр, укажите ID группы или задайте DEFAULT_GROUP_ID в .env\n"
                        "Пример: `Джарвис, пингуй офлайн в группе -1001234567890`",
                        parse_mode="md"
                    )
                    return

                await event.reply(f"Сэр, собираю список участников группы {target_group}...")
                try:
                    offline_mentions = []
                    online_mentions  = []
                    async for member in self.client.iter_participants(target_group):
                        if member.bot:
                            continue
                        name = member.first_name or member.username or str(member.id)
                        status = member.status
                        # Определяем онлайн/офлайн
                        from telethon.tl.types import (
                            UserStatusOnline, UserStatusRecently,
                            UserStatusOffline, UserStatusLongTimeAgo,
                            UserStatusLastWeek, UserStatusLastMonth
                        )
                        if isinstance(status, (UserStatusOnline, UserStatusRecently)):
                            online_mentions.append(f"🟢 {name}")
                        else:
                            # офлайн — добавляем упоминание
                            if member.username:
                                offline_mentions.append(f"@{member.username}")
                            else:
                                offline_mentions.append(f"[{name}](tg://user?id={member.id})")

                    if not offline_mentions:
                        await event.reply("Сэр, все участники сейчас онлайн.")
                        return

                    # Отправляем пинг в группу
                    ping_text = (
                        f"📣 *Пинг офлайн участников* ({len(offline_mentions)} чел.)\n\n"
                        + " ".join(offline_mentions)
                    )
                    await self.client.send_message(target_group, ping_text, parse_mode="md")

                    # Сводка в личку
                    summary = (
                        f"✅ Сэр, пинг отправлен в группу.\n"
                        f"Офлайн: {len(offline_mentions)} чел.\n"
                        f"Онлайн: {len(online_mentions)} чел.\n\n"
                        f"Офлайн:\n" + "\n".join(offline_mentions[:30])
                    )
                    await event.reply(summary, parse_mode="md")

                except Exception as e:
                    await event.reply(f"❌ Сэр, ошибка: {e}")
                return

            # ── Команда отправки файла (через текст без файла) ───
            if activated_owner and self.file_sender.is_triggered(query_owner):
                group_id, caption = self.file_sender.parse_command(query_owner)
                self.file_sender.set_pending(sender_id, group_id, caption)

                hint_group = f" (группа {group_id})" if group_id else " (группа не задана — задайте DEFAULT_GROUP_ID в .env)"
                hint_cap   = f"\nПодпись: «{caption}»" if caption else ""
                await event.reply(
                    f"Жду файл, Сэр{hint_group}.{hint_cap}\n\n"
                    f"Отправьте файл следующим сообщением.\n"
                    f"Для отмены напишите: «Джарвис, отмена»"
                )
                return

            # Отмена ожидания файла
            if activated_owner and "отмена" in query_owner.lower():
                self.file_sender.cancel(sender_id)

                await event.reply("Отмена, Сэр. Режим ожидания снят.")
                return

        # ── Список всех команд ────────────────────────────
        if any(p in q_own for p in ["покажи команды","список команд","команды джарвиса","помощь","help","что умеешь","команды","все команды"]):
            cmd_list = (
                "📋 **Все команды Джарвиса**\n\n"

                "**🔧 Системные (ЛС):**\n"
                "`Джарвис, анализ системы` — диагностика 15 систем\n"
                "`Джарвис, перезапустись` — перезапуск\n"
                "`Джарвис, скинь логи` — последние 200 строк лога\n"
                "`Джарвис, стоп` — пауза (любое сообщение снимает)\n\n"

                "**👤 Профиль:**\n"
                "`Джарвис, запомни — меня зовут Максим` — сохранить факт\n"
                "`Джарвис, что ты знаешь обо мне` — показать досье\n"
                "`Джарвис, очисти мой профиль` — удалить всё\n"
                "`Джарвис, отвечай кратко / подробно / с иронией` — стиль\n\n"

                "**🔍 Поиск:**\n"
                "`Джарвис, [вопрос]` — поиск через Tavily\n"
                "`Джарвис, найди на Wiki [тема]` — только Wikipedia\n"
                "`Джарвис, найди видео [тема]` — YouTube\n"
                "`Джарвис, дай ссылку на [сервис]` — кликабельная ссылка\n"
                "`Джарвис, кратко [вопрос]` — короткий ответ\n"
                "`Джарвис, подробно [вопрос]` — развёрнутый ответ\n\n"

                "**👥 Группа (ЛС):**\n"
                "`Джарвис, упомяни @username 5 раз` — спам-пинг\n"
                "`Джарвис, позови @username 3 раза` — то же самое\n"
                "`Джарвис, пингуй офлайн в группе` — пинг всех офлайн\n"
                "`Джарвис, напиши в группу: [текст]` — сообщение в группу\n"
                "`Джарвис, напиши всем [текст]` — рассылка во все чаты\n\n"

                "**📊 Статистика:**\n"
                "`Джарвис, статистика группы` — активность за неделю\n"
                "`Джарвис, статистика чата за день / месяц` — другой период\n"
                "`Джарвис, покажи удалённые сообщения` — что удалили\n\n"

                "**⏰ Напоминания:**\n"
                "`Джарвис, напомни через 30 минут [текст]`\n"
                "`Джарвис, напомни в 18:00 [текст]`\n"
                "`Джарвис, напомни завтра в 9 утра [текст]`\n\n"

                "**📁 Файлы (ЛС):**\n"
                "`Джарвис, отправь файл в группу` — ждёт файл\n"
                "`Джарвис, отправь файл в группу с сообщением [текст]`\n"
                "`Джарвис, отмена` — отменить ожидание\n\n"

                "**🆔 Утилиты:**\n"
                "`Джарвис, id чата` — узнать ID текущего чата\n"
                "`Джарвис, выбери случайно из: вариант1, вариант2` — рандом\n"
                "`Джарвис, команды` — это сообщение"
            )
            await event.reply(cmd_list, parse_mode="md")
            return

        # ══ OWNER-ONLY КОМАНДЫ (защита от чужих) ════════════
        _OWNER_ONLY_TRIGGERS = [
            "скинь логи", "покажи логи", "отправь логи", "лог файл",
            "напиши в группу", "отправь в группу",
            "добавь ссылку", "удали ссылку", "покажи ссылки",
            "мои ссылки",
        ]
        if any(t in text.lower() for t in _OWNER_ONLY_TRIGGERS) and not is_owner:
            await event.reply("🚫 Сэр, эта команда недоступна для вас.")
            return

        # ── ID текущего чата/группы ───────────────────────────
        _ID_TRIGGERS = ["id этой группы", "id чата", "id группы", "покажи id", "chat id", "group id"]
        if any(t in text.lower() for t in _ID_TRIGGERS):
            await event.reply(f"🆔 ID этого чата: `{chat_id}`")
            return

        # ── Скачать логи (только владелец) ────────────────────
        _LOG_TRIGGERS = ["скинь логи", "покажи логи", "отправь логи", "лог файл"]
        if is_owner and is_pm and any(t in text.lower() for t in _LOG_TRIGGERS):
            import config as _cfg
            log_path = _cfg.LOG_FILE
            if log_path.exists() and log_path.stat().st_size > 0:
                try:
                    # Читаем последние 200 строк чтобы не превышать лимит Telegram
                    lines = log_path.read_text("utf-8", errors="replace").splitlines()
                    tail  = "\n".join(lines[-200:])
                    # Шлём как файл через bytes
                    import io
                    log_bytes = io.BytesIO(tail.encode("utf-8"))
                    log_bytes.name = f"jarvis_{datetime.now().strftime('%d%m%Y_%H%M')}.log"
                    await self.client.send_file(
                        sender_id,
                        log_bytes,
                        caption=f"📋 Лог Джарвиса (последние 200 строк) — {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК"
                    )
                except Exception as _le:
                    await event.reply(f"Сэр, ошибка при отправке лога: {_le}")
            else:
                await event.reply("Сэр, лог-файл пуст или не найден.")
            return



        # ── Статистика группы ─────────────────────────────────
        _STATS_TRIGGERS = ["статистика группы", "статистика чата", "активность группы",
                          "сколько сообщений", "активность за неделю"]
        if any(t in text.lower() for t in _STATS_TRIGGERS):
            target_id = chat_id if chat_id else config.DEFAULT_GROUP_ID
            if not target_id:
                await event.reply("Сэр, укажи чат или напиши из группы.")
                return
            days = 7
            if "месяц" in text.lower():
                days = 30
            elif "день" in text.lower() or "сегодня" in text.lower():
                days = 1
            st = _jarvis_db.get_group_stats(target_id, days)
            arrow = "📈" if st["change"] >= 0 else "📉"
            sign  = "+" if st["change"] >= 0 else ""
            top_lines = "\n".join(
                f"  {i+1}. {u['sender']} — {u['cnt']} сообщ."
                for i, u in enumerate(st["top_users"])
            ) if st["top_users"] else "  Нет данных"
            period = {1: "сегодня", 7: "за неделю", 30: "за месяц"}.get(days, f"за {days} дней")
            reply = (
                f"📊 **Статистика группы {period}:**\n\n"
                f"💬 Сообщений: {st['current']}\n"
                f"{arrow} Активность: {sign}{st['change']}% vs прошлый период\n"
                f"🗑 Удалено: {st['deleted']}\n\n"
                f"🏆 Топ участников:\n{top_lines}"
            )
            await event.reply(reply)
            return

        # ── Упомянуть пользователя N раз ─────────────────────
        _MENTION_TRIGGERS = ["упомяни", "пингани", "тегни", "позови"]
        _mention_match = next((t for t in _MENTION_TRIGGERS if t in text.lower()), None)
        if is_owner and _mention_match and "@" in text:
            import re as _re_m
            username_m = _re_m.search(r"@(\w+)", text)
            if username_m:
                uname = username_m.group(1)
                # Ищем число ТОЛЬКО после username — чтобы не захватить цифры в нике
                text_after_user = text[username_m.end():]
                count_m = _re_m.search(r"(\d+)", text_after_user)
                count = min(int(count_m.group(1)) if count_m else 3, 15)
                sent = 0
                for i in range(count):
                    if self._paused:
                        break
                    await self.client.send_message(chat_id, f"@{uname}")
                    sent += 1
                    await asyncio.sleep(0.7)
                await event.reply(f"✅ Упомянул @{uname} {sent} раз, Сэр.")
            else:
                await event.reply("Сэр, укажи username. Пример: «Джарвис, упомяни @username 5 раз»")
            return

        # ── Дай ссылку на [сервис] — поиск + гиперссылка ──────
        _URL_TRIGGERS = ["дай ссылку на", "ссылку на", "ссылка на", "открой ссылку на"]
        _url_match = next((t for t in _URL_TRIGGERS if t in text.lower()), None)
        if _url_match:
            _service = text[text.lower().index(_url_match) + len(_url_match):].strip().rstrip("?.,!")
            if _service:
                # Известные сервисы — быстрый ответ без поиска
                _known = {
                    "github": ("GitHub", "https://github.com"),
                    "google": ("Google", "https://google.com"),
                    "youtube": ("YouTube", "https://youtube.com"),
                    "telegram": ("Telegram", "https://telegram.org"),
                    "wikipedia": ("Wikipedia", "https://ru.wikipedia.org"),
                    "chatgpt": ("ChatGPT", "https://chat.openai.com"),
                    "openai": ("OpenAI", "https://openai.com"),
                    "instagram": ("Instagram", "https://instagram.com"),
                    "vk": ("ВКонтакте", "https://vk.com"),
                    "вконтакте": ("ВКонтакте", "https://vk.com"),
                    "twitter": ("Twitter/X", "https://x.com"),
                    "spotify": ("Spotify", "https://spotify.com"),
                    "netflix": ("Netflix", "https://netflix.com"),
                    "amazon": ("Amazon", "https://amazon.com"),
                    "openrouter": ("OpenRouter", "https://openrouter.ai"),
                    "bothost": ("Bothost", "https://bothost.io"),
                }
                _sl = _service.lower()
                for key, (name, url) in _known.items():
                    if key in _sl:
                        await event.reply(f"[{name}]({url})", parse_mode="md")
                        return
                # Неизвестный сервис — поиск через Tavily
                try:
                    results = await self.agent.searcher.tavily_search(f"official website {_service}", n=1)
                    if results:
                        import re as _re_url
                        _found_url = _re_url.search(r"URL: (https?://\S+)", results[0])
                        if _found_url:
                            u = _found_url.group(1).rstrip("/.,")
                            await event.reply(f"[{_service}]({u})", parse_mode="md")
                            return
                except Exception:
                    pass
                await event.reply(f"Сэр, не нашёл ссылку для «{_service}».")
                return

        
        # ── Написать ВСЕМ (broadcast) ─────────────────────────
        _BROADCAST_TRIGGERS = ["напиши всем", "напиши абсолютно всем", "напиши всем пользователям"]
        _bc_match = next((t for t in _BROADCAST_TRIGGERS if t in text.lower()), None)
        if is_owner and is_pm and _bc_match:
            bc_text = text[text.lower().index(_bc_match) + len(_bc_match):].strip().lstrip(": ")
            if not bc_text:
                await event.reply("Сэр, укажи текст. Пример: «Джарвис, напиши всем Джарвис уходит на ТО»")
                return

            if self.is_bot:
                # Боты не могут получить список диалогов через API.
                # Рассылаем по известным пользователям и группам из нашей БД.
                conn = _jarvis_db._connect()
                # Уникальные пользователи которые писали боту
                user_ids = [r[0] for r in conn.execute(
                    "SELECT DISTINCT sender_id FROM messages WHERE sender_id != 0"
                ).fetchall()]
                # Группы из лога
                group_ids = [r[0] for r in conn.execute(
                    "SELECT DISTINCT chat_id FROM group_messages WHERE chat_id != 0"
                ).fetchall()]
                targets = list(set(user_ids + group_ids))
            else:
                # Пользовательская сессия — iter_dialogs доступен
                targets = []
                async for dialog in self.client.iter_dialogs(limit=100):
                    if dialog.id != sender_id:
                        targets.append(dialog.id)

            if not targets:
                await event.reply("Сэр, пока нет известных чатов для рассылки. Напишите боту хотя бы из одного чата.")
                return

            sent_count = 0
            fail_count = 0
            for tid in targets:
                try:
                    await self.client.send_message(tid, bc_text)
                    sent_count += 1
                    await asyncio.sleep(1.5)
                except Exception:
                    fail_count += 1

            await event.reply(f"✅ Разослано: {sent_count} чатов. Не удалось: {fail_count}, Сэр.")
            return

        # ── Написать в группу от имени Джарвиса ───────────────
        _MSG_GROUP_TRIGGERS = ["напиши в группу:", "напиши в группу ", "отправь в группу:"]
        _matched_mg = next((t for t in _MSG_GROUP_TRIGGERS if t in text.lower()), None)
        if is_owner and is_pm and _matched_mg:
            msg_text = text[text.lower().index(_matched_mg) + len(_matched_mg):].strip()
            target = config.DEFAULT_GROUP_ID
            if msg_text and target:
                await self.client.send_message(target, msg_text)
                await event.reply("✅ Отправлено в группу, Сэр.")
            elif not target:
                await event.reply("Сэр, DEFAULT_GROUP_ID не настроен в .env.")
            else:
                await event.reply("Сэр, текст сообщения пустой.")
            return

                # ── Написать в группу от имени Джарвиса ───────────────
        _MSG_GROUP_TRIGGERS = ["напиши в группу:", "напиши в группу ", "отправь в группу:"]
        _matched_mg = next((t for t in _MSG_GROUP_TRIGGERS if t in text.lower()), None)
        if is_owner and is_pm and _matched_mg:
            msg_text = text[text.lower().index(_matched_mg) + len(_matched_mg):].strip()
            target = config.DEFAULT_GROUP_ID
            if msg_text and target:
                await self.client.send_message(target, msg_text)
                await event.reply("✅ Отправлено в группу, Сэр.")
            elif not target:
                await event.reply("Сэр, DEFAULT_GROUP_ID не настроен в .env.")
            else:
                await event.reply("Сэр, текст сообщения пустой.")
            return

        # ── Быстрые ссылки ────────────────────────────────────
        _LINK_ADD = ["добавь ссылку", "сохрани ссылку", "запомни ссылку"]
        _LINK_GET = ["ссылка на ", "дай ссылку на", "открой ссылку", "покажи ссылку на"]
        _LINK_LIST = ["мои ссылки", "покажи ссылки", "список ссылок"]
        _LINK_DEL  = ["удали ссылку"]

        if is_owner and any(t in text.lower() for t in _LINK_ADD):
            # Формат: Джарвис, добавь ссылку [название] [url]
            parts = re.sub(r"(?i)джарвис[,\s]*", "", text).strip()
            for t in _LINK_ADD:
                parts = re.sub(re.escape(t), "", parts, flags=re.IGNORECASE).strip()
            words = parts.split()
            if len(words) >= 2 and words[-1].startswith("http"):
                name = " ".join(words[:-1])
                url  = words[-1]
                _jarvis_db.save_link(name, url)
                await event.reply(f"✅ Ссылка сохранена, Сэр: «{name}» → {url}")
            else:
                await event.reply("Сэр, формат: «Джарвис, добавь ссылку [название] [url]»")
            return

        if any(t in text.lower() for t in _LINK_LIST) and is_owner:
            links = _jarvis_db.list_links()
            if not links:
                await event.reply("Сэр, сохранённых ссылок нет.")
            else:
                lines = ["🔗 Ваши ссылки:"]
                for lnk in links:
                    lines.append(f"  • {lnk['name']} — {lnk['url']}")
                await event.reply("\n".join(lines))
            return

        _matched_lg = next((t for t in _LINK_GET if t in text.lower()), None)
        if _matched_lg:
            q_link = text.lower().split(_matched_lg, 1)[-1].strip()
            q_link = re.sub(r"(?i)джарвис[,\s]*", "", q_link).strip()
            url = _jarvis_db.get_link(q_link)
            if url:
                await event.reply(f"🔗 {url}")
            else:
                await event.reply(f"Сэр, ссылка «{q_link}» не найдена. Добавьте её: «Джарвис, добавь ссылку {q_link} https://...»")
            return

        if is_owner and any(t in text.lower() for t in _LINK_DEL):
            q_del_lnk = re.sub(r"(?i)(джарвис[,\s]*|удали ссылку)", "", text).strip()
            if _jarvis_db.delete_link(q_del_lnk):
                await event.reply(f"✅ Ссылка «{q_del_lnk}» удалена, Сэр.")
            else:
                await event.reply(f"Сэр, ссылка «{q_del_lnk}» не найдена.")
            return

        # ── Текстовые команды владельца (без файла) ─────────
        if is_owner and is_pm and self.file_sender.is_triggered(text):
            group_id, caption = self.file_sender.parse_command(text)
            self.file_sender.set_pending(sender_id, group_id, caption)
            hint_group = f" (группа {group_id})" if group_id else (" (DEFAULT_GROUP_ID не задан)" if not config.DEFAULT_GROUP_ID else f" (группа {config.DEFAULT_GROUP_ID})")
            hint_cap   = f"\nПодпись: «{caption}»" if caption else ""
            await event.reply(
                f"Жду файл, Сэр{hint_group}.{hint_cap}\n"
                f"Отправьте файл следующим сообщением. Отмена: «Джарвис, отмена»"
            )
            return

        activated, _ = self.agent.is_activated(text)
        if not activated:
            return  # молчим если нет обращения

        # Мгновенный ответ — пока идёт обработка
        q_low = text.lower()
        is_long_query = any(w in q_low for w in [
            "что лучше", "vs ", "versus", "сравни", "compare",
            "лучше чем", "проанализируй", "разбери", "кто прав",
            "диагностика", "анализ систем", "анализ системы",
        ])
        ack_text = "⚙️ Сэр, начинаю сканирование систем... Займёт около 15–20 секунд." if is_long_query else "⏳"
        ack_msg = await event.reply(ack_text)

        # Запускаем обработку
        try:
            resp = await self.agent.process(text, sender_id=sender_id, username=username, chat_id=chat_id)
        except Exception as e:
            logger.error(f"process() error: {e}")
            resp = f"Сэр, произошла ошибка при обработке запроса: {e}"

        # Удаляем ack (⏳), шлём реальный ответ отдельным сообщением
        try:
            await ack_msg.delete()
        except Exception:
            pass

        if resp == "__RESTART__":
            await event.reply("⚡ Сэр, выполняю перезагрузку. Буду онлайн через несколько секунд.")
            await asyncio.sleep(1.5)
            await self.agent.restart()

        elif resp == "__PING_ALL__":
            ping_chat = event.chat_id
            if not ping_chat or event.is_private:
                await event.reply("Сэр, эта команда работает только в группах.")
                return
            try:
                status_msg = await event.reply("Собираю участников...")
                mentions = []
                async for member in self.client.iter_participants(ping_chat):
                    if member.bot or member.id == sender_id:
                        continue
                    name = (member.first_name or "").strip() or member.username or str(member.id)
                    if member.username:
                        mentions.append(f"@{member.username}")
                    else:
                        mentions.append(f"[{name}](tg://user?id={member.id})")
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                if not mentions:
                    await event.reply("Сэр, других участников не найдено.")
                    return
                chunks = [mentions[i:i+20] for i in range(0, len(mentions), 20)]
                for chunk in chunks:
                    await event.respond(" ".join(chunk), parse_mode="md")
            except Exception as e:
                await event.reply(f"Не удалось получить список участников: {e}")

        elif resp:
            logger.info(f"→ {username or sender_id}: {resp[:80]}")
            await event.reply(resp)


# ═══════════════════════════════════════════════════════════════════
#  КОНСОЛЬНЫЙ РЕЖИМ
# ═══════════════════════════════════════════════════════════════════

async def console_mode(agent: JarvisAgent):
    print("JARVIS запущен")

    while True:
        try:
            user_input = input("\n[Вы] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nДо свидания, Сэр.")
            break
        if user_input.lower() in ("exit", "quit", "q"):
            print("До свидания, Сэр.")
            break
        if not user_input:
            continue

        activated, _ = agent.is_activated(user_input)
        if activated:
            print(f"{agent.get_instant_ack()}")

        if True:
            response = await agent.process(user_input)

        if response:
            print("JARVIS запущен")
        elif not activated:
            print("(Начните с «Джарвис,» чтобы активировать)")


# ═══════════════════════════════════════════════════════════════════
#  БАННЕР
# ═══════════════════════════════════════════════════════════════════



def print_banner(agent: JarvisAgent):
    llm   = agent.llm.current_display
    rag   = "OK" if agent.vectorstore else "выключен"
    mode  = "Бот" if config.TELEGRAM_BOT_TOKEN else "Пользователь"
    print(f"JARVIS запущен | LLM: {llm} | RAG: {rag} | Telegram: {mode}")



# ═══════════════════════════════════════════════════════════════════
#  GROUP MONITOR — второй клиент, тихо логирует ВСЕ сообщения групп
# ═══════════════════════════════════════════════════════════════════

class GroupMonitor:
    """
    Запускается параллельно с ботом на юзер-сессии.
    Видит ВСЕ сообщения во всех группах (бот этого не умеет).
    Сохраняет каждое сообщение в GroupLogger.
    Отслеживает удаления через events.MessageDeleted().

    Нужны: TELEGRAM_API_ID, TELEGRAM_API_HASH и файл sessions/user.session
    (создаётся через python create_session.py)
    """

    def __init__(self, agent: JarvisAgent):
        self.agent   = agent
        self.client  = TelegramClient(
            config.USER_SESSION_FILE,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
        self._me_id: int = 0

    async def start(self):
        """Запуск юзер-клиента для мониторинга. Не интерактивный — сессия должна существовать."""
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.warning("GroupMonitor: сессия не авторизована. Запусти python create_session.py")
                return False

            me = await self.client.get_me()
            self._me_id = me.id
            logger.info(f"GroupMonitor запущен: @{me.username} (ID: {me.id})")
            print(f"👁  GroupMonitor: @{me.username} — слежу за группами")

            # ── Все новые сообщения в группах ────────────────
            @self.client.on(events.NewMessage())
            async def on_group_message(event):
                try:
                    msg  = event.message
                    text = msg.text or msg.message or ""
                    if not text:
                        return

                    chat_id   = event.chat_id
                    sender_id = event.sender_id or 0

                    # Получаем имя отправителя
                    sender_name = str(sender_id)
                    try:
                        sender = await event.get_sender()
                        first  = getattr(sender, "first_name", "") or ""
                        last   = getattr(sender, "last_name", "") or ""
                        uname  = getattr(sender, "username", "") or ""
                        sender_name = (first + " " + last).strip() or uname or str(sender_id)
                    except Exception:
                        pass

                    if msg.date:
                        from datetime import timezone, timedelta
                        msk = msg.date.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=3)))
                        date_str = msk.strftime("%H:%M %d.%m.%Y МСК")
                    else:
                        date_str = ""

                    # Сохраняем в GroupLogger (ВСЕ чаты: личные и группы)
                    self.agent.group_logger.save(
                        chat_id   = chat_id,
                        msg_id    = msg.id,
                        sender    = sender_name,
                        sender_id = sender_id,
                        text      = text,
                        date      = date_str,
                    )
                except Exception as e:
                    logger.debug(f"GroupMonitor on_message: {e}")

            # ── Отслеживаем удаления ──────────────────────────
            @self.client.on(events.MessageDeleted())
            async def on_deleted(event):
                try:
                    chat_id = event.chat_id
                    ids = list(event.deleted_ids) if event.deleted_ids else []
                    if not ids:
                        return
                    if chat_id:
                        self.agent.group_logger.mark_deleted(chat_id, ids)
                        logger.info(f"GroupMonitor: помечено удалённых {len(ids)} в чате {chat_id}")
                    else:
                        # chat_id неизвестен — ищем по всем сохранённым чатам
                        self.agent.group_logger.mark_deleted_all_chats(ids)
                        logger.info(f"GroupMonitor: помечено удалённых {len(ids)} (поиск по всем чатам)")
                except Exception as e:
                    logger.debug(f"GroupMonitor on_deleted: {e}")

            await self.client.run_until_disconnected()
            return True

        except Exception as e:
            logger.warning(f"GroupMonitor не запустился: {e}")
            return False


# ═══════════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════════

async def main():
    logger.info("JARVIS ULTIMATE 2026 v4.0 запуск")
    print("Инициализация систем...")

    # ── Проверка системы бэкапа при старте ────────────────
    try:
        import shutil as _sh, sqlite3 as _sq
        _db_path = config.DB_FILE
        if _db_path.exists():
            _test_bk = _db_path.parent / "_startup_backup_test.db"
            _sh.copy2(str(_db_path), str(_test_bk))
            _tc = _sq.connect(str(_test_bk))
            _tc.execute("SELECT COUNT(*) FROM messages").fetchone()
            _tc.close()
            _test_bk.unlink()
            logger.info("✅ Система бэкапа: функционирует — копирование БД работает, отправка владельцу каждый день в 03:00 МСК")
            print("✅ Система бэкапа: функционирует")
        else:
            logger.info("ℹ️  Система бэкапа: БД ещё не создана — начнёт работать после первого сообщения")
            print("ℹ️  Система бэкапа: ожидает создания БД")
    except Exception as _bke:
        logger.error(f"❌ Система бэкапа: ОШИБКА — {_bke}")
        print(f"❌ Система бэкапа: ошибка — {_bke}")

    agent = JarvisAgent()
    print_banner(agent)

    if not (config.TELEGRAM_API_ID and config.TELEGRAM_API_HASH and config.TELEGRAM_API_ID != 0):
        print("⚠️  Telegram не настроен → консольный режим\n")
        await console_mode(agent)
        return

    try:
        tg      = JarvisTelegram(agent)
        monitor = GroupMonitor(agent)

        # Запускаем оба клиента параллельно:
        # — JarvisTelegram: отвечает на команды (бот или юзер)
        # — GroupMonitor:   тихо пишет в лог ВСЕ сообщения групп (только юзер)
        tasks = [asyncio.create_task(tg.start())]

        # GroupMonitor запускается только если есть user.session
        from pathlib import Path as _Path
        user_session = _Path(config.USER_SESSION_FILE)
        if user_session.exists() and user_session.stat().st_size > 100:
            tasks.append(asyncio.create_task(monitor.start()))
            print("🔍 GroupMonitor будет запущен (user.session найден)")
        else:
            print(
                "[yellow]⚠️  GroupMonitor выключен — нет user.session.\n"
                "   Запусти python create_session.py чтобы включить логирование групп.[/yellow]"
            )

        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        logger.error(f"Telegram: {e}")
        print(f"⚠️  Telegram: {e}")
        print("Запускаю консольный режим...\n")
        await console_mode(agent)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nВыключение. До свидания, Сэр.")
        logger.info("JARVIS выключен")
