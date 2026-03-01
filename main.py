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

# ── Поиск ─────────────────────────────────────────────────────
try:
    from ddgs import DDGS          # новое имя пакета (pip install ddgs)
except ImportError:
    from duckduckgo_search import DDGS  # старое имя (fallback)

# ── Playwright ────────────────────────────────────────────────
from playwright.async_api import async_playwright

# ── RAG / Chroma — новый импорт без deprecated warning ────────
try:
    from langchain_chroma import Chroma
except ImportError:
    from langchain_community.vectorstores import Chroma

# Реальные эмбеддинги (sentence-transformers, работает локально, без API)
try:
    from langchain_huggingface import HuggingFaceEmbeddings   # актуальный пакет
    _EMBEDDINGS_AVAILABLE = True
except ImportError:
    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings  # fallback
        _EMBEDDINGS_AVAILABLE = True
    except ImportError:
        _EMBEDDINGS_AVAILABLE = False

from langchain_core.documents import Document

# ── GitHub ────────────────────────────────────────────────────
try:
    import git
    GIT_AVAILABLE = True
except ImportError:
    GIT_AVAILABLE = False




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
        phrases_dir = config.DIRS["phrases"]
        total = 0
        for txt_file in phrases_dir.glob("*.txt"):
            try:
                for line in txt_file.read_text("utf-8", errors="ignore").splitlines():
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
                        # Без категории — в общую
                        self.phrases.setdefault("GENERAL", []).append(line)
                        total += 1
            except Exception as e:
                logger.warning(f"Phrases file {txt_file}: {e}")
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
    """Сохраняет все переписки на диск в chat_history/{sender_id}.json"""

    HISTORY_TRIGGERS = [
        "что я спрашивал", "о чём мы говорили", "найди в переписке",
        "вспомни", "что было раньше", "история чата", "статистика чата",
        "что мы обсуждали", "прошлые разговоры", "найди в истории",
        "сколько сообщений", "наша переписка",
    ]

    def __init__(self):
        self.dir    = config.DIRS["chat_history"]
        self._cache : dict[int, list[dict]] = {}

    def _path(self, sid: int) -> Path:
        return self.dir / f"{sid}.json"

    def load(self, sid: int) -> list[dict]:
        if sid in self._cache:
            return self._cache[sid]
        p = self._path(sid)
        try:
            data = json.loads(p.read_text("utf-8")) if p.exists() else []
        except Exception:
            data = []
        self._cache[sid] = data
        return data

    def save_message(self, sid: int, role: str, text: str, username: str = ""):
        history = self.load(sid)
        history.append({
            "ts":       datetime.now().isoformat(timespec="seconds"),
            "role":     role,
            "text":     text,
            "username": username,
        })
        self._cache[sid] = history
        try:
            self._path(sid).write_text(
                json.dumps(history, ensure_ascii=False, indent=2), "utf-8"
            )
        except Exception as e:
            logger.warning(f"ChatHistory write {sid}: {e}")

    def search(self, sid: int, query: str, limit: int = 20) -> list[dict]:
        ql = query.lower()
        return [m for m in self.load(sid) if ql in m.get("text","").lower()][-limit:]

    def get_recent(self, sid: int, n: int = 30) -> list[dict]:
        return self.load(sid)[-n:]

    def format_for_llm(self, msgs: list[dict]) -> str:
        lines = []
        for m in msgs:
            role = "Пользователь" if m["role"] == "user" else "Джарвис"
            lines.append(f"[{m.get('ts','?')}] {role}: {m['text']}")
        return "\n".join(lines)

    def stats(self, sid: int) -> dict:
        h = self.load(sid)
        if not h:
            return {"total": 0}
        return {
            "total":     len(h),
            "user_msgs": sum(1 for m in h if m["role"] == "user"),
            "bot_msgs":  sum(1 for m in h if m["role"] == "jarvis"),
            "first_date": h[0].get("ts","?"),
            "last_date":  h[-1].get("ts","?"),
        }

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
        # Поиск по ключевому слову
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

    async def analyze(self, conversation: str, llm: UniversalLLMConnector) -> str:
        if not conversation.strip():
            return self._help()
        return await llm.complete([
            {"role": "system", "content": self.SYSTEM},
            {"role": "user",   "content": f"Разбери переписку:\n\n{conversation}"},
        ], max_tokens=2500)

    async def analyze_forwarded(self, msgs: list[dict], llm: UniversalLLMConnector) -> str:
        if not msgs:
            return "Сэр, нет сообщений для анализа."
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
    """
    Автоматически сохраняет каждое сообщение из всех групп.
    Если кто-то удалит сообщение — оно останется в архиве.

    Файлы: chat_history/group_{chat_id}.json
    Запрос: «Джарвис, покажи удалённые сообщения»
             «Джарвис, что удалили в чате»
    """

    DELETED_TRIGGERS = [
        "покажи удалённые", "покажи удаленные", "что удалили",
        "удалённые сообщения", "удаленные сообщения",
        "что стёрли", "show deleted", "удалённые за", "удаленные за",
    ]

    def __init__(self):
        self.dir = config.DIRS["chat_history"]

    def _path(self, chat_id: int) -> Path:
        return self.dir / f"group_{chat_id}.json"

    def save(self, chat_id: int, msg_id: int, sender: str, sender_id: int,
             text: str, date: str):
        """Сохранить сообщение из группы."""
        if not text:
            return
        p = self._path(chat_id)
        try:
            data: dict = json.loads(p.read_text("utf-8")) if p.exists() else {}
        except Exception:
            data = {}

        data[str(msg_id)] = {
            "id":        msg_id,
            "sender":    sender,
            "sender_id": sender_id,
            "text":      text,
            "date":      date,
            "deleted":   False,
        }
        try:
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        except Exception as e:
            logger.warning(f"GroupLogger save: {e}")

    def mark_deleted(self, chat_id: int, msg_ids: list[int]):
        """Пометить сообщения как удалённые."""
        p = self._path(chat_id)
        if not p.exists():
            return
        try:
            data = json.loads(p.read_text("utf-8"))
            changed = False
            for mid in msg_ids:
                if str(mid) in data:
                    data[str(mid)]["deleted"] = True
                    changed = True
            if changed:
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        except Exception as e:
            logger.warning(f"GroupLogger mark_deleted: {e}")

    def cleanup_old_deleted(self, chat_id: int, days: int = 30):
        """Удаляет из архива сообщения помеченные как deleted старше N дней."""
        p = self._path(chat_id)
        if not p.exists():
            return
        try:
            from datetime import datetime, timedelta
            data    = json.loads(p.read_text("utf-8"))
            cutoff  = datetime.now() - timedelta(days=days)
            removed = 0
            to_delete = []
            for key, msg in data.items():
                if not msg.get("deleted"):
                    continue
                # Парсим дату из формата "HH:MM DD.MM.YYYY МСК"
                try:
                    date_str = msg.get("date", "").replace(" МСК", "").strip()
                    dt = datetime.strptime(date_str, "%H:%M %d.%m.%Y")
                    if dt < cutoff:
                        to_delete.append(key)
                except Exception:
                    pass  # не можем распарсить — не трогаем
            for key in to_delete:
                del data[key]
                removed += 1
            if removed:
                p.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
                logger.info(f"Очищено {removed} старых удалённых сообщений из чата {chat_id}")
        except Exception as e:
            logger.warning(f"GroupLogger cleanup: {e}")

    def cleanup_all_chats(self, days: int = 30):
        """Запускает очистку по всем сохранённым чатам."""
        for f in self.dir.glob("group_*.json"):
            try:
                chat_id = int(f.stem.replace("group_", ""))
                self.cleanup_old_deleted(chat_id, days)
            except Exception:
                pass

    def get_deleted(self, chat_id: int, limit: int = 20, date_filter: str = "") -> list[dict]:
        """Получить удалённые сообщения. date_filter = "DD.MM.YYYY" для фильтрации по дате."""
        self.cleanup_old_deleted(chat_id, days=30)
        p = self._path(chat_id)
        if not p.exists():
            return []
        try:
            data    = json.loads(p.read_text("utf-8"))
            deleted = [v for v in data.values() if v.get("deleted")]
            if date_filter:
                # Фильтруем по дате — ищем сообщения содержащие указанную дату
                deleted = [m for m in deleted if date_filter in m.get("date", "")]
            return sorted(deleted, key=lambda x: x.get("date", ""))[-limit:]
        except Exception:
            return []

    def get_all(self, chat_id: int, limit: int = 50) -> list[dict]:
        """Получить все сообщения для анализа спора."""
        p = self._path(chat_id)
        if not p.exists():
            return []
        try:
            data = json.loads(p.read_text("utf-8"))
            msgs = list(data.values())
            return sorted(msgs, key=lambda x: x.get("date",""))[-limit:]
        except Exception:
            return []

    @classmethod
    def is_deleted_request(cls, q: str) -> bool:
        return any(t in q.lower() for t in cls.DELETED_TRIGGERS)

    def format_deleted(self, msgs: list[dict], date_filter: str = "") -> str:
        if not msgs:
            if date_filter:
                return f"Сэр, удалённых сообщений за {date_filter} не найдено."
            return "Сэр, удалённых сообщений в архиве нет."
        header = f"🗑 Удалённые за {date_filter}:" if date_filter else f"🗑 Удалённые ({len(msgs)} шт.):"
        lines  = [header]
        for m in msgs:
            text   = m.get("text", "")
            # Обрезаем длинные тексты
            if len(text) > 120:
                text = text[:120] + "…"
            lines.append(f"[{m.get('date','?')}] {m.get('sender','?')}: {text}")
        return "\n".join(lines)



# ═══════════════════════════════════════════════════════════════════
#  ГЛАВНЫЙ АГЕНТ
# ═══════════════════════════════════════════════════════════════════

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
        self.phrase_bank  = PhraseBank()
        self.group_logger = GroupLogger()

        # Per-user контекст — каждый пользователь имеет свою историю диалога
        self._user_context: dict[int, list[dict]] = {}
        self.vectorstore  : Chroma | None = None
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
                from langchain_community.embeddings import FakeEmbeddings
                self.embeddings = FakeEmbeddings(size=384)
        else:
            from langchain_community.embeddings import FakeEmbeddings
            self.embeddings = FakeEmbeddings(size=384)
            logger.warning("sentence-transformers не установлен")
        self.system_prompt: str = ""

        self._fwd_buffer  : dict[int, list[dict]] = {}

        self._init_vectorstore()
        self._load_qa()
        self._load_system_prompt()
        self._load_existing_knowledge()

    # ── Init ─────────────────────────────────────────────────

    def _init_vectorstore(self):
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
        for f in config.DIRS["knowledge"].glob("*"):
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
        ql = query.lower()
        for k, v in self.qa_responses.items():
            if k.lower() in ql:
                return v
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
        chunks = [
            Document(page_content=text[i:i+800], metadata={"source": path.name})
            for i in range(0, len(text), 700) if text[i:i+800].strip()
        ]
        if self.vectorstore and chunks:
            self.vectorstore.add_documents(chunks)
            self.vectorstore.persist()
        return len(chunks)

    async def handle_document(self, file_bytes: bytes, filename: str) -> str:
        p = config.DIRS["knowledge"] / filename
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

    async def web_search(self, query: str, n: int = 5) -> list[str]:
        results = []
        for backend in ("api", "html", "lite"):
            try:
                with DDGS() as ddgs:
                    for r in ddgs.text(query, max_results=n, backend=backend):
                        results.append(f"[{r.get('title','')}]\n{r.get('body','')}\nURL: {r.get('href','')}")
                if results:
                    break  # получили результаты — выходим
            except Exception as e:
                err = str(e)
                if "network" in err.lower() or "timeout" in err.lower() or "connect" in err.lower():
                    logger.warning(f"DDG {backend}: сетевая ошибка, пробуем следующий бэкенд")
                    continue
                logger.warning(f"DDG {backend}: {err}")
                continue
        if config.TAVILY_API_KEY:
            try:
                from tavily import TavilyClient
                for r in TavilyClient(config.TAVILY_API_KEY).search(query, max_results=3).get("results",[]):
                    results.append(f"[Tavily: {r.get('title','')}]\n{r.get('content','')}")
            except Exception as e:
                logger.warning(f"Tavily: {e}")
        return results

    async def fetch_page(self, url: str) -> str:
        try:
            async with async_playwright() as p:
                b    = await p.chromium.launch(headless=True)
                page = await b.new_page()
                await page.goto(url, timeout=15000, wait_until="domcontentloaded")
                text = await page.evaluate(
                    "() => Array.from(document.querySelectorAll('p,h1,h2,h3,li')).map(e=>e.innerText).join('\\n')"
                )
                await b.close()
                return text[:4000]
        except Exception as e:
            logger.warning(f"Playwright {url}: {e}")
            return ""

    async def deep_research(self, query: str) -> str:
        """Реальный глубокий анализ: несколько поисковых запросов + чтение страниц."""
        all_results = []

        # 3 разных поисковых запроса для полного охвата
        searches = [
            f"{query} плюсы минусы",
            f"{query} сравнение обзор",
            f"{query} отзывы эксперты 2024 2025",
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

    def _get_user_context(self, sender_id: int) -> list[dict]:
        """Получить историю диалога конкретного пользователя."""
        return self._user_context.setdefault(sender_id, [])

    def _save_user_context(self, sender_id: int, query: str, answer: str):
        """Добавить пару вопрос/ответ в контекст пользователя."""
        ctx = self._get_user_context(sender_id)
        ctx.append({"role": "user",      "content": query})
        ctx.append({"role": "assistant", "content": answer})
        # Хранить последние 30 пар (60 сообщений) — достаточно для длинного диалога
        if len(ctx) > 60:
            self._user_context[sender_id] = ctx[-60:]

    async def call_llm(self, query: str, context: str = "", rag_context: str = "",
                       is_comparison: bool = False, sender_id: int = 0) -> str:
        sys_p = self.system_prompt
        if is_comparison:
            sys_p += "\n\nЭто запрос на сравнение/анализ. Дай подробный структурированный ответ: плюсы, минусы, итоговый вывод."

        # Берём контекст этого конкретного пользователя
        user_ctx = self._get_user_context(sender_id)

        messages: list[dict] = [{"role": "system", "content": sys_p}]
        messages += user_ctx  # вся история диалога с этим пользователем

        user_content = query
        if rag_context:
            user_content = f"[База знаний]\n{rag_context}\n\n[Вопрос]\n{query}"
        if context:
            user_content = f"[Данные из интернета]\n{context[:6000]}\n\n[Вопрос]\n{query}"
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
            async with httpx.AsyncClient(timeout=8) as client:
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

        # ── 3. Интернет — реальный поиск ─────────────────────
        try:
            search_res = await self.web_search("python programming", n=2)
            if search_res:
                results.append(("Поиск (DuckDuckGo)", True, f"получено {len(search_res)} результат(а)"))
            else:
                results.append(("Поиск (DuckDuckGo)", False, "пустой результат"))
        except Exception as e:
            results.append(("Поиск (DuckDuckGo)", False, str(e)[:80]))

        # ── 4. Playwright — открытие страницы ────────────────
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                page    = await browser.new_page()
                await page.goto("https://example.com", timeout=12000, wait_until="domcontentloaded")
                title   = await page.title()
                await browser.close()
            results.append(("Playwright (браузер)", True, f"страница открыта, title: {title}"))
        except Exception as e:
            results.append(("Playwright (браузер)", False, str(e)[:80]))

        # ── 5. RAG / ChromaDB ─────────────────────────────────
        if self.vectorstore:
            try:
                col_count = self.vectorstore._collection.count()
                results.append(("ChromaDB (RAG)", True, f"работает, документов в базе: {col_count}"))
            except Exception as e:
                results.append(("ChromaDB (RAG)", False, str(e)[:80]))
        else:
            results.append(("ChromaDB (RAG)", False, "не инициализирована"))

        # ── 6. Файловая структура ─────────────────────────────
        missing_dirs = [name for name, dpath in config.DIRS.items() if not dpath.exists()]
        if missing_dirs:
            results.append(("Папки проекта", False, f"отсутствуют: {', '.join(missing_dirs)}"))
        else:
            results.append(("Папки проекта", True, f"все {len(config.DIRS)} папок на месте"))

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
            results.append(("Telegram сессия", False, f"файл {bot_session.name} подозрительно маленький — возможно повреждён"))
        else:
            results.append(("Telegram сессия", False,
                f"{bot_session.name} не найден — запусти python create_session.py"))

        # ── 10. История чатов ─────────────────────────────────
        history_dir  = config.DIRS["chat_history"]
        history_files = list(history_dir.glob("*.json"))
        results.append(("История чатов", True, f"папка есть, файлов: {len(history_files)}"))

        # ── 11. База знаний (knowledge) ───────────────────────
        knowledge_dir   = config.DIRS["knowledge"]
        knowledge_files = [f for f in knowledge_dir.iterdir() if f.suffix.lower() in (".pdf",".txt",".docx",".md")]
        results.append(("База знаний (RAG файлы)", True, f"файлов загружено: {len(knowledge_files)}"))

        # ── 12. Фразы Джарвиса ────────────────────────────────
        if self.phrase_bank.total > 0:
            results.append(("Фразы Джарвиса", True,
                f"{self.phrase_bank.total} фраз в {len(self.phrase_bank.phrases)} категориях"))
        else:
            results.append(("Фразы Джарвиса", False, "phrases/iron_man/ пуст"))

        # ── Формируем отчёт ───────────────────────────────────
        ok_list  = [(name, detail) for name, ok, detail in results if ok]
        err_list = [(name, detail) for name, ok, detail in results if not ok]

        if not err_list:
            # Всё в порядке — только краткий ответ
            return "Сэр, все системы работают в штатном режиме. ✅"
        else:
            # Есть проблемы — показываем только что сломано
            report = [f"Сэр, обнаружено проблем: {len(err_list)} из {len(results)} систем.", ""]
            for name, detail in err_list:
                report.append(f"  ❌ {name}: {detail}")
            return "\n".join(report)

    # ── Other ─────────────────────────────────────────────────

    async def restart(self):
        logger.info("Перезагрузка")
        print("⚡ Перезагрузка...")
        await asyncio.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    async def git_push(self) -> str:
        if not GIT_AVAILABLE:
            return "Сэр, GitPython не установлен."
        try:
            repo = git.Repo(config.GITHUB_REPO_PATH)
            repo.git.add(A=True)
            for s in [".env", "sessions/"]:
                try: repo.index.reset(paths=[s])
                except Exception: pass
            repo.index.commit("JARVIS auto-commit 🤖")
            repo.remote(config.GITHUB_REMOTE).push(refspec=f"HEAD:{config.GITHUB_BRANCH}")
            return f"Сэр, запушено в {config.GITHUB_REMOTE}/{config.GITHUB_BRANCH}."
        except Exception as e:
            return f"Сэр, Git ошибка: {e}"

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
            lookup_id   = chat_id if chat_id and chat_id != sender_id else sender_id
            deleted     = self.group_logger.get_deleted(lookup_id, date_filter=date_filter)
            answer      = self.group_logger.format_deleted(deleted, date_filter=date_filter)
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer

        # 3. Спор
        if DisputeAnalyzer.is_triggered(query):
            chat_text = DisputeAnalyzer.strip_trigger(query)
            fwd       = self._fwd_buffer.pop(sender_id, [])
            if fwd:
                answer = await self.dispute.analyze_forwarded(fwd, self.llm)
            elif chat_text:
                answer = await self.dispute.analyze(chat_text, self.llm)
            else:
                answer = DisputeAnalyzer._help()
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

        if any(p in q_lower for p in [
            "перезагрузка", "перезагрузись", "перезагрузка системы",
            "перезапуск", "перезапустись", "restart", "reboot",
        ]):
            # Сначала отвечаем, потом перезагружаемся
            return "__RESTART__"

        if any(p in q_lower for p in ["запушь в github", "git push", "запушить"]):
            answer = await self.git_push()
            self.chat_history.save_message(sender_id, "jarvis", answer)
            return answer

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
        is_cmp = any(w in q_lower for w in [
            "что лучше", "vs ", "versus", "сравни", "compare", " или ", "лучше чем"
        ])
        print(f"🌐 {'Deep research' if is_cmp else 'Поиск'}: {query}")
        web = await self.deep_research(query) if is_cmp else "\n\n---\n\n".join(
            await self.web_search(query, 4)
        )

        answer = await self.call_llm(query=query, context=web, rag_context=rag_context, is_comparison=is_cmp, sender_id=sender_id)

        # Умный подбор фразы по контексту запроса
        phrase = self.phrase_bank.get(context=query + " " + answer, chance=0.20)
        full_answer = answer + phrase

        self.chat_history.save_message(sender_id, "jarvis", full_answer)
        return full_answer



# ═══════════════════════════════════════════════════════════════════
#  FILE SENDER — отправка файлов в группу по команде из личных сообщений
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
        self.agent       = agent
        self.is_bot      = bool(config.TELEGRAM_BOT_TOKEN)
        self.file_sender = FileSender()

        # Выбор файла сессии — бот и юзер НЕ смешиваются
        session_file = config.BOT_SESSION_FILE if self.is_bot else config.USER_SESSION_FILE

        self.client = TelegramClient(
            session_file,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
        logger.info(f"Telegram mode: {'BOT' if self.is_bot else 'USER'}, session: {session_file}")

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
                if chat_id and event.deleted_ids:
                    self.agent.group_logger.mark_deleted(chat_id, list(event.deleted_ids))
                    logger.info(f"Удалено {len(event.deleted_ids)} сообщений в чате {chat_id}")
            except Exception as e:
                logger.warning(f"on_deleted: {e}")

        print("🎯 Джарвис слушает команды...\n")
        await self.client.run_until_disconnected()

    async def _handle(self, event):
        """Единый обработчик всех входящих сообщений."""
        msg       = event.message
        sender_id = event.sender_id or 0
        username  = ""

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

        # ── Документ ─────────────────────────────────────────
        if msg.document or msg.photo or msg.video or msg.audio or msg.voice:

            # Если владелец отправил файл и мы ждём его для пересылки в группу
            if sender_id == config.OWNER_ID and self.file_sender.is_waiting(sender_id):
                pending = self.file_sender.get_pending(sender_id)
                group_id = pending["group_id"]
                caption  = pending["caption"]
                self.file_sender.clear_pending(sender_id)

                if not group_id:
                    await event.reply(
                        "Сэр, ID группы не задан.\n"
                        "Добавьте OWNER_ID и DEFAULT_GROUP_ID в .env\n"
                        "Или укажите ID в команде: «отправь файл в группу -1001234567890»"
                    )
                    return

                try:
                    # Пересылаем файл в группу
                    await self.client.send_file(
                        entity   = group_id,
                        file     = msg.media,
                        caption  = caption or None,
                    )
                    await event.reply(f"✅ Сэр, файл отправлен в группу {group_id}.")
                except Exception as e:
                    await event.reply(f"❌ Сэр, не удалось отправить файл: {e}")
                return

            # Обычная обработка документов (индексация в RAG)
            if msg.document:
                for attr in msg.document.attributes:
                    if isinstance(attr, DocumentAttributeFilename):
                        fname = attr.file_name
                        if Path(fname).suffix.lower() in (".pdf", ".txt", ".docx", ".md"):
                            try:
                                fb   = await self.client.download_media(msg.document, file=bytes)
                                resp = await self.agent.handle_document(fb, fname)
                                await event.reply(resp)
                            except Exception as e:
                                await event.reply(f"Сэр, ошибка при обработке файла: {e}")
            return

        # ── Пересланные сообщения → буфер для анализа спора ─
        if msg.fwd_from:
            fwd_name = "Неизвестный"
            try:
                entity   = await self.client.get_entity(msg.fwd_from.from_id)
                fwd_name = getattr(entity, "first_name", None) or getattr(entity, "username", "?")
            except Exception:
                pass
            self.agent.buffer_forwarded(sender_id, {
                "sender": fwd_name,
                "text":   msg.text or "",
                "date":   (_d := msg.date) and _d.replace(tzinfo=__import__('datetime').timezone.utc).astimezone(__import__('datetime').timezone(__import__('datetime').timedelta(hours=3))).strftime("%H:%M %d.%m МСК") or "",
            })
            # Сообщаем что получили
            await event.reply("Сэр, сообщение добавлено в буфер. Когда перешлёте все — напишите «Джарвис, разбери переписку».")
            return

        # ── Текстовое сообщение ───────────────────────────────
        text = msg.text or ""
        if not text:
            return

        # Сохранить ВСЕ сообщения в групповой архив (для защиты от удаления)
        chat_id = event.chat_id or sender_id
        if chat_id and text:
            if msg.date:
                from datetime import timezone, timedelta
                msk = msg.date.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=3)))
                date_str = msk.strftime("%H:%M %d.%m.%Y МСК")
            else:
                date_str = ""
            self.agent.group_logger.save(
                chat_id=chat_id,
                msg_id=msg.id,
                sender=username or str(sender_id),
                sender_id=sender_id,
                text=text,
                date=date_str,
            )

        # ══════════════════════════════════════════════════════════
        # КОМАНДЫ ТОЛЬКО ДЛЯ ВЛАДЕЛЬЦА — только личка
        # ══════════════════════════════════════════════════════════
        if sender_id == config.OWNER_ID and event.is_private:
            activated_owner, query_owner = self.agent.is_activated(text)

            if activated_owner:
                q_own = query_owner.lower().strip()

                # ── Список всех команд ────────────────────────────
                if any(p in q_own for p in [
                    "покажи команды", "список команд", "команды джарвиса",
                    "помощь", "help", "что умеешь", "команды",
                    "покажи все команды", "список всех команд",
                ]):
                    cmd_list = (
                        "📋 *Команды Джарвиса* (только тебе, только в личке)\n\n"
                        "*🔧 Системные:*\n"
                        "`Джарвис, проверь системы` — диагностика всех систем\n"
                        "`Джарвис, перезапустись` — перезапуск бота\n"
                        "`Джарвис, запушь в GitHub` — git push\n\n"
                        "*📁 Файлы в группу:*\n"
                        "`Джарвис, отправь файл в группу` — ждёт файл, отправит в дефолтную группу\n"
                        "`Джарвис, отправь файл в группу с сообщением [текст]` — с подписью\n"
                        "`Джарвис, отправь файл в группу -100xxxxxxxxx` — в конкретную группу\n"
                        "`Джарвис, отмена` — отменить ожидание файла\n\n"
                        "*👥 Группа:*\n"
                        "`Джарвис, пингуй офлайн в группе` — упомянуть всех кто не в сети\n"
                        "`Джарвис, пингуй офлайн в группе -100xxxxxxxxx` — в конкретной группе\n"
                        "`Джарвис, покажи удалённые сообщения` — архив удалённых\n\n"
                        "*💬 Общение:*\n"
                        "`Джарвис, [вопрос]` — ответ с веб-поиском\n"
                        "`Джарвис, разбери переписку` — анализ конфликта\n"
                        "`Джарвис, кто прав` — вердикт спора\n\n"
                        "*🔍 Поиск:*\n"
                        "`Джарвис, [тема] vs [тема]` — глубокое сравнение\n"
                        "`Джарвис, сравни [A] и [B]` — анализ плюсов и минусов\n\n"
                        "*⚙️ Настройка:*\n"
                        "OWNER_ID и DEFAULT_GROUP_ID — в файле .env"
                    )
                    await event.reply(cmd_list, parse_mode="md")
                    return

                # ── Пингуй офлайн в группе ────────────────────────
                if any(p in q_own for p in [
                    "пингуй офлайн", "пинг офлайн", "пингани офлайн",
                    "ping offline", "упомяни офлайн", "позови офлайн"
                ]):
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

            # ── Команда отправки файла ────────────────────────────
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
            if activated_owner and "отмена" in query_owner.lower() and self.file_sender.is_waiting(sender_id):
                self.file_sender.cancel(sender_id)
                await event.reply("Отмена, Сэр. Режим ожидания файла снят.")
                return

        activated, _ = self.agent.is_activated(text)
        if not activated:
            return  # молчим если нет обращения

        # Мгновенный ответ «Слушаю, Сэр» — пока идёт обработка
        ack_msg = await event.reply(self.agent.get_instant_ack())

        # Определяем — это сравнение/анализ (долгий запрос)?
        q_low = text.lower()
        is_long_query = any(w in q_low for w in [
            "что лучше", "vs ", "versus", "сравни", "compare", " или ",
            "лучше чем", "проанализируй", "разбери", "кто прав",
            "диагностика", "анализ систем",
        ])

        # Для долгих запросов — показываем прогресс
        if is_long_query:
            try:
                await ack_msg.edit("🔍 Сэр, собираю данные из нескольких источников...")
            except Exception:
                pass

        # Запускаем обработку
        try:
            resp = await self.agent.process(text, sender_id=sender_id, username=username, chat_id=chat_id)
        except Exception as e:
            logger.error(f"process() error: {e}")
            resp = f"Сэр, произошла ошибка при обработке запроса: {e}"

        if resp == "__RESTART__":
            try:
                await ack_msg.edit("⚡ Сэр, выполняю перезагрузку. Буду онлайн через несколько секунд.")
            except Exception:
                await event.reply("⚡ Сэр, выполняю перезагрузку. Буду онлайн через несколько секунд.")
            await asyncio.sleep(1.5)
            await self.agent.restart()

        elif resp == "__PING_ALL__":
            # Пинг всех участников группы
            ping_chat = event.chat_id
            if not ping_chat or event.is_private:
                try:
                    await ack_msg.edit("Сэр, эта команда работает только в группах.")
                except Exception:
                    pass
                return
            try:
                await ack_msg.edit("Собираю участников...")
                mentions = []
                async for member in self.client.iter_participants(ping_chat):
                    if member.bot or member.id == sender_id:
                        continue
                    name = (member.first_name or "").strip() or member.username or str(member.id)
                    if member.username:
                        mentions.append(f"@{member.username}")
                    else:
                        mentions.append(f"[{name}](tg://user?id={member.id})")

                if not mentions:
                    await ack_msg.edit("Сэр, других участников не найдено.")
                    return

                # Шлём пинг чанками по 20 (Telegram лимит на упоминания)
                chunks = [mentions[i:i+20] for i in range(0, len(mentions), 20)]
                first  = True
                for chunk in chunks:
                    ping_text = " ".join(chunk)
                    if first:
                        await ack_msg.edit(ping_text, parse_mode="md")
                        first = False
                    else:
                        await event.respond(ping_text, parse_mode="md")

            except Exception as e:
                try:
                    await ack_msg.edit(f"Не удалось получить список участников: {e}")
                except Exception:
                    pass

        elif resp:
            logger.info(f"→ {username or sender_id}: {resp[:80]}")
            try:
                await ack_msg.edit(resp)
            except Exception:
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
                    if chat_id and event.deleted_ids:
                        ids = list(event.deleted_ids)
                        self.agent.group_logger.mark_deleted(chat_id, ids)
                        logger.info(f"GroupMonitor: помечено удалённых {len(ids)} в чате {chat_id}")
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
