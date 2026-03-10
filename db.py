"""
╔══════════════════════════════════════════════════════════╗
║  JARVIS — db.py  v7.0  (чистый SQLite + Яндекс.Диск)   ║
║                                                          ║
║  ✅ SQLite WAL — без VPN, без интернета                  ║
║  ✅ При старте — автозагрузка актуальной БД с Я.Диска   ║
║  ✅ Ночной бэкап → Яндекс.Диск (хранит 14 бэкапов)     ║
║  ✅ Восстановление по дате через команду                 ║
║  ✅ Все таблицы через CREATE TABLE IF NOT EXISTS         ║
╚══════════════════════════════════════════════════════════╝
"""

import collections, json, os, shutil, sqlite3, tempfile, threading, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from loguru import logger


# ── Утилиты ──────────────────────────────────────────────────────────────

def _msk() -> str:
    return datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

def _iso() -> str:
    return datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")


# ── Синглтон ─────────────────────────────────────────────────────────────

_instance: Optional["JarvisDB"] = None

def get_db() -> "JarvisDB":
    global _instance
    if _instance is None:
        _instance = JarvisDB()
    return _instance


# ── LRU-кэш профилей ─────────────────────────────────────────────────────

class _Cache:
    """LRU-кэш. TTL=5 мин, max=500 записей."""
    def __init__(self, maxsize=500, ttl=300):
        self._d, self._t = {}, {}
        self._q = collections.deque()
        self.maxsize, self.ttl = maxsize, ttl
        self._hits = self._miss = 0

    def get(self, k):
        v = self._d.get(k)
        if v and time.time() - self._t.get(k, 0) < self.ttl:
            self._hits += 1; return v
        self._miss += 1; return None

    def set(self, k, v):
        if k not in self._d and len(self._d) >= self.maxsize:
            old = self._q.popleft()
            self._d.pop(old, None); self._t.pop(old, None)
        self._d[k] = v; self._t[k] = time.time()
        if k not in self._q: self._q.append(k)

    def drop(self, k):
        self._d.pop(k, None); self._t.pop(k, None)

    def clear(self):
        self._d.clear(); self._t.clear(); self._q.clear()

    def info(self) -> str:
        n = self._hits + self._miss
        p = round(self._hits / n * 100) if n else 0
        return f"cache={len(self._d)} hit={p}%"


    # ── Файлы с кодом ────────────────────────────────────────
    def save_code_file(self, sender_id: int, filename: str, content: str,
                       language: str = "") -> int:
        r = self._q(
            "INSERT INTO code_files (sender_id, filename, language, content, size_bytes) "
            "VALUES (?,?,?,?,?) RETURNING id",
            (sender_id, filename, language, content, len(content.encode())),
            fetch="one"
        )
        return (r or {}).get("id", 0)

    def get_code_files(self, sender_id: int, limit: int = 10) -> list[dict]:
        return self._q(
            "SELECT id, filename, language, size_bytes, saved_at, "
            "substr(content,1,100) as preview "
            "FROM code_files WHERE sender_id=? ORDER BY saved_at DESC LIMIT ?",
            (sender_id, limit), fetch="all"
        ) or []

    def get_code_file(self, file_id: int, sender_id: int) -> Optional[dict]:
        return self._q(
            "SELECT * FROM code_files WHERE id=? AND sender_id=?",
            (file_id, sender_id), fetch="one"
        )

    def update_code_review(self, file_id: int, review: str):
        self._q("UPDATE code_files SET last_review=? WHERE id=?", (review[:2000], file_id))

    def delete_code_file(self, file_id: int, sender_id: int):
        self._q("DELETE FROM code_files WHERE id=? AND sender_id=?", (file_id, sender_id))


# ═════════════════════════════════════════════════════════════════════════
#  JARVIS DB
# ═════════════════════════════════════════════════════════════════════════

class JarvisDB:
    """
    Единственная БД Джарвиса — SQLite (WAL mode).

    Таблицы:
      user_messages     — история ЛС
      user_profiles     — профили пользователей (JSON факты)
      group_messages    — лог групповых чатов
      group_profiles    — данные групп
      bot_chats         — чаты где бот активен
      reminders         — напоминания
      links             — быстрые ссылки
      weekly_stats      — еженедельная статистика
      akinator_knowledge— база знаний акинатора
    """

    # Без лимитов — хранить всё
    GRP_DAYS  = 365    # групповые сообщения хранятся 1 год

    # ── Init ──────────────────────────────────────────────────────────────

    def __init__(self):
        import config as cfg
        self._path = str(cfg.DB_FILE)
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._cache = _Cache()
        self._buf:  list = []
        self._bufl = threading.Lock()
        self._flushed = time.time()
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._open()
        self._schema()
        logger.info(f"✅ SQLite: {self._path}")

    # ── Connection ────────────────────────────────────────────────────────

    def _open(self) -> sqlite3.Connection:
        if self._conn is None:
            c = sqlite3.connect(self._path, check_same_thread=False, timeout=30)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")       # конкурентный доступ
            c.execute("PRAGMA synchronous=NORMAL")        # баланс скорость/надёжность
            c.execute("PRAGMA cache_size=-131072")        # 128 MB RAM кэш (было 32MB)
            c.execute("PRAGMA temp_store=MEMORY")         # временные таблицы в RAM
            c.execute("PRAGMA mmap_size=1073741824")      # 1 GB memory-map (было 256MB)
            c.execute("PRAGMA page_size=8192")            # 8KB страницы (эффективнее для больших данных)
            c.execute("PRAGMA foreign_keys=ON")
            c.execute("PRAGMA auto_vacuum=INCREMENTAL")   # постепенная очистка, без VACUUM
            c.execute("PRAGMA wal_autocheckpoint=1000")   # WAL checkpoint каждые 1000 страниц
            c.execute("PRAGMA busy_timeout=10000")        # 10 сек таймаут на блокировку
            self._conn = c
        return self._conn

    def _q(self, sql: str, p: tuple = (), fetch: str = "none"):
        """Universal execute. fetch = none | one | all | lastrow"""
        with self._lock:
            c = self._open()
            try:
                cur = c.execute(sql, p)
                if   fetch == "one":     r = self._r(cur.fetchone())
                elif fetch == "all":     r = [self._r(x) for x in cur.fetchall()]
                elif fetch == "lastrow": r = cur.lastrowid
                else:                    r = None
                c.commit()
                return r
            except sqlite3.IntegrityError:
                return None
            except Exception as e:
                c.rollback()
                logger.error(f"❌ SQL [{sql[:55]}]: {e}")
                raise

    def _many(self, sql: str, rows: list):
        with self._lock:
            c = self._open()
            try:
                c.executemany(sql, rows); c.commit()
            except Exception as e:
                c.rollback(); logger.warning(f"⚠️ executemany: {e}")

    @staticmethod
    def _r(row) -> Optional[dict]:
        return dict(row) if row else None

    def ping(self) -> bool:
        try: self._q("SELECT 1"); return True
        except: return False

    # ── Schema ────────────────────────────────────────────────────────────

    def _schema(self):
        self._open().executescript("""
-- ══ ЛИЧНЫЕ СООБЩЕНИЯ ════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS user_messages (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id INTEGER NOT NULL,
    role      TEXT    NOT NULL DEFAULT 'user',
    text      TEXT    NOT NULL,
    username  TEXT    DEFAULT '',
    ts        TEXT    DEFAULT (datetime('now','+3 hours')),
    msg_id    INTEGER DEFAULT 0,
    chat_id   INTEGER DEFAULT 0,
    deleted   INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_um_sid    ON user_messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_um_ts     ON user_messages(ts DESC);
CREATE INDEX IF NOT EXISTS idx_um_sid_ts ON user_messages(sender_id, ts DESC);  -- составной для get_recent
CREATE INDEX IF NOT EXISTS idx_um_role   ON user_messages(sender_id, role, ts DESC);

-- ══ ПРОФИЛИ ПОЛЬЗОВАТЕЛЕЙ ════════════════════════════════════════
CREATE TABLE IF NOT EXISTS user_profiles (
    uid     INTEGER PRIMARY KEY,
    facts   TEXT    DEFAULT '[]',
    style   TEXT    DEFAULT 'normal',
    updated TEXT    DEFAULT (datetime('now','+3 hours'))
);

-- ══ ГРУППОВЫЕ СООБЩЕНИЯ ══════════════════════════════════════════
CREATE TABLE IF NOT EXISTS group_messages (
    msg_id    INTEGER NOT NULL,
    chat_id   INTEGER NOT NULL,
    sender    TEXT    DEFAULT '',
    sender_id INTEGER DEFAULT 0,
    text      TEXT    NOT NULL,
    date      TEXT    DEFAULT '',
    deleted   INTEGER DEFAULT 0,
    saved_at  TEXT    DEFAULT (datetime('now','+3 hours')),
    PRIMARY KEY (msg_id, chat_id)
);
CREATE INDEX IF NOT EXISTS idx_gm_cid     ON group_messages(chat_id);
CREATE INDEX IF NOT EXISTS idx_gm_ts      ON group_messages(saved_at DESC);
CREATE INDEX IF NOT EXISTS idx_gm_del     ON group_messages(deleted);
CREATE INDEX IF NOT EXISTS idx_gm_cid_ts  ON group_messages(chat_id, saved_at DESC);  -- составной
CREATE INDEX IF NOT EXISTS idx_gm_cid_del ON group_messages(chat_id, deleted, saved_at DESC);

-- ══ ПРОФИЛИ ГРУПП ════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS group_profiles (
    chat_id    INTEGER PRIMARY KEY,
    title      TEXT    DEFAULT '',
    username   TEXT    DEFAULT '',
    msg_count  INTEGER DEFAULT 0,
    first_seen TEXT    DEFAULT (datetime('now','+3 hours')),
    last_seen  TEXT    DEFAULT (datetime('now','+3 hours'))
);

-- ══ ЧАТЫ БОТА ════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS bot_chats (
    chat_id   INTEGER PRIMARY KEY,
    chat_type TEXT    DEFAULT 'private',
    title     TEXT    DEFAULT '',
    seen_at   TEXT    DEFAULT (datetime('now','+3 hours'))
);

-- ══ НАПОМИНАНИЯ ══════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS reminders (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    uid     INTEGER NOT NULL,
    text    TEXT    NOT NULL,
    fire_at TEXT    NOT NULL,
    repeat  TEXT    DEFAULT '',
    done    INTEGER DEFAULT 0,
    created TEXT    DEFAULT (datetime('now','+3 hours'))
);
CREATE INDEX IF NOT EXISTS idx_rem_uid  ON reminders(uid);
CREATE INDEX IF NOT EXISTS idx_rem_fire ON reminders(fire_at);

-- ══ БЫСТРЫЕ ССЫЛКИ ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS links (
    name   TEXT PRIMARY KEY,
    url    TEXT NOT NULL,
    added  TEXT DEFAULT (datetime('now','+3 hours'))
);

-- ══ ЕЖЕНЕДЕЛЬНАЯ СТАТИСТИКА ══════════════════════════════════════
CREATE TABLE IF NOT EXISTS weekly_stats (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    week_start   TEXT,
    week_end     TEXT,
    user_msgs    INTEGER DEFAULT 0,
    group_msgs   INTEGER DEFAULT 0,
    users_count  INTEGER DEFAULT 0,
    groups_count INTEGER DEFAULT 0,
    top_user     TEXT    DEFAULT '',
    top_group    TEXT    DEFAULT '',
    created_at   TEXT    DEFAULT (datetime('now','+3 hours'))
);

-- ══ АКИНАТОР — БАЗА ЗНАНИЙ ═══════════════════════════════════════
CREATE TABLE IF NOT EXISTS akinator_knowledge (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    character   TEXT NOT NULL,
    question    TEXT NOT NULL,
    answer      TEXT NOT NULL,
    confirmed   INTEGER DEFAULT 0,
    wrong_guess INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now','+3 hours')),
    updated_at  TEXT DEFAULT (datetime('now','+3 hours'))
);
CREATE INDEX IF NOT EXISTS idx_ak_char ON akinator_knowledge(character);

CREATE TABLE IF NOT EXISTS code_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id   INTEGER NOT NULL,
    filename    TEXT    NOT NULL,
    language    TEXT    DEFAULT '',
    content     TEXT    NOT NULL,
    size_bytes  INTEGER DEFAULT 0,
    saved_at    TEXT    DEFAULT (datetime('now','localtime')),
    last_review TEXT    DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_cf_sid ON code_files(sender_id);
CREATE INDEX IF NOT EXISTS idx_cf_ts  ON code_files(saved_at DESC);
        """)
        self._conn.commit()
        self._migrate_ml_tables()

    def _migrate_ml_tables(self):
        """Дропает ML таблицы если схема устарела, затем создаёт заново."""
        # Что должно быть в каждой таблице
        required = {
            "ml_patterns":        {"response", "category", "count", "last_seen"},
            "ml_user_prefs":      {"pref_type", "value", "weight"},
            "ml_knowledge":       {"content", "source", "confidence"},
            "ml_training_log":    {"session_id", "msgs_analyzed", "knowledge_new"},
            "ml_response_quality":{"query", "response", "score"},
        }
        cur = self._conn.cursor()
        dropped_any = False
        for table, needed_cols in required.items():
            try:
                cur.execute(f"PRAGMA table_info({table})")
                existing = {row[1] for row in cur.fetchall()}
                if existing and not needed_cols.issubset(existing):
                    cur.execute(f"DROP TABLE IF EXISTS {table}")
                    logger.info(f"🔄 ML: пересоздаю {table} (старая схема)")
                    dropped_any = True
            except Exception as e:
                logger.warning(f"ML check {table}: {e}")
        if dropped_any:
            self._conn.commit()
        # Создаём отсутствующие таблицы
        cur.executescript("""
CREATE TABLE IF NOT EXISTS ml_patterns (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id INTEGER NOT NULL DEFAULT 0,
    category  TEXT NOT NULL DEFAULT '',
    pattern   TEXT NOT NULL,
    response  TEXT DEFAULT '',
    count     INTEGER DEFAULT 1,
    last_seen TEXT DEFAULT (datetime('now','+3 hours')),
    UNIQUE(pattern, sender_id)
);
CREATE INDEX IF NOT EXISTS idx_mlp_cat ON ml_patterns(category);
CREATE INDEX IF NOT EXISTS idx_mlp_cnt ON ml_patterns(count DESC);

CREATE TABLE IF NOT EXISTS ml_user_prefs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id  INTEGER NOT NULL,
    pref_type  TEXT NOT NULL,
    value      TEXT NOT NULL DEFAULT '',
    weight     REAL DEFAULT 1.0,
    updated_at TEXT DEFAULT (datetime('now','+3 hours')),
    UNIQUE(sender_id, pref_type)
);
CREATE INDEX IF NOT EXISTS idx_mlup_sid ON ml_user_prefs(sender_id);

CREATE TABLE IF NOT EXISTS ml_knowledge (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    topic      TEXT NOT NULL UNIQUE,
    content    TEXT NOT NULL DEFAULT '',
    source     TEXT DEFAULT 'auto',
    confidence REAL DEFAULT 0.5,
    used_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now','+3 hours')),
    updated_at TEXT DEFAULT (datetime('now','+3 hours'))
);
CREATE INDEX IF NOT EXISTS idx_mlk_conf ON ml_knowledge(confidence DESC);

CREATE TABLE IF NOT EXISTS ml_training_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id    TEXT DEFAULT '',
    ts            TEXT DEFAULT (datetime('now','+3 hours')),
    msgs_analyzed INTEGER DEFAULT 0,
    patterns_new  INTEGER DEFAULT 0,
    knowledge_new INTEGER DEFAULT 0,
    duration_s    REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ml_response_quality (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    query_hash TEXT NOT NULL UNIQUE,
    query      TEXT NOT NULL DEFAULT '',
    response   TEXT NOT NULL DEFAULT '',
    score      REAL DEFAULT 0.5,
    ts         TEXT DEFAULT (datetime('now','+3 hours'))
);
        """)
        self._conn.commit()

    # ── Bulk export (для бэкапа) ─────────────────────────────────────────

    def get_all_messages(self, limit: int = 50000) -> list[dict]:
        return self._q(
            "SELECT * FROM user_messages ORDER BY ts DESC LIMIT ?",
            (limit,), fetch="all"
        ) or []

    def get_all_group_messages(self, limit: int = 50000) -> list[dict]:
        return self._q(
            "SELECT * FROM group_messages ORDER BY saved_at DESC LIMIT ?",
            (limit,), fetch="all"
        ) or []

    # ══════════════════════════════════════════════════════════════════════
    #  USER MESSAGES
    # ══════════════════════════════════════════════════════════════════════

    def save_message(self, sender_id: int, role: str, text: str,
                     username: str = "", msg_id: int = 0, chat_id: int = 0):
        # Без лимита — храним всё. Текст до 16000 символов (длинные ответы LLM)
        self._q("INSERT INTO user_messages (sender_id,role,text,username,msg_id,chat_id) VALUES (?,?,?,?,?,?)",
                (sender_id, role, text[:16000], username, msg_id, chat_id))

    def get_recent(self, sender_id: int, n: int = 50) -> list[dict]:
        """Последние n сообщений. Дефолт 50 — достаточно для хорошего контекста."""
        rows = self._q(
            "SELECT role,text,ts FROM user_messages WHERE sender_id=? AND deleted=0 "
            "ORDER BY ts DESC LIMIT ?", (sender_id, n), fetch="all") or []
        return list(reversed(rows))

    def search_messages(self, sender_id: int, query: str, limit: int = 20) -> list[dict]:
        return self._q(
            "SELECT role,text,ts FROM user_messages WHERE sender_id=? AND text LIKE ? "
            "ORDER BY ts DESC LIMIT ?",
            (sender_id, f"%{query}%", limit), fetch="all") or []

    def message_stats(self, sender_id: int) -> dict:
        return self._q(
            "SELECT COUNT(*) total, MIN(ts) first, MAX(ts) last "
            "FROM user_messages WHERE sender_id=? AND role='user'",
            (sender_id,), fetch="one") or {}

    def get_messages_by_date(self, date_from: str, date_to: str = "",
                              sender_id: int = 0, limit: int = 100) -> list[dict]:
        if not date_to:
            date_to = (date_from + " 23:59:59") if len(date_from) == 10 else date_from
        p: list = [date_from, date_to]
        sql = "SELECT sender_id,username,role,text,ts FROM user_messages WHERE ts>=? AND ts<=?"
        if sender_id:
            sql += " AND sender_id=?"; p.append(sender_id)
        sql += " ORDER BY ts DESC LIMIT ?"; p.append(limit)
        return self._q(sql, tuple(p), fetch="all") or []

    def delete_messages_by_date(self, date_from: str, date_to: str = "",
                                 sender_id: int = 0):
        if not date_to:
            date_to = (date_from + " 23:59:59") if len(date_from) == 10 else date_from
        p: list = [date_from, date_to]
        sql = "DELETE FROM user_messages WHERE ts>=? AND ts<=?"
        if sender_id:
            sql += " AND sender_id=?"; p.append(sender_id)
        self._q(sql, tuple(p))

    # ══════════════════════════════════════════════════════════════════════
    #  GROUP MESSAGES
    # ══════════════════════════════════════════════════════════════════════

    def save_group_msg(self, chat_id: int, msg_id: int, sender: str,
                       sender_id: int, text: str, date: str = ""):
        with self._bufl:
            self._buf.append((msg_id, chat_id, sender, sender_id, text[:2000], date))
        if time.time() - self._flushed > 10 or len(self._buf) > 50:
            self.flush()

    def flush(self):
        with self._bufl:
            buf, self._buf = self._buf[:], []
        if buf:
            self._many(
                "INSERT OR IGNORE INTO group_messages "
                "(msg_id,chat_id,sender,sender_id,text,date) VALUES (?,?,?,?,?,?)", buf)
        self._flushed = time.time()

    def mark_deleted(self, chat_id: int, msg_ids: list):
        if not msg_ids: return
        ph = ",".join("?" * len(msg_ids))
        self._q(f"UPDATE group_messages SET deleted=1 WHERE chat_id=? AND msg_id IN ({ph})",
                (chat_id, *msg_ids))

    def mark_deleted_all_chats(self, msg_ids: list):
        if not msg_ids: return
        ph = ",".join("?" * len(msg_ids))
        self._q(f"UPDATE group_messages SET deleted=1 WHERE msg_id IN ({ph})", tuple(msg_ids))

    def get_deleted(self, chat_id: int, limit: int = 20, date_filter: str = "") -> list[dict]:
        sql = ("SELECT msg_id,sender,sender_id,text,date,saved_at FROM group_messages "
               "WHERE chat_id=? AND deleted=1")
        p: list = [chat_id]
        if date_filter:
            sql += " AND saved_at LIKE ?"; p.append(f"{date_filter}%")
        sql += " ORDER BY saved_at DESC LIMIT ?"; p.append(limit)
        return self._q(sql, tuple(p), fetch="all") or []

    def get_group_stats(self, chat_id: int, days: int = 7) -> dict:
        since = (datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
                 - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        prev  = (datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
                 - timedelta(days=days*2)).strftime("%Y-%m-%d %H:%M:%S")
        cur_c  = (self._q("SELECT COUNT(*) c FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=0",
                           (chat_id, since), fetch="one") or {}).get("c", 0)
        prev_c = (self._q("SELECT COUNT(*) c FROM group_messages WHERE chat_id=? AND saved_at>=? AND saved_at<? AND deleted=0",
                           (chat_id, prev, since), fetch="one") or {}).get("c", 0)
        top    = self._q("SELECT sender,COUNT(*) cnt FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=0 "
                          "GROUP BY sender ORDER BY cnt DESC LIMIT 5",
                          (chat_id, since), fetch="all") or []
        change = round((cur_c - prev_c) / max(prev_c, 1) * 100)
        return {"total": cur_c, "change": change, "top_users": top}

    # ══════════════════════════════════════════════════════════════════════
    #  USER PROFILES
    # ══════════════════════════════════════════════════════════════════════

    def load_profile(self, uid: int) -> dict:
        hit = self._cache.get(uid)
        if hit: return hit
        row = self._q("SELECT facts,style FROM user_profiles WHERE uid=?", (uid,), fetch="one")
        prof = {"facts": json.loads(row["facts"] or "[]"), "style": row.get("style","normal")} if row \
               else {"facts": [], "style": "normal"}
        self._cache.set(uid, prof)
        return prof

    def save_profile(self, uid: int, profile: dict):
        self._q("""INSERT INTO user_profiles (uid,facts,style,updated) VALUES (?,?,?,?)
                   ON CONFLICT(uid) DO UPDATE SET facts=excluded.facts,
                   style=excluded.style, updated=excluded.updated""",
                (uid, json.dumps(profile.get("facts",[]), ensure_ascii=False),
                 profile.get("style","normal"), _msk()))
        self._cache.set(uid, profile)

    def delete_profile(self, uid: int):
        self._q("DELETE FROM user_profiles WHERE uid=?", (uid,))
        self._cache.drop(uid)

    def get_all_profiles(self) -> list[dict]:
        return self._q("SELECT uid,facts,style FROM user_profiles ORDER BY uid", fetch="all") or []

    # ══════════════════════════════════════════════════════════════════════
    #  GROUP PROFILES
    # ══════════════════════════════════════════════════════════════════════

    def update_group_profile(self, chat_id: int, title: str = "", username: str = ""):
        self._q("""INSERT INTO group_profiles (chat_id,title,username,last_seen) VALUES (?,?,?,?)
                   ON CONFLICT(chat_id) DO UPDATE SET
                   title=COALESCE(NULLIF(excluded.title,''),title),
                   username=COALESCE(NULLIF(excluded.username,''),username),
                   last_seen=excluded.last_seen, msg_count=msg_count+1""",
                (chat_id, title, username, _msk()))

    def get_group_profile(self, chat_id: int) -> Optional[dict]:
        return self._q("SELECT * FROM group_profiles WHERE chat_id=?", (chat_id,), fetch="one")

    def list_groups(self) -> list[dict]:
        return self._q("SELECT chat_id,title,msg_count,last_seen FROM group_profiles ORDER BY msg_count DESC",
                       fetch="all") or []

    # ══════════════════════════════════════════════════════════════════════
    #  BOT CHATS
    # ══════════════════════════════════════════════════════════════════════

    def register_bot_chat(self, chat_id: int, chat_type: str = "private", title: str = ""):
        self._q("""INSERT INTO bot_chats (chat_id,chat_type,title,seen_at) VALUES (?,?,?,?)
                   ON CONFLICT(chat_id) DO UPDATE SET seen_at=excluded.seen_at,
                   chat_type=excluded.chat_type,
                   title=COALESCE(NULLIF(excluded.title,''),title)""",
                (chat_id, chat_type, title, _msk()))

    def get_bot_chat_ids(self) -> set:
        rows = self._q("SELECT chat_id FROM bot_chats", fetch="all") or []
        return {r["chat_id"] for r in rows}

    # ══════════════════════════════════════════════════════════════════════
    #  REMINDERS
    # ══════════════════════════════════════════════════════════════════════

    def add_reminder(self, uid: int, text: str, fire_at: str, repeat: str = "") -> int:
        return self._q("INSERT INTO reminders (uid,text,fire_at,repeat,created) VALUES (?,?,?,?,?)",
                       (uid, text, fire_at, repeat, _msk()), fetch="lastrow") or 0

    def get_reminders(self, uid: int) -> list[dict]:
        return self._q("SELECT id,text,fire_at,repeat FROM reminders WHERE uid=? AND done=0 ORDER BY fire_at",
                       (uid,), fetch="all") or []

    def get_due_reminders(self) -> list[dict]:
        return self._q("SELECT * FROM reminders WHERE done=0 AND fire_at<=?", (_iso(),), fetch="all") or []

    def mark_reminder_done(self, rid: int):
        self._q("UPDATE reminders SET done=1 WHERE id=?", (rid,))

    def delete_reminder(self, uid: int, rid: int) -> bool:
        self._q("UPDATE reminders SET done=1 WHERE id=? AND uid=?", (rid, uid))
        return True

    def get_all_reminders(self) -> list[dict]:
        return self._q("SELECT * FROM reminders ORDER BY created DESC", fetch="all") or []

    # ══════════════════════════════════════════════════════════════════════
    #  LINKS
    # ══════════════════════════════════════════════════════════════════════

    def save_link(self, name: str, url: str):
        self._q("INSERT OR REPLACE INTO links (name,url,added) VALUES (?,?,?)",
                (name.lower().strip(), url.strip(), _msk()))

    def get_link(self, name: str) -> Optional[str]:
        row = self._q("SELECT url FROM links WHERE name LIKE ?",
                      (f"%{name.lower().strip()}%",), fetch="one")
        return row["url"] if row else None

    def list_links(self) -> list[dict]:
        return self._q("SELECT name,url,added FROM links ORDER BY name", fetch="all") or []

    def delete_link(self, name: str) -> bool:
        self._q("DELETE FROM links WHERE name=?", (name.lower().strip(),))
        return True

    # ══════════════════════════════════════════════════════════════════════
    #  WEEKLY STATS
    # ══════════════════════════════════════════════════════════════════════

    def save_weekly_stats(self, stats: dict):
        self._q("""INSERT INTO weekly_stats
                   (week_start,week_end,user_msgs,group_msgs,users_count,groups_count,top_user,top_group)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (stats.get("week_start",""), stats.get("week_end",""),
                 stats.get("user_msgs",0),   stats.get("group_msgs",0),
                 stats.get("users_count",0), stats.get("groups_count",0),
                 stats.get("top_user",""),   stats.get("top_group","")))

    def get_weekly_report(self) -> dict:
        conn = self._open()
        ago  = (datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
                - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            um   = conn.execute("SELECT COUNT(*) FROM user_messages WHERE ts>=? AND role='user'", (ago,)).fetchone()[0]
            gm   = conn.execute("SELECT COUNT(*) FROM group_messages WHERE saved_at>=? AND deleted=0", (ago,)).fetchone()[0]
            uc   = conn.execute("SELECT COUNT(DISTINCT sender_id) FROM user_messages WHERE ts>=? AND role='user'", (ago,)).fetchone()[0]
            gc   = conn.execute("SELECT COUNT(DISTINCT chat_id) FROM group_messages WHERE saved_at>=?", (ago,)).fetchone()[0]
            tu   = conn.execute("SELECT username,COUNT(*) c FROM user_messages WHERE ts>=? AND role='user' GROUP BY sender_id ORDER BY c DESC LIMIT 1", (ago,)).fetchone()
            tg   = conn.execute("SELECT chat_id,COUNT(*) c FROM group_messages WHERE saved_at>=? AND deleted=0 GROUP BY chat_id ORDER BY c DESC LIMIT 1", (ago,)).fetchone()
            dw   = conn.execute("SELECT COUNT(*) FROM group_messages WHERE saved_at>=? AND deleted=1", (ago,)).fetchone()[0]
            top_u = f"{tu['username']} ({tu['c']} сообщ.)" if tu else "—"
            if tg:
                gp = self.get_group_profile(tg["chat_id"])
                top_g = f"{gp['title'] if gp else tg['chat_id']} ({tg['c']} сообщ.)"
            else:
                top_g = "—"
            return {"user_msgs": um, "group_msgs": gm, "users_count": uc, "groups_count": gc,
                    "top_user": top_u, "top_group": top_g, "deleted_week": dw,
                    "week_start": ago[:10],
                    "week_end": datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d")}
        except Exception as e:
            logger.warning(f"⚠️ weekly_report: {e}"); return {}

    # ══════════════════════════════════════════════════════════════════════
    #  MAINTENANCE
    # ══════════════════════════════════════════════════════════════════════

    def get_db_stats(self) -> dict:
        c = self._open()
        try:
            return {
                "user_msgs":  c.execute("SELECT COUNT(*) FROM user_messages").fetchone()[0],
                "group_msgs": c.execute("SELECT COUNT(*) FROM group_messages").fetchone()[0],
                "profiles":   c.execute("SELECT COUNT(*) FROM user_profiles").fetchone()[0],
                "reminders":  c.execute("SELECT COUNT(*) FROM reminders WHERE done=0").fetchone()[0],
                "links":      c.execute("SELECT COUNT(*) FROM links").fetchone()[0],
                "size_mb":    round(os.path.getsize(self._path) / 1024 / 1024, 2),
                "cache":      self._cache.info(),
            }
        except Exception as e:
            logger.warning(f"⚠️ db_stats: {e}"); return {}

    def cleanup_old_data(self):
        cutoff = (datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
                  - timedelta(days=self.GRP_DAYS)).strftime("%Y-%m-%d")
        self._q("DELETE FROM group_messages WHERE saved_at<?", (cutoff,))
        self._q("DELETE FROM reminders WHERE done=1 AND created<?",
                ((datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
                  - timedelta(days=30)).strftime("%Y-%m-%d"),))

    def vacuum(self):
        with self._lock:
            try: self._open().execute("VACUUM")
            except Exception as e: logger.warning(f"⚠️ VACUUM: {e}")

    # ══════════════════════════════════════════════════════════════════════
    #  BACKUP / RESTORE (локальный)
    # ══════════════════════════════════════════════════════════════════════

    def make_backup_copy(self, dest: str) -> bool:
        """SQLite online backup API — безопасно во время записи."""
        self.flush()
        try:
            dst = sqlite3.connect(dest)
            with self._lock:
                self._open().backup(dst)
            dst.close()
            return True
        except Exception as e:
            logger.error(f"❌ backup_copy: {e}"); return False

    def restore_from_path(self, src: str) -> bool:
        """Восстановить БД из файла. Сбрасывает кэш и переподключает."""
        try:
            if self._conn:
                self._conn.close(); self._conn = None
            shutil.copy2(src, self._path)
            self._open()
            self._cache.clear()
            logger.info(f"✅ DB restored from {src}")
            return True
        except Exception as e:
            logger.error(f"❌ restore: {e}"); return False

    # ══════════════════════════════════════════════════════════════════════
    #  ЯНДЕКС.ДИСК — BACKUP + AUTO-RESTORE
    # ══════════════════════════════════════════════════════════════════════

    async def yadisk_upload(self, token: str, folder: str, label: str) -> bool:
        """
        Загружает текущую БД как Jarvis_YYYY-MM-DD.db на Яндекс.Диск.
        Создаёт папку автоматически.
        """
        import httpx
        tmp = tempfile.mktemp(suffix=".db")
        try:
            if not self.make_backup_copy(tmp):
                return False
            async with httpx.AsyncClient(timeout=180, verify=False) as cl:
                h = {"Authorization": f"OAuth {token}"}
                # Создаём папку (ошибка = уже существует, игнорируем)
                await cl.put("https://cloud-api.yandex.net/v1/disk/resources",
                             params={"path": folder}, headers=h)
                fname = f"Jarvis_{label}.db"
                # Получаем URL для загрузки
                r = await cl.get("https://cloud-api.yandex.net/v1/disk/resources/upload",
                                 params={"path": f"{folder}/{fname}", "overwrite": "true"},
                                 headers=h)
                if r.status_code != 200:
                    logger.error(f"❌ Яндекс upload link HTTP {r.status_code}")
                    return False
                url = r.json().get("href", "")
                if not url:
                    return False
                # Загружаем файл
                with open(tmp, "rb") as f:
                    up = await cl.put(url, content=f.read())
                if up.status_code in (201, 202):
                    logger.info(f"✅ Яндекс.Диск: {fname} загружен в {folder}/")
                    return True
                logger.error(f"❌ Яндекс upload HTTP {up.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ yadisk_upload: {e}"); return False
        finally:
            try: os.unlink(tmp)
            except: pass

    async def yadisk_cleanup(self, token: str, folder: str, keep: int = 14):
        """Оставляет только keep последних .db бэкапов, удаляет старые."""
        import httpx
        try:
            async with httpx.AsyncClient(timeout=30, verify=False) as cl:
                h  = {"Authorization": f"OAuth {token}"}
                r  = await cl.get("https://cloud-api.yandex.net/v1/disk/resources",
                                  params={"path": folder, "limit": 200, "sort": "-created"},
                                  headers=h)
                if r.status_code != 200:
                    return
                items = r.json().get("_embedded", {}).get("items", [])
                bks = sorted(
                    [i for i in items
                     if i.get("type") == "file"
                     and i.get("name","").startswith("Jarvis_")
                     and i.get("name","").endswith(".db")],
                    key=lambda x: x.get("name",""), reverse=True
                )
                for old in bks[keep:]:
                    await cl.delete("https://cloud-api.yandex.net/v1/disk/resources",
                                    params={"path": f"{folder}/{old['name']}", "permanently": "true"},
                                    headers=h)
                    logger.info(f"🗑 Яндекс.Диск: удалён {old['name']}")
        except Exception as e:
            logger.warning(f"⚠️ yadisk_cleanup: {e}")

    async def yadisk_list(self, token: str, folder: str) -> list[dict]:
        """Список .db бэкапов на Яндекс.Диске (новые первые)."""
        import httpx
        try:
            async with httpx.AsyncClient(timeout=15, verify=False) as cl:
                h = {"Authorization": f"OAuth {token}"}
                r = await cl.get("https://cloud-api.yandex.net/v1/disk/resources",
                                 params={"path": folder, "limit": 200, "sort": "-created"},
                                 headers=h)
                if r.status_code != 200:
                    return []
                items = r.json().get("_embedded", {}).get("items", [])
                return sorted(
                    [{"name": i["name"],
                      "size_mb": round(i.get("size",0)/1024/1024, 1),
                      "date": i["name"].replace("Jarvis_","").replace(".db","")}
                     for i in items
                     if i.get("type") == "file"
                     and i.get("name","").startswith("Jarvis_")
                     and i.get("name","").endswith(".db")],
                    key=lambda x: x["name"], reverse=True
                )
        except Exception as e:
            logger.warning(f"⚠️ yadisk_list: {e}"); return []

    async def yadisk_restore_latest(self, token: str, folder: str) -> bool:
        """
        Скачивает самый свежий .db с Яндекс.Диска и восстанавливает БД.
        Вызывается АВТОМАТИЧЕСКИ при старте если БД пустая/новая.
        """
        import httpx
        bks = await self.yadisk_list(token, folder)
        if not bks:
            logger.info("ℹ️  Яндекс.Диск: бэкапов нет — стартуем с чистой БД")
            return False
        latest = bks[0]
        logger.info(f"⬇️  Загружаю актуальную БД: {latest['name']} ({latest['size_mb']} MB)...")
        tmp = tempfile.mktemp(suffix=".db")
        try:
            async with httpx.AsyncClient(timeout=300, verify=False) as cl:
                h = {"Authorization": f"OAuth {token}"}
                r = await cl.get("https://cloud-api.yandex.net/v1/disk/resources/download",
                                 params={"path": f"{folder}/{latest['name']}"}, headers=h)
                if r.status_code != 200:
                    logger.error(f"❌ download link HTTP {r.status_code}")
                    return False
                href = r.json().get("href","")
                if not href:
                    return False
                data = await cl.get(href, timeout=300)
                with open(tmp, "wb") as f:
                    f.write(data.content)
            ok = self.restore_from_path(tmp)
            if ok:
                logger.info(f"✅ БД восстановлена с Яндекс.Диска: {latest['name']}")
            return ok
        except Exception as e:
            logger.error(f"❌ yadisk_restore_latest: {e}"); return False
        finally:
            try: os.unlink(tmp)
            except: pass

    async def yadisk_restore_by_date(self, token: str, folder: str,
                                      date_str: str) -> tuple[bool, str]:
        """
        Восстанавливает БД по дате. Принимает dd.mm.yyyy или yyyy-mm-dd.
        Возвращает (успех, сообщение).
        """
        import httpx
        # Нормализуем дату → YYYY-MM-DD
        try:
            if "." in date_str:
                d, m, y = date_str.strip().split(".")
                label = f"{y}-{int(m):02d}-{int(d):02d}"
            else:
                label = date_str.strip()
        except Exception:
            return False, f"Неверный формат даты: {date_str}"

        bks   = await self.yadisk_list(token, folder)
        found = next((b for b in bks if label in b["name"]), None)
        if not found:
            avail = ", ".join(b["date"] for b in bks[:5]) or "нет бэкапов"
            return False, f"Бэкап за {date_str} не найден.\nДоступные: {avail}"

        tmp = tempfile.mktemp(suffix=".db")
        try:
            async with httpx.AsyncClient(timeout=300, verify=False) as cl:
                h = {"Authorization": f"OAuth {token}"}
                r = await cl.get("https://cloud-api.yandex.net/v1/disk/resources/download",
                                 params={"path": f"{folder}/{found['name']}"}, headers=h)
                if r.status_code != 200:
                    return False, f"Ошибка ссылки: HTTP {r.status_code}"
                href = r.json().get("href","")
                if not href:
                    return False, "Не удалось получить ссылку"
                data = await cl.get(href, timeout=300)
                with open(tmp, "wb") as f:
                    f.write(data.content)
            ok = self.restore_from_path(tmp)
            msg = f"✅ БД восстановлена из {found['name']}" if ok else "❌ Ошибка восстановления"
            return ok, msg
        except Exception as e:
            return False, f"❌ Ошибка: {e}"
        finally:
            try: os.unlink(tmp)
            except: pass

    # ══════════════════════════════════════════════════════════════════════
    #  ML — ПАТТЕРНЫ, ЗНАНИЯ, КАЧЕСТВО
    # ══════════════════════════════════════════════════════════════════════

    def ml_save_pattern(self, sender_id: int, category: str, pattern: str, response: str = ""):
        """Сохранить/обновить паттерн поведения пользователя."""
        self._q("""
            INSERT INTO ml_patterns (sender_id, category, pattern, response, count, last_seen)
            VALUES (?, ?, ?, ?, 1, datetime('now','+3 hours'))
            ON CONFLICT(pattern, sender_id) DO UPDATE SET
                count    = count + 1,
                last_seen = datetime('now','+3 hours'),
                response  = CASE WHEN excluded.response != '' THEN excluded.response ELSE response END
        """, (sender_id, category, pattern[:500], response[:2000]))

    def ml_get_patterns(self, sender_id: int = 0, category: str = "", limit: int = 50) -> list[dict]:
        """Топ паттернов — для контекста в промпте."""
        if sender_id and category:
            return self._q("SELECT * FROM ml_patterns WHERE sender_id=? AND category=? ORDER BY count DESC LIMIT ?",
                           (sender_id, category, limit), fetch="all") or []
        if sender_id:
            return self._q("SELECT * FROM ml_patterns WHERE sender_id=? ORDER BY count DESC LIMIT ?",
                           (sender_id, limit), fetch="all") or []
        return self._q("SELECT * FROM ml_patterns ORDER BY count DESC LIMIT ?",
                       (limit,), fetch="all") or []

    def ml_save_knowledge(self, topic: str, content: str, source: str = "auto", confidence: float = 0.5):
        """Сохранить факт/знание в базу — из разговоров."""
        import hashlib
        key = hashlib.md5(topic.encode()).hexdigest()[:16]
        self._q("""
            INSERT INTO ml_knowledge (topic, content, source, confidence, updated_at)
            VALUES (?, ?, ?, ?, datetime('now','+3 hours'))
            ON CONFLICT(topic) DO UPDATE SET
                content    = excluded.content,
                confidence = MAX(confidence, excluded.confidence),
                updated_at = datetime('now','+3 hours')
        """, (topic[:200], content[:3000], source, confidence))

    def ml_get_knowledge(self, topic: str = "", limit: int = 20) -> list[dict]:
        if topic:
            return self._q("SELECT * FROM ml_knowledge WHERE topic LIKE ? ORDER BY confidence DESC LIMIT ?",
                           (f"%{topic}%", limit), fetch="all") or []
        return self._q("SELECT * FROM ml_knowledge ORDER BY confidence DESC LIMIT ?",
                       (limit,), fetch="all") or []

    def ml_save_prefs(self, sender_id: int, pref_type: str, value: str, weight: float = 1.0):
        """Сохранить предпочтение пользователя."""
        self._q("""
            INSERT INTO ml_user_prefs (sender_id, pref_type, value, weight, updated_at)
            VALUES (?, ?, ?, ?, datetime('now','+3 hours'))
            ON CONFLICT(sender_id, pref_type) DO UPDATE SET
                value      = excluded.value,
                weight     = MIN(weight + 0.1, 5.0),
                updated_at = datetime('now','+3 hours')
        """, (sender_id, pref_type[:100], value[:500], weight))

    def ml_get_prefs(self, sender_id: int) -> list[dict]:
        return self._q("SELECT * FROM ml_user_prefs WHERE sender_id=? ORDER BY weight DESC",
                       (sender_id,), fetch="all") or []

    def ml_save_response_quality(self, query_hash: str, query: str, response: str, score: float):
        self._q("""
            INSERT INTO ml_response_quality (query_hash, query, response, score)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(query_hash) DO UPDATE SET
                score = (score + excluded.score) / 2.0
        """, (query_hash, query[:300], response[:1000], score))

    def ml_log_training(self, session_id: str, msgs_analyzed: int, patterns_new: int,
                        knowledge_new: int, duration_s: float):
        self._q("""INSERT INTO ml_training_log
                   (session_id, msgs_analyzed, patterns_new, knowledge_new, duration_s)
                   VALUES (?, ?, ?, ?, ?)""",
                (session_id, msgs_analyzed, patterns_new, knowledge_new, round(duration_s, 2)))

    def ml_last_trained_ts(self) -> str:
        row = self._q("SELECT MAX(ts) ts FROM ml_training_log", fetch="one")
        return (row or {}).get("ts", "")

    def ml_stats(self) -> dict:
        p = self._q("SELECT COUNT(*) cnt FROM ml_patterns", fetch="one") or {}
        k = self._q("SELECT COUNT(*) cnt FROM ml_knowledge", fetch="one") or {}
        t = self._q("SELECT COUNT(*) cnt FROM ml_training_log", fetch="one") or {}
        last = self._q("SELECT MAX(ts) ts FROM ml_training_log", fetch="one") or {}
        return {
            "patterns":  p.get("cnt", 0),
            "knowledge": k.get("cnt", 0),
            "sessions":  t.get("cnt", 0),
            "last_ts":   last.get("ts", "—"),
        }

    # ══════════════════════════════════════════════════════════════════════
    #  CLOSE
    # ══════════════════════════════════════════════════════════════════════

    def close(self):
        self.flush()
        if self._conn:
            try: self._conn.close()
            except: pass
            self._conn = None

    # ══════════════════════════════════════════════════════════════════════
    #  LEGACY (обратная совместимость с main.py)
    # ══════════════════════════════════════════════════════════════════════

    # Алиасы для старых вызовов _execute, _cur и т.д.
    def _execute(self, sql, params=(), fetch="none"): return self._q(sql, params, fetch)
    def _safe_commit(self): pass
    def _cur(self, dict_row=True): return self._open().cursor()
    def _ph(self): return "?"
    def _fetchall(self, cur): return [self._r(r) for r in cur.fetchall()]
    def _fetchone(self, cur): return self._r(cur.fetchone())
