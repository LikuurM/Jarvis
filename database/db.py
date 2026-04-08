"""
JARVIS Database — полная схема и менеджер SQLite.
Все таблицы, индексы, FTS5 для полнотекстового поиска.
"""
import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Any
from contextlib import contextmanager

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH

logger = logging.getLogger("jarvis.db")


SCHEMA = """
-- ═══════════════════════════════════════════════════════════
-- СООБЩЕНИЯ — все платформы
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS messages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    platform            TEXT NOT NULL,          -- telegram/vk/x/reddit
    chat_type           TEXT,                   -- personal/group/channel
    chat_id             TEXT,
    chat_title          TEXT,
    sender_id           TEXT,
    sender_name         TEXT,
    content             TEXT,
    media_type          TEXT,                   -- text/photo/video/audio/doc/sticker
    media_description   TEXT,                   -- результат Vision анализа
    reply_to_id         INTEGER,
    forwarded_from      TEXT,
    importance_score    REAL DEFAULT 0,
    embedding_id        TEXT,                   -- ID вектора в ChromaDB
    processed           INTEGER DEFAULT 0,
    timestamp           DATETIME,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════
-- КОНТАКТЫ — люди из всех платформ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS contacts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    platform            TEXT NOT NULL,
    platform_id         TEXT NOT NULL,
    username            TEXT,
    display_name        TEXT,
    bio                 TEXT,
    trust_score         REAL DEFAULT 3.0,       -- 0-10
    interaction_count   INTEGER DEFAULT 0,
    sentiment_avg       REAL DEFAULT 0.5,       -- 0=негатив, 1=позитив
    last_interaction    DATETIME,
    is_important        INTEGER DEFAULT 0,
    notes               TEXT,                   -- заметки ДЖАРВИСА
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, platform_id)
);

-- ═══════════════════════════════════════════════════════════
-- БАЗА ЗНАНИЙ — факты, инсайты, веб-контент
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS knowledge (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content             TEXT NOT NULL,
    summary             TEXT,
    source_url          TEXT,
    source_type         TEXT,                   -- web/kaggle/arxiv/wikipedia/file
    category            TEXT,
    tags                TEXT,                   -- JSON массив
    confidence_score    REAL DEFAULT 0.5,       -- 0-1
    verified_sources    TEXT,                   -- JSON список источников
    is_disputed         INTEGER DEFAULT 0,
    embedding_id        TEXT,
    expires_at          DATETIME,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════
-- ЭПИЗОДИЧЕСКАЯ ПАМЯТЬ — события
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS episodic_memory (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type          TEXT,                   -- conversation/discovery/alert/action
    summary             TEXT NOT NULL,
    participants        TEXT,                   -- JSON
    topics              TEXT,                   -- JSON
    importance_score    REAL DEFAULT 5.0,
    original_msg_ids    TEXT,                   -- JSON список message.id
    compressed_at       DATETIME,
    timestamp           DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════
-- ГРАФ ЗНАНИЙ — сущности (узлы)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS entities (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    aliases             TEXT,                   -- JSON: ["псевдоним1", "псевдоним2"]
    entity_type         TEXT,                   -- person/place/org/concept/event
    description         TEXT,
    importance_score    REAL DEFAULT 5.0,
    first_seen          DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen           DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, entity_type)
);

-- ═══════════════════════════════════════════════════════════
-- ГРАФ ЗНАНИЙ — отношения (рёбра)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS relationships (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    entity1_id          INTEGER NOT NULL,
    entity2_id          INTEGER NOT NULL,
    relation_type       TEXT NOT NULL,          -- knows/discussed/works_at/interested_in
    strength            REAL DEFAULT 0.5,       -- 0-1
    evidence_count      INTEGER DEFAULT 1,
    context             TEXT,
    first_seen          DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen           DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (entity1_id) REFERENCES entities(id),
    FOREIGN KEY (entity2_id) REFERENCES entities(id)
);

-- ═══════════════════════════════════════════════════════════
-- ЗАГРУЖЕННЫЕ ФАЙЛЫ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS uploaded_files (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    original_name       TEXT NOT NULL,
    extension           TEXT,
    file_type           TEXT,                   -- telegram_export/document/spreadsheet/etc
    user_description    TEXT,                   -- что пользователь сказал об этом файле
    file_size_bytes     INTEGER,
    status              TEXT DEFAULT 'received', -- received/analyzing/done/failed
    progress_percent    INTEGER DEFAULT 0,
    records_extracted   INTEGER DEFAULT 0,
    knowledge_chunks    INTEGER DEFAULT 0,
    entities_found      INTEGER DEFAULT 0,
    summary             TEXT,
    source_period_start DATETIME,
    source_period_end   DATETIME,
    participants        TEXT,                   -- JSON
    raw_deleted         INTEGER DEFAULT 0,
    error_message       TEXT,
    uploaded_at         DATETIME DEFAULT CURRENT_TIMESTAMP,
    processed_at        DATETIME
);

-- ═══════════════════════════════════════════════════════════
-- ЧАНКИ ИЗ ФАЙЛОВ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS file_chunks (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id             INTEGER NOT NULL,
    chunk_index         INTEGER,
    content             TEXT NOT NULL,
    chunk_type          TEXT,                   -- message/fact/table_row/insight/code
    sender_name         TEXT,
    sender_id           TEXT,
    message_date        DATETIME,
    page_number         INTEGER,
    section_title       TEXT,
    importance_score    REAL DEFAULT 5.0,
    embedding_id        TEXT,
    FOREIGN KEY (file_id) REFERENCES uploaded_files(id)
);

-- ═══════════════════════════════════════════════════════════
-- АВТООТВЕТЫ (Digital Twin лог)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS auto_replies (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    platform            TEXT,
    chat_id             TEXT,
    sender_name         TEXT,
    original_message    TEXT,
    my_reply            TEXT,
    confidence_score    REAL,
    autonomy_level      INTEGER,                -- 1/2/3
    was_reviewed        INTEGER DEFAULT 0,
    user_rating         INTEGER,                -- -1/0/1
    timestamp           DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════
-- ОЧЕРЕДЬ ЗАДАЧ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tasks_queue (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type           TEXT NOT NULL,
    priority            INTEGER DEFAULT 3,      -- 1=critical 2=high 3=normal 4=low
    status              TEXT DEFAULT 'pending', -- pending/processing/done/failed
    payload             TEXT,                   -- JSON
    agent_assigned      TEXT,
    attempts            INTEGER DEFAULT 0,
    max_attempts        INTEGER DEFAULT 3,
    scheduled_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
    started_at          DATETIME,
    completed_at        DATETIME,
    error_message       TEXT
);

-- ═══════════════════════════════════════════════════════════
-- ПРЕДПОЧТЕНИЯ ПОЛЬЗОВАТЕЛЯ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS preferences (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    category            TEXT NOT NULL,          -- style/topics/alerts/autonomy
    key                 TEXT NOT NULL,
    value               TEXT,
    confidence          REAL DEFAULT 0.5,
    source              TEXT DEFAULT 'inferred', -- inferred/explicit
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(category, key)
);

-- ═══════════════════════════════════════════════════════════
-- СТИЛЬ ОБЩЕНИЯ (для Digital Twin)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS style_profile (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    avg_message_length  REAL DEFAULT 50,
    emoji_frequency     REAL DEFAULT 0.3,
    favorite_emoji      TEXT,                   -- JSON список
    punctuation_style   TEXT DEFAULT 'casual',
    formality_level     REAL DEFAULT 0.3,
    avg_response_delay  INTEGER DEFAULT 180,    -- секунды
    filler_words        TEXT,                   -- JSON список
    humor_frequency     REAL DEFAULT 0.2,
    samples_count       INTEGER DEFAULT 0,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════
-- KAGGLE ДАТАСЕТЫ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS kaggle_datasets (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id          TEXT UNIQUE,
    title               TEXT,
    description         TEXT,
    download_date       DATETIME,
    file_size_mb        REAL,
    rows_count          INTEGER,
    columns_info        TEXT,                   -- JSON
    key_insights        TEXT,
    embedding_ids       TEXT,                   -- JSON
    usability_score     REAL,
    raw_deleted         INTEGER DEFAULT 0,
    topic_query         TEXT                    -- запрос при котором нашли
);

-- ═══════════════════════════════════════════════════════════
-- ВЕБ КЭШ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS web_cache (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    url                 TEXT UNIQUE,
    title               TEXT,
    content_summary     TEXT,
    full_content        TEXT,
    embedding_id        TEXT,
    fetched_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at          DATETIME
);

-- ═══════════════════════════════════════════════════════════
-- ЛОГИ АГЕНТОВ
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS agent_logs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name          TEXT NOT NULL,
    action              TEXT,
    input_summary       TEXT,
    output_summary      TEXT,
    model_used          TEXT,
    tokens_used         INTEGER,
    duration_ms         INTEGER,
    success             INTEGER DEFAULT 1,
    error_message       TEXT,
    timestamp           DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════
-- FTS5 — полнотекстовый поиск
-- ═══════════════════════════════════════════════════════════
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    content,
    sender_name,
    content='messages',
    content_rowid='id',
    tokenize='unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_fts USING fts5(
    content,
    summary,
    tags,
    content='knowledge',
    content_rowid='id',
    tokenize='unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS file_chunks_fts USING fts5(
    content,
    sender_name,
    content='file_chunks',
    content_rowid='id',
    tokenize='unicode61'
);

-- ═══════════════════════════════════════════════════════════
-- ИНДЕКСЫ для скорости
-- ═══════════════════════════════════════════════════════════
CREATE INDEX IF NOT EXISTS idx_messages_platform    ON messages(platform);
CREATE INDEX IF NOT EXISTS idx_messages_chat_id     ON messages(chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_sender      ON messages(sender_id);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp   ON messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_importance  ON messages(importance_score);

CREATE INDEX IF NOT EXISTS idx_contacts_platform    ON contacts(platform, platform_id);
CREATE INDEX IF NOT EXISTS idx_contacts_trust       ON contacts(trust_score);

CREATE INDEX IF NOT EXISTS idx_knowledge_type       ON knowledge(source_type);
CREATE INDEX IF NOT EXISTS idx_knowledge_confidence ON knowledge(confidence_score);

CREATE INDEX IF NOT EXISTS idx_tasks_status         ON tasks_queue(status, priority);
CREATE INDEX IF NOT EXISTS idx_tasks_scheduled      ON tasks_queue(scheduled_at);

CREATE INDEX IF NOT EXISTS idx_entities_name        ON entities(name);
CREATE INDEX IF NOT EXISTS idx_entities_type        ON entities(entity_type);

CREATE INDEX IF NOT EXISTS idx_relationships_e1     ON relationships(entity1_id);
CREATE INDEX IF NOT EXISTS idx_relationships_e2     ON relationships(entity2_id);

CREATE INDEX IF NOT EXISTS idx_file_chunks_file     ON file_chunks(file_id);
"""

# Триггеры для поддержки FTS5 в актуальном состоянии
FTS_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS messages_fts_insert
    AFTER INSERT ON messages BEGIN
        INSERT INTO messages_fts(rowid, content, sender_name)
        VALUES (new.id, new.content, new.sender_name);
    END;

CREATE TRIGGER IF NOT EXISTS messages_fts_delete
    AFTER DELETE ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content, sender_name)
        VALUES ('delete', old.id, old.content, old.sender_name);
    END;

CREATE TRIGGER IF NOT EXISTS knowledge_fts_insert
    AFTER INSERT ON knowledge BEGIN
        INSERT INTO knowledge_fts(rowid, content, summary, tags)
        VALUES (new.id, new.content, new.summary, new.tags);
    END;

CREATE TRIGGER IF NOT EXISTS file_chunks_fts_insert
    AFTER INSERT ON file_chunks BEGIN
        INSERT INTO file_chunks_fts(rowid, content, sender_name)
        VALUES (new.id, new.content, new.sender_name);
    END;
"""


class Database:
    """Менеджер базы данных ДЖАРВИСА."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()
        logger.info(f"БД инициализирована: {db_path}")

    def _init_db(self):
        """Создаём все таблицы и индексы."""
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.executescript(FTS_TRIGGERS)
            conn.execute("PRAGMA journal_mode=WAL")    # лучше производительность
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")    # 64MB кэш
            conn.execute("PRAGMA foreign_keys=ON")
            conn.commit()

    @contextmanager
    def connect(self):
        """Контекстный менеджер для соединения."""
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"DB error: {e}")
            raise
        finally:
            conn.close()

    def execute(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        """Выполнить SELECT запрос."""
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchall()

    def execute_one(self, query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """Выполнить SELECT, вернуть одну строку."""
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            return cursor.fetchone()

    def execute_write(self, query: str, params: tuple = ()) -> int:
        """Выполнить INSERT/UPDATE/DELETE, вернуть lastrowid."""
        with self.connect() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.lastrowid

    def execute_many(self, query: str, params_list: list[tuple]) -> int:
        """Batch INSERT."""
        with self.connect() as conn:
            conn.executemany(query, params_list)
            conn.commit()
            return len(params_list)

    # ── Сообщения ────────────────────────────────────────────────────────────

    def save_message(self, platform: str, chat_type: str, chat_id: str,
                     chat_title: str, sender_id: str, sender_name: str,
                     content: str, media_type: str = "text",
                     media_description: str = None,
                     importance_score: float = 0,
                     timestamp: datetime = None) -> int:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        return self.execute_write(
            """INSERT INTO messages
               (platform, chat_type, chat_id, chat_title, sender_id, sender_name,
                content, media_type, media_description, importance_score, timestamp)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (platform, chat_type, chat_id, chat_title, sender_id, sender_name,
             content, media_type, media_description, importance_score,
             timestamp.isoformat())
        )

    def get_messages(self, chat_id: str = None, platform: str = None,
                     limit: int = 50, min_importance: float = 0) -> list:
        q = "SELECT * FROM messages WHERE importance_score >= ?"
        p: list = [min_importance]
        if chat_id:
            q += " AND chat_id = ?"
            p.append(chat_id)
        if platform:
            q += " AND platform = ?"
            p.append(platform)
        q += " ORDER BY timestamp DESC LIMIT ?"
        p.append(limit)
        return self.execute(q, tuple(p))

    def search_messages(self, query: str, limit: int = 20) -> list:
        """Полнотекстовый поиск по сообщениям."""
        return self.execute(
            """SELECT m.* FROM messages m
               JOIN messages_fts fts ON m.id = fts.rowid
               WHERE messages_fts MATCH ?
               ORDER BY rank LIMIT ?""",
            (query, limit)
        )

    # ── Контакты / Trust Score ───────────────────────────────────────────────

    def upsert_contact(self, platform: str, platform_id: str,
                       username: str = None, display_name: str = None) -> int:
        existing = self.execute_one(
            "SELECT id, interaction_count FROM contacts WHERE platform=? AND platform_id=?",
            (platform, platform_id)
        )
        if existing:
            self.execute_write(
                """UPDATE contacts SET interaction_count=interaction_count+1,
                   last_interaction=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (existing["id"],)
            )
            return existing["id"]
        return self.execute_write(
            """INSERT INTO contacts (platform, platform_id, username, display_name,
               last_interaction) VALUES (?,?,?,?,CURRENT_TIMESTAMP)""",
            (platform, platform_id, username, display_name)
        )

    def update_trust_score(self, contact_id: int, delta: float):
        self.execute_write(
            """UPDATE contacts
               SET trust_score = MAX(0, MIN(10, trust_score + ?)),
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (delta, contact_id)
        )

    def get_contact(self, platform: str, platform_id: str) -> Optional[sqlite3.Row]:
        return self.execute_one(
            "SELECT * FROM contacts WHERE platform=? AND platform_id=?",
            (platform, platform_id)
        )

    # ── База знаний ──────────────────────────────────────────────────────────

    def save_knowledge(self, content: str, summary: str = None,
                       source_url: str = None, source_type: str = "web",
                       category: str = None, tags: list = None,
                       confidence: float = 0.7) -> int:
        return self.execute_write(
            """INSERT INTO knowledge
               (content, summary, source_url, source_type, category, tags, confidence_score)
               VALUES (?,?,?,?,?,?,?)""",
            (content, summary, source_url, source_type, category,
             json.dumps(tags or []), confidence)
        )

    def search_knowledge(self, query: str, limit: int = 10) -> list:
        return self.execute(
            """SELECT k.* FROM knowledge k
               JOIN knowledge_fts fts ON k.id = fts.rowid
               WHERE knowledge_fts MATCH ?
               ORDER BY rank LIMIT ?""",
            (query, limit)
        )

    # ── Граф знаний ──────────────────────────────────────────────────────────

    def upsert_entity(self, name: str, entity_type: str,
                      description: str = None) -> int:
        existing = self.execute_one(
            "SELECT id FROM entities WHERE name=? AND entity_type=?",
            (name, entity_type)
        )
        if existing:
            self.execute_write(
                "UPDATE entities SET last_seen=CURRENT_TIMESTAMP WHERE id=?",
                (existing["id"],)
            )
            return existing["id"]
        return self.execute_write(
            "INSERT INTO entities (name, entity_type, description) VALUES (?,?,?)",
            (name, entity_type, description)
        )

    def add_relationship(self, entity1_id: int, entity2_id: int,
                         relation_type: str, context: str = None):
        existing = self.execute_one(
            """SELECT id, evidence_count FROM relationships
               WHERE entity1_id=? AND entity2_id=? AND relation_type=?""",
            (entity1_id, entity2_id, relation_type)
        )
        if existing:
            self.execute_write(
                """UPDATE relationships
                   SET evidence_count=evidence_count+1, last_seen=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (existing["id"],)
            )
        else:
            self.execute_write(
                """INSERT INTO relationships
                   (entity1_id, entity2_id, relation_type, context)
                   VALUES (?,?,?,?)""",
                (entity1_id, entity2_id, relation_type, context)
            )

    # ── Файлы ────────────────────────────────────────────────────────────────

    def create_file_record(self, name: str, extension: str,
                           file_type: str, size: int) -> int:
        return self.execute_write(
            """INSERT INTO uploaded_files
               (original_name, extension, file_type, file_size_bytes)
               VALUES (?,?,?,?)""",
            (name, extension, file_type, size)
        )

    def update_file_status(self, file_id: int, status: str,
                           progress: int = None, **kwargs):
        sets = ["status=?"]
        params: list = [status]
        if progress is not None:
            sets.append("progress_percent=?")
            params.append(progress)
        for k, v in kwargs.items():
            sets.append(f"{k}=?")
            params.append(v)
        if status == "done":
            sets.append("processed_at=CURRENT_TIMESTAMP")
        params.append(file_id)
        self.execute_write(
            f"UPDATE uploaded_files SET {', '.join(sets)} WHERE id=?",
            tuple(params)
        )

    def save_file_chunk(self, file_id: int, content: str, chunk_index: int,
                        chunk_type: str = "text", sender_name: str = None,
                        message_date: datetime = None,
                        importance: float = 5.0) -> int:
        return self.execute_write(
            """INSERT INTO file_chunks
               (file_id, content, chunk_index, chunk_type, sender_name,
                message_date, importance_score)
               VALUES (?,?,?,?,?,?,?)""",
            (file_id, content, chunk_index, chunk_type, sender_name,
             message_date.isoformat() if message_date else None, importance)
        )

    # ── Задачи ───────────────────────────────────────────────────────────────

    def add_task(self, task_type: str, payload: dict,
                 priority: int = 3, scheduled_at: datetime = None) -> int:
        return self.execute_write(
            """INSERT INTO tasks_queue (task_type, payload, priority, scheduled_at)
               VALUES (?,?,?,?)""",
            (task_type, json.dumps(payload), priority,
             scheduled_at.isoformat() if scheduled_at else
             datetime.now(timezone.utc).isoformat())
        )

    def get_pending_tasks(self, limit: int = 10) -> list:
        return self.execute(
            """SELECT * FROM tasks_queue
               WHERE status='pending' AND scheduled_at <= CURRENT_TIMESTAMP
               ORDER BY priority ASC, scheduled_at ASC
               LIMIT ?""",
            (limit,)
        )

    def complete_task(self, task_id: int, success: bool = True,
                      error: str = None):
        status = "done" if success else "failed"
        self.execute_write(
            """UPDATE tasks_queue
               SET status=?, completed_at=CURRENT_TIMESTAMP, error_message=?
               WHERE id=?""",
            (status, error, task_id)
        )

    # ── Автоответы ───────────────────────────────────────────────────────────

    def save_auto_reply(self, platform: str, chat_id: str, sender_name: str,
                        original: str, reply: str, confidence: float,
                        level: int) -> int:
        return self.execute_write(
            """INSERT INTO auto_replies
               (platform, chat_id, sender_name, original_message, my_reply,
                confidence_score, autonomy_level)
               VALUES (?,?,?,?,?,?,?)""",
            (platform, chat_id, sender_name, original, reply, confidence, level)
        )

    def get_unreviewed_replies(self) -> list:
        return self.execute(
            "SELECT * FROM auto_replies WHERE was_reviewed=0 ORDER BY timestamp DESC"
        )

    # ── Логи агентов ─────────────────────────────────────────────────────────

    def log_agent(self, agent_name: str, action: str, model: str = None,
                  tokens: int = 0, duration_ms: int = 0,
                  success: bool = True, error: str = None,
                  input_summary: str = None, output_summary: str = None):
        self.execute_write(
            """INSERT INTO agent_logs
               (agent_name, action, model_used, tokens_used, duration_ms,
                success, error_message, input_summary, output_summary)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (agent_name, action, model, tokens, duration_ms,
             1 if success else 0, error, input_summary, output_summary)
        )

    # ── Утилиты ──────────────────────────────────────────────────────────────

    def vacuum(self):
        """Дефрагментация БД (ночью)."""
        with self.connect() as conn:
            conn.execute("VACUUM")
        logger.info("VACUUM выполнен")

    def get_stats(self) -> dict:
        """Статистика БД."""
        stats = {}
        for table in ["messages", "contacts", "knowledge", "entities",
                       "uploaded_files", "file_chunks", "auto_replies"]:
            row = self.execute_one(f"SELECT COUNT(*) as cnt FROM {table}")
            stats[table] = row["cnt"] if row else 0
        return stats


# Глобальный экземпляр
db = Database()
