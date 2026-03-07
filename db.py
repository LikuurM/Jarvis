"""
JARVIS — db.py
SQLite база данных. Заменяет все JSON файлы.
Одна точка входа: get_db() → JarvisDB
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from loguru import logger

DB_FILE = Path(__file__).parent / "Jarvis.db"


def get_db() -> "JarvisDB":
    return JarvisDB(DB_FILE)


class JarvisDB:
    """
    Единая SQLite база для Джарвиса.
    Таблицы:
      - messages        (история чатов)
      - group_messages  (логи группы)
      - user_profiles   (профили пользователей)
      - reminders       (напоминания)
      - links           (быстрые ссылки)
    """

    def __init__(self, path: Path):
        self.path = path
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")   # надёжность
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _init_db(self):
        """Создаёт таблицы если их нет."""
        conn = self._connect()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id   INTEGER NOT NULL,
                role        TEXT    NOT NULL,
                text        TEXT    NOT NULL,
                username    TEXT    DEFAULT '',
                ts          TEXT    DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);
            CREATE INDEX IF NOT EXISTS idx_messages_ts     ON messages(ts);

            CREATE TABLE IF NOT EXISTS group_messages (
                msg_id      INTEGER NOT NULL,
                chat_id     INTEGER NOT NULL,
                sender      TEXT    DEFAULT '',
                sender_id   INTEGER DEFAULT 0,
                text        TEXT    NOT NULL,
                date        TEXT    DEFAULT '',
                deleted     INTEGER DEFAULT 0,
                saved_at    TEXT    DEFAULT (datetime('now')),
                PRIMARY KEY (msg_id, chat_id)
            );
            CREATE INDEX IF NOT EXISTS idx_gm_chat    ON group_messages(chat_id);
            CREATE INDEX IF NOT EXISTS idx_gm_deleted ON group_messages(deleted);
            -- Добавляем saved_at если таблица уже существует (миграция)
            

            CREATE TABLE IF NOT EXISTS user_profiles (
                uid     INTEGER PRIMARY KEY,
                facts   TEXT    DEFAULT '[]',
                style   TEXT    DEFAULT 'normal',
                updated TEXT    DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS reminders (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                uid       INTEGER NOT NULL,
                text      TEXT    NOT NULL,
                fire_at   TEXT    NOT NULL,
                repeat    TEXT    DEFAULT '',
                done      INTEGER DEFAULT 0,
                created   TEXT    DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_rem_uid  ON reminders(uid);
            CREATE INDEX IF NOT EXISTS idx_rem_done ON reminders(done);

            CREATE TABLE IF NOT EXISTS links (
                name      TEXT    PRIMARY KEY,
                url       TEXT    NOT NULL,
                added     TEXT    DEFAULT (datetime('now'))
            );
        """)
        conn.commit()
        logger.info(f"SQLite DB: {self.path}")

    # ═══════════════════════════════════════════
    # MESSAGES — история чатов
    # ═══════════════════════════════════════════

    def save_message(self, sender_id: int, role: str, text: str, username: str = ""):
        conn = self._connect()
        conn.execute(
            "INSERT INTO messages (sender_id, role, text, username) VALUES (?,?,?,?)",
            (sender_id, role, text, username)
        )
        conn.commit()

    def get_recent(self, sender_id: int, n: int = 30) -> list[dict]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT ?",
            (sender_id, n)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def search_messages(self, sender_id: int, query: str, limit: int = 20) -> list[dict]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM messages WHERE sender_id=? AND text LIKE ? ORDER BY id DESC LIMIT ?",
            (sender_id, f"%{query}%", limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def message_stats(self, sender_id: int) -> dict:
        conn = self._connect()
        total = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE sender_id=?", (sender_id,)
        ).fetchone()[0]
        user_msgs = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE sender_id=? AND role='user'", (sender_id,)
        ).fetchone()[0]
        first = conn.execute(
            "SELECT ts FROM messages WHERE sender_id=? ORDER BY id ASC LIMIT 1", (sender_id,)
        ).fetchone()
        last = conn.execute(
            "SELECT ts FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT 1", (sender_id,)
        ).fetchone()
        return {
            "total":      total,
            "user_msgs":  user_msgs,
            "bot_msgs":   total - user_msgs,
            "first_date": first[0] if first else "—",
            "last_date":  last[0]  if last  else "—",
        }

    # ═══════════════════════════════════════════
    # GROUP MESSAGES — логи группы
    # ═══════════════════════════════════════════

    def save_group_msg(self, chat_id: int, msg_id: int, sender: str,
                       sender_id: int, text: str, date: str):
        if not text:
            return
        conn = self._connect()
        conn.execute("""
            INSERT OR REPLACE INTO group_messages
            (msg_id, chat_id, sender, sender_id, text, date, deleted, saved_at)
            VALUES (?,?,?,?,?,?,0,datetime('now'))
        """, (msg_id, chat_id, sender, sender_id, text, date))
        conn.commit()

    def mark_deleted(self, chat_id: int, msg_ids: list[int]):
        conn = self._connect()
        for mid in msg_ids:
            conn.execute(
                "UPDATE group_messages SET deleted=1 WHERE chat_id=? AND msg_id=?",
                (chat_id, mid)
            )
        conn.commit()

    def mark_deleted_all_chats(self, msg_ids: list[int]):
        conn = self._connect()
        for mid in msg_ids:
            conn.execute(
                "UPDATE group_messages SET deleted=1 WHERE msg_id=?", (mid,)
            )
        conn.commit()

    def get_deleted(self, chat_id: int, limit: int = 20, date_filter: str = "") -> list[dict]:
        conn = self._connect()
        if date_filter:
            rows = conn.execute("""
                SELECT * FROM group_messages
                WHERE chat_id=? AND deleted=1 AND date LIKE ?
                ORDER BY msg_id DESC LIMIT ?
            """, (chat_id, f"%{date_filter}%", limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM group_messages
                WHERE chat_id=? AND deleted=1
                ORDER BY msg_id DESC LIMIT ?
            """, (chat_id, limit)).fetchall()
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════
    # USER PROFILES
    # ═══════════════════════════════════════════

    def load_profile(self, uid: int) -> dict:
        conn = self._connect()
        row = conn.execute(
            "SELECT * FROM user_profiles WHERE uid=?", (uid,)
        ).fetchone()
        if not row:
            return {"uid": uid, "facts": [], "style": "normal"}
        d = dict(row)
        d["facts"] = json.loads(d.get("facts", "[]"))
        return d

    def save_profile(self, uid: int, profile: dict):
        conn = self._connect()
        facts_json = json.dumps(profile.get("facts", []), ensure_ascii=False)
        conn.execute("""
            INSERT INTO user_profiles (uid, facts, style, updated)
            VALUES (?,?,?, datetime('now'))
            ON CONFLICT(uid) DO UPDATE SET
                facts=excluded.facts,
                style=excluded.style,
                updated=excluded.updated
        """, (uid, facts_json, profile.get("style", "normal")))
        conn.commit()

    def delete_profile(self, uid: int):
        conn = self._connect()
        conn.execute("DELETE FROM user_profiles WHERE uid=?", (uid,))
        conn.commit()

    # ═══════════════════════════════════════════
    # REMINDERS
    # ═══════════════════════════════════════════

    def add_reminder(self, uid: int, text: str, fire_at: str, repeat: str = "") -> int:
        conn = self._connect()
        cur = conn.execute(
            "INSERT INTO reminders (uid, text, fire_at, repeat) VALUES (?,?,?,?)",
            (uid, text, fire_at, repeat)
        )
        conn.commit()
        return cur.lastrowid

    def get_reminders(self, uid: int) -> list[dict]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM reminders WHERE uid=? AND done=0 ORDER BY fire_at ASC",
            (uid,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_due_reminders(self) -> list[dict]:
        conn = self._connect()
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        rows = conn.execute(
            "SELECT * FROM reminders WHERE done=0 AND fire_at <= ?", (now,)
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_reminder_done(self, rid: int):
        conn = self._connect()
        conn.execute("UPDATE reminders SET done=1 WHERE id=?", (rid,))
        conn.commit()

    def delete_reminder(self, uid: int, rid: int) -> bool:
        conn = self._connect()
        cur = conn.execute(
            "UPDATE reminders SET done=1 WHERE id=? AND uid=?", (rid, uid)
        )
        conn.commit()
        return cur.rowcount > 0

    # ═══════════════════════════════════════════
    # LINKS — быстрые ссылки
    # ═══════════════════════════════════════════

    def save_link(self, name: str, url: str):
        conn = self._connect()
        conn.execute("""
            INSERT INTO links (name, url, added)
            VALUES (?,?, datetime('now'))
            ON CONFLICT(name) DO UPDATE SET url=excluded.url, added=excluded.added
        """, (name.lower().strip(), url.strip()))
        conn.commit()

    def get_link(self, name: str) -> str | None:
        conn = self._connect()
        row = conn.execute(
            "SELECT url FROM links WHERE name LIKE ?", (f"%{name.lower().strip()}%",)
        ).fetchone()
        return row[0] if row else None

    def list_links(self) -> list[dict]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT name, url, added FROM links ORDER BY added DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_link(self, name: str) -> bool:
        conn = self._connect()
        cur = conn.execute("DELETE FROM links WHERE name=?", (name.lower().strip(),))
        conn.commit()
        return cur.rowcount > 0

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # ═══════════════════════════════════════════
    # GROUP STATISTICS
    # ═══════════════════════════════════════════

    def get_group_stats(self, chat_id: int, days: int = 7) -> dict:
        """Статистика группы за N дней."""
        conn = self._connect()
        # Миграция: добавляем saved_at если нет
        try:
            conn.execute("ALTER TABLE group_messages ADD COLUMN saved_at TEXT DEFAULT (datetime('now'))")
            conn.commit()
        except Exception:
            pass  # уже есть

        from datetime import datetime, timedelta
        now = datetime.now()
        period_start = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        prev_start   = (now - timedelta(days=days*2)).strftime("%Y-%m-%d %H:%M:%S")

        cur = conn.execute(
            "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=0",
            (chat_id, period_start)
        ).fetchone()[0]
        prev = conn.execute(
            "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND saved_at>=? AND saved_at<? AND deleted=0",
            (chat_id, prev_start, period_start)
        ).fetchone()[0]

        top_users = conn.execute(
            """SELECT sender, COUNT(*) as cnt FROM group_messages
               WHERE chat_id=? AND saved_at>=? AND deleted=0
               GROUP BY sender_id ORDER BY cnt DESC LIMIT 5""",
            (chat_id, period_start)
        ).fetchall()

        deleted = conn.execute(
            "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=1",
            (chat_id, period_start)
        ).fetchone()[0]

        change = 0
        if prev > 0:
            change = round((cur - prev) / prev * 100)

        return {
            "current": cur,
            "previous": prev,
            "change": change,
            "deleted": deleted,
            "top_users": [dict(r) for r in top_users],
            "days": days,
        }
