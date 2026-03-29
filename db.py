"""
╔══════════════════════════════════════════════════════════════════╗
║  JARVIS — db.py  v6.0  (SQLite local + Yandex.Disk backup)      ║
║                                                                  ║
║  ✅ SQLite — работает БЕЗ VPN, БЕЗ интернета                     ║
║  ✅ Яндекс.Диск — бэкап/восстановление автоматически            ║
║  ✅ Раздельные таблицы users / groups                            ║
║  ✅ Удаление по дате и времени                                   ║
║  ✅ Статистика групп и пользователей                             ║
║  ✅ In-memory кэш профилей                                       ║
║  ✅ Batch write буфер для групповых сообщений                    ║
║  ✅ Автоочистка старых данных                                    ║
║  ✅ WAL режим — параллельные чтение/запись без блокировок        ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json, os, time, sqlite3, threading, collections, shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from loguru import logger

_db_instance = None


def get_db() -> "JarvisDB":
    global _db_instance
    if _db_instance is None:
        _db_instance = JarvisDB()
    return _db_instance


def _now_msk() -> str:
    return datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")


# ──────────────────────────────────────────────────────────────
# PROFILE CACHE
# ──────────────────────────────────────────────────────────────

class _ProfileCache:
    """LRU-кэш профилей. TTL 5 минут, макс 500 записей."""
    def __init__(self, max_size=500, ttl=300):
        self._cache, self._times = {}, {}
        self._order = collections.deque()
        self.max_size, self.ttl = max_size, ttl
        self._hits = self._misses = 0

    def get(self, uid):
        e = self._cache.get(uid)
        if e and time.time() - self._times.get(uid, 0) < self.ttl:
            self._hits += 1; return e
        self._misses += 1; return None

    def set(self, uid, profile):
        if uid not in self._cache and len(self._cache) >= self.max_size:
            old = self._order.popleft()
            self._cache.pop(old, None); self._times.pop(old, None)
        self._cache[uid] = profile; self._times[uid] = time.time()
        if uid not in self._order: self._order.append(uid)

    def invalidate(self, uid):
        self._cache.pop(uid, None); self._times.pop(uid, None)

    def clear(self):
        self._cache.clear(); self._times.clear()
        self._order.clear(); self._hits = self._misses = 0

    def stats(self) -> str:
        total = self._hits + self._misses
        ratio = round(self._hits / total * 100) if total else 0
        return f"ProfileCache: {len(self._cache)} записей, {ratio}% hit rate"


# ──────────────────────────────────────────────────────────────
# MAIN DB CLASS
# ──────────────────────────────────────────────────────────────

class JarvisDB:
    """
    SQLite база данных Джарвиса.

    Структура:
      user_messages   — история ЛС с ботом (по пользователям)
      group_messages  — лог групповых чатов (по группам)
      user_profiles   — профили пользователей
      group_profiles  — профили групп (название, статистика)
      reminders       — напоминания
      links           — быстрые ссылки
    """

    MESSAGES_MAX_PER_USER = 2000
    GROUP_MSG_MAX_DAYS    = 90

    def __init__(self):
        import config as _cfg
        # Путь к БД — в папке database/
        _db_dir = getattr(_cfg, 'DIR_DATABASE', _cfg.BASE_DIR / 'database')
        _db_dir.mkdir(parents=True, exist_ok=True)
        self._path = str(getattr(_cfg, 'DB_FILE', _db_dir / 'Jarvis.db'))
        self._conn: Optional[sqlite3.Connection] = None
        self._lock          = threading.Lock()
        self._profile_cache = _ProfileCache()
        self._gm_buffer: list = []
        self._gm_lock       = threading.Lock()
        self._last_flush    = time.time()
        self._init_db()

    # ── Connection ─────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                self._path,
                check_same_thread=False,
                timeout=30,
            )
            self._conn.row_factory = sqlite3.Row
            # WAL режим — параллельные чтение и запись без блокировок
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA cache_size=-32000")   # 32MB кэш
            self._conn.execute("PRAGMA temp_store=MEMORY")
            self._conn.execute("PRAGMA mmap_size=268435456") # 256MB mmap
        return self._conn

    def _execute(self, sql: str, params: tuple = (), fetch: str = "none"):
        """Универсальный execute с блокировкой."""
        with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(sql, params)
                if   fetch == "one": result = self._row_to_dict(cur.fetchone())
                elif fetch == "all": result = [self._row_to_dict(r) for r in cur.fetchall()]
                elif fetch == "lastrow": result = cur.lastrowid
                else: result = None
                conn.commit()
                return result
            except sqlite3.IntegrityError:
                return None  # дубль — не страшно
            except Exception as e:
                conn.rollback()
                # Не логируем ожидаемые ошибки миграции
                if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
                    logger.error(f"❌ DB: {sql[:60]} — {e}")
                raise

    def _executemany(self, sql: str, rows: list):
        with self._lock:
            conn = self._connect()
            try:
                conn.executemany(sql, rows)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.warning(f"⚠️ executemany: {e}")

    @staticmethod
    def _row_to_dict(row) -> Optional[dict]:
        if row is None: return None
        return dict(row)

    def _fetchall(self, cur) -> list[dict]:
        return [self._row_to_dict(r) for r in cur.fetchall()]

    def _fetchone(self, cur) -> Optional[dict]:
        return self._row_to_dict(cur.fetchone())

    def _ph(self) -> str:
        return "?"

    # ── Init ────────────────────────────────────────────────────

    def _init_db(self):
        """Создаёт все таблицы и индексы."""
        conn = self._connect()
        conn.executescript("""
            -- ══ ПОЛЬЗОВАТЕЛИ ══════════════════════════════════

            CREATE TABLE IF NOT EXISTS user_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id   INTEGER NOT NULL,
                role        TEXT    NOT NULL DEFAULT 'user',
                text        TEXT    NOT NULL,
                username    TEXT    DEFAULT '',
                ts          TEXT    DEFAULT (datetime('now','+3 hours')),
                msg_id      INTEGER DEFAULT 0,
                chat_id     INTEGER DEFAULT 0,
                deleted     INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_um_sender  ON user_messages(sender_id);
            CREATE INDEX IF NOT EXISTS idx_um_chat    ON user_messages(chat_id);
            CREATE INDEX IF NOT EXISTS idx_um_deleted ON user_messages(deleted);
            CREATE INDEX IF NOT EXISTS idx_um_ts      ON user_messages(ts DESC);

            CREATE TABLE IF NOT EXISTS user_profiles (
                uid     INTEGER PRIMARY KEY,
                facts   TEXT    DEFAULT '[]',
                style   TEXT    DEFAULT 'normal',
                updated TEXT    DEFAULT (datetime('now','+3 hours'))
            );

            -- ══ ГРУППЫ ════════════════════════════════════════

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
            CREATE INDEX IF NOT EXISTS idx_gm_chat    ON group_messages(chat_id);
            CREATE INDEX IF NOT EXISTS idx_gm_deleted ON group_messages(deleted);
            CREATE INDEX IF NOT EXISTS idx_gm_saved   ON group_messages(saved_at DESC);
            CREATE INDEX IF NOT EXISTS idx_gm_sender  ON group_messages(sender_id);

            CREATE TABLE IF NOT EXISTS group_profiles (
                chat_id     INTEGER PRIMARY KEY,
                title       TEXT    DEFAULT '',
                username    TEXT    DEFAULT '',
                member_count INTEGER DEFAULT 0,
                first_seen  TEXT    DEFAULT (datetime('now','+3 hours')),
                last_seen   TEXT    DEFAULT (datetime('now','+3 hours')),
                notes       TEXT    DEFAULT ''
            );

            -- ══ НАПОМИНАНИЯ ═══════════════════════════════════

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
            CREATE INDEX IF NOT EXISTS idx_rem_done ON reminders(done);
            CREATE INDEX IF NOT EXISTS idx_rem_fire ON reminders(fire_at);

            -- ══ ССЫЛКИ ════════════════════════════════════════

            CREATE TABLE IF NOT EXISTS links (
                name    TEXT PRIMARY KEY,
                url     TEXT NOT NULL,
                added   TEXT DEFAULT (datetime('now','+3 hours'))
            );

            -- ══ СТАТИСТИКА ════════════════════════════════════

            CREATE TABLE IF NOT EXISTS weekly_stats (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start  TEXT NOT NULL,
                week_end    TEXT NOT NULL,
                user_msgs   INTEGER DEFAULT 0,
                group_msgs  INTEGER DEFAULT 0,
                users_count INTEGER DEFAULT 0,
                groups_count INTEGER DEFAULT 0,
                top_user    TEXT DEFAULT '',
                top_group   TEXT DEFAULT '',
                created_at  TEXT DEFAULT (datetime('now','+3 hours'))
            );
        """)
        conn.commit()

        # Миграция: если есть старая таблица messages → переносим
        try:
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            if 'messages' in tables and 'user_messages' in tables:
                count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
                if count > 0:
                    conn.execute("""
                        INSERT OR IGNORE INTO user_messages
                            (id, sender_id, role, text, username, ts, msg_id, chat_id, deleted)
                        SELECT id, sender_id, role, text, username, ts, msg_id, chat_id, deleted
                        FROM messages
                    """)
                    conn.commit()
                    logger.info(f"🔄 Миграция: перенесено {count} сообщений из messages → user_messages")
        except Exception:
            pass

        logger.info(f"✅ SQLite БД готова: {self._path}")

    # ══════════════════════════════════════════
    # MAINTENANCE
    # ══════════════════════════════════════════

    def cleanup_old_data(self):
        """Автоочистка: старые сообщения, обрезка истории, выполненные напоминания."""
        try:
            conn = self._connect()
            with self._lock:
                # 1. Старые групповые сообщения
                conn.execute("""
                    DELETE FROM group_messages
                    WHERE saved_at < datetime('now', '-90 days', '+3 hours')
                """)
                gm = conn.total_changes

                # 2. Обрезаем историю ЛС — последние 2000 на пользователя
                conn.execute("""
                    DELETE FROM user_messages WHERE id IN (
                        SELECT id FROM (
                            SELECT id, ROW_NUMBER() OVER (
                                PARTITION BY sender_id ORDER BY id DESC
                            ) AS rn FROM user_messages
                        ) WHERE rn > ?
                    )
                """, (self.MESSAGES_MAX_PER_USER,))
                msg = conn.total_changes - gm

                # 3. Старые выполненные напоминания
                conn.execute("""
                    DELETE FROM reminders
                    WHERE done=1 AND created < datetime('now', '-30 days', '+3 hours')
                """)
                rem = conn.total_changes - gm - msg

                conn.commit()

            if gm or msg or rem:
                logger.info(f"🧹 Очистка БД: group -{gm}, messages -{msg}, reminders -{rem}")
        except Exception as e:
            logger.warning(f"⚠️ Автоочистка: {e}")

    def vacuum(self):
        """VACUUM + ANALYZE для оптимизации SQLite."""
        try:
            with self._lock:
                self._connect().execute("PRAGMA optimize")
                self._connect().execute("ANALYZE")
            logger.info("✅ SQLite ANALYZE выполнен")
        except Exception as e:
            logger.warning(f"⚠️ VACUUM: {e}")

    def get_db_stats(self) -> dict:
        try:
            conn = self._connect()
            um   = conn.execute("SELECT COUNT(*) FROM user_messages").fetchone()[0]
            gm   = conn.execute("SELECT COUNT(*) FROM group_messages").fetchone()[0]
            rem  = conn.execute("SELECT COUNT(*) FROM reminders WHERE done=0").fetchone()[0]
            prof = conn.execute("SELECT COUNT(*) FROM user_profiles").fetchone()[0]
            grp  = conn.execute("SELECT COUNT(*) FROM group_profiles").fetchone()[0]
            lnk  = conn.execute("SELECT COUNT(*) FROM links").fetchone()[0]
            size = round(os.path.getsize(self._path) / 1024, 1)
            return {
                "user_messages": um, "group_messages": gm,
                "reminders": rem, "profiles": prof,
                "groups": grp, "links": lnk,
                "db_size_kb": size,
                "cache": self._profile_cache.stats(),
            }
        except Exception as e:
            return {"error": str(e)}

    def ping(self) -> bool:
        try:
            self._execute("SELECT 1", fetch="one")
            return True
        except:
            return False

    # ══════════════════════════════════════════
    # USER MESSAGES
    # ══════════════════════════════════════════

    def save_message(self, sender_id: int, role: str, text: str,
                     username: str = "", msg_id: int = 0, chat_id: int = 0):
        if not text or not str(text).strip():
            return
        self._execute(
            "INSERT INTO user_messages (sender_id,role,text,username,msg_id,chat_id,ts) VALUES (?,?,?,?,?,?,?)",
            (sender_id, role, str(text).strip(), username or "", msg_id, chat_id, _now_msk())
        )

    def get_recent(self, sender_id: int, n: int = 30) -> list[dict]:
        try:
            rows = self._execute(
                "SELECT * FROM user_messages WHERE sender_id=? ORDER BY id DESC LIMIT ?",
                (sender_id, n), fetch="all") or []
            return list(reversed(rows))
        except:
            return []

    def search_messages(self, sender_id: int, query: str, limit: int = 20) -> list[dict]:
        try:
            return self._execute(
                "SELECT * FROM user_messages WHERE sender_id=? AND text LIKE ? ORDER BY id DESC LIMIT ?",
                (sender_id, f"%{query}%", limit), fetch="all") or []
        except:
            return []

    def message_stats(self, sender_id: int) -> dict:
        try:
            row = self._execute(
                "SELECT COUNT(*) as total, SUM(CASE WHEN role='user' THEN 1 ELSE 0 END) as um,"
                " MIN(ts) as first_date, MAX(ts) as last_date"
                " FROM user_messages WHERE sender_id=?",
                (sender_id,), fetch="one") or {}
            total = row.get("total", 0) or 0
            um    = row.get("um", 0) or 0
            return {"total": total, "user_msgs": um, "bot_msgs": total - um,
                    "first_date": row.get("first_date", "—"), "last_date": row.get("last_date", "—")}
        except:
            return {"total": 0, "user_msgs": 0, "bot_msgs": 0, "first_date": "—", "last_date": "—"}

    def get_all_messages(self, limit: int = 50000) -> list[dict]:
        try:
            return self._execute(
                "SELECT * FROM user_messages ORDER BY id DESC LIMIT ?",
                (limit,), fetch="all") or []
        except:
            return []

    # ══════════════════════════════════════════
    # DELETE BY DATE/TIME — удаление по периоду
    # ══════════════════════════════════════════

    def delete_messages_by_date(self, date_from: str, date_to: str = "",
                                 chat_id: int = 0, scope: str = "all") -> dict:
        """
        Удаляет сообщения за указанный период.

        scope: "users" | "groups" | "all"
        date_from: "2026-03-07" или "2026-03-07 14:00"
        date_to:   если пустой — до конца дня date_from

        Возвращает {"user_msgs": N, "group_msgs": N}
        """
        # Нормализуем дату
        if len(date_from) == 10:  # только дата
            ts_from = date_from + " 00:00:00"
            ts_to   = date_to if date_to else (date_from + " 23:59:59")
        else:
            ts_from = date_from
            ts_to   = date_to if date_to else date_from

        if len(ts_to) == 10:
            ts_to += " 23:59:59"

        deleted = {"user_msgs": 0, "group_msgs": 0}

        with self._lock:
            conn = self._connect()

            if scope in ("users", "all"):
                if chat_id:
                    cur = conn.execute(
                        "DELETE FROM user_messages WHERE ts>=? AND ts<=? AND chat_id=?",
                        (ts_from, ts_to, chat_id))
                else:
                    cur = conn.execute(
                        "DELETE FROM user_messages WHERE ts>=? AND ts<=?",
                        (ts_from, ts_to))
                deleted["user_msgs"] = cur.rowcount

            if scope in ("groups", "all"):
                if chat_id:
                    cur = conn.execute(
                        "DELETE FROM group_messages WHERE saved_at>=? AND saved_at<=? AND chat_id=?",
                        (ts_from, ts_to, chat_id))
                else:
                    cur = conn.execute(
                        "DELETE FROM group_messages WHERE saved_at>=? AND saved_at<=?",
                        (ts_from, ts_to))
                deleted["group_msgs"] = cur.rowcount

            conn.commit()

        logger.info(f"🗑 Удалено по дате [{ts_from}..{ts_to}]: {deleted}")
        return deleted

    def get_messages_by_date(self, date_from: str, date_to: str = "",
                              chat_id: int = 0, limit: int = 50) -> list[dict]:
        """Возвращает сообщения за период (для просмотра перед удалением)."""
        if len(date_from) == 10:
            ts_from = date_from + " 00:00:00"
            ts_to   = date_to if date_to else (date_from + " 23:59:59")
        else:
            ts_from = date_from
            ts_to   = date_to if date_to else date_from
        if len(ts_to) == 10:
            ts_to += " 23:59:59"

        results = []
        try:
            if chat_id:
                rows = self._execute(
                    "SELECT * FROM user_messages WHERE ts>=? AND ts<=? AND chat_id=? ORDER BY ts DESC LIMIT ?",
                    (ts_from, ts_to, chat_id, limit), fetch="all") or []
            else:
                rows = self._execute(
                    "SELECT * FROM user_messages WHERE ts>=? AND ts<=? ORDER BY ts DESC LIMIT ?",
                    (ts_from, ts_to, limit), fetch="all") or []
            results.extend(rows)
        except:
            pass
        return results

    # ══════════════════════════════════════════
    # GROUP MESSAGES
    # ══════════════════════════════════════════

    def save_group_msg(self, chat_id: int, msg_id: int, sender: str,
                       sender_id: int, text: str, date: str):
        if not text:
            return
        row = (msg_id, chat_id, sender or "", sender_id, str(text).strip(), date or "", _now_msk())
        with self._gm_lock:
            self._gm_buffer.append(row)
            needs_flush = (
                len(self._gm_buffer) >= 20 or
                time.time() - self._last_flush >= 3.0
            )
        if needs_flush:
            self._flush_gm_buffer()

    def _flush_gm_buffer(self):
        with self._gm_lock:
            rows = list(self._gm_buffer)
            self._gm_buffer.clear()
            self._last_flush = time.time()
        if not rows:
            return
        try:
            self._executemany("""
                INSERT OR REPLACE INTO group_messages
                    (msg_id, chat_id, sender, sender_id, text, date, deleted, saved_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            """, rows)
        except Exception as e:
            logger.warning(f"⚠️ GM flush {len(rows)} строк: {e}")

    def flush(self):
        self._flush_gm_buffer()

    def mark_deleted(self, chat_id: int, msg_ids: list[int]):
        if not msg_ids:
            return
        placeholders = ",".join("?" * len(msg_ids))
        try:
            with self._lock:
                conn = self._connect()
                conn.execute(
                    f"UPDATE group_messages SET deleted=1 WHERE chat_id=? AND msg_id IN ({placeholders})",
                    [chat_id] + msg_ids)
                conn.execute(
                    f"UPDATE user_messages SET deleted=1 WHERE chat_id=? AND msg_id IN ({placeholders})",
                    [chat_id] + msg_ids)
                conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ mark_deleted: {e}")

    def mark_deleted_all_chats(self, msg_ids: list[int]):
        if not msg_ids:
            return
        placeholders = ",".join("?" * len(msg_ids))
        try:
            with self._lock:
                conn = self._connect()
                conn.execute(f"UPDATE group_messages SET deleted=1 WHERE msg_id IN ({placeholders})", msg_ids)
                conn.execute(f"UPDATE user_messages SET deleted=1 WHERE msg_id IN ({placeholders})", msg_ids)
                conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ mark_deleted_all: {e}")

    def get_deleted(self, chat_id: int, limit: int = 20, date_filter: str = "") -> list[dict]:
        try:
            if date_filter:
                return self._execute(
                    "SELECT * FROM group_messages WHERE chat_id=? AND deleted=1 AND date LIKE ? ORDER BY msg_id DESC LIMIT ?",
                    (chat_id, f"%{date_filter}%", limit), fetch="all") or []
            return self._execute(
                "SELECT * FROM group_messages WHERE chat_id=? AND deleted=1 ORDER BY msg_id DESC LIMIT ?",
                (chat_id, limit), fetch="all") or []
        except:
            return []

    def get_all_group_messages(self, limit: int = 50000) -> list[dict]:
        self.flush()
        try:
            return self._execute(
                "SELECT * FROM group_messages ORDER BY saved_at DESC LIMIT ?",
                (limit,), fetch="all") or []
        except:
            return []

    # ══════════════════════════════════════════
    # GROUP PROFILES
    # ══════════════════════════════════════════

    def update_group_profile(self, chat_id: int, title: str = "",
                              username: str = "", member_count: int = 0):
        """Обновляет или создаёт профиль группы."""
        try:
            self._execute("""
                INSERT INTO group_profiles (chat_id, title, username, member_count, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    title=EXCLUDED.title,
                    username=EXCLUDED.username,
                    member_count=EXCLUDED.member_count,
                    last_seen=EXCLUDED.last_seen
            """, (chat_id, title or "", username or "", member_count, _now_msk(), _now_msk()))
        except Exception as e:
            # SQLite не поддерживает EXCLUDED, используем INSERT OR REPLACE
            try:
                existing = self._execute("SELECT * FROM group_profiles WHERE chat_id=?", (chat_id,), fetch="one")
                first_seen = existing["first_seen"] if existing else _now_msk()
                self._execute("""
                    INSERT OR REPLACE INTO group_profiles
                        (chat_id, title, username, member_count, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (chat_id, title or "", username or "", member_count, first_seen, _now_msk()))
            except Exception as e2:
                logger.warning(f"⚠️ update_group_profile: {e2}")

    def get_group_profile(self, chat_id: int) -> Optional[dict]:
        return self._execute("SELECT * FROM group_profiles WHERE chat_id=?", (chat_id,), fetch="one")

    def list_groups(self) -> list[dict]:
        return self._execute("SELECT * FROM group_profiles ORDER BY last_seen DESC", fetch="all") or []

    def get_group_stats(self, chat_id: int, days: int = 7) -> dict:
        """Статистика группы за N дней."""
        try:
            conn = self._connect()
            now_msk = datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
            period_start = (now_msk - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            prev_start   = (now_msk - timedelta(days=days * 2)).strftime("%Y-%m-%d %H:%M:%S")

            # Всего сообщений за период (не удалённых)
            current = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=0",
                (chat_id, period_start)).fetchone()[0]

            # Предыдущий период для сравнения
            prev = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND saved_at>=? AND saved_at<? AND deleted=0",
                (chat_id, prev_start, period_start)).fetchone()[0]

            # Удалённые за весь период (включая те что были удалены после отправки)
            deleted_cnt = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=1",
                (chat_id, period_start)).fetchone()[0]

            # Топ участников по количеству сообщений
            top_users = [dict(r) for r in conn.execute("""
                SELECT MAX(sender) as sender, COUNT(*) as cnt FROM group_messages
                WHERE chat_id=? AND saved_at>=? AND deleted=0
                GROUP BY sender_id ORDER BY cnt DESC LIMIT 5
            """, (chat_id, period_start)).fetchall()]

            # Уникальных участников
            unique_users = conn.execute(
                "SELECT COUNT(DISTINCT sender_id) FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=0",
                (chat_id, period_start)).fetchone()[0]

            # Активность по часам — топ-3 пика
            by_hour = [dict(r) for r in conn.execute("""
                SELECT strftime('%H', saved_at) as hour, COUNT(*) as cnt
                FROM group_messages WHERE chat_id=? AND saved_at>=? AND deleted=0
                GROUP BY hour ORDER BY cnt DESC LIMIT 3
            """, (chat_id, period_start)).fetchall()]

            # Процент изменения — ограничиваем ±999%
            if prev > 0:
                raw_change = round((current - prev) / prev * 100)
                change = max(-999, min(999, raw_change))
            else:
                change = 0

            profile = self.get_group_profile(chat_id) or {}

            return {
                "current": current, "previous": prev, "change": change,
                "deleted": deleted_cnt, "top_users": top_users,
                "unique_users": unique_users, "days": days,
                "by_hour": by_hour, "title": profile.get("title", str(chat_id)),
            }
        except Exception as e:
            logger.warning(f"⚠️ get_group_stats: {e}")
            return {"current": 0, "previous": 0, "change": 0, "deleted": 0,
                    "top_users": [], "unique_users": 0, "days": days, "by_hour": [], "title": ""}

    # ══════════════════════════════════════════
    # USER PROFILES
    # ══════════════════════════════════════════

    def load_profile(self, uid: int) -> dict:
        cached = self._profile_cache.get(uid)
        if cached is not None:
            return cached
        try:
            row = self._execute("SELECT * FROM user_profiles WHERE uid=?", (uid,), fetch="one")
            if not row:
                profile = {"uid": uid, "facts": [], "style": "normal"}
            else:
                profile = dict(row)
                try:
                    profile["facts"] = json.loads(profile.get("facts") or "[]")
                except:
                    profile["facts"] = []
            self._profile_cache.set(uid, profile)
            return profile
        except:
            return {"uid": uid, "facts": [], "style": "normal"}

    def save_profile(self, uid: int, profile: dict):
        try:
            facts_json = json.dumps(profile.get("facts", []), ensure_ascii=False)
            self._execute("""
                INSERT OR REPLACE INTO user_profiles (uid, facts, style, updated)
                VALUES (?, ?, ?, ?)
            """, (uid, facts_json, profile.get("style", "normal"), _now_msk()))
            self._profile_cache.set(uid, profile)
        except Exception as e:
            logger.error(f"❌ save_profile uid={uid}: {e}")

    def delete_profile(self, uid: int):
        try:
            self._execute("DELETE FROM user_profiles WHERE uid=?", (uid,))
            self._profile_cache.invalidate(uid)
        except Exception as e:
            logger.error(f"❌ delete_profile: {e}")

    def get_all_profiles(self) -> list[dict]:
        try:
            return self._execute("SELECT * FROM user_profiles", fetch="all") or []
        except:
            return []

    # ══════════════════════════════════════════
    # REMINDERS
    # ══════════════════════════════════════════

    def add_reminder(self, uid: int, text: str, fire_at: str, repeat: str = "") -> int:
        try:
            rid = self._execute(
                "INSERT INTO reminders (uid,text,fire_at,repeat,created) VALUES (?,?,?,?,?)",
                (uid, text, fire_at, repeat, _now_msk()), fetch="lastrow")
            return rid or 0
        except Exception as e:
            logger.error(f"❌ add_reminder: {e}"); return 0

    def get_reminders(self, uid: int) -> list[dict]:
        try:
            return self._execute(
                "SELECT * FROM reminders WHERE uid=? AND done=0 ORDER BY fire_at ASC",
                (uid,), fetch="all") or []
        except:
            return []

    def get_due_reminders(self) -> list[dict]:
        try:
            return self._execute(
                "SELECT * FROM reminders WHERE done=0 AND fire_at<=?",
                (_now_iso(),), fetch="all") or []
        except:
            return []

    def mark_reminder_done(self, rid: int):
        try:
            self._execute("UPDATE reminders SET done=1 WHERE id=?", (rid,))
        except Exception as e:
            logger.warning(f"⚠️ mark_reminder_done: {e}")

    def delete_reminder(self, uid: int, rid: int) -> bool:
        try:
            self._execute("UPDATE reminders SET done=1 WHERE id=? AND uid=?", (rid, uid))
            return True
        except:
            return False

    def get_all_reminders(self) -> list[dict]:
        try:
            return self._execute("SELECT * FROM reminders ORDER BY created DESC", fetch="all") or []
        except:
            return []

    # ══════════════════════════════════════════
    # LINKS
    # ══════════════════════════════════════════

    def save_link(self, name: str, url: str):
        try:
            self._execute(
                "INSERT OR REPLACE INTO links (name,url,added) VALUES (?,?,?)",
                (name.lower().strip(), url.strip(), _now_msk()))
        except Exception as e:
            logger.error(f"❌ save_link: {e}")

    def get_link(self, name: str) -> Optional[str]:
        try:
            row = self._execute(
                "SELECT url FROM links WHERE name LIKE ?",
                (f"%{name.lower().strip()}%",), fetch="one")
            return row["url"] if row else None
        except:
            return None

    def list_links(self) -> list[dict]:
        try:
            return self._execute("SELECT name,url,added FROM links ORDER BY name ASC", fetch="all") or []
        except:
            return []

    def delete_link(self, name: str) -> bool:
        try:
            self._execute("DELETE FROM links WHERE name=?", (name.lower().strip(),))
            return True
        except:
            return False

    def get_group_stats_alltime(self, chat_id: int) -> dict:
        """Статистика группы за всё время — для ручного запроса."""
        try:
            conn = self._connect()

            # Всего сообщений за всё время
            total = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND deleted=0",
                (chat_id,)).fetchone()[0]

            # Удалённые за всё время
            deleted_total = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE chat_id=? AND deleted=1",
                (chat_id,)).fetchone()[0]

            # Уникальных участников за всё время
            unique_users = conn.execute(
                "SELECT COUNT(DISTINCT sender_id) FROM group_messages WHERE chat_id=? AND deleted=0",
                (chat_id,)).fetchone()[0]

            # Топ-5 участников за всё время
            top_users = [dict(r) for r in conn.execute("""
                SELECT MAX(sender) as sender, COUNT(*) as cnt FROM group_messages
                WHERE chat_id=? AND deleted=0
                GROUP BY sender_id ORDER BY cnt DESC LIMIT 5
            """, (chat_id,)).fetchall()]

            # Первое и последнее сообщение (диапазон данных)
            first_row = conn.execute(
                "SELECT saved_at FROM group_messages WHERE chat_id=? ORDER BY saved_at ASC LIMIT 1",
                (chat_id,)).fetchone()
            last_row = conn.execute(
                "SELECT saved_at FROM group_messages WHERE chat_id=? ORDER BY saved_at DESC LIMIT 1",
                (chat_id,)).fetchone()

            first_date = (first_row["saved_at"] or "")[:10] if first_row else "—"
            last_date  = (last_row["saved_at"] or "")[:10] if last_row else "—"

            # Самый активный день
            top_day_row = conn.execute("""
                SELECT strftime('%Y-%m-%d', saved_at) as day, COUNT(*) as cnt
                FROM group_messages WHERE chat_id=? AND deleted=0
                GROUP BY day ORDER BY cnt DESC LIMIT 1
            """, (chat_id,)).fetchone()
            top_day = f"{top_day_row['day']} ({top_day_row['cnt']} сообщ.)" if top_day_row else "—"

            profile = self.get_group_profile(chat_id) or {}

            return {
                "total": total, "deleted_total": deleted_total,
                "unique_users": unique_users, "top_users": top_users,
                "first_date": first_date, "last_date": last_date,
                "top_day": top_day, "title": profile.get("title", str(chat_id)),
            }
        except Exception as e:
            logger.warning(f"⚠️ get_group_stats_alltime: {e}")
            return {"total": 0, "deleted_total": 0, "unique_users": 0,
                    "top_users": [], "first_date": "—", "last_date": "—",
                    "top_day": "—", "title": ""}



    def save_weekly_stats(self, stats: dict):
        try:
            self._execute("""
                INSERT INTO weekly_stats
                    (week_start, week_end, user_msgs, group_msgs,
                     users_count, groups_count, top_user, top_group)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                stats.get("week_start",""), stats.get("week_end",""),
                stats.get("user_msgs",0), stats.get("group_msgs",0),
                stats.get("users_count",0), stats.get("groups_count",0),
                stats.get("top_user",""), stats.get("top_group",""),
            ))
        except Exception as e:
            logger.warning(f"⚠️ save_weekly_stats: {e}")

    def get_weekly_report(self) -> dict:
        """Собирает данные для еженедельного отчёта."""
        try:
            conn = self._connect()
            week_ago = (datetime.now(timezone(timedelta(hours=3))).replace(tzinfo=None)
                        - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

            user_msgs = conn.execute(
                "SELECT COUNT(*) FROM user_messages WHERE ts>=? AND role='user'",
                (week_ago,)).fetchone()[0]
            group_msgs = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE saved_at>=? AND deleted=0",
                (week_ago,)).fetchone()[0]
            users_count = conn.execute(
                "SELECT COUNT(DISTINCT sender_id) FROM user_messages WHERE ts>=? AND role='user'",
                (week_ago,)).fetchone()[0]
            groups_count = conn.execute(
                "SELECT COUNT(DISTINCT chat_id) FROM group_messages WHERE saved_at>=?",
                (week_ago,)).fetchone()[0]

            top_user_row = conn.execute("""
                SELECT username, COUNT(*) as cnt FROM user_messages
                WHERE ts>=? AND role='user' GROUP BY sender_id ORDER BY cnt DESC LIMIT 1
            """, (week_ago,)).fetchone()
            top_user = f"{top_user_row['username']} ({top_user_row['cnt']} сообщ.)" if top_user_row else "—"

            top_group_row = conn.execute("""
                SELECT chat_id, COUNT(*) as cnt FROM group_messages
                WHERE saved_at>=? AND deleted=0 GROUP BY chat_id ORDER BY cnt DESC LIMIT 1
            """, (week_ago,)).fetchone()
            if top_group_row:
                gp = self.get_group_profile(top_group_row["chat_id"])
                top_group = f"{gp['title'] if gp else top_group_row['chat_id']} ({top_group_row['cnt']} сообщ.)"
            else:
                top_group = "—"

            deleted_week = conn.execute(
                "SELECT COUNT(*) FROM group_messages WHERE saved_at>=? AND deleted=1",
                (week_ago,)).fetchone()[0]

            return {
                "user_msgs": user_msgs, "group_msgs": group_msgs,
                "users_count": users_count, "groups_count": groups_count,
                "top_user": top_user, "top_group": top_group,
                "deleted_week": deleted_week,
                "week_start": week_ago[:10],
                "week_end": datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d"),
            }
        except Exception as e:
            logger.warning(f"⚠️ get_weekly_report: {e}")
            return {}

    # ══════════════════════════════════════════
    # BACKUP / RESTORE
    # ══════════════════════════════════════════

    def make_backup_copy(self, dest_path: str) -> bool:
        """Создаёт копию БД через SQLite backup API (безопасно при записи)."""
        self.flush()
        try:
            dest = sqlite3.connect(dest_path)
            with self._lock:
                self._connect().backup(dest)
            dest.close()
            return True
        except Exception as e:
            logger.error(f"❌ Бэкап SQLite: {e}")
            return False

    def restore_from_path(self, src_path: str) -> bool:
        """Восстанавливает БД из файла."""
        try:
            import os as _os2
            # Проверяем что файл валидный SQLite
            test_conn = sqlite3.connect(src_path)
            test_conn.execute("SELECT name FROM sqlite_master LIMIT 1")
            test_conn.close()
            # Закрываем текущее соединение
            if self._conn:
                self._conn.close()
                self._conn = None
            # Создаём папку если не существует
            _os2.makedirs(_os2.path.dirname(self._path), exist_ok=True)
            shutil.copy2(src_path, self._path)
            self._connect()  # переподключаемся
            self._profile_cache.clear()
            logger.info(f"✅ БД восстановлена из {src_path}")
            return True
        except sqlite3.DatabaseError as e:
            logger.error(f"❌ Восстановление: файл не является SQLite БД: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Восстановление: {e}")
            return False

    # ══════════════════════════════════════════
    # LEGACY ALIASES (для совместимости с main.py)
    # ══════════════════════════════════════════

    def _q(self, sql: str, params: tuple = (), fetch: str = "none"):
        """Алиас для _execute — используется в main.py."""
        return self._execute(sql, params, fetch)

    def _safe_commit(self): pass  # в SQLite commit внутри _execute
    def _cur(self, dict_row=True): return self._connect().cursor()
    def _fetchall(self, cur): return [self._row_to_dict(r) for r in cur.fetchall()]
    def _fetchone(self, cur): return self._row_to_dict(cur.fetchone())
    def _ph(self): return "?"

    def close(self):
        self.flush()
        if self._conn:
            try: self._conn.close()
            except: pass
        self._conn = None

    def register_bot_chat(self, chat_id: int, chat_type: str = "group", title: str = "") -> None:
        """Регистрирует чат где работает бот."""
        try:
            self._q(
                "CREATE TABLE IF NOT EXISTS bot_chats "
                "(chat_id INTEGER PRIMARY KEY, chat_type TEXT, title TEXT, last_seen TEXT)"
            )
            # Миграция: добавляем last_seen если колонки нет
            try:
                self._q("ALTER TABLE bot_chats ADD COLUMN last_seen TEXT")
            except Exception:
                pass  # уже есть
            from datetime import datetime
            self._q(
                "INSERT OR REPLACE INTO bot_chats (chat_id, chat_type, title, last_seen) "
                "VALUES (?, ?, ?, ?)",
                (chat_id, chat_type, title, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
        except Exception:
            pass

    def get_bot_chat_ids(self) -> set:
        """Возвращает список всех чатов где работает бот."""
        try:
            self._q(
                "CREATE TABLE IF NOT EXISTS bot_chats "
                "(chat_id INTEGER PRIMARY KEY, chat_type TEXT, title TEXT, last_seen TEXT)"
            )
            rows = self._q("SELECT chat_id FROM bot_chats", fetch="all") or []
            return {r["chat_id"] if isinstance(r, dict) else r[0] for r in rows}
        except Exception:
            return set()

    # ══════════════════════════════════════════
    # ML — МАШИННОЕ ОБУЧЕНИЕ
    # ══════════════════════════════════════════

    def _ensure_ml_tables(self):
        """Создаёт ML таблицы если не существуют. Автомиграция старых БД."""
        self._execute("""
            CREATE TABLE IF NOT EXISTS ml_user_prefs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id INTEGER NOT NULL,
                pref_type TEXT NOT NULL,
                value TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(sender_id, pref_type)
            )
        """)
        self._execute("""
            CREATE TABLE IF NOT EXISTS ml_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id INTEGER NOT NULL,
                topic TEXT NOT NULL,
                sample TEXT,
                response TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        self._execute("""
            CREATE TABLE IF NOT EXISTS ml_knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                content TEXT NOT NULL,
                source TEXT DEFAULT 'jarvis',
                confidence REAL DEFAULT 0.5,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        self._execute("""
            CREATE TABLE IF NOT EXISTS ml_training_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                messages_processed INTEGER DEFAULT 0,
                patterns_found INTEGER DEFAULT 0,
                knowledge_items INTEGER DEFAULT 0,
                duration_sec REAL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        # Автомиграция — добавляем столбцы если их нет в старых БД
        for _sql in [
            "ALTER TABLE ml_patterns ADD COLUMN topic TEXT",
            "ALTER TABLE ml_patterns ADD COLUMN sample TEXT",
            "ALTER TABLE ml_patterns ADD COLUMN response TEXT",
            "ALTER TABLE ml_knowledge ADD COLUMN confidence REAL DEFAULT 0.5",
            "ALTER TABLE ml_user_prefs ADD COLUMN confidence REAL DEFAULT 0.5",
            "ALTER TABLE ml_user_prefs ADD COLUMN updated_at TEXT",
            "ALTER TABLE ml_training_log ADD COLUMN messages_processed INTEGER DEFAULT 0",
            "ALTER TABLE ml_training_log ADD COLUMN patterns_found INTEGER DEFAULT 0",
            "ALTER TABLE ml_training_log ADD COLUMN knowledge_items INTEGER DEFAULT 0",
            "ALTER TABLE ml_training_log ADD COLUMN duration_sec REAL DEFAULT 0",
        ]:
            try: self._execute(_sql)
            except Exception: pass

    def ml_save_pattern(self, sender_id: int, topic: str, sample: str, response: str):
        """Сохраняет паттерн поведения пользователя."""
        try:
            self._ensure_ml_tables()
            self._execute(
                "INSERT INTO ml_patterns (sender_id, topic, sample, response) VALUES (?,?,?,?)",
                (sender_id, topic, sample[:300], response[:300])
            )
        except Exception as e:
            logger.debug(f"ml_save_pattern: {e}")

    def ml_save_prefs(self, sender_id: int, pref_type: str, value: str, confidence: float = 0.5):
        """Сохраняет предпочтение пользователя (upsert)."""
        try:
            self._ensure_ml_tables()
            existing = self._execute(
                "SELECT confidence FROM ml_user_prefs WHERE sender_id=? AND pref_type=?",
                (sender_id, pref_type), fetch="one"
            )
            if existing:
                new_conf = min(1.0, (existing.get("confidence", 0.5) + confidence) / 1.5)
                self._execute(
                    "UPDATE ml_user_prefs SET value=?, confidence=?, updated_at=datetime('now') "
                    "WHERE sender_id=? AND pref_type=?",
                    (value, new_conf, sender_id, pref_type)
                )
            else:
                self._execute(
                    "INSERT INTO ml_user_prefs (sender_id, pref_type, value, confidence) VALUES (?,?,?,?)",
                    (sender_id, pref_type, value, confidence)
                )
        except Exception as e:
            logger.debug(f"ml_save_prefs: {e}")

    def ml_save_knowledge(self, topic: str, content: str, source: str = "jarvis", confidence: float = 0.5):
        """Сохраняет знание в базу."""
        try:
            self._ensure_ml_tables()
            self._execute(
                "INSERT INTO ml_knowledge (topic, content, source, confidence) VALUES (?,?,?,?)",
                (topic[:100], content[:1000], source, confidence)
            )
        except Exception as e:
            logger.debug(f"ml_save_knowledge: {e}")

    def ml_log_training(self, session_id: str, messages: int, patterns: int, knowledge: int, duration: float):
        """Логирует сессию обучения."""
        try:
            self._ensure_ml_tables()
            self._execute(
                "INSERT INTO ml_training_log (session_id, messages_processed, patterns_found, knowledge_items, duration_sec) "
                "VALUES (?,?,?,?,?)",
                (session_id, messages, patterns, knowledge, duration)
            )
        except Exception as e:
            logger.debug(f"ml_log_training: {e}")

    def ml_get_prefs(self, sender_id: int) -> list[dict]:
        """Возвращает все предпочтения пользователя."""
        try:
            self._ensure_ml_tables()
            return self._execute(
                "SELECT pref_type, value, confidence FROM ml_user_prefs "
                "WHERE sender_id=? ORDER BY confidence DESC",
                (sender_id,), fetch="all"
            ) or []
        except Exception:
            return []

    def ml_get_stats(self) -> dict:
        """Возвращает статистику ML обучения."""
        try:
            self._ensure_ml_tables()
            patterns  = self._execute("SELECT COUNT(*) as n FROM ml_patterns",  fetch="one") or {}
            knowledge = self._execute("SELECT COUNT(*) as n FROM ml_knowledge", fetch="one") or {}
            sessions  = self._execute("SELECT COUNT(*) as n FROM ml_training_log", fetch="one") or {}
            last_sess = self._execute(
                "SELECT created_at, messages_processed FROM ml_training_log ORDER BY id DESC LIMIT 1",
                fetch="one"
            ) or {}
            return {
                "patterns":  patterns.get("n", 0),
                "knowledge": knowledge.get("n", 0),
                "sessions":  sessions.get("n", 0),
                "last_session": last_sess.get("created_at", "никогда"),
                "last_messages": last_sess.get("messages_processed", 0),
            }
        except Exception as e:
            return {"error": str(e)}

