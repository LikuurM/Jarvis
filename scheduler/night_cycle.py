"""
JARVIS Night Cycle — ночной цикл задач (APScheduler).
Запускается каждую ночь в 3:00, пока пользователь спит.
"""
import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db import db
from agents.base_agent import AgentContext
from config import TELEGRAM_OWNER_ID

logger = logging.getLogger("jarvis.scheduler")


def _make_context() -> AgentContext:
    """Создать системный контекст для ночных задач."""
    return AgentContext(
        original_query="night_cycle",
        user_id=TELEGRAM_OWNER_ID,
        chat_id="system",
        platform="system"
    )


# ── Ночные задачи ─────────────────────────────────────────────────────────────

async def task_scan_social():
    """00:00 — Сканирование соцсетей."""
    logger.info("🌙 Ночной скан соцсетей...")
    try:
        from agents.social_agent import SocialRadarAgent
        agent = SocialRadarAgent()
        result = await agent.run(_make_context())
        logger.info(f"Соцсети: {result.content}")
    except Exception as e:
        logger.error(f"Ошибка скана соцсетей: {e}")


async def task_fact_check():
    """01:30 — Переверификация фактов."""
    logger.info("🔍 Переверификация фактов...")
    try:
        from agents.fact_checker import FactCheckerAgent
        agent = FactCheckerAgent()
        count = await agent.verify_knowledge_batch()
        logger.info(f"Верифицировано: {count} записей")
    except Exception as e:
        logger.error(f"Ошибка верификации: {e}")


async def task_mine_knowledge():
    """02:00 — Добыча знаний (Kaggle, arXiv)."""
    logger.info("⛏️ Добыча знаний...")
    try:
        from agents.knowledge_miner import KnowledgeMinerAgent
        agent = KnowledgeMinerAgent()
        result = await agent.run(_make_context())
        logger.info(f"Знания: {result.content}")
    except Exception as e:
        logger.error(f"Ошибка добычи знаний: {e}")


async def task_consolidate_memory():
    """02:30 — Консолидация памяти."""
    logger.info("🧵 Консолидация памяти...")
    try:
        from agents.memory_agent import MemoryWeaverAgent
        agent = MemoryWeaverAgent()
        count = await agent.consolidate_daily()
        logger.info(f"Память: консолидировано {count} записей")
    except Exception as e:
        logger.error(f"Ошибка консолидации: {e}")


async def task_self_improvement():
    """03:00 — Самосовершенствование."""
    logger.info("🧠 Самосовершенствование...")
    try:
        from agents.self_improvement import SelfImprovementAgent
        agent = SelfImprovementAgent()
        result = await agent.run(_make_context())
        logger.info(f"SelfImprovement: {result.content[:100]}")
    except Exception as e:
        logger.error(f"Ошибка self-improvement: {e}")


async def task_backup():
    """03:30 — Бекап базы данных."""
    logger.info("💾 Создаю бекап...")
    try:
        import shutil
        from config import DB_PATH, BACKUPS_DIR

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUPS_DIR / f"jarvis_{timestamp}.db"

        shutil.copy2(DB_PATH, backup_path)
        logger.info(f"Бекап создан: {backup_path}")

        # Удаляем старые бекапы (оставляем 30)
        backups = sorted(BACKUPS_DIR.glob("jarvis_*.db"),
                         key=lambda f: f.stat().st_mtime)
        for old in backups[:-30]:
            old.unlink()

    except Exception as e:
        logger.error(f"Ошибка бекапа: {e}")


async def task_maintenance():
    """05:00 — Обслуживание БД."""
    logger.info("🔧 Обслуживание БД...")
    try:
        db.vacuum()

        # Удаляем старый веб-кэш (> 7 дней)
        db.execute_write(
            "DELETE FROM web_cache WHERE expires_at < CURRENT_TIMESTAMP"
        )

        # Очищаем кэш TTS
        from tools.tts import tts
        tts.cleanup_cache(max_files=100)

        logger.info("Обслуживание завершено")
    except Exception as e:
        logger.error(f"Ошибка обслуживания: {e}")


async def task_morning_briefing():
    """06:00 — Подготовка утреннего брифинга."""
    logger.info("☀️ Готовлю утренний брифинг...")
    try:
        # Собираем важные события за ночь
        important = db.execute(
            """SELECT summary FROM episodic_memory
               WHERE importance_score >= 7
               AND timestamp > datetime('now', '-8 hours')
               ORDER BY importance_score DESC LIMIT 5"""
        )

        events = [dict(e)["summary"] for e in important]

        # Непросмотренные автоответы
        auto_replies = db.execute(
            "SELECT COUNT(*) as cnt FROM auto_replies WHERE was_reviewed=0"
        )
        replies_count = dict(auto_replies[0])["cnt"] if auto_replies else 0

        # Новые знания за ночь
        new_knowledge = db.execute(
            """SELECT COUNT(*) as cnt FROM knowledge
               WHERE created_at > datetime('now', '-8 hours')"""
        )
        knowledge_count = dict(new_knowledge[0])["cnt"] if new_knowledge else 0

        # Сохраняем брифинг
        briefing = {
            "events": events,
            "auto_replies_count": replies_count,
            "new_knowledge": knowledge_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        import json
        db.execute_write(
            """INSERT OR REPLACE INTO preferences (category, key, value, source)
               VALUES ('system', 'morning_briefing', ?, 'scheduler')""",
            (json.dumps(briefing, ensure_ascii=False),)
        )

        logger.info(
            f"Брифинг готов: {len(events)} событий, "
            f"{replies_count} автоответов, {knowledge_count} новых знаний"
        )

    except Exception as e:
        logger.error(f"Ошибка подготовки брифинга: {e}")


async def task_send_morning_message(bot=None):
    """06:01 — Отправить утреннее сообщение владельцу."""
    if not bot or not TELEGRAM_OWNER_ID:
        return

    try:
        import json
        from core.personality import personality, ResponseTone

        # Получаем сохранённый брифинг
        briefing_row = db.execute_one(
            "SELECT value FROM preferences WHERE category='system' AND key='morning_briefing'"
        )

        if not briefing_row:
            return

        briefing = json.loads(dict(briefing_row)["value"])

        # Форматируем через PersonalityAgent
        response = personality.format_morning_briefing(
            events=briefing.get("events", []),
            auto_replies_count=briefing.get("auto_replies_count", 0),
            pending_tasks=[],
            interesting_finds=[
                f"Добавлено {briefing.get('new_knowledge', 0)} новых знаний за ночь"
            ] if briefing.get("new_knowledge", 0) > 0 else [],
            stats={}
        )

        await bot.send_message(
            TELEGRAM_OWNER_ID,
            response.text,
            parse_mode="Markdown"
        )
        logger.info("Утреннее сообщение отправлено")

    except Exception as e:
        logger.error(f"Ошибка отправки брифинга: {e}")


# ── Планировщик ───────────────────────────────────────────────────────────────

class NightCycleScheduler:
    """Планировщик ночного цикла ДЖАРВИСА."""

    def __init__(self, bot=None):
        self.bot = bot
        self.scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        self._setup_jobs()

    def _setup_jobs(self):
        """Настроить все задания."""

        # 00:00 — Скан соцсетей
        self.scheduler.add_job(
            task_scan_social,
            CronTrigger(hour=0, minute=0),
            id="scan_social",
            name="Скан соцсетей"
        )

        # 01:30 — Верификация фактов
        self.scheduler.add_job(
            task_fact_check,
            CronTrigger(hour=1, minute=30),
            id="fact_check",
            name="Верификация фактов"
        )

        # 02:00 — Добыча знаний
        self.scheduler.add_job(
            task_mine_knowledge,
            CronTrigger(hour=2, minute=0),
            id="mine_knowledge",
            name="Добыча знаний"
        )

        # 02:30 — Консолидация памяти
        self.scheduler.add_job(
            task_consolidate_memory,
            CronTrigger(hour=2, minute=30),
            id="consolidate_memory",
            name="Консолидация памяти"
        )

        # 03:00 — Самосовершенствование
        self.scheduler.add_job(
            task_self_improvement,
            CronTrigger(hour=3, minute=0),
            id="self_improvement",
            name="Самосовершенствование"
        )

        # 03:30 — Бекап
        self.scheduler.add_job(
            task_backup,
            CronTrigger(hour=3, minute=30),
            id="backup",
            name="Бекап БД"
        )

        # 05:00 — Обслуживание
        self.scheduler.add_job(
            task_maintenance,
            CronTrigger(hour=5, minute=0),
            id="maintenance",
            name="Обслуживание"
        )

        # 06:00 — Утренний брифинг (подготовка)
        self.scheduler.add_job(
            task_morning_briefing,
            CronTrigger(hour=6, minute=0),
            id="morning_briefing",
            name="Подготовка брифинга"
        )

        # 06:01 — Отправка брифинга
        async def _send():
            await task_send_morning_message(self.bot)

        self.scheduler.add_job(
            _send,
            CronTrigger(hour=6, minute=1),
            id="send_briefing",
            name="Отправка брифинга"
        )

        # Каждый час — мини-скан (только RSS и GitHub)
        self.scheduler.add_job(
            self._hourly_scan,
            CronTrigger(minute=0),
            id="hourly_scan",
            name="Почасовой скан"
        )

        logger.info(f"Настроено {len(self.scheduler.get_jobs())} задач")

    async def _hourly_scan(self):
        """Почасовой лёгкий скан."""
        logger.debug("Почасовой скан...")
        try:
            from agents.social_agent import SocialRadarAgent
            agent = SocialRadarAgent()
            # Только RSS и GitHub (быстро)
            rss = await agent.scan_rss()
            gh = await agent.scan_github_trending()

            # Сохраняем важное
            for item in (rss + gh)[:5]:
                if item.get("content"):
                    db.save_knowledge(
                        content=item["content"][:500],
                        summary=item.get("title", ""),
                        source_url=item.get("url", ""),
                        source_type=item.get("category", "web"),
                        confidence=0.5
                    )
        except Exception as e:
            logger.debug(f"Почасовой скан ошибка: {e}")

    def start(self):
        """Запустить планировщик."""
        self.scheduler.start()
        logger.info("✓ Планировщик запущен")

    def stop(self):
        """Остановить планировщик."""
        self.scheduler.shutdown()

    def get_jobs_info(self) -> list[dict]:
        """Информация о всех задачах."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time)
                            if job.next_run_time else "не запланировано"
            })
        return jobs
