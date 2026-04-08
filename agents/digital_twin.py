"""
JARVIS Digital Twin — StyleCloner + AutoReply.
Изучает как вы пишете → отвечает за вас когда вы не в сети.
"""
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Optional

from agents.base_agent import BaseAgent, AgentContext, AgentResult
from core.model_router import TaskType
from tools.alerts import AlertLevel, Alert

logger = logging.getLogger("jarvis.digital_twin")


@dataclass
class StyleProfile:
    """Профиль стиля общения пользователя."""
    avg_length: float = 50
    emoji_freq: float = 0.2
    favorite_emoji: list = None
    formality: float = 0.3       # 0=casual, 1=formal
    avg_delay_sec: int = 120
    filler_words: list = None
    humor_freq: float = 0.1
    uses_dots: bool = False       # ставит точки в конце
    uses_caps: bool = False       # пишет с заглавной
    samples: int = 0

    def __post_init__(self):
        if self.favorite_emoji is None:
            self.favorite_emoji = ["👍", "🔥"]
        if self.filler_words is None:
            self.filler_words = []


class StyleClonerAgent(BaseAgent):
    """
    Агент клонирования стиля.
    Изучает как вы пишете и воспроизводит это.
    """

    name = "StyleCloner"
    description = "Клонирование стиля общения для Digital Twin"

    system_prompt = """Ты имитируешь стиль общения конкретного пользователя.
    Сохраняй его лексику, длину сообщений, пунктуацию, эмодзи.
    Не добавляй ничего лишнего — только то что характерно для него."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._profile: Optional[StyleProfile] = None

    async def run(self, context: AgentContext) -> AgentResult:
        """Обновить StyleProfile из свежих сообщений."""
        profile = await self.build_profile()
        return self.success(
            f"StyleProfile обновлён. Образцов: {profile.samples}",
            data=profile.__dict__
        )

    async def build_profile(self) -> StyleProfile:
        """Построить профиль стиля из истории сообщений."""
        # Берём последние 500 сообщений владельца
        messages = self.db.execute(
            """SELECT content FROM messages
               WHERE platform='telegram'
               AND importance_score >= 2
               ORDER BY timestamp DESC LIMIT 500"""
        )

        texts = [dict(m)["content"] for m in messages
                 if dict(m).get("content") and len(dict(m)["content"]) > 2]

        if not texts:
            return StyleProfile()

        total = len(texts)

        # Средняя длина
        avg_len = sum(len(t) for t in texts) / total

        # Эмодзи
        emoji_pat = re.compile(
            "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F9FF\u2600-\u26FF]+",
            flags=re.UNICODE
        )
        emoji_texts = [t for t in texts if emoji_pat.search(t)]
        emoji_freq = len(emoji_texts) / total

        # Любимые эмодзи
        all_emoji = []
        for t in texts[:200]:
            all_emoji.extend(emoji_pat.findall(t))
        from collections import Counter
        emoji_counter = Counter(all_emoji)
        fav_emoji = [e for e, _ in emoji_counter.most_common(5)]

        # Формальность
        with_dots = sum(1 for t in texts if t.rstrip().endswith("."))
        formality = with_dots / total

        # Слова-паразиты
        all_words = " ".join(texts[:100]).lower().split()
        fillers = []
        candidates = ["короче", "ну", "типа", "вообще", "блин",
                       "ладно", "окей", "ок", "понял", "понятно"]
        for c in candidates:
            if all_words.count(c) >= 3:
                fillers.append(c)

        # Заглавные буквы
        with_caps = sum(1 for t in texts if t and t[0].isupper())
        uses_caps = (with_caps / total) > 0.5

        profile = StyleProfile(
            avg_length=avg_len,
            emoji_freq=emoji_freq,
            favorite_emoji=fav_emoji or ["👍"],
            formality=formality,
            filler_words=fillers,
            uses_dots=formality > 0.5,
            uses_caps=uses_caps,
            samples=total
        )

        # Сохраняем в БД
        self._save_profile(profile)
        self._profile = profile
        return profile

    def _save_profile(self, profile: StyleProfile):
        """Сохранить профиль в БД."""
        existing = self.db.execute_one("SELECT id FROM style_profile LIMIT 1")
        if existing:
            self.db.execute_write(
                """UPDATE style_profile SET
                   avg_message_length=?, emoji_frequency=?,
                   favorite_emoji=?, formality_level=?,
                   filler_words=?, samples_count=?,
                   updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (profile.avg_length, profile.emoji_freq,
                 json.dumps(profile.favorite_emoji, ensure_ascii=False),
                 profile.formality,
                 json.dumps(profile.filler_words, ensure_ascii=False),
                 profile.samples, dict(existing)["id"])
            )
        else:
            self.db.execute_write(
                """INSERT INTO style_profile
                   (avg_message_length, emoji_frequency, favorite_emoji,
                    formality_level, filler_words, samples_count)
                   VALUES (?,?,?,?,?,?)""",
                (profile.avg_length, profile.emoji_freq,
                 json.dumps(profile.favorite_emoji, ensure_ascii=False),
                 profile.formality,
                 json.dumps(profile.filler_words, ensure_ascii=False),
                 profile.samples)
            )

    def get_profile(self) -> StyleProfile:
        """Загрузить профиль из БД."""
        if self._profile:
            return self._profile

        row = self.db.execute_one("SELECT * FROM style_profile LIMIT 1")
        if not row:
            return StyleProfile()

        r = dict(row)
        try:
            fav = json.loads(r.get("favorite_emoji") or '["👍"]')
            fillers = json.loads(r.get("filler_words") or '[]')
        except Exception:
            fav, fillers = ["👍"], []

        self._profile = StyleProfile(
            avg_length=r.get("avg_message_length", 50),
            emoji_freq=r.get("emoji_frequency", 0.2),
            favorite_emoji=fav,
            formality=r.get("formality_level", 0.3),
            filler_words=fillers,
            samples=r.get("samples_count", 0)
        )
        return self._profile

    async def apply_style(self, content: str,
                          context: AgentContext) -> str:
        """Применить стиль пользователя к тексту ответа."""
        profile = self.get_profile()

        # Строим промпт с описанием стиля
        style_desc = (
            f"Пиши как этот человек:\n"
            f"- Средняя длина сообщения: {profile.avg_length:.0f} символов\n"
            f"- Эмодзи: {'часто' if profile.emoji_freq > 0.3 else 'редко'}"
            f" (любимые: {', '.join(profile.favorite_emoji[:3])})\n"
            f"- Стиль: {'формальный' if profile.formality > 0.5 else 'неформальный'}\n"
            f"- Слова-паразиты: {', '.join(profile.filler_words[:3]) or 'нет'}\n"
            f"- Заглавная буква в начале: {'да' if profile.uses_caps else 'нет'}\n"
            f"- Точка в конце: {'да' if profile.uses_dots else 'нет'}\n\n"
            f"Перепиши это сообщение в его стиле (только текст ответа):\n{content}"
        )

        response = await self.ask_llm(
            style_desc,
            context,
            task_type=TaskType.SIMPLE,
            max_tokens=300,
            temperature=0.8
        )

        if response.success:
            return response.content.strip()

        # Fallback: базовое применение стиля
        return self._apply_style_basic(content, profile)

    def _apply_style_basic(self, text: str, profile: StyleProfile) -> str:
        """Базовое применение стиля без LLM."""
        result = text.strip()

        # Заглавная буква
        if profile.uses_caps and result:
            result = result[0].upper() + result[1:]
        elif not profile.uses_caps and result:
            result = result[0].lower() + result[1:]

        # Точка в конце
        if profile.uses_dots and not result.endswith((".", "!", "?")):
            result += "."
        elif not profile.uses_dots and result.endswith("."):
            result = result[:-1]

        # Слово-паразит иногда
        if profile.filler_words and random.random() < 0.2:
            filler = random.choice(profile.filler_words)
            if not result.lower().startswith(filler):
                result = filler + ", " + result

        # Эмодзи в конце
        if profile.emoji_freq > 0.3 and random.random() < profile.emoji_freq:
            emoji = random.choice(profile.favorite_emoji[:3])
            result = result + " " + emoji

        return result


class AutoReplyAgent(BaseAgent):
    """
    Агент автоответов Digital Twin.

    Решает:
    1. Нужно ли отвечать?
    2. На каком уровне автономии?
    3. Что ответить (в стиле пользователя)?
    4. Логировать и уведомить пользователя.
    """

    name = "AutoReply"
    description = "Автоматические ответы за пользователя"

    system_prompt = """Ты отвечаешь от имени пользователя пока он не в сети.
    Используй только информацию из контекста разговора.
    Если не знаешь — скажи "отвечу позже".
    Никогда не обещай ничего конкретного."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.style_cloner = StyleClonerAgent()

    async def run(self, context: AgentContext) -> AgentResult:
        """Обработать входящее сообщение — нужно ли отвечать?"""
        return self.success("AutoReply готов", data={})

    async def should_reply(self, message: str, sender_trust: float,
                            chat_type: str) -> tuple[bool, int]:
        """
        Решить нужно ли отвечать и на каком уровне.

        Returns:
            (should_reply, autonomy_level)
        """
        # Никогда не отвечаем незнакомым в группах
        if chat_type == "group" and sender_trust < 5:
            return False, 0

        # Незнакомые — только если напрямую спрашивают
        if sender_trust < 3:
            return False, 0

        # Опасные темы — не отвечаем без пользователя
        danger_words = ["деньги", "переведи", "займи", "скинь",
                        "пароль", "перевод", "крипта", "инвест"]
        if any(w in message.lower() for w in danger_words):
            return False, 0

        # Конфликтные темы
        conflict_words = ["дурак", "идиот", "ненавижу", "убью"]
        if any(w in message.lower() for w in conflict_words):
            return False, 0

        # Определяем уровень
        if sender_trust >= 8:
            level = 2  # хорошо знакомые → умеренный
        elif sender_trust >= 5:
            level = 1  # знакомые → минимальный
        else:
            level = 0

        return level > 0, level

    async def generate_reply(self, message: str, sender_name: str,
                              trust: float, level: int,
                              context: AgentContext) -> Optional[str]:
        """Сгенерировать ответ в стиле пользователя."""

        if level == 1:
            # Минимальный — стандартные ответы
            templates = [
                "ок, позже отвечу",
                "да, напишу скоро",
                "занят, напишу чуть позже",
                "понял, скоро отвечу",
            ]
            base = random.choice(templates)
            return self.style_cloner._apply_style_basic(
                base, self.style_cloner.get_profile()
            )

        elif level >= 2:
            # Умеренный — используем LLM + память

            # Ищем релевантную память
            mem_results = self.db.search_messages(message[:50], limit=3)
            mem_context = "\n".join([
                dict(r).get("content", "")[:100]
                for r in mem_results
            ])

            prompt = (
                f"Сообщение от {sender_name}: \"{message}\"\n"
                f"Контекст из памяти: {mem_context}\n\n"
                f"Напиши короткий ответ (1-2 предложения). "
                f"Если не знаешь — скажи 'напишу позже'."
            )

            response = await self.ask_llm(
                prompt, context,
                task_type=TaskType.SIMPLE,
                max_tokens=100, temperature=0.7
            )

            if not response.success:
                return "ок, скоро напишу"

            # Применяем стиль пользователя
            styled = await self.style_cloner.apply_style(
                response.content, context
            )

            # Добавляем имитацию задержки в логи
            profile = self.style_cloner.get_profile()
            delay = int(profile.avg_delay_sec * random.uniform(0.5, 1.5))
            logger.info(f"AutoReply: задержка {delay}сек перед отправкой")

            return styled

        return None

    async def process_incoming(self, message: str, sender_name: str,
                                sender_id: str, chat_id: str,
                                platform: str, chat_type: str,
                                bot=None, alert_system=None) -> bool:
        """
        Полный цикл обработки входящего сообщения.
        Возвращает True если ответили.
        """
        # Получаем trust score
        contact = self.db.get_contact(platform, sender_id)
        trust = dict(contact)["trust_score"] if contact else 3.0

        # Решаем отвечать ли
        should, level = await self.should_reply(message, trust, chat_type)

        # Создаём алерт если нужно
        if alert_system:
            from tools.alerts import classify_alert, Alert
            alert_level = classify_alert(message, trust)

            if alert_level.value >= 3:  # HIGH и выше
                alert = Alert(
                    level=alert_level,
                    message=f"Сообщение от *{sender_name}*: _{message[:150]}_",
                    source=platform,
                    sender=sender_name,
                    chat_id=chat_id
                )
                await alert_system.add(alert)

        if not should:
            return False

        # Генерируем ответ
        ctx = AgentContext(
            original_query=message,
            user_id=0,
            chat_id=chat_id,
            platform=platform
        )

        reply = await self.generate_reply(
            message, sender_name, trust, level, ctx
        )

        if not reply:
            return False

        # Имитируем задержку (как настоящий человек)
        profile = self.style_cloner.get_profile()
        delay = int(profile.avg_delay_sec * random.uniform(0.3, 1.0))
        delay = min(delay, 300)  # максимум 5 минут

        import asyncio
        await asyncio.sleep(delay)

        # Отправляем через бот
        if bot:
            try:
                await bot.send_message(chat_id, reply)
                logger.info(f"AutoReply отправлен в {chat_id}: {reply[:50]}")
            except Exception as e:
                logger.error(f"AutoReply ошибка отправки: {e}")
                return False

        # Логируем
        self.db.save_auto_reply(
            platform=platform,
            chat_id=chat_id,
            sender_name=sender_name,
            original=message,
            reply=reply,
            confidence=0.7 + (trust / 50),
            level=level
        )

        return True
