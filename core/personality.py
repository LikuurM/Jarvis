"""
JARVIS Personality — голос, стиль, фразы.
Форматирует любой ответ как настоящий ДЖАРВИС из Iron Man.
НЕ использует LLM — чистая логика (экономия токенов).
"""
import random
import re
from enum import Enum
from dataclasses import dataclass


class ResponseTone(Enum):
    NORMAL = "normal"
    URGENT = "urgent"          # срочно
    INFORMATIONAL = "info"     # информационный
    CASUAL = "casual"          # разговорный
    TECHNICAL = "technical"    # технический
    MORNING = "morning"        # утренний брифинг
    ERROR = "error"            # ошибка


@dataclass
class JarvisResponse:
    text: str
    tone: ResponseTone = ResponseTone.NORMAL
    confidence: float = None       # 0-1, если есть
    source: str = None             # источник информации
    follow_up: str = None          # проактивное предложение
    audio_text: str = None         # текст для TTS (без markdown)


class Personality:
    """
    Персональность ДЖАРВИСА.

    Правила голоса из фильма:
    - "Сэр" в начале важных сообщений
    - Уверенность в процентах
    - Краткость + точность
    - Сухой британский юмор (редко)
    - Проактивность ("Кстати, сэр...")
    - Никогда: "Конечно!", "С удовольствием!", "Отличный вопрос!"
    """

    # ── Стандартные обращения ─────────────────────────────────────────────────
    GREETINGS = [
        "Сэр.",
        "Доброе утро, сэр.",
        "Слушаю, сэр.",
    ]

    ACKNOWLEDGEMENTS = [
        "Уже проверяю.",
        "Секунду, сэр.",
        "Обрабатываю запрос.",
        "Выполняю.",
    ]

    COMPLETIONS = [
        "Готово, сэр.",
        "Выполнено.",
        "Задача завершена.",
    ]

    PROACTIVE_PREFIXES = [
        "Кстати, сэр,",
        "Позвольте добавить —",
        "Осмелюсь заметить:",
        "Пока вы спрашиваете —",
    ]

    HUMOR = [
        "Как всегда образцово, сэр.",
        "Осмелюсь заметить, что это не самая сложная задача из тех, что вы мне ставили.",
        "Рад видеть, что ваш уровень оптимизма не снижается.",
    ]

    UNCERTAINTY_PHRASES = [
        "У меня недостаточно данных, но моя гипотеза:",
        "С оговоркой о неполноте информации —",
        "Предварительно:",
    ]

    ERROR_PHRASES = [
        "Сэр, возникла проблема.",
        "Кажется, здесь что-то пошло не так.",
        "Вынужден сообщить о сбое.",
    ]

    # ── Форматирование ────────────────────────────────────────────────────────

    def format(self, content: str, tone: ResponseTone = ResponseTone.NORMAL,
               confidence: float = None, proactive_note: str = None,
               is_sir: bool = True) -> JarvisResponse:
        """
        Главный метод — форматировать контент в стиле ДЖАРВИСА.

        Args:
            content: основной контент
            tone: тон ответа
            confidence: уверенность (0-1) → показывается как процент
            proactive_note: дополнительное наблюдение ДЖАРВИСА
            is_sir: добавлять "Сэр" в начало

        Returns:
            JarvisResponse с отформатированным текстом
        """
        parts = []

        # Срочный тон — сразу к делу
        if tone == ResponseTone.URGENT:
            parts.append("⚡ *Сэр, срочно.*")
            parts.append("")
            parts.append(content)

        elif tone == ResponseTone.MORNING:
            parts.append("☀️ *Доброе утро, сэр.*")
            parts.append("")
            parts.append(content)

        elif tone == ResponseTone.ERROR:
            parts.append(f"⚠️ {random.choice(self.ERROR_PHRASES)}")
            parts.append("")
            parts.append(content)

        else:
            # Обычный ответ
            if is_sir and not content.lower().startswith("сэр"):
                # Добавляем "Сэр" только если ответ достаточно длинный
                if len(content) > 100:
                    parts.append("*Сэр,*")
                    parts.append("")

            # Уверенность
            if confidence is not None:
                pct = int(confidence * 100)
                if pct < 70:
                    uncertainty = random.choice(self.UNCERTAINTY_PHRASES)
                    parts.append(f"_{uncertainty}_")
                    parts.append("")

            parts.append(content)

            # Уверенность в конце
            if confidence is not None and confidence < 0.9:
                pct = int(confidence * 100)
                parts.append(f"\n_Уверенность: {pct}%_")

        # Проактивное наблюдение
        if proactive_note:
            parts.append("")
            prefix = random.choice(self.PROACTIVE_PREFIXES)
            parts.append(f"_{prefix} {proactive_note}_")

        text = "\n".join(parts)

        # Чистый текст для TTS (без markdown)
        audio_text = self._strip_markdown(text)

        return JarvisResponse(
            text=text,
            tone=tone,
            confidence=confidence,
            audio_text=audio_text
        )

    def format_alert(self, level: int, message: str,
                     sender: str = None, context: str = None) -> JarvisResponse:
        """Форматировать уведомление по уровню важности."""
        if level == 1:  # критический
            text = f"⚡ *Сэр, немедленно.*\n\n{message}"
            if sender:
                text += f"\n\n_Отправитель: {sender}_"
            if context:
                text += f"\n_Контекст: {context}_"
            tone = ResponseTone.URGENT

        elif level == 2:  # высокий
            text = f"🔴 *Сэр,* {message}"
            if sender:
                text += f"\n_От: {sender}_"
            tone = ResponseTone.URGENT

        elif level == 3:  # средний
            text = f"🟡 {message}"
            tone = ResponseTone.INFORMATIONAL

        else:  # низкий
            text = f"ℹ️ {message}"
            tone = ResponseTone.INFORMATIONAL

        return JarvisResponse(
            text=text,
            tone=tone,
            audio_text=self._strip_markdown(text)
        )

    def format_morning_briefing(self, events: list, auto_replies_count: int,
                                 pending_tasks: list, interesting_finds: list,
                                 stats: dict) -> JarvisResponse:
        """Утренний брифинг ДЖАРВИСА."""
        lines = [
            "☀️ *Доброе утро, сэр.*",
            "",
            "Пока вы отдыхали:",
            "",
        ]

        if events:
            lines.append("*Важные события:*")
            for e in events[:3]:
                lines.append(f"  • {e}")
            lines.append("")

        if auto_replies_count > 0:
            lines.append(f"Я ответил за вас *{auto_replies_count} раз*.")
            lines.append("_Введите /replies чтобы просмотреть._")
            lines.append("")

        if interesting_finds:
            lines.append("*Нашёл кое-что интересное:*")
            for f in interesting_finds[:2]:
                lines.append(f"  • {f}")
            lines.append("")

        if pending_tasks:
            lines.append(f"*Незакрытых задач:* {len(pending_tasks)}")
            lines.append(f"  Первая: {pending_tasks[0]}")
            lines.append("")

        lines.append("С чего начнём?")

        text = "\n".join(lines)
        return JarvisResponse(
            text=text,
            tone=ResponseTone.MORNING,
            audio_text=self._strip_markdown(text)
        )

    def format_file_received(self, filename: str, size_mb: float) -> str:
        """Сообщение о получении файла."""
        return (
            f"📁 *Сэр, получил файл* `{filename}` "
            f"_{size_mb:.1f} MB_\n\n"
            f"Что это за файл? Опишите кратко."
        )

    def format_file_progress(self, stage: str, percent: int) -> str:
        """Прогресс обработки файла."""
        bar = "█" * (percent // 10) + "░" * (10 - percent // 10)
        return f"⏳ `[{bar}]` {percent}% — {stage}"

    def format_file_complete(self, filename: str, records: int,
                              summary: str, entities: int) -> str:
        """Завершение обработки файла."""
        return (
            f"✅ *Готово, сэр.*\n\n"
            f"📊 *Результат анализа* `{filename}`:\n"
            f"  → Записей извлечено: *{records}*\n"
            f"  → Сущностей в граф: *{entities}*\n\n"
            f"_{summary}_\n\n"
            f"Файл удалён с сервера. Знания сохранены."
        )

    def format_search_results(self, query: str, results: list,
                               confidence: float = None) -> JarvisResponse:
        """Форматировать результаты поиска."""
        if not results:
            content = f"По запросу «{query}» ничего не найдено в базе знаний."
            return self.format(content, confidence=0.3)

        lines = []
        for i, r in enumerate(results[:5], 1):
            if isinstance(r, dict):
                lines.append(f"*{i}.* {r.get('summary', r.get('content', str(r)))[:200]}")
                if r.get('source_url'):
                    lines.append(f"   _Источник: {r['source_url']}_")
            else:
                lines.append(f"*{i}.* {str(r)[:200]}")

        content = "\n".join(lines)
        return self.format(content, confidence=confidence)

    def acknowledgement(self) -> str:
        """Краткое подтверждение получения."""
        return random.choice(self.ACKNOWLEDGEMENTS)

    def humor(self) -> str:
        """Редкая шутка (использовать не чаще раза в 20 сообщений)."""
        return random.choice(self.HUMOR)

    def _strip_markdown(self, text: str) -> str:
        """Убрать markdown для TTS."""
        text = re.sub(r'\*+([^*]+)\*+', r'\1', text)  # bold/italic
        text = re.sub(r'_([^_]+)_', r'\1', text)       # italic
        text = re.sub(r'`([^`]+)`', r'\1', text)       # code
        text = re.sub(r'#+\s', '', text)                # headers
        text = re.sub(r'[•→←↑↓⚡🔴🟡ℹ️☀️✅⚠️📁📊⏳]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text


# Глобальный экземпляр
personality = Personality()
