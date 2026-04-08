"""
JARVIS TTS — голос ДЖАРВИСА через edge-tts.
Британский акцент, кэширование, отдача через FastAPI.
"""
import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("jarvis.tts")


class JarvisTTS:
    """
    Синтез речи в стиле ДЖАРВИСА.
    Использует Microsoft edge-tts (бесплатно, без лимитов).

    Голоса:
    - Английский: en-GB-RyanNeural (британский акцент, как в фильме)
    - Русский: ru-RU-DmitryNeural
    """

    CACHE_DIR = Path("data/tts_cache")

    def __init__(self):
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from config import TTS_VOICE_EN, TTS_VOICE_RU
        self.voice_en = TTS_VOICE_EN
        self.voice_ru = TTS_VOICE_RU
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get_voice(self, text: str) -> str:
        """Определить язык и выбрать голос."""
        # Простая эвристика: если > 50% кириллица → русский
        cyrillic = sum(1 for c in text if "\u0400" <= c <= "\u04FF")
        if cyrillic > len(text) * 0.3:
            return self.voice_ru
        return self.voice_en

    def _get_cache_path(self, text: str, voice: str) -> Path:
        """Путь к кэшированному аудио файлу."""
        key = hashlib.md5(f"{voice}:{text}".encode()).hexdigest()
        return self.CACHE_DIR / f"{key}.mp3"

    async def synthesize(self, text: str, voice: str = None,
                          force: bool = False) -> Optional[Path]:
        """
        Синтезировать речь.

        Args:
            text: текст для синтеза
            voice: голос (если None — определяется автоматически)
            force: пересоздать даже если есть кэш

        Returns:
            Path к MP3 файлу или None при ошибке
        """
        if not text or len(text.strip()) < 2:
            return None

        voice = voice or self._get_voice(text)

        # Проверяем кэш
        cache_path = self._get_cache_path(text, voice)
        if cache_path.exists() and not force:
            return cache_path

        try:
            import edge_tts

            # Очищаем текст от markdown
            clean_text = self._clean_for_tts(text)

            if len(clean_text) < 2:
                return None

            communicate = edge_tts.Communicate(clean_text, voice)
            await communicate.save(str(cache_path))

            logger.debug(f"TTS: синтезировано {len(clean_text)} символов → {cache_path.name}")
            return cache_path

        except ImportError:
            logger.warning("edge-tts не установлен. Установите: pip install edge-tts")
            return None
        except Exception as e:
            logger.error(f"TTS ошибка: {e}")
            return None

    async def synthesize_and_get_bytes(self, text: str,
                                        voice: str = None) -> Optional[bytes]:
        """Синтезировать и вернуть байты MP3."""
        path = await self.synthesize(text, voice)
        if path and path.exists():
            with open(path, "rb") as f:
                return f.read()
        return None

    def _clean_for_tts(self, text: str) -> str:
        """Очистить текст от markdown и специальных символов."""
        import re

        # Убираем markdown
        text = re.sub(r"\*+([^*]+)\*+", r"\1", text)
        text = re.sub(r"_([^_]+)_", r"\1", text)
        text = re.sub(r"`([^`]+)`", r"\1", text)
        text = re.sub(r"#+\s", "", text)
        text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)

        # Убираем эмодзи и спецсимволы
        text = re.sub(
            r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
            r"\U0001F680-\U0001F9FF\u2600-\u26FF⚡🔴🟡ℹ️]",
            "", text
        )

        # Убираем лишние пробелы
        text = re.sub(r"\s+", " ", text).strip()

        # Ограничиваем длину (TTS не любит очень длинные тексты)
        if len(text) > 2000:
            text = text[:2000] + "..."

        return text

    def cleanup_cache(self, max_files: int = 100):
        """Очистить старые файлы кэша."""
        cache_files = sorted(
            self.CACHE_DIR.glob("*.mp3"),
            key=lambda f: f.stat().st_mtime
        )

        if len(cache_files) > max_files:
            for old_file in cache_files[:-max_files]:
                old_file.unlink()
                logger.debug(f"TTS кэш: удалён {old_file.name}")

    async def get_available_voices(self) -> list[dict]:
        """Получить список доступных голосов."""
        try:
            import edge_tts
            voices = await edge_tts.list_voices()
            # Фильтруем: русские и британские
            filtered = [
                v for v in voices
                if v["Locale"] in ["ru-RU", "en-GB", "en-US"]
            ]
            return filtered
        except Exception:
            return []


# Глобальный экземпляр
tts = JarvisTTS()
