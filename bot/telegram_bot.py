"""
JARVIS Telegram Bot — основной интерфейс на aiogram 3.x.
Обрабатывает сообщения, команды, файлы.
"""
import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, Document, PhotoSize, Audio, Voice,
    Video, Animation
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_OWNER_ID, TEMP_DIR
from database.db import db
from agents.orchestrator import MasterOrchestrator
from agents.base_agent import AgentContext
from tools.file_handler import FileHandler
from core.personality import personality, ResponseTone

logger = logging.getLogger("jarvis.bot")


# ── FSM состояния ─────────────────────────────────────────────────────────────
class FileStates(StatesGroup):
    waiting_description = State()  # ждём описание файла


# ── Глобальные объекты ────────────────────────────────────────────────────────
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
orchestrator = MasterOrchestrator()

# Хранилище контекстов разговоров (chat_id → AgentContext)
_contexts: dict[str, AgentContext] = {}

# Хранилище файлов ожидающих описания (chat_id → file_path)
_pending_files: dict[str, dict] = {}


def get_context(chat_id: str, user_id: int = 0) -> AgentContext:
    """Получить или создать контекст для чата."""
    if chat_id not in _contexts:
        _contexts[chat_id] = AgentContext(
            original_query="",
            user_id=user_id,
            chat_id=chat_id,
            platform="telegram",
        )
    return _contexts[chat_id]


def is_owner(user_id: int) -> bool:
    """Проверить что это владелец."""
    return user_id == TELEGRAM_OWNER_ID


# ── Middleware: только от владельца ──────────────────────────────────────────
@dp.message.middleware()
async def owner_only(handler, event: Message, data):
    """Принимать сообщения только от владельца."""
    if not is_owner(event.from_user.id):
        await event.answer("🔒 Доступ запрещён.")
        return
    return await handler(event, data)


# ── /start ────────────────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message):
    ctx = get_context(str(message.chat.id), message.from_user.id)
    ctx.original_query = "/start"
    result = await orchestrator.process_command("start", ctx)
    await message.answer(result.content, parse_mode="Markdown")


# ── /help ─────────────────────────────────────────────────────────────────────
@dp.message(Command("help"))
async def cmd_help(message: Message):
    ctx = get_context(str(message.chat.id))
    result = await orchestrator.process_command("help", ctx)
    await message.answer(result.content, parse_mode="Markdown")


# ── /status ───────────────────────────────────────────────────────────────────
@dp.message(Command("status"))
async def cmd_status(message: Message):
    ctx = get_context(str(message.chat.id))
    result = await orchestrator.process_command("status", ctx)
    await message.answer(result.content, parse_mode="Markdown")


# ── /stats ────────────────────────────────────────────────────────────────────
@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    ctx = get_context(str(message.chat.id))
    result = await orchestrator.process_command("stats", ctx)
    await message.answer(result.content, parse_mode="Markdown")


# ── /replies ──────────────────────────────────────────────────────────────────
@dp.message(Command("replies"))
async def cmd_replies(message: Message):
    ctx = get_context(str(message.chat.id))
    result = await orchestrator.process_command("replies", ctx)
    await message.answer(result.content, parse_mode="Markdown")


# ── /memory ───────────────────────────────────────────────────────────────────
@dp.message(Command("memory"))
async def cmd_memory(message: Message):
    query = message.text.replace("/memory", "").strip()
    if not query:
        await message.answer("Сэр, укажите запрос: `/memory что искать`",
                              parse_mode="Markdown")
        return

    ctx = get_context(str(message.chat.id), message.from_user.id)
    ctx.original_query = query

    from agents.memory_agent import MemoryWeaverAgent
    mem_agent = MemoryWeaverAgent()
    result = await mem_agent.run(ctx)

    if result.success and result.data:
        lines = [f"*Результаты поиска по:* `{query}`\n"]
        for i, item in enumerate(result.data[:5], 1):
            content = item.get("content", "")[:200]
            source = item.get("source", "")
            lines.append(f"*{i}.* [{source}] {content}")
        await message.answer("\n".join(lines), parse_mode="Markdown")
    else:
        await message.answer("Ничего не найдено в памяти, сэр.",
                              parse_mode="Markdown")


# ── Текстовые сообщения ───────────────────────────────────────────────────────
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_message(message: Message, state: FSMContext):
    """Обработать обычное текстовое сообщение."""
    chat_id = str(message.chat.id)
    text = message.text.strip()

    # Проверяем: ожидаем ли описание файла?
    current_state = await state.get_state()
    if current_state == FileStates.waiting_description.state:
        await handle_file_description(message, state, text)
        return

    # Показываем что обрабатываем
    processing_msg = await message.answer(
        personality.acknowledgement(), parse_mode="Markdown"
    )

    # Получаем контекст
    ctx = get_context(chat_id, message.from_user.id)
    ctx.original_query = text

    # Обрабатываем через оркестратор
    result = await orchestrator.run(ctx)

    # Удаляем сообщение "обрабатываю"
    try:
        await processing_msg.delete()
    except Exception:
        pass

    if result.success:
        # Разбиваем длинные ответы (Telegram лимит 4096 символов)
        response_text = result.content
        if len(response_text) > 4000:
            chunks = [response_text[i:i+4000]
                      for i in range(0, len(response_text), 4000)]
            for chunk in chunks:
                await message.answer(chunk, parse_mode="Markdown")
        else:
            await message.answer(response_text, parse_mode="Markdown")
    else:
        await message.answer(
            f"⚠️ Сэр, возникла проблема: {result.error}",
            parse_mode="Markdown"
        )


# ── Файлы ─────────────────────────────────────────────────────────────────────
@dp.message(F.document)
async def handle_document(message: Message, state: FSMContext):
    """Обработать документ."""
    await _receive_file(
        message=message,
        state=state,
        file_id=message.document.file_id,
        filename=message.document.file_name or "document",
        size=message.document.file_size or 0
    )


@dp.message(F.photo)
async def handle_photo(message: Message, state: FSMContext):
    """Обработать фото."""
    photo = message.photo[-1]  # самое большое
    await _receive_file(
        message=message,
        state=state,
        file_id=photo.file_id,
        filename="photo.jpg",
        size=photo.file_size or 0
    )


@dp.message(F.audio | F.voice)
async def handle_audio(message: Message, state: FSMContext):
    """Обработать аудио или голосовое."""
    audio = message.audio or message.voice
    filename = getattr(audio, "file_name", None) or "audio.ogg"
    await _receive_file(
        message=message,
        state=state,
        file_id=audio.file_id,
        filename=filename,
        size=audio.file_size or 0
    )


@dp.message(F.video)
async def handle_video(message: Message, state: FSMContext):
    """Обработать видео."""
    video = message.video
    await _receive_file(
        message=message,
        state=state,
        file_id=video.file_id,
        filename=video.file_name or "video.mp4",
        size=video.file_size or 0
    )


async def _receive_file(message: Message, state: FSMContext,
                         file_id: str, filename: str, size: int):
    """Получить файл и спросить описание."""
    chat_id = str(message.chat.id)
    size_mb = size / (1024 * 1024)

    # Скачиваем файл
    await message.answer("📥 Скачиваю файл...", parse_mode="Markdown")

    try:
        file = await bot.get_file(file_id)
        local_path = TEMP_DIR / filename

        await bot.download_file(file.file_path, destination=str(local_path))

        # Сохраняем в _pending_files
        from tools.file_handler import FileHandler
        handler = FileHandler()
        file_type = handler.detect_file_type(filename)

        # Создаём запись в БД
        db_file_id = db.create_file_record(
            filename, Path(filename).suffix, file_type, size
        )

        _pending_files[chat_id] = {
            "local_path": local_path,
            "filename": filename,
            "size_mb": size_mb,
            "file_type": file_type,
            "db_file_id": db_file_id,
        }

        # Спрашиваем описание
        await message.answer(
            personality.format_file_received(filename, size_mb),
            parse_mode="Markdown"
        )

        await state.set_state(FileStates.waiting_description)

    except Exception as e:
        logger.error(f"Ошибка скачивания файла: {e}")
        await message.answer(
            f"⚠️ Сэр, не удалось скачать файл: {e}"
        )


async def handle_file_description(message: Message, state: FSMContext,
                                    description: str):
    """Получили описание файла — начинаем обработку."""
    chat_id = str(message.chat.id)

    if chat_id not in _pending_files:
        await state.clear()
        await message.answer("Сэр, файл не найден. Загрузите снова.")
        return

    file_info = _pending_files.pop(chat_id)
    await state.clear()

    local_path = file_info["local_path"]
    db_file_id = file_info["db_file_id"]
    filename = file_info["filename"]

    # Прогресс сообщения
    progress_msg = await message.answer(
        personality.format_file_progress("Начинаю анализ...", 0),
        parse_mode="Markdown"
    )

    # Callback для обновления прогресса
    async def update_progress(fid: int, stage: str, percent: int):
        try:
            await progress_msg.edit_text(
                personality.format_file_progress(stage, percent),
                parse_mode="Markdown"
            )
        except Exception:
            pass

    # Обрабатываем файл
    ctx = get_context(chat_id, message.from_user.id)
    handler = FileHandler(progress_callback=update_progress)

    try:
        result = await handler.process(
            file_path=local_path,
            user_description=description,
            file_id=db_file_id,
            from_llm_context=ctx
        )

        # Финальное сообщение
        await progress_msg.edit_text(
            personality.format_file_complete(
                filename=filename,
                records=result.get("records", 0),
                summary=result.get("summary", ""),
                entities=result.get("entities", 0)
            ),
            parse_mode="Markdown"
        )

    except Exception as e:
        await progress_msg.edit_text(
            f"⚠️ Сэр, ошибка при обработке файла:\n`{str(e)[:200]}`",
            parse_mode="Markdown"
        )


# ── Запуск ────────────────────────────────────────────────────────────────────
async def start_bot():
    """Запустить бота."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    logger.info("ДЖАРВИС запускается...")
    logger.info(f"Owner ID: {TELEGRAM_OWNER_ID}")

    # Инициализируем __init__.py
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(start_bot())
