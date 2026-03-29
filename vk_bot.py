"""
Jarvis VK Bot — интеграция ВКонтакте.

Установка:
    pip install vk_api

Настройка .env:
    VK_TOKEN=vk1.a.xxxx       # токен группы ВК (Управление → Работа с API → Ключи доступа)
    VK_GROUP_ID=123456789      # ID группы ВК (без минуса)
    VK_OWNER_ID=111111111      # VK ID владельца (твой личный ID)

Запуск: автоматически из main.py если VK_TOKEN задан.
"""

import os
import asyncio
import logging
import random
import threading

logger = logging.getLogger("jarvis.vk")

_VK_AVAILABLE = False
try:
    import vk_api
    from vk_api.longpoll import VkLongPoll, VkEventType
    _VK_AVAILABLE = True
except ImportError:
    pass


class VKBot:
    """
    Обёртка над vk_api для Jarvis.
    Работает в отдельном потоке (не мешает asyncio Telegram).
    """

    def __init__(self, token: str, group_id: int, owner_vk_id: int, agent):
        self._token       = token
        self._group_id    = group_id
        self._owner_id    = owner_vk_id
        self._agent       = agent          # JarvisAgent — общий с TG
        self._loop        = None           # asyncio loop главного потока
        self._running     = False
        self._thread      = None
        self._vk          = None
        self._vk_session  = None

    # ── Запуск ──────────────────────────────────────────────────

    def start(self, loop: asyncio.AbstractEventLoop):
        """Запускает Long Poll в отдельном потоке."""
        if not _VK_AVAILABLE:
            logger.warning("⚠️ vk_api не установлен. Запустите: pip install vk_api")
            return
        if not self._token:
            logger.info("ℹ️ VK_TOKEN не задан — VK бот отключён")
            return
        self._loop    = loop
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True, name="jarvis-vk")
        self._thread.start()
        logger.info(f"✅ VK бот запущен (группа {self._group_id})")

    def stop(self):
        self._running = False

    # ── Long Poll цикл (отдельный поток) ────────────────────────

    def _run(self):
        try:
            self._vk_session = vk_api.VkApi(token=self._token)
            self._vk         = self._vk_session.get_api()
            longpoll         = VkLongPoll(self._vk_session, group_id=self._group_id)
            logger.info("🔌 VK Long Poll подключён")

            for event in longpoll.listen():
                if not self._running:
                    break
                if event.type == VkEventType.MESSAGE_NEW and event.to_me:
                    try:
                        self._handle(event)
                    except Exception as e:
                        logger.error(f"❌ VK handle: {e}")

        except Exception as e:
            logger.error(f"❌ VK Long Poll упал: {e}")

    # ── Обработка сообщения ─────────────────────────────────────

    def _handle(self, event):
        text      = (event.text or "").strip()
        user_id   = event.user_id
        peer_id   = event.peer_id

        if not text:
            self._send(peer_id, "Сэр, отправьте текстовое сообщение.")
            return

        # Активация: VK-бот реагирует на любое ЛС, и на «Джарвис» в беседах
        if event.from_chat:
            if "джарвис" not in text.lower() and "jarvis" not in text.lower():
                return

        logger.info(f"📩 VK [{user_id}]: {text[:60]}")

        # Запускаем process() через asyncio из главного потока
        future = asyncio.run_coroutine_threadsafe(
            self._process(text, user_id, peer_id),
            self._loop
        )
        try:
            answer = future.result(timeout=60)
        except Exception as e:
            answer = f"Сэр, ошибка обработки: {e}"

        if answer:
            self._send(peer_id, answer)

    async def _process(self, text: str, user_id: int, peer_id: int) -> str:
        """Вызывает JarvisAgent.process() — общий с Telegram."""
        try:
            # Если сообщение из ЛС — передаём как есть
            # Если из беседы — добавляем "Джарвис," если нет
            if "джарвис" not in text.lower() and "jarvis" not in text.lower():
                text = "Джарвис, " + text

            # VK user_id → используем как sender_id
            # Owner check: если VK owner ID совпадает
            answer = await self._agent.process(
                text       = text,
                sender_id  = user_id,
                username   = f"vk_{user_id}",
                chat_id    = peer_id,
            )
            return answer or ""
        except Exception as e:
            logger.error(f"❌ VK process: {e}")
            return "Сэр, произошла ошибка."

    # ── Отправка сообщения ──────────────────────────────────────

    def _send(self, peer_id: int, text: str):
        """Отправляет сообщение в VK. Telegram markdown → plain text."""
        # Убираем markdown разметку (VK не понимает **/__)
        import re
        clean = re.sub(r"\*\*(.+?)\*\*", r"\1", text)   # **bold**
        clean = re.sub(r"__(.+?)__",    r"\1", clean)   # __italic__
        clean = re.sub(r"`(.+?)`",      r"\1", clean)   # `code`

        # VK ограничение: 4096 символов
        chunks = [clean[i:i+4096] for i in range(0, len(clean), 4096)]
        for chunk in chunks:
            try:
                self._vk.messages.send(
                    peer_id    = peer_id,
                    message    = chunk,
                    random_id  = random.randint(1, 2**31),
                )
            except Exception as e:
                logger.error(f"❌ VK send: {e}")


# ── Фабрика — создаётся из main.py ──────────────────────────────

def create_vk_bot(agent) -> "VKBot | None":
    """
    Создаёт VKBot если VK_TOKEN задан в .env.
    Вызывается из main.py при старте.
    """
    token    = os.getenv("VK_TOKEN", "")
    group_id = int(os.getenv("VK_GROUP_ID", "0"))
    owner_id = int(os.getenv("VK_OWNER_ID", "0"))

    if not token:
        return None
    if not group_id:
        logger.warning("⚠️ VK_GROUP_ID не задан в .env")
        return None

    return VKBot(token=token, group_id=group_id, owner_vk_id=owner_id, agent=agent)
