import os, asyncio, logging, random, threading, re

try:
    from loguru import logger
except ImportError:
    logger = logging.getLogger("jarvis.vk")

_VK_AVAILABLE = False
try:
    import vk_api
    from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
    _VK_AVAILABLE = True
except ImportError:
    pass

_FATAL_VK_ERRORS = [
    "longpoll for this group is not enabled",
    "access_token has expired",
    "invalid_client",
    "user authorization failed",
]


class VKBot:
    def __init__(self, token, group_id, owner_vk_id, agent):
        self._token      = token
        self._group_id   = group_id
        self._owner_id   = owner_vk_id
        self._agent      = agent
        self._loop       = None
        self._running    = False
        self._thread     = None
        self._vk         = None
        self._vk_session = None

    def start(self, loop):
        if not _VK_AVAILABLE:
            logger.warning("⚠️ vk_api не установлен — pip install vk_api")
            return
        if not self._token or not self._group_id:
            logger.info("ℹ️ VK не настроен")
            return
        self._loop    = loop
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True, name="jarvis-vk")
        self._thread.start()
        logger.info(f"✅ VK бот запущен (группа {self._group_id})")

    def stop(self):
        self._running = False

    def _run(self):
        while self._running:
            try:
                self._vk_session = vk_api.VkApi(token=self._token)
                self._vk         = self._vk_session.get_api()
                longpoll         = VkBotLongPoll(self._vk_session, self._group_id)
                logger.info("📡 VK Long Poll слушает...")

                for event in longpoll.listen():
                    if not self._running:
                        break
                    try:
                        self._dispatch(event)
                    except Exception as e:
                        logger.error(f"❌ VK dispatch: {e}")

            except Exception as e:
                err = str(e)
                if any(fe in err.lower() for fe in _FATAL_VK_ERRORS):
                    self._running = False
                    logger.error(f"❌ VK конфигурация (останавливаюсь): {err}")
                    if "longpoll" in err.lower():
                        logger.error(
                            "👉 Включи Long Poll: vk.com/club237147968 "
                            "→ Управление → Работа с API → Long Poll API → Включить → v5.131 "
                            "→ Типы событий → Входящие сообщения ✓"
                        )
                    break
                elif self._running:
                    logger.error(f"❌ VK упал: {err}. Перезапуск через 15 сек...")
                    import time; time.sleep(15)
                else:
                    break

    def _dispatch(self, event):
        # Логируем ВСЕ события для диагностики
        logger.info(f"📥 VK event type={event.type}, raw={str(event.object)[:200]}")

        if event.type != VkBotEventType.MESSAGE_NEW:
            return

        obj = event.object

        # vk_api возвращает разные структуры в зависимости от версии
        # Пробуем оба варианта
        if isinstance(obj, dict):
            msg     = obj.get("message", obj)
            text    = (msg.get("text") or obj.get("text") or "").strip()
            from_id = msg.get("from_id") or obj.get("from_id") or 0
            peer_id = msg.get("peer_id") or obj.get("peer_id") or 0
        else:
            # Объект с атрибутами
            text    = getattr(obj, "text", "") or ""
            from_id = getattr(obj, "from_id", 0) or 0
            peer_id = getattr(obj, "peer_id", 0) or 0

        text = str(text).strip()

        logger.info(f"📩 VK сообщение: from={from_id} peer={peer_id} text='{text[:80]}'")

        if not text or not peer_id:
            logger.info("⚠️ VK: пустой текст или peer_id — пропускаем")
            return

        if from_id < 0:
            logger.info(f"⚠️ VK: from_id={from_id} < 0 — это бот, пропускаем")
            return

        is_chat = peer_id > 2_000_000_000

        if is_chat:
            has_trigger = "джарвис" in text.lower() or "jarvis" in text.lower()
            if not has_trigger:
                logger.info(f"⚠️ VK: беседа без триггера — пропускаем")
                return

        logger.info(f"✅ VK обрабатываю: [{'беседа' if is_chat else 'лс'}:{from_id}]: {text[:60]}")
        self._handle(text, from_id, peer_id)

    def _handle(self, text, from_id, peer_id):
        if "джарвис" not in text.lower() and "jarvis" not in text.lower():
            text = "Джарвис, " + text

        future = asyncio.run_coroutine_threadsafe(
            self._process(text, from_id, peer_id), self._loop
        )
        try:
            answer = future.result(timeout=60)
        except Exception as e:
            logger.error(f"❌ VK future: {e}")
            answer = "Произошла ошибка, попробуй ещё раз."

        if answer:
            self._send(peer_id, answer)

    async def _process(self, text, from_id, peer_id):
        try:
            result = await self._agent.process(
                text=text, sender_id=from_id,
                username=f"vk_{from_id}", chat_id=peer_id,
            )
            return result or ""
        except Exception as e:
            logger.error(f"❌ VK agent: {e}")
            return "Ошибка обработки запроса."

    def _send(self, peer_id, text):
        clean = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
        clean = re.sub(r"__(.+?)__",     r"\1", clean)
        clean = re.sub(r"`(.+?)`",       r"\1", clean)
        clean = re.sub(r"\[(.+?)\]\(.+?\)", r"\1", clean)

        for chunk in [clean[i:i+4096] for i in range(0, max(len(clean), 1), 4096)]:
            if not chunk.strip():
                continue
            try:
                self._vk.messages.send(
                    peer_id=peer_id, message=chunk,
                    random_id=random.randint(1, 2**31),
                )
                logger.info(f"✅ VK отправлено peer={peer_id}: {chunk[:40]}...")
            except Exception as e:
                logger.error(f"❌ VK send peer={peer_id}: {e}")


def create_vk_bot(agent):
    token    = os.getenv("VK_TOKEN", "").strip()
    group_id = int(os.getenv("VK_GROUP_ID", "0") or "0")
    owner_id = int(os.getenv("VK_OWNER_ID", "0") or "0")
    if not token:
        return None
    return VKBot(token=token, group_id=group_id, owner_vk_id=owner_id, agent=agent)
