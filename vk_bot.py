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

_FATAL_ERRORS = [
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
                self._vk_session = vk_api.VkApi(
                    token=self._token, api_version="5.199"
                )
                self._vk     = self._vk_session.get_api()
                longpoll     = VkBotLongPoll(self._vk_session, self._group_id)
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
                if any(fe in err.lower() for fe in _FATAL_ERRORS):
                    self._running = False
                    logger.error(f"❌ VK конфигурация (останавливаюсь): {err}")
                    if "longpoll" in err.lower():
                        logger.error(
                            "👉 Включи Long Poll: vk.com/club237147968 "
                            "→ Управление → Работа с API → Long Poll API "
                            "→ Включить → версия 5.199 "
                            "→ Типы событий → Входящее сообщение ✓"
                        )
                    break
                elif self._running:
                    logger.error(f"❌ VK упал: {err}. Перезапуск через 15 сек...")
                    import time; time.sleep(15)
                else:
                    break

    def _dispatch(self, event):
        if event.type != VkBotEventType.MESSAGE_NEW:
            return

        obj = event.object
        if isinstance(obj, dict):
            msg     = obj.get("message", obj)
            text    = (msg.get("text") or obj.get("text") or "").strip()
            from_id = msg.get("from_id") or obj.get("from_id") or 0
            peer_id = msg.get("peer_id") or obj.get("peer_id") or 0
            vk_id   = msg.get("id") or obj.get("id") or 0
        else:
            text    = str(getattr(obj, "text", "") or "").strip()
            from_id = getattr(obj, "from_id", 0) or 0
            peer_id = getattr(obj, "peer_id", 0) or 0
            vk_id   = getattr(obj, "id", 0) or 0

        if not text or not peer_id or from_id < 0:
            return

        is_chat = peer_id > 2_000_000_000

        # В беседе — только на триггер
        if is_chat:
            if "джарвис" not in text.lower() and "jarvis" not in text.lower():
                return

        # Получаем имя отправителя
        sender_name = f"vk_{from_id}"
        try:
            u = self._vk.users.get(user_ids=from_id)
            if u:
                sender_name = f"{u[0].get('first_name','')} {u[0].get('last_name','')}".strip()
        except Exception:
            pass

        # Сохраняем входящее в VK-таблицу
        try:
            from db import get_db
            _db = get_db()
            _db.vk_save_message(
                peer_id=peer_id, from_id=from_id,
                sender_name=sender_name, text=text,
                role="user", vk_msg_id=vk_id
            )
        except Exception:
            pass

        self._handle(text, from_id, peer_id, sender_name)

    def _handle(self, text, from_id, peer_id, sender_name=""):
        if "джарвис" not in text.lower() and "jarvis" not in text.lower():
            text = "Джарвис, " + text

        future = asyncio.run_coroutine_threadsafe(
            self._process(text, from_id, peer_id, sender_name), self._loop
        )
        try:
            answer = future.result(timeout=60)
        except Exception as e:
            logger.error(f"❌ VK future: {e}")
            answer = "Произошла ошибка, попробуй ещё раз."

        if answer:
            self._send(peer_id, answer, from_id)

    async def _process(self, text, from_id, peer_id, sender_name=""):
        try:
            result = await self._agent.process(
                text=text, sender_id=from_id,
                username=sender_name or f"vk_{from_id}",
                chat_id=peer_id,
            )
            return result or ""
        except Exception as e:
            logger.error(f"❌ VK agent: {e}")
            return "Ошибка обработки запроса."

    def _send(self, peer_id, text, from_id=0):
        # Убираем Telegram-markdown
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
            except Exception as e:
                logger.error(f"❌ VK send: {e}")
                return

        # Сохраняем ответ Джарвиса в VK-таблицу
        try:
            from db import get_db
            get_db().vk_save_message(
                peer_id=peer_id, from_id=0,
                sender_name="Jarvis", text=text,
                role="jarvis"
            )
        except Exception:
            pass


def create_vk_bot(agent):
    token    = os.getenv("VK_TOKEN", "").strip()
    group_id = int(os.getenv("VK_GROUP_ID", "0") or "0")
    owner_id = int(os.getenv("VK_OWNER_ID", "0") or "0")
    if not token:
        return None
    return VKBot(token=token, group_id=group_id, owner_vk_id=owner_id, agent=agent)
