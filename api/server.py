"""
JARVIS FastAPI Server — REST API для iPhone интеграции.
Apple Shortcuts + PWA + WebSocket.
"""
import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, UploadFile, File, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import API_SECRET_TOKEN, TEMP_DIR, TELEGRAM_OWNER_ID
from database.db import db
from agents.orchestrator import MasterOrchestrator
from agents.base_agent import AgentContext
from core.personality import personality
from tools.tts import tts

logger = logging.getLogger("jarvis.api")

app = FastAPI(
    title="JARVIS API",
    description="J.A.R.V.I.S. — Just A Rather Very Intelligent System",
    version="1.0.0"
)

# CORS для PWA
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

orchestrator = MasterOrchestrator()

# Активные WebSocket соединения
_ws_connections: list[WebSocket] = []

# ── Модели запросов ───────────────────────────────────────────────────────────

class AskRequest(BaseModel):
    query: str
    context: dict = {}
    voice: bool = False          # нужен ли голосовой ответ
    platform: str = "ios"

class AskResponse(BaseModel):
    text: str
    audio_url: Optional[str] = None
    model: str = ""
    duration_ms: int = 0
    tokens: int = 0

class StatusResponse(BaseModel):
    status: str
    db_stats: dict
    providers: dict
    uptime: float

# ── Авторизация ───────────────────────────────────────────────────────────────

def verify_token(authorization: str = Header(None)) -> bool:
    """Проверить токен авторизации."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Нет токена авторизации")
    token = authorization.replace("Bearer ", "").strip()
    if token != API_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Неверный токен")
    return True

# ── Старт сервера ─────────────────────────────────────────────────────────────

_start_time = time.time()

@app.on_event("startup")
async def startup():
    logger.info("JARVIS API запущен")

# ── Эндпоинты ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    """Корневой эндпоинт — проверка жизни."""
    return {
        "status": "online",
        "name": "J.A.R.V.I.S.",
        "version": "1.0.0",
        "message": "Online and ready, sir."
    }

@app.get("/api/status", response_model=StatusResponse)
async def get_status(auth: bool = Depends(verify_token)):
    """Статус системы для iPhone виджета."""
    from core.model_router import router
    return StatusResponse(
        status="online",
        db_stats=db.get_stats(),
        providers=router.get_provider_stats(),
        uptime=time.time() - _start_time
    )

@app.post("/api/ask", response_model=AskResponse)
async def ask_jarvis(request: AskRequest,
                     auth: bool = Depends(verify_token)):
    """
    Главный эндпоинт — задать вопрос ДЖАРВИСУ.

    Использование в Apple Shortcuts:
    POST /api/ask
    Body: {"query": "что нового?", "voice": true}
    Response: {"text": "...", "audio_url": "..."}
    """
    start = time.time()

    if not request.query.strip():
        raise HTTPException(status_code=400, detail="Пустой запрос")

    # Создаём контекст
    ctx = AgentContext(
        original_query=request.query,
        user_id=TELEGRAM_OWNER_ID,
        chat_id=f"iphone_{request.platform}",
        platform=request.platform,
        metadata=request.context
    )

    # Получаем ответ от ДЖАРВИСА
    result = await orchestrator.run(ctx)
    duration = int((time.time() - start) * 1000)

    response_text = result.content if result.success else (
        result.error or "Сэр, возникла ошибка."
    )

    # Генерируем аудио если нужно
    audio_url = None
    if request.voice and result.success:
        audio_path = await tts.synthesize(response_text)
        if audio_path:
            # Имя файла → URL
            audio_url = f"/api/audio/{audio_path.name}"

    return AskResponse(
        text=response_text,
        audio_url=audio_url,
        model=result.model_used,
        duration_ms=duration,
        tokens=result.tokens_used
    )

@app.get("/api/audio/{filename}")
async def get_audio(filename: str, auth: bool = Depends(verify_token)):
    """Отдать аудио файл (голос ДЖАРВИСА)."""
    audio_path = tts.CACHE_DIR / filename
    if not audio_path.exists():
        raise HTTPException(status_code=404, detail="Аудио не найдено")
    return FileResponse(
        path=str(audio_path),
        media_type="audio/mpeg",
        filename=filename
    )

@app.get("/api/tts")
async def text_to_speech(text: str, auth: bool = Depends(verify_token)):
    """
    Синтез речи на лету.
    Использование в Shortcuts: GET /api/tts?text=Привет+сэр
    """
    if not text:
        raise HTTPException(status_code=400, detail="Нет текста")

    audio_path = await tts.synthesize(text[:500])
    if not audio_path:
        raise HTTPException(status_code=500, detail="Ошибка синтеза речи")

    return FileResponse(
        path=str(audio_path),
        media_type="audio/mpeg"
    )

@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...),
    description: str = "",
    auth: bool = Depends(verify_token)
):
    """Загрузить файл для анализа ДЖАРВИСОМ."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Нет файла")

    # Сохраняем временно
    save_path = TEMP_DIR / file.filename
    content = await file.read()
    with open(save_path, "wb") as f:
        f.write(content)

    # Создаём запись в БД
    from tools.file_handler import FileHandler
    handler = FileHandler()
    file_type = handler.detect_file_type(file.filename)
    file_id = db.create_file_record(
        file.filename,
        Path(file.filename).suffix,
        file_type,
        len(content)
    )

    # Запускаем обработку в фоне
    ctx = AgentContext(
        original_query=description or f"Файл {file.filename}",
        user_id=TELEGRAM_OWNER_ID,
        chat_id="iphone_upload"
    )

    asyncio.create_task(
        handler.process(save_path, description or file.filename, file_id, ctx)
    )

    return {
        "file_id": file_id,
        "filename": file.filename,
        "type": file_type,
        "status": "processing",
        "message": "Файл принят, анализирую..."
    }

@app.get("/api/file/{file_id}/status")
async def file_status(file_id: int, auth: bool = Depends(verify_token)):
    """Статус обработки файла."""
    row = db.execute_one(
        "SELECT * FROM uploaded_files WHERE id=?", (file_id,)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Файл не найден")

    r = dict(row)
    return {
        "file_id": file_id,
        "status": r["status"],
        "progress": r["progress_percent"],
        "records": r["records_extracted"],
        "summary": r["summary"],
        "error": r["error_message"]
    }

@app.get("/api/memory/search")
async def search_memory(q: str, limit: int = 5,
                          auth: bool = Depends(verify_token)):
    """Поиск в памяти ДЖАРВИСА."""
    if not q:
        raise HTTPException(status_code=400, detail="Пустой запрос")

    # FTS поиск
    msg_results = db.search_messages(q, limit=limit)
    know_results = db.search_knowledge(q, limit=limit)

    results = []
    for r in msg_results:
        rd = dict(r)
        results.append({
            "type": "message",
            "content": rd.get("content", "")[:300],
            "sender": rd.get("sender_name"),
            "timestamp": rd.get("timestamp"),
        })
    for r in know_results:
        rd = dict(r)
        results.append({
            "type": "knowledge",
            "content": rd.get("summary") or rd.get("content", "")[:300],
            "source": rd.get("source_url"),
            "confidence": rd.get("confidence_score"),
        })

    return {"query": q, "results": results[:limit]}

@app.get("/api/briefing")
async def get_morning_briefing(auth: bool = Depends(verify_token)):
    """Получить утренний брифинг."""
    import json
    row = db.execute_one(
        "SELECT value FROM preferences WHERE category='system' AND key='morning_briefing'"
    )
    if not row:
        return {"briefing": "Данных нет. Брифинг готовится в 06:00."}

    data = json.loads(dict(row)["value"])
    return data

@app.get("/api/stats")
async def get_stats(auth: bool = Depends(verify_token)):
    """Статистика базы знаний."""
    stats = db.get_stats()
    return {"stats": stats, "total": sum(stats.values())}

# ── WebSocket для PWA ─────────────────────────────────────────────────────────

@app.websocket("/api/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket для PWA приложения.
    Позволяет стримить ответы ДЖАРВИСА в реальном времени.
    """
    await websocket.accept()
    _ws_connections.append(websocket)

    try:
        while True:
            # Получаем сообщение от PWA
            data = await websocket.receive_json()

            # Проверяем токен
            if data.get("token") != API_SECRET_TOKEN:
                await websocket.send_json({"error": "Неверный токен"})
                continue

            query = data.get("query", "").strip()
            if not query:
                continue

            # Отправляем подтверждение
            await websocket.send_json({
                "type": "thinking",
                "message": "Обрабатываю..."
            })

            # Получаем ответ
            ctx = AgentContext(
                original_query=query,
                user_id=TELEGRAM_OWNER_ID,
                chat_id="pwa_ws",
                platform="pwa"
            )
            result = await orchestrator.run(ctx)

            # Отправляем ответ
            await websocket.send_json({
                "type": "response",
                "text": result.content if result.success else result.error,
                "model": result.model_used,
                "tokens": result.tokens_used
            })

    except WebSocketDisconnect:
        _ws_connections.remove(websocket)
    except Exception as e:
        logger.error(f"WebSocket ошибка: {e}")
        if websocket in _ws_connections:
            _ws_connections.remove(websocket)

async def broadcast_alert(message: str):
    """Отправить алерт всем подключённым PWA клиентам."""
    for ws in _ws_connections[:]:
        try:
            await ws.send_json({"type": "alert", "message": message})
        except Exception:
            _ws_connections.remove(ws)

# ── PWA статические файлы ─────────────────────────────────────────────────────

@app.get("/app")
async def pwa_app():
    """PWA интерфейс ДЖАРВИСА."""
    pwa_path = Path(__file__).parent / "pwa.html"
    if pwa_path.exists():
        return FileResponse(str(pwa_path), media_type="text/html")
    return JSONResponse({"message": "PWA файл не найден. Добавьте api/pwa.html"})

# ── Запуск ────────────────────────────────────────────────────────────────────

def run_server():
    """Запустить FastAPI сервер."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import API_HOST, API_PORT

    uvicorn.run(
        "api.server:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,
        log_level="info"
    )
