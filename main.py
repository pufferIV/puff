from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import edge_tts

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def stream_tts(text: str, voice: str, rate: str):
    """Génère Edge TTS et envoie chaque paquet audio immédiatement."""
    communicate = edge_tts.Communicate(text, voice, rate=rate)

    async for chunk in communicate.stream():
        if chunk["type"] == "audio" and chunk["data"]:
            yield chunk["data"]


@app.get("/tts")
async def tts_get(
    text: str = Query(...),
    voice: str = Query("ru-RU-SvetlanaNeural"),
    rate: str = Query("+0%"),
):
    return StreamingResponse(
        stream_tts(text, voice, rate),
        media_type="audio/mpeg",
        headers={
            "Cache-Control": "no-cache, no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/tts")
async def tts_post(request: Request):
    data = await request.json()

    text = str(data.get("text", "")).strip()
    voice = str(data.get("voice", "ru-RU-SvetlanaNeural"))
    rate = str(data.get("rate", "+0%"))

    if not text:
        return {"error": "text is required"}

    return StreamingResponse(
        stream_tts(text, voice, rate),
        media_type="audio/mpeg",
        headers={
            "Cache-Control": "no-cache, no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Accel-Buffering": "no",
        },
    )
