"""
Minimal Edge TTS proxy for RusseGram.

Wraps Microsoft Edge's free neural TTS (via the `edge-tts` library, no API
key required) behind a tiny HTTP API, so the Android app can request
Russian speech (male or female) without bundling any model on-device.

Endpoints
---------
GET  /                -> health check
POST /tts             -> { "text": "...", "voice": "male"|"female", "speed": 1.0 }
                          returns audio/mpeg (MP3) bytes
GET  /tts?text=..&voice=..&speed=..
                          same thing, as a GET (handy for quick testing in a browser)
"""

import re

import edge_tts
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

app = FastAPI(title="RusseGram Edge TTS proxy")

# The app is called from an Android WebView, not a browser page with a real
# origin, so we simply allow everything rather than fight CORS.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Microsoft Edge neural voices for Russian.
VOICES = {
    "male": "ru-RU-DmitryNeural",
    "female": "ru-RU-SvetlanaNeural",
}

MAX_TEXT_LENGTH = 4000


class TtsRequest(BaseModel):
    text: str
    voice: str = "female"
    speed: float = 1.0


def resolve_voice(voice: str) -> str:
    return VOICES.get(voice.lower(), VOICES["female"])


def speed_to_rate(speed: float) -> str:
    """Convert a 0.7-1.4 style multiplier (as used by the app) into the
    edge-tts '+N%'/'-N%' rate string."""
    speed = max(0.5, min(2.0, speed))
    percent = round((speed - 1.0) * 100)
    return f"{percent:+d}%"


async def synthesize(text: str, voice: str, speed: float) -> bytes:
    text = text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Empty text")
    if len(text) > MAX_TEXT_LENGTH:
        raise HTTPException(status_code=400, detail="Text too long")

    # edge-tts chokes on some raw XML-sensitive characters; keep it simple.
    text = re.sub(r"\s+", " ", text).strip()

    communicate = edge_tts.Communicate(
        text=text,
        voice=resolve_voice(voice),
        rate=speed_to_rate(speed),
    )

    chunks = []
    async for chunk in communicate.stream():
        if chunk["type"] == "audio":
            chunks.append(chunk["data"])

    if not chunks:
        raise HTTPException(status_code=502, detail="TTS engine returned no audio")

    return b"".join(chunks)


@app.get("/")
async def health():
    return {"status": "ok"}


@app.post("/tts")
async def tts_post(payload: TtsRequest):
    audio = await synthesize(payload.text, payload.voice, payload.speed)
    return Response(content=audio, media_type="audio/mpeg")


@app.get("/tts")
async def tts_get(
    text: str = Query(...),
    voice: str = Query("female"),
    speed: float = Query(1.0),
):
    audio = await synthesize(text, voice, speed)
    return Response(content=audio, media_type="audio/mpeg")

