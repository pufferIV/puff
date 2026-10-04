from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import asyncio
from collections import OrderedDict
import edge_tts

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache serveur : une fois une phrase entièrement générée, les prochaines
# lectures peuvent être servies immédiatement sans rappeler Edge TTS.
AUDIO_CACHE = OrderedDict()
CACHE_MAX_ITEMS = 128
CACHE_LOCK = asyncio.Lock()

# Une seule génération Edge TTS à la fois par phrase identique.
IN_FLIGHT = {}
IN_FLIGHT_LOCK = asyncio.Lock()


async def generate_audio_stream(text: str, voice: str, rate: str):
    """Génère le MP3 et transmet chaque chunk immédiatement."""
    communicate = edge_tts.Communicate(text, voice, rate=rate)
    async for chunk in communicate.stream():
        if chunk["type"] == "audio" and chunk["data"]:
            yield chunk["data"]


async def get_cached_audio(text: str, voice: str, rate: str):
    key = (text, voice, rate)
    async with CACHE_LOCK:
        cached = AUDIO_CACHE.get(key)
        if cached is not None:
            AUDIO_CACHE.move_to_end(key)
        return cached


async def store_cached_audio(text: str, voice: str, rate: str, audio: bytes):
    key = (text, voice, rate)
    async with CACHE_LOCK:
        AUDIO_CACHE[key] = audio
        AUDIO_CACHE.move_to_end(key)
        while len(AUDIO_CACHE) > CACHE_MAX_ITEMS:
            AUDIO_CACHE.popitem(last=False)


async def generate_and_cache(text: str, voice: str, rate: str):
    """Version complète utilisée pour remplir le cache serveur."""
    audio_data = bytearray()
    last_error = None

    for attempt in range(2):
        try:
            communicate = edge_tts.Communicate(text, voice, rate=rate)
            audio_data.clear()

            async for chunk in communicate.stream():
                if chunk["type"] == "audio" and chunk["data"]:
                    audio_data.extend(chunk["data"])

            if audio_data:
                audio = bytes(audio_data)
                await store_cached_audio(text, voice, rate, audio)
                return audio

            raise RuntimeError("Edge TTS returned no audio")
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                await asyncio.sleep(0.15)

    raise last_error or RuntimeError("Edge TTS failed")


async def stream_tts(text: str, voice: str, rate: str):
    """Sert le cache immédiatement, sinon transmet Edge TTS directement."""
    cached = await get_cached_audio(text, voice, rate)
    if cached is not None:
        chunk_size = 32 * 1024
        for pos in range(0, len(cached), chunk_size):
            yield cached[pos:pos + chunk_size]
        return

    # Aucun contrôle préalable, aucun retry et aucun délai :
    # chaque chunk reçu d'Edge TTS est envoyé immédiatement au client.
    audio_data = bytearray()
    communicate = edge_tts.Communicate(text, voice, rate=rate)

    async for chunk in communicate.stream():
        if chunk["type"] == "audio" and chunk["data"]:
            data = chunk["data"]
            audio_data.extend(data)
            yield data

    if audio_data:
        await store_cached_audio(text, voice, rate, bytes(audio_data))


@app.get("/")
async def root():
    return Response(
        content=b"ok",
        media_type="text/plain",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/health")
async def health():
    # Keep-alive uniquement : aucune génération Edge TTS.
    return Response(
        content=b"ok",
        media_type="text/plain",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/tts")
async def tts(
    text: str,
    voice: str = "ru-RU-SvetlanaNeural",
    rate: str = "+0%",
):
    text = text.strip()

    if not text:
        return Response(
            content=b"",
            media_type="audio/mpeg",
            headers={"Cache-Control": "no-store"},
        )

    # La réponse est volontairement chunked : le navigateur peut commencer
    # la lecture pendant qu'Edge TTS termine encore la génération.
    return StreamingResponse(
        stream_tts(text, voice, rate),
        media_type="audio/mpeg",
        headers={
            "Cache-Control": "public, max-age=86400",
            "Accept-Ranges": "bytes",
            "X-TTS-Streaming": "1",
        },
    )
