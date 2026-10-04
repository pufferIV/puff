from fastapi import FastAPI, Response
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

# Cache serveur : une phrase déjà générée ne repasse plus par Edge TTS.
# Cela rend les répétitions / exercices réutilisés quasiment instantanés.
AUDIO_CACHE = OrderedDict()
CACHE_MAX_ITEMS = 64
CACHE_LOCK = asyncio.Lock()

async def generate_audio(text: str, voice: str, rate: str):
    communicate = edge_tts.Communicate(text, voice, rate=rate)
    chunks = []
    async for chunk in communicate.stream():
        if chunk["type"] == "audio" and chunk["data"]:
            data = chunk["data"]
            chunks.append(data)
            # Le générateur complet est utilisé pour remplir le cache.
            # Le endpoint /tts, lui, peut envoyer les morceaux immédiatement.
    return b"".join(chunks)

@app.get("/")
async def root():
    return Response(content=b"ok", media_type="text/plain")

@app.get("/health")
async def health():
    # Endpoint sans Edge TTS : le keep-alive ne demande aucun audio.
    return Response(content=b"ok", media_type="text/plain", headers={"Cache-Control": "no-store"})

@app.get("/tts")
async def tts(text: str, voice: str = "ru-RU-SvetlanaNeural", rate: str = "+0%"):
    if not text.strip():
        return Response(content=b"", media_type="audio/mpeg", headers={"Cache-Control": "no-store"})

    key = (text, voice, rate)

    # Cache mémoire : renvoi immédiat si la phrase existe déjà.
    async with CACHE_LOCK:
        cached = AUDIO_CACHE.get(key)
        if cached is not None:
            AUDIO_CACHE.move_to_end(key)
            return Response(
                content=cached,
                media_type="audio/mpeg",
                headers={"Cache-Control": "public, max-age=86400"}
            )

    # IMPORTANT : StreamingResponse commence à envoyer les octets dès que
    # Edge TTS les fournit. Le client Audio peut donc commencer la lecture
    # sans attendre la fin complète de la génération.
    async def audio_stream():
        communicate = edge_tts.Communicate(text, voice, rate=rate)
        full_audio = bytearray()
        completed = False

        try:
            async for chunk in communicate.stream():
                if chunk["type"] == "audio" and chunk["data"]:
                    data = chunk["data"]
                    full_audio.extend(data)
                    yield data
            completed = True
        finally:
            if completed and full_audio:
                async with CACHE_LOCK:
                    AUDIO_CACHE[key] = bytes(full_audio)
                    AUDIO_CACHE.move_to_end(key)
                    while len(AUDIO_CACHE) > CACHE_MAX_ITEMS:
                        AUDIO_CACHE.popitem(last=False)

    return StreamingResponse(
        audio_stream(),
        media_type="audio/mpeg",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
            "Accept-Ranges": "none",
        },
    )
