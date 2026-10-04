from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

# Cache mémoire du serveur.
# Une phrase déjà générée ne repasse donc pas par Edge TTS.
AUDIO_CACHE = OrderedDict()
CACHE_MAX_ITEMS = 128
CACHE_LOCK = asyncio.Lock()

# Évite deux générations Edge TTS identiques en parallèle.
IN_FLIGHT = {}
IN_FLIGHT_LOCK = asyncio.Lock()


async def generate_audio(text: str, voice: str, rate: str) -> bytes:
    last_error = None

    # Une petite seconde tentative protège contre les erreurs transitoires
    # "NoAudioReceived" de la connexion Edge TTS.
    for attempt in range(2):
        try:
            communicate = edge_tts.Communicate(text, voice, rate=rate)
            audio_data = bytearray()

            async for chunk in communicate.stream():
                if chunk["type"] == "audio" and chunk["data"]:
                    audio_data.extend(chunk["data"])

            if audio_data:
                return bytes(audio_data)

            raise RuntimeError("Edge TTS returned no audio")

        except Exception as exc:
            last_error = exc
            if attempt == 0:
                await asyncio.sleep(0.15)

    raise last_error or RuntimeError("Edge TTS failed")


async def get_audio(text: str, voice: str, rate: str) -> bytes:
    key = (text, voice, rate)

    async with CACHE_LOCK:
        cached = AUDIO_CACHE.get(key)
        if cached is not None:
            AUDIO_CACHE.move_to_end(key)
            return cached

    # Une seule génération pour une même phrase.
    async with IN_FLIGHT_LOCK:
        task = IN_FLIGHT.get(key)
        if task is None:
            task = asyncio.create_task(generate_audio(text, voice, rate))
            IN_FLIGHT[key] = task

    try:
        audio = await task

        async with CACHE_LOCK:
            AUDIO_CACHE[key] = audio
            AUDIO_CACHE.move_to_end(key)

            while len(AUDIO_CACHE) > CACHE_MAX_ITEMS:
                AUDIO_CACHE.popitem(last=False)

        return audio

    finally:
        async with IN_FLIGHT_LOCK:
            if IN_FLIGHT.get(key) is task:
                IN_FLIGHT.pop(key, None)


@app.get("/")
async def root():
    return Response(
        content=b"ok",
        media_type="text/plain",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/health")
async def health():
    # IMPORTANT : aucun appel à Edge TTS ici.
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

    try:
        audio = await get_audio(text, voice, rate)
    except Exception as exc:
        print(f"Edge TTS error: {type(exc).__name__}: {exc}")
        raise HTTPException(status_code=502, detail="Edge TTS generation failed")

    # MP3 complet : Android WebView reçoit toujours un fichier valide.
    return Response(
        content=audio,
        media_type="audio/mpeg",
        headers={
            # Autorise le cache HTTP du navigateur/WebView en plus du cache
            # localStorage de l'application.
            "Cache-Control": "public, max-age=86400",
            "Content-Length": str(len(audio)),
            "Accept-Ranges": "bytes",
        },
    )
