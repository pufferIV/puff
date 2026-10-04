import asyncio
import re
from collections import OrderedDict

import edge_tts
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Réglages de vitesse
# ---------------------------------------------------------------------------
MAX_TEXT_CHARS = 3000          # garde-fou
FIRST_CHUNK_CHARS = 90         # 1er morceau court => premiers octets plus vite
NEXT_CHUNK_CHARS = 220         # morceaux suivants, générés EN PARALLÈLE
MAX_PARALLEL_EDGE = 6          # connexions Edge TTS simultanées max

CACHE_MAX_ITEMS = 512          # cache par morceau (phrase)
CACHE_MAX_BYTES = 80 * 1024 * 1024

DEFAULT_VOICE = "ru-RU-SvetlanaNeural"
VOICE_RE = re.compile(r"^[a-z]{2,3}-[A-Z]{2}-[A-Za-z0-9]+Neural$")
RATE_RE = re.compile(r"^[+-]\d{1,3}%$")

# key = (texte, voix, rate) -> bytes
AUDIO_CACHE: "OrderedDict[tuple, bytes]" = OrderedDict()
CACHE_BYTES = 0

# Générations en cours : plusieurs requêtes identiques (préchargement + lecture)
# partagent UNE seule génération Edge TTS.
IN_FLIGHT: dict = {}
_BG_TASKS: set = set()
_SEM = None


def _sem() -> asyncio.Semaphore:
    global _SEM
    if _SEM is None:
        _SEM = asyncio.Semaphore(MAX_PARALLEL_EDGE)
    return _SEM


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
def cache_get(key):
    audio = AUDIO_CACHE.get(key)
    if audio is not None:
        AUDIO_CACHE.move_to_end(key)
    return audio


def cache_put(key, audio: bytes):
    global CACHE_BYTES
    old = AUDIO_CACHE.pop(key, None)
    if old is not None:
        CACHE_BYTES -= len(old)
    AUDIO_CACHE[key] = audio
    CACHE_BYTES += len(audio)
    while AUDIO_CACHE and (len(AUDIO_CACHE) > CACHE_MAX_ITEMS or CACHE_BYTES > CACHE_MAX_BYTES):
        _, dropped = AUDIO_CACHE.popitem(last=False)
        CACHE_BYTES -= len(dropped)


# ---------------------------------------------------------------------------
# Diffusion : un producteur Edge TTS, N consommateurs
# ---------------------------------------------------------------------------
class Job:
    def __init__(self):
        self.chunks = []
        self.done = False
        self.error = None
        self.cond = asyncio.Condition()

    async def push(self, data: bytes):
        async with self.cond:
            self.chunks.append(data)
            self.cond.notify_all()

    async def finish(self, error=None):
        async with self.cond:
            self.done = True
            self.error = error
            self.cond.notify_all()

    async def iterate(self):
        i = 0
        while True:
            async with self.cond:
                await self.cond.wait_for(lambda: i < len(self.chunks) or self.done)
                if i < len(self.chunks):
                    data = self.chunks[i]
                elif self.error is not None:
                    raise self.error
                else:
                    return
            i += 1
            yield data


async def _produce(key, job: Job):
    """Tâche indépendante de la connexion du client : si l'app coupe la lecture,
    la génération se termine quand même et remplit le cache."""
    text, voice, rate = key
    try:
        async with _sem():
            for attempt in range(2):
                try:
                    communicate = edge_tts.Communicate(text, voice, rate=rate)
                    async for chunk in communicate.stream():
                        if chunk["type"] == "audio" and chunk["data"]:
                            await job.push(chunk["data"])
                    if job.chunks:
                        break
                    raise RuntimeError("Edge TTS returned no audio")
                except Exception:
                    # Si des octets sont déjà partis vers le client, on ne relance pas.
                    if job.chunks or attempt == 1:
                        raise
                    await asyncio.sleep(0.1)
        cache_put(key, b"".join(job.chunks))
        await job.finish()
    except Exception as exc:  # noqa: BLE001
        await job.finish(exc)
    finally:
        IN_FLIGHT.pop(key, None)


def get_source(text: str, voice: str, rate: str):
    """bytes si en cache, sinon un Job (déjà démarré ou rejoint)."""
    key = (text, voice, rate)
    cached = cache_get(key)
    if cached is not None:
        return cached
    job = IN_FLIGHT.get(key)
    if job is None:
        job = Job()
        IN_FLIGHT[key] = job
        task = asyncio.create_task(_produce(key, job))
        _BG_TASKS.add(task)
        task.add_done_callback(_BG_TASKS.discard)
    return job


# ---------------------------------------------------------------------------
# Découpe du texte en phrases
# ---------------------------------------------------------------------------
_SENT_SPLIT = re.compile(r"(?<=[.!?…])\s+|\n+")
_SOFT_SPLIT = re.compile(r"(?<=[,;:—–])\s+")


def _split_long(piece: str, limit: int):
    if len(piece) <= limit:
        return [piece]
    out, cur = [], ""
    parts = _SOFT_SPLIT.split(piece)
    if len(parts) == 1:
        parts = piece.split(" ")
        sep = " "
    else:
        sep = " "
    for part in parts:
        candidate = (cur + sep + part).strip() if cur else part
        if len(candidate) > limit and cur:
            out.append(cur)
            cur = part
        else:
            cur = candidate
    if cur:
        out.append(cur)
    return out


def split_text(text: str):
    sentences = [s.strip() for s in _SENT_SPLIT.split(text) if s and s.strip()]
    chunks, cur = [], ""
    for sentence in sentences:
        limit = FIRST_CHUNK_CHARS if not chunks else NEXT_CHUNK_CHARS
        for piece in _split_long(sentence, limit):
            limit = FIRST_CHUNK_CHARS if not chunks else NEXT_CHUNK_CHARS
            candidate = (cur + " " + piece).strip() if cur else piece
            if cur and len(candidate) > limit:
                chunks.append(cur)
                cur = piece
            else:
                cur = candidate
    if cur:
        chunks.append(cur)
    return chunks or [text]


# ---------------------------------------------------------------------------
# Streaming MP3 : tous les morceaux démarrent en parallèle, envoyés dans l'ordre
# ---------------------------------------------------------------------------
async def stream_tts(text: str, voice: str, rate: str):
    chunks = split_text(text)
    # Démarre TOUTES les générations tout de suite (le sémaphore limite la charge).
    sources = [get_source(chunk, voice, rate) for chunk in chunks]

    for source in sources:
        if isinstance(source, (bytes, bytearray)):
            view = memoryview(source)
            step = 32 * 1024
            for pos in range(0, len(view), step):
                yield bytes(view[pos:pos + step])
            continue
        try:
            async for data in source.iterate():
                yield data
        except Exception:  # noqa: BLE001
            # Un morceau a échoué : on poursuit avec la suite plutôt que de couper net.
            continue


async def _warmup():
    """Chauffe DNS / TLS / imports au démarrage pour que la 1re vraie requête soit rapide."""
    try:
        communicate = edge_tts.Communicate("да", DEFAULT_VOICE, rate="+0%")
        async for _ in communicate.stream():
            pass
    except Exception:  # noqa: BLE001
        pass


@app.on_event("startup")
async def on_startup():
    _sem()
    task = asyncio.create_task(_warmup())
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return Response(content=b"ok", media_type="text/plain", headers={"Cache-Control": "no-store"})


@app.get("/health")
async def health():
    # Keep-alive uniquement : aucune génération Edge TTS.
    return Response(content=b"ok", media_type="text/plain", headers={"Cache-Control": "no-store"})


@app.get("/tts")
async def tts(text: str, voice: str = DEFAULT_VOICE, rate: str = "+0%"):
    text = text.strip()[:MAX_TEXT_CHARS]
    if not VOICE_RE.match(voice):
        voice = DEFAULT_VOICE
    if not RATE_RE.match(rate):
        rate = "+0%"

    if not text:
        return Response(content=b"", media_type="audio/mpeg", headers={"Cache-Control": "no-store"})

    return StreamingResponse(
        stream_tts(text, voice, rate),
        media_type="audio/mpeg",
        headers={
            "Cache-Control": "public, max-age=86400",
            "X-Accel-Buffering": "no",   # pas de buffering par un proxy
            "X-TTS-Streaming": "1",
        },
    )
