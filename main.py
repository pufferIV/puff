import io
import os
import wave
import threading
from pathlib import Path

import torch
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, JSONResponse

# ============================================================
# RusseGram — Silero TTS v5.5 russe
# ============================================================

MODEL_URL = "https://models.silero.ai/models/tts/ru/v5_5_ru.pt"
MODEL_FILE = Path(os.getenv("SILERO_MODEL_FILE", "v5_5_ru.pt"))

# 24 kHz = bon compromis qualité / taille / rapidité.
DEFAULT_SAMPLE_RATE = 24000

# Sur un petit serveur CPU, éviter de saturer la machine.
TORCH_THREADS = max(1, int(os.getenv("TORCH_THREADS", "2")))
torch.set_num_threads(TORCH_THREADS)
try:
    torch.set_num_interop_threads(1)
except RuntimeError:
    pass

app = FastAPI(title="RusseGram Silero TTS", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

model = None
model_lock = threading.Lock()
generation_lock = threading.Lock()

ALLOWED_SPEAKERS = {
    "aidar",
    "baya",
    "kseniya",
    "xenia",
    "eugene",
}


def load_model():
    global model

    if model is not None:
        return model

    with model_lock:
        if model is not None:
            return model

        if not MODEL_FILE.exists():
            print("Téléchargement du modèle Silero v5.5 russe...")
            torch.hub.download_url_to_file(MODEL_URL, str(MODEL_FILE))

        print("Chargement du modèle Silero v5.5 russe...")
        package = torch.package.PackageImporter(str(MODEL_FILE))
        loaded = package.load_pickle("tts_models", "model")
        loaded.to(torch.device("cpu"))
        loaded.eval()

        model = loaded
        print("Silero prêt.")
        return model


def tensor_to_wav_bytes(audio_tensor, sample_rate: int) -> bytes:
    if hasattr(audio_tensor, "detach"):
        audio_tensor = audio_tensor.detach().cpu()

    audio_tensor = audio_tensor.float().flatten()

    # Évite les valeurs hors plage PCM.
    audio_tensor = torch.clamp(audio_tensor, -1.0, 1.0)
    pcm = (audio_tensor * 32767.0).to(torch.int16).numpy().tobytes()

    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        wav.writeframes(pcm)

    return buffer.getvalue()


@app.on_event("startup")
def startup():
    # Charge le modèle dès le démarrage afin que la première phrase
    # n'ait pas à payer le coût du chargement.
    load_model()


@app.get("/")
def health():
    return {
        "ok": True,
        "engine": "Silero TTS v5.5",
        "language": "ru",
        "sample_rate": DEFAULT_SAMPLE_RATE,
        "speakers": sorted(ALLOWED_SPEAKERS),
    }


@app.get("/health")
def health_check():
    return {"status": "ok", "engine": "silero-v5.5-ru"}


@app.post("/tts")
async def tts(payload: dict):
    try:
        text = str(payload.get("text", "")).strip()
        speaker = str(payload.get("speaker", "xenia")).lower()
        sample_rate = int(payload.get("sample_rate", DEFAULT_SAMPLE_RATE))

        if not text:
            return JSONResponse(
                {"error": "Le texte est vide."},
                status_code=400,
            )

        if speaker not in ALLOWED_SPEAKERS:
            return JSONResponse(
                {"error": f"Voix inconnue: {speaker}"},
                status_code=400,
            )

        if sample_rate not in (8000, 24000, 48000):
            sample_rate = DEFAULT_SAMPLE_RATE

        tts_model = load_model()

        # Silero utilise déjà l'accent russe automatiquement et gère
        # les homographes pour les modèles russes v5.
        # Une seule génération à la fois évite de saturer le CPU
        # sur les petits plans serveur.
        with generation_lock:
            with torch.inference_mode():
                audio = tts_model.apply_tts(
                    text=text,
                    speaker=speaker,
                    sample_rate=sample_rate,
                )

        wav_bytes = tensor_to_wav_bytes(audio, sample_rate)

        return Response(
            content=wav_bytes,
            media_type="audio/wav",
            headers={
                "Cache-Control": "public, max-age=31536000",
                "X-Content-Type-Options": "nosniff",
            },
        )

    except Exception as exc:
        print("Erreur Silero TTS:", repr(exc))
        return JSONResponse(
            {"error": str(exc)},
            status_code=500,
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )
