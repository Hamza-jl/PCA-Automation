"""
orgchart_vision.py — Analyse un organigramme via un modèle vision local.

Deux backends disponibles :
  1. llama-server direct  (GPU complet, projector offloadé, rapide ~25s)
     Lance llama-server.exe d'Ollama directement, sans le scheduler conservateur.
  2. Ollama standard      (fallback, projector CPU, lent ~2-3 min)

Public API:
    list_vision_models() -> list[str]
    analyze_orgchart(image_bytes, model, timeout) -> list[dict]
        Each dict: {name, niveau1, niveau2, niveau3}
"""
from __future__ import annotations

import base64
import io
import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Image resize
# ---------------------------------------------------------------------------
try:
    from PIL import Image as _PILImage
    _HAS_PIL = True
except ImportError:
    _HAS_PIL = False

_MAX_SIDE = 1024  # px


def _resize_image(image_bytes: bytes, max_side: int = _MAX_SIDE) -> bytes:
    if not _HAS_PIL:
        return image_bytes
    img = _PILImage.open(io.BytesIO(image_bytes))
    w, h = img.size
    if max(w, h) <= max_side:
        return image_bytes
    scale = max_side / max(w, h)
    img = img.resize((int(w * scale), int(h * scale)), _PILImage.LANCZOS)
    buf = io.BytesIO()
    fmt = img.format if img.format in ("PNG", "JPEG", "WEBP") else "PNG"
    img.save(buf, format=fmt)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------
_SYSTEM_PROMPT = """Tu es un expert en analyse d'organigrammes.
Analyse l'image fournie et extrait TOUTES les entités organisationnelles visibles (directions, services, départements, comités, postes).
Détermine la hiérarchie de chaque entité (qui est son parent, son grand-parent).

Réponds UNIQUEMENT avec un objet JSON valide, sans texte avant ou après, dans ce format exact :
{
  "entities": [
    {"name": "Nom de l'entité", "niveau1": "parent direct ou vide", "niveau2": "grand-parent ou vide", "niveau3": "arrière-grand-parent ou vide"},
    ...
  ]
}

Règles :
- niveau1 = parent direct dans la hiérarchie (vide si racine)
- niveau2 = grand-parent (vide si inexistant)
- niveau3 = arrière-grand-parent (vide si inexistant)
- Inclure TOUTES les boîtes visibles dans l'organigramme
- Ne pas inventer d'entités non visibles dans l'image
- Répondre uniquement en JSON, aucune explication"""

# ---------------------------------------------------------------------------
# Ollama model list
# ---------------------------------------------------------------------------
OLLAMA_BASE = "http://localhost:11434"

_VISION_NAME_HINTS = (
    "vision", "vl", "vlm", "minicpm", "moondream", "llava",
    "granite3", "qwen2.5vl", "deepseek-vl", "internvl", "phi3-vision",
    "smolvlm", "pixtral", "cogvlm", "idefics",
)


def list_vision_models() -> list[str]:
    """
    Return installed vision models.
    Reads Ollama manifests directly — works even when Ollama is not running.
    Falls back to Ollama API if manifests directory not found.
    """
    vision = []
    manifests_root = _OLLAMA_MODELS_DIR / "manifests" / "registry.ollama.ai" / "library"
    if manifests_root.exists():
        for model_dir in manifests_root.iterdir():
            if not model_dir.is_dir():
                continue
            for tag_file in model_dir.iterdir():
                if not tag_file.is_file():
                    continue
                name_lower = model_dir.name.lower()
                if any(h in name_lower for h in _VISION_NAME_HINTS):
                    tag = tag_file.name
                    vision.append(f"{model_dir.name}:{tag}")
        if vision:
            return sorted(vision)
    # Fallback: try Ollama API
    try:
        r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=5)
        r.raise_for_status()
        for m in r.json().get("models", []):
            name = m.get("name", "").lower()
            families = m.get("details", {}).get("families") or []
            if "clip" in families or any(h in name for h in _VISION_NAME_HINTS):
                vision.append(m["name"])
    except Exception:
        pass
    return vision


# ---------------------------------------------------------------------------
# Direct llama-server backend (bypasses Ollama scheduler → full GPU)
# ---------------------------------------------------------------------------
_LS_PORT = 11438
_LS_PROC: subprocess.Popen | None = None
_LS_LOADED_MODEL: str = ""

_OLLAMA_MODELS_DIR = Path.home() / ".ollama" / "models"
_LLAMA_SERVER_EXE = (
    Path(os.environ.get("LOCALAPPDATA", ""))
    / "Programs" / "Ollama" / "lib" / "ollama" / "llama-server.exe"
)


def _resolve_blobs(model_name: str) -> tuple[Path | None, Path | None]:
    """
    Find model + mmproj blob paths for an Ollama model via its manifest.
    Returns (model_blob, mmproj_blob).
    For qwen25vl-style models the same file is used for both.
    For granite-style models the projector is a separate blob.
    """
    try:
        name, tag = (model_name.split(":", 1) + ["latest"])[:2]
        manifest_path = (
            _OLLAMA_MODELS_DIR / "manifests" / "registry.ollama.ai" / "library" / name / tag
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        model_blob = None
        mmproj_blob = None
        for layer in manifest.get("layers", []):
            mt = layer.get("mediaType", "")
            digest = layer["digest"].replace(":", "-")
            blob = _OLLAMA_MODELS_DIR / "blobs" / digest
            if mt == "application/vnd.ollama.image.model":
                model_blob = blob
            elif mt == "application/vnd.ollama.image.projector":
                mmproj_blob = blob
        # qwen25vl packs everything in one file — use model blob for both
        if model_blob and mmproj_blob is None:
            mmproj_blob = model_blob
        return model_blob, mmproj_blob
    except Exception:
        pass
    return None, None


def _ls_healthy() -> bool:
    try:
        r = httpx.get(f"http://127.0.0.1:{_LS_PORT}/health", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def _start_llama_server(model_name: str) -> bool:
    """
    Launch llama-server.exe directly for GPU-accelerated vision inference.
    Returns True if server started successfully, False otherwise.
    Logs progress to stdout so it appears in the FastAPI console.
    """
    global _LS_PROC, _LS_LOADED_MODEL

    # Already running for this model?
    if _LS_LOADED_MODEL == model_name and _ls_healthy():
        print(f"[llama-server] already running for {model_name}")
        return True

    # Stop old process if switching models
    if _LS_PROC and _LS_PROC.poll() is None:
        print("[llama-server] stopping previous instance…")
        _LS_PROC.terminate()
        try:
            _LS_PROC.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _LS_PROC.kill()
        _LS_PROC = None
        _LS_LOADED_MODEL = ""

    if not _LLAMA_SERVER_EXE.exists():
        print(f"[llama-server] NOT FOUND at {_LLAMA_SERVER_EXE}")
        return False

    model_blob, mmproj_blob = _resolve_blobs(model_name)
    if model_blob is None or not model_blob.exists():
        print(f"[llama-server] blob not found for {model_name}")
        return False

    print(f"[llama-server] starting with full GPU offload for {model_name}…")
    print(f"[llama-server] model blob : {model_blob}")
    print(f"[llama-server] mmproj blob: {mmproj_blob}")

    cmd = [
        str(_LLAMA_SERVER_EXE),
        "--model",   str(model_blob),
        "--mmproj",         str(mmproj_blob),
        "--port",           str(_LS_PORT),
        "--host",           "127.0.0.1",
        "--no-webui",
        "--offline",
        "-c",               "2048",
        "-np",              "1",
        "--log-verbosity",  "1",
        "--no-log-prefix",
        "--no-log-timestamps",
        "--no-jinja",
        "--chat-template",  "chatml",
        "--image-min-tokens", "1024",
        "--no-mmap",
        "--flash-attn",     "auto",
        "-b",               "512",
        "-ub",              "512",
        "-ngl",             "99",
    ]

    ollama_lib = _LLAMA_SERVER_EXE.parent
    cuda_lib   = ollama_lib / "cuda_v12"

    env = os.environ.copy()
    env["OLLAMA_LIBRARY_PATH"] = f"{ollama_lib};{cuda_lib}"
    env["CUDA_VISIBLE_DEVICES"] = "0"
    env["PATH"] = f"{ollama_lib};{cuda_lib};" + env.get("PATH", "")

    _LS_PROC = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        cwd=str(ollama_lib),
    )
    _LS_LOADED_MODEL = model_name

    # Wait up to 120s for server to become healthy, streaming logs
    deadline = time.time() + 120
    while time.time() < deadline:
        if _LS_PROC.poll() is not None:
            print("[llama-server] process exited unexpectedly")
            return False
        # drain a line of output
        try:
            line = _LS_PROC.stdout.readline()
            if line:
                print(f"[llama-server] {line.rstrip()}")
        except Exception:
            pass
        if _ls_healthy():
            print("[llama-server] ready ✓")
            return True
        time.sleep(0.5)

    print("[llama-server] timed out waiting for startup")
    return False


def _analyze_via_llama_server(image_b64: str, timeout: float) -> str:
    """Call the local llama-server /v1/chat/completions endpoint."""
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                {"type": "text", "text": "Analyse cet organigramme et retourne le JSON des entités organisationnelles."},
            ],
        },
    ]
    payload = {
        "messages": messages,
        "temperature": 0.1,
        "n_predict": 2048,
        "stream": False,
    }
    r = httpx.post(
        f"http://127.0.0.1:{_LS_PORT}/v1/chat/completions",
        json=payload,
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Ollama fallback backend
# ---------------------------------------------------------------------------
def _analyze_via_ollama(image_b64: str, model: str, timeout: float,
                        prompt_override: str | None = None) -> str:
    # Unload other models to free VRAM
    try:
        ps = httpx.get(f"{OLLAMA_BASE}/api/ps", timeout=5).json()
        for m in ps.get("models", []):
            if m.get("name") != model:
                httpx.post(
                    f"{OLLAMA_BASE}/api/generate",
                    json={"model": m["name"], "keep_alive": "0"},
                    timeout=30,
                )
    except Exception:
        pass

    payload = {
        "model": model,
        "system": _SYSTEM_PROMPT,
        "prompt": prompt_override or "Analyse cet organigramme et retourne le JSON des entités organisationnelles.",
        "images": [image_b64],
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 4096, "num_ctx": 4096},
    }
    r = httpx.post(f"{OLLAMA_BASE}/api/generate", json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json().get("response", "")


# ---------------------------------------------------------------------------
# JSON parsing
# ---------------------------------------------------------------------------
def _parse_entities_fallback(raw_text: str) -> list[dict]:
    candidates = re.findall(r'"([^"]{3,80})"', raw_text)
    seen: set[str] = set()
    entities = []
    skip = {"name", "niveau1", "niveau2", "niveau3", "entities"}
    json_noise = re.compile(r'^[\s\[\]{},:"]+$')
    for name in candidates:
        name = name.strip()
        if name.lower() in skip:
            continue
        if json_noise.match(name):
            continue
        if name not in seen:
            seen.add(name)
            entities.append({"name": name, "niveau1": "", "niveau2": "", "niveau3": ""})
    return entities


def _extract_entities(raw: str) -> list[dict]:
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```\s*$", "", cleaned.strip(), flags=re.MULTILINE)
    try:
        data = json.loads(cleaned)
        entities = data.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError
        result = [
            {
                "name":    str(e.get("name", "")).strip(),
                "niveau1": str(e.get("niveau1", "") or "").strip(),
                "niveau2": str(e.get("niveau2", "") or "").strip(),
                "niveau3": str(e.get("niveau3", "") or "").strip(),
            }
            for e in entities
            if isinstance(e, dict) and e.get("name")
        ]
        if result:
            return result
    except (json.JSONDecodeError, ValueError):
        pass
    return _parse_entities_fallback(raw)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def analyze_orgchart(
    image_bytes: bytes,
    model: str,
    timeout: float = 600.0,
) -> list[dict]:
    """
    Analyse an org chart image with a local vision model.
    Tries llama-server direct (full GPU) first, falls back to Ollama.
    """
    if not model:
        raise RuntimeError("Aucun modèle vision sélectionné.")

    image_bytes = _resize_image(image_bytes)
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")

    # --- Backend 1: direct llama-server (full GPU, fast) ---
    ls_error = ""
    if _LLAMA_SERVER_EXE.exists():
        print(f"[orgchart] trying direct llama-server backend (port {_LS_PORT})…")
        started = _start_llama_server(model)
        if started:
            try:
                print("[orgchart] sending image to llama-server…")
                raw = _analyze_via_llama_server(image_b64, timeout)
                print("[orgchart] llama-server responded ✓")
                result = _extract_entities(raw)
                if result:
                    return result
            except Exception as e:
                ls_error = str(e)
                print(f"[orgchart] llama-server inference failed: {e} — falling back to Ollama")
        else:
            ls_error = "llama-server startup failed"
            print("[orgchart] llama-server startup failed — falling back to Ollama")
    else:
        ls_error = f"llama-server.exe not found at {_LLAMA_SERVER_EXE}"
        print(f"[orgchart] {ls_error} — using Ollama")

    # --- Backend 2: Ollama (projector on CPU, slower) ---
    print("[orgchart] using Ollama backend…")
    try:
        raw = _analyze_via_ollama(image_b64, model, timeout)
    except httpx.ConnectError:
        raise RuntimeError(
            f"Backend direct (llama-server): {ls_error}\n\n"
            "Backend Ollama: non disponible. Lancez `ollama serve` ou rouvrez Ollama."
        )
    except httpx.TimeoutException:
        raise RuntimeError(
            f"Le modèle {model} a mis trop de temps à répondre (>{int(timeout)}s). "
            "Essayez avec granite3.2-vision:2b ou une image plus petite."
        )
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Erreur Ollama {e.response.status_code}: {e.response.text[:300]}")

    result = _extract_entities(raw)
    if result:
        return result

    raise RuntimeError(
        f"Le modèle n'a pas retourné de JSON valide.\n\nRéponse brute :\n{raw[:600]}"
    )
