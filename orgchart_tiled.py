"""
orgchart_tiled.py  —  Tiled vision extraction + band-by-band hierarchy linking
                       ("Analyse avancée")

Public API
──────────
analyze_orgchart_tiled(image_bytes, model)  ->  dict  {"entities":[…], "levels":[…]}

Entity schema — same as orgchart_ocr.py, so it plugs into the existing
"Générer les fiches BIA" step and entity-review UI unchanged:
{
  "role":    str   # function / position label
  "person":  str   # name(s) of the holder(s), "" if none
  "name":    str   # role + " " + person
  "niveau1": str   # direct parent entity's name
  "niveau2": str   # grandparent
  "niveau3": str   # great-grandparent
}

Approach
────────
1. Split the image into overlapping horizontal bands. A dense chart with many
   small boxes exceeds a vision model's effective legibility at full-image
   resolution — smaller, denser per-band crops read far more accurately.
2. Extract each band independently (role/person/color per box), then merge
   and de-duplicate boxes that fall in the overlap between two bands.
3. Link the hierarchy band-by-band: each band's entities are only compared
   against the nearest non-empty band above it, one linking call per pair.
   This keeps the model's disambiguation task small — comparing a whole
   chart's entities in a single call causes it to default most children onto
   the same "safe" parent, since it can't reliably tell many candidates
   apart at once.
"""
from __future__ import annotations

import base64
import io
import json
import re
from typing import Any

import httpx
from PIL import Image

OLLAMA_BASE = "http://localhost:11434"

NUM_BANDS = 4
BAND_OVERLAP_FRAC = 0.15
TILE_TIMEOUT = 300.0
LINK_TIMEOUT = 200.0

MARGIN_LABEL_NOTE = """Only extract text that sits inside a bordered box connected to other
boxes by lines — those are the org chart entities. Ignore any other text on the page (a page
title, category labels running along a margin, legends, etc.) — do not create nodes for those."""

TILE_EXTRACTION_PROMPT = f"""You are looking at a CROPPED HORIZONTAL SLICE of a larger org chart
(organigramme) image. This slice may cut boxes off at the very top or bottom edge — only
extract boxes that are fully or mostly visible; skip anything cut off with less than half
of it showing.

{MARGIN_LABEL_NOTE}

Extract every actual box visible in this slice as a flat JSON array. For each box output an object with:
- "id": a short unique string id you invent (e.g. "n1", "n2", ...)
- "text": the exact text inside the box, verbatim. If a box has multiple lines (e.g. a role
  title and a person's name), join them with " | " into ONE node — never split one box into
  multiple nodes, even when one line is in noticeably larger or bolder font than the other
  (a large title directly above a smaller name, both inside the same border, is still ONE box).
- "color": one of "green", "red", "gray", "white" describing the box's fill color (use "white" if no fill/plain).

Rules:
- Include every box exactly once.
- Do not invent boxes that are not in the image. Do not create nodes for left-margin labels or titles.
- Read names and text carefully, character by character — do not guess or approximate spelling.
- Output ONLY a valid JSON array, no markdown fences, no commentary, no explanation.
- Output COMPACT JSON: no indentation, no line breaks, no extra spaces.
"""

BAND_LINKING_PROMPT_TEMPLATE = """You are linking two adjacent levels of an org chart hierarchy.

PARENT CANDIDATES (the level directly above — every child below must pick one of these):
{parents_json}

CHILDREN TO LINK (the level directly below — each one needs exactly one parent from the list above):
{children_json}

For each child, decide which parent candidate it most plausibly reports to / is grouped
under, based on typical org chart conventions and the entities' names/roles.

There are exactly {child_count} children listed above. Your output array MUST contain
exactly {child_count} objects, one per child — do not skip any, even if unsure (guess your
best answer rather than omitting it).

Output a JSON array with one object per child, in EXACTLY this shape (id and parent_id
must be "id" values from the lists above, not their text):
[{{"id": "b1_n0", "parent_id": "b0_n0"}}]

Output ONLY this JSON array, compact, no markdown fences, no commentary.
"""


def tiled_available() -> bool:
    """Pillow + httpx are core project deps — this mode has no extra install."""
    return True


# ── image tiling ─────────────────────────────────────────────────────────────
def _split_into_bands(image_bytes: bytes, num_bands: int = NUM_BANDS,
                       overlap_frac: float = BAND_OVERLAP_FRAC) -> list[bytes]:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    width, height = img.size
    band_height = height / num_bands
    overlap = band_height * overlap_frac

    tiles: list[bytes] = []
    for i in range(num_bands):
        top = max(0, i * band_height - overlap)
        bottom = min(height, (i + 1) * band_height + overlap)
        crop = img.crop((0, int(top), width, int(bottom)))
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        tiles.append(buf.getvalue())
    return tiles


# ── JSON parsing helpers ────────────────────────────────────────────────────
def _extract_json_value(raw: str) -> Any:
    raw = raw.strip()
    raw = re.sub(r"^```(json)?", "", raw).strip()
    raw = re.sub(r"```$", "", raw).strip()

    array_start, array_end = raw.find("["), raw.rfind("]")
    obj_start, obj_end = raw.find("{"), raw.rfind("}")
    has_array = array_start != -1 and array_end != -1 and array_end > array_start
    has_obj = obj_start != -1 and obj_end != -1 and obj_end > obj_start

    if has_array and (not has_obj or array_start < obj_start):
        candidate = raw[array_start:array_end + 1]
    elif has_obj:
        candidate = raw[obj_start:obj_end + 1]
    else:
        raise ValueError(f"Aucun JSON trouvé dans la réponse du modèle : {raw[:500]}")

    return _loads_with_repair(candidate)


def _loads_with_repair(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Common LLM slip: missing comma between array elements or object
        # properties (e.g. "}\n{" with no comma in between).
        repaired = re.sub(r'([}\]"\d])(\s+)([\{\["])', r'\1,\2\3', text)
        return json.loads(repaired)


def _extract_node_array(raw: str) -> list[dict]:
    parsed = _extract_json_value(raw)
    if isinstance(parsed, dict):
        for value in parsed.values():
            if isinstance(value, list):
                return value
        nodes = []
        for key, value in parsed.items():
            if isinstance(value, dict):
                value.setdefault("id", key)
                nodes.append(value)
        return nodes
    return parsed if isinstance(parsed, list) else []


# ── Ollama call ──────────────────────────────────────────────────────────────
def _call_ollama(prompt: str, image_b64: str | None, model: str,
                  num_predict: int, num_ctx: int, timeout: float) -> str:
    payload: dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"num_predict": num_predict, "num_ctx": num_ctx},
    }
    if image_b64:
        payload["images"] = [image_b64]
    try:
        r = httpx.post(f"{OLLAMA_BASE}/api/generate", json=payload, timeout=timeout)
        r.raise_for_status()
    except httpx.ConnectError:
        raise RuntimeError("Ollama non disponible. Lancez `ollama serve`.")
    except httpx.TimeoutException:
        raise RuntimeError(f"Le modèle {model} a dépassé le timeout ({int(timeout)}s).")
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Erreur Ollama {e.response.status_code}: {e.response.text[:300]}")
    return r.json().get("response", "")


# ── Stage 1: per-band extraction ────────────────────────────────────────────
def _dedupe(nodes: list[dict]) -> list[dict]:
    """Boxes in the overlap region between two adjacent bands get extracted
    twice; keep only the first occurrence of each distinct role+person."""
    seen: set[str] = set()
    result: list[dict] = []
    for node in nodes:
        key = re.sub(r"\s+", " ", (node["role"] + " " + node["person"]).strip().upper())
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(node)
    return result


def _split_role_person(text: str) -> tuple[str, str]:
    """Same convention as orgchart_ocr.py's _build_entities: the model joins
    a box's lines with " | " (e.g. role title on one line, person's name on
    the next); split on the first one, everything before is the role."""
    parts = text.split(" | ", 1)
    role = parts[0].strip()
    person = parts[1].strip() if len(parts) > 1 else ""
    return role, person


def _extract_bands(image_bytes: bytes, model: str, on_step=None) -> list[dict]:
    tiles = _split_into_bands(image_bytes)
    all_nodes: list[dict] = []

    for band_index, tile_bytes in enumerate(tiles):
        if on_step:
            on_step(band_index, len(tiles), f"Lecture de la bande {band_index + 1}/{len(tiles)}…")
        tile_b64 = base64.b64encode(tile_bytes).decode()
        raw_output = _call_ollama(
            TILE_EXTRACTION_PROMPT, tile_b64, model,
            num_predict=2000, num_ctx=6144, timeout=TILE_TIMEOUT,
        )
        try:
            raw_tile_nodes = _extract_node_array(raw_output)
        except Exception:
            raw_tile_nodes = []

        for i, node in enumerate(raw_tile_nodes):
            text = str(node.get("text") or "").strip()
            if not text:
                continue
            role, person = _split_role_person(text)
            all_nodes.append({
                "id": f"b{band_index}_n{i}",
                "role": role,
                "person": person,
                "color": node.get("color") or "white",
                "band": band_index,
            })

    return _dedupe(all_nodes)


# ── Stage 2: band-by-band hierarchy linking ─────────────────────────────────
def _label(node: dict) -> str:
    return (node["role"] + (" " + node["person"] if node["person"] else "")).strip()


def _resolve_link(value: Any, id_set: set, text_to_id: dict) -> str | None:
    if value in (None, "", "null"):
        return None
    value = str(value)
    if value in id_set:
        return value
    return text_to_id.get(value.strip().upper())


def _parse_links(raw_output: str, children: list[dict], parents: list[dict]) -> dict:
    """Tolerant parser: the linking model may return the requested
    [{"id":,"parent_id":}] array, or a dict keyed by child id mapping to a
    parent id or (more naturally, since it's what a language model tends to
    reach for) the parent's text — resolve any of these against the known
    node list."""
    parsed = _extract_json_value(raw_output)

    pool = children + parents
    id_set = {n["id"] for n in pool}
    text_to_id: dict[str, str] = {}
    for n in pool:
        text_to_id.setdefault(_label(n).upper(), n["id"])

    # Unwrap a dict that just wraps the real array under some key.
    if isinstance(parsed, dict):
        list_values = [v for v in parsed.values() if isinstance(v, list)]
        if len(parsed) == 1 and list_values:
            parsed = list_values[0]

    parent_by_id: dict[str, str | None] = {}
    if isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            lid = item.get("id")
            if lid is None:
                continue
            parent_by_id[str(lid)] = _resolve_link(item.get("parent_id"), id_set, text_to_id)
    elif isinstance(parsed, dict):
        for key, value in parsed.items():
            if isinstance(value, dict):
                parent_by_id[key] = _resolve_link(value.get("parent_id") or value.get("parent"), id_set, text_to_id)
            else:
                parent_by_id[key] = _resolve_link(value, id_set, text_to_id)

    return parent_by_id


def _link_band_pair(children: list[dict], parents: list[dict], model: str) -> dict:
    parents_json = json.dumps([{"id": p["id"], "text": _label(p)} for p in parents], ensure_ascii=False)
    children_json = json.dumps([{"id": c["id"], "text": _label(c)} for c in children], ensure_ascii=False)
    prompt = BAND_LINKING_PROMPT_TEMPLATE.format(
        parents_json=parents_json, children_json=children_json, child_count=len(children)
    )
    raw_output = _call_ollama(prompt, None, model, num_predict=1500, num_ctx=4096, timeout=LINK_TIMEOUT)
    return _parse_links(raw_output, children, parents)


def _link_hierarchy(nodes: list[dict], model: str, on_step=None) -> list[dict]:
    if not nodes:
        return nodes

    by_band: dict[int, list[dict]] = {}
    for node in nodes:
        by_band.setdefault(node["band"], []).append(node)
    max_band = max(by_band)

    for node in by_band.get(0, []):
        node["parent_id"] = None

    # Precompute which bands actually need a linking call (some may have no
    # children, or no non-empty band above them), so the progress total is
    # known upfront instead of drifting mid-run.
    link_jobs: list[tuple[int, list[dict], list[dict]]] = []
    for band in range(1, max_band + 1):
        children = by_band.get(band, [])
        if not children:
            continue

        parents_pool: list[dict] = []
        for candidate_band in range(band - 1, -1, -1):
            parents_pool = by_band.get(candidate_band, [])
            if parents_pool:
                break

        if parents_pool:
            link_jobs.append((band, children, parents_pool))
        else:
            for node in children:
                node["parent_id"] = None

    for i, (band, children, parents_pool) in enumerate(link_jobs):
        if on_step:
            on_step(i, len(link_jobs), f"Liaison hiérarchique {i + 1}/{len(link_jobs)} (bande {band})…")

        parent_by_id = _link_band_pair(children, parents_pool, model)
        for node in children:
            parent_id = parent_by_id.get(node["id"])
            # A node naming itself as its own parent is a model slip, not a
            # real relationship — without this guard it silently falls back
            # to "top level" below, which looks like a phantom self-loop.
            node["parent_id"] = None if parent_id == node["id"] else parent_id

    return nodes


# ── Stage 3: build niveau1/niveau2/niveau3 from the resolved tree ──────────
def _compute_ancestors(nodes: list[dict]) -> dict[str, dict]:
    by_id = {n["id"]: n for n in nodes}

    def _ancestor_chain(node: dict) -> list[str]:
        chain: list[str] = []
        current = node
        seen: set[str] = set()
        while current.get("parent_id") and current["parent_id"] in by_id and current["parent_id"] not in seen:
            seen.add(current["parent_id"])
            current = by_id[current["parent_id"]]
            chain.append(_label(current))
        return chain

    out: dict[str, dict] = {}
    for node in nodes:
        chain = _ancestor_chain(node)
        out[node["id"]] = {
            "niveau1": chain[0] if len(chain) > 0 else "",
            "niveau2": chain[1] if len(chain) > 1 else "",
            "niveau3": chain[2] if len(chain) > 2 else "",
            "depth": len(chain),
        }
    return out


# ── Public entry point ──────────────────────────────────────────────────────
# Extraction and linking get a fixed share of the overall progress bar each,
# since the exact number of linking calls isn't known until extraction has
# already run (some bands may turn out empty).
_EXTRACT_WEIGHT = 0.6
_LINK_WEIGHT = 0.4


def analyze_orgchart_tiled(image_bytes: bytes, model: str, on_progress=None) -> dict:
    """
    Analyse avancée : découpe l'image en bandes horizontales chevauchantes,
    extrait chaque bande indépendamment (crops plus petits et plus nets =
    meilleure lecture qu'une seule passe sur l'image entière), puis relie la
    hiérarchie bande par bande (chaque bande n'est comparée qu'à la bande la
    plus proche au-dessus, jamais à tout le graphe en une fois — c'est ce qui
    évite que le modèle rattache par défaut la majorité des entités au même
    parent "sûr").

    on_progress, if given, is called with {"pct": 0-100, "message": str} at
    each meaningful step (one per band extracted, one per band-pair linked).
    """
    def _extract_step(current, total, message):
        if on_progress:
            pct = round((current / max(total, 1)) * _EXTRACT_WEIGHT * 100)
            on_progress({"pct": pct, "message": message})

    def _link_step(current, total, message):
        if on_progress:
            pct = round(_EXTRACT_WEIGHT * 100 + (current / max(total, 1)) * _LINK_WEIGHT * 100)
            on_progress({"pct": pct, "message": message})

    nodes = _extract_bands(image_bytes, model, on_step=_extract_step)
    nodes = _link_hierarchy(nodes, model, on_step=_link_step)
    if on_progress:
        on_progress({"pct": 100, "message": "Finalisation…"})
    ancestors = _compute_ancestors(nodes)

    entities: list[dict] = []
    by_depth: dict[int, list[dict]] = {}
    for node in nodes:
        anc = ancestors[node["id"]]
        name = _label(node)
        entity = {
            "role": node["role"], "person": node["person"], "name": name,
            "niveau1": anc["niveau1"], "niveau2": anc["niveau2"], "niveau3": anc["niveau3"],
        }
        entities.append(entity)
        by_depth.setdefault(anc["depth"], []).append(entity)

    levels = [
        {"label": f"Niveau {depth}", "entities": by_depth[depth]}
        for depth in sorted(by_depth)
    ]

    return {"entities": entities, "levels": levels}
