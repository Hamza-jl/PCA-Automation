"""
orgchart_ocr.py  —  OCR + Hybrid + GLM-OCR diagram analyser

Public API
──────────
ocr_available()                              -> bool
glmocr_available()                           -> bool
analyze_orgchart_ocr(image_bytes)            -> list[dict]
analyze_orgchart_hybrid(image_bytes, model)  -> dict  {"entities":[…], "levels":[…]}
analyze_orgchart_glmocr(image_bytes)         -> dict  {"entities":[…], "levels":[…]}

Entity schema (all modes)
─────────────────────────
{
  "role":    str   # the function / position label  (e.g. "DIRECTEUR GENERAL")
  "person":  str   # the name(s) of the holder(s)   (e.g. "MR WALID SAKKA")
                   # empty string when no person is named
  "name":    str   # role + " " + person — kept for BIA fiche generation
  "niveau1": str   # direct parent entity name
  "niveau2": str   # grandparent
  "niveau3": str   # great-grandparent
}

Hybrid pipeline (3 stages)
──────────────────────────
Stage 1 — OCR   : EasyOCR → merge fragments → noise-filter → detect level labels
Stage 2 — VLM   : vision model reads the image independently
Stage 3 — Merge : text-only VLM reconciliation — corrects OCR errors, merges split
                  boxes (role on one line + person on next), filters title noise,
                  fills gaps each side missed, separates role from person name,
                  groups by structural level.
                  Returns {"levels":[{label, entities:[…]}, …]}
"""
from __future__ import annotations

import io
import json
import re
from typing import Any

# ── optional deps ──────────────────────────────────────────────────────────────
try:
    import easyocr
    import numpy as np
    from PIL import Image as _PILImage
    _HAS_EASYOCR = True
except ImportError:
    _HAS_EASYOCR = False

_reader: Any = None


def ocr_available() -> bool:
    return _HAS_EASYOCR


def _get_reader(use_gpu: bool = True) -> Any:
    global _reader
    if _reader is None:
        try:
            _reader = easyocr.Reader(["fr", "en"], gpu=use_gpu, verbose=False)
        except Exception:
            _reader = easyocr.Reader(["fr", "en"], gpu=False, verbose=False)
    return _reader


# ── regexes ────────────────────────────────────────────────────────────────────
# Matches structural level labels found in the left margin of org charts
_LABEL_RE = re.compile(
    r"conseil|assembl[eé]e?|pr[eé]sident(?!\s+[a-z])|direction\s+g[eé]n[eé]rale?"
    r"|directeur\s+g[eé]n[eé]ral|directions?\s+centrales?"
    r"|directions?\s*[/&]\s*d[eé]partements?"
    r"|divisions?\s*[/&]\s*services?"
    r"|agences?|bureaux?\b|zone\b|r[eé]gion"
    r"|board\s+of|executive\s+(?:committee|board)|department[s]?\b|division[s]?\b"
    r"|branch(?:es)?\b|unit[s]?\b|service[s]?\b|committee\b",
    re.IGNORECASE,
)

# Pure-noise: mostly digits/symbols, known header keywords
_NOISE_RE = re.compile(
    r"^[\W\d\s]{1,4}$"
    r"|^\d{1,4}$"
    r"|^(organigramme|organigrama|organigram|org\.?\s*chart)\b"
    r"|^(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t"
    r"|septembre|octobre|novembre|d[eé]cembre)\b"
    r"|^\d{4}$"
    r"|^page\s*\d",
    re.IGNORECASE,
)


def _is_noise(text: str, cy: float, img_h: float) -> bool:
    text = text.strip()
    if not text:
        return True
    if _NOISE_RE.match(text):
        return True
    # Top 4 % of image = title / date zone
    if cy < img_h * 0.04:
        return True
    # Less than 35 % real letters → OCR artifact (e.g. "0 R & 4 N I")
    alpha = sum(1 for c in text if c.isalpha())
    if len(text) >= 5 and alpha / len(text) < 0.35:
        return True
    return False


# ── image resize ───────────────────────────────────────────────────────────────
_MAX_SIDE = 1024

def _resize_image(image_bytes: bytes, max_side: int = _MAX_SIDE) -> bytes:
    try:
        pil = _PILImage.open(io.BytesIO(image_bytes))
        w, h = pil.size
        if max(w, h) <= max_side:
            return image_bytes
        scale = max_side / max(w, h)
        pil = pil.resize((int(w * scale), int(h * scale)), _PILImage.LANCZOS)
        buf = io.BytesIO()
        fmt = pil.format if pil.format in ("PNG", "JPEG", "WEBP") else "PNG"
        pil.save(buf, format=fmt)
        return buf.getvalue()
    except Exception:
        return image_bytes


# ── helpers ────────────────────────────────────────────────────────────────────
def _x_overlap(a: dict, b: dict) -> float:
    lo = max(a["x_min"], b["x_min"])
    hi = min(a["x_max"], b["x_max"])
    span = max(a["x_max"] - a["x_min"], b["x_max"] - b["x_min"]) + 1e-9
    return max(0.0, hi - lo) / span


def _make_entity(role: str, person: str, niveau1: str = "",
                 niveau2: str = "", niveau3: str = "") -> dict:
    role   = (role   or "").strip()
    person = (person or "").strip()
    name   = (role + (" " + person if person else "")).strip()
    return {"role": role, "person": person, "name": name,
            "niveau1": niveau1, "niveau2": niveau2, "niveau3": niveau3}


# ── Stage 1-a: raw OCR ─────────────────────────────────────────────────────────
def _raw_ocr(image_bytes: bytes, use_gpu: bool = True) -> tuple[list[dict], int, int]:
    pil = _PILImage.open(io.BytesIO(image_bytes)).convert("RGB")
    img_w, img_h = pil.size
    reader = _get_reader(use_gpu)
    raw = reader.readtext(np.array(pil), paragraph=False)

    boxes: list[dict] = []
    for bbox, text, conf in raw:
        text = text.strip()
        xs = [p[0] for p in bbox]; ys = [p[1] for p in bbox]
        cy = sum(ys) / 4; cx = sum(xs) / 4
        if conf < 0.25 or _is_noise(text, cy, img_h):
            continue
        boxes.append({
            "text":  text,
            "cx": cx, "cy": cy,
            "x_min": min(xs), "x_max": max(xs),
            "y_min": min(ys), "y_max": max(ys),
            "h": max(ys) - min(ys),
            "conf": conf,
        })
    return boxes, img_w, img_h


# ── Stage 1-b: merge nearby fragments into single cells ────────────────────────
def _merge_fragments(boxes: list[dict], img_h: int) -> list[dict]:
    """
    Two fragments merge when:
      • vertical gap  < 80 % of the taller box's height
      • X ranges overlap by at least 10 %
    This catches role-label + person-name combos in the same visual cell.
    Iterates until no further merges happen.
    """
    changed = True
    while changed:
        changed = False
        used   = [False] * len(boxes)
        result: list[dict] = []
        for i, a in enumerate(boxes):
            if used[i]:
                continue
            group = [a]
            for j, b in enumerate(boxes):
                if i == j or used[j]:
                    continue
                v_gap = max(0.0, max(b["y_min"], a["y_min"]) - min(b["y_max"], a["y_max"]))
                max_h = max(a["h"], b["h"]) + 1e-9
                x_ov  = _x_overlap(a, b)
                if v_gap < max_h * 0.80 and x_ov > 0.10:
                    group.append(b)
                    used[j] = True
                    changed  = True
            used[i] = True
            group_sorted = sorted(group, key=lambda g: g["cy"])
            merged_text  = " | ".join(g["text"] for g in group_sorted)
            all_xs = [v for g in group for v in (g["x_min"], g["x_max"])]
            all_ys = [v for g in group for v in (g["y_min"], g["y_max"])]
            result.append({
                "text":  merged_text,
                "cx":    sum(all_xs) / len(all_xs),
                "cy":    sum(all_ys) / len(all_ys),
                "x_min": min(all_xs), "x_max": max(all_xs),
                "y_min": min(all_ys), "y_max": max(all_ys),
                "h":     max(all_ys) - min(all_ys),
                "conf":  max(g["conf"] for g in group),
            })
        boxes = result
    return boxes


# ── Stage 1-c: separate level labels from content boxes ────────────────────────
def _split_labels(boxes: list[dict], img_w: int) -> tuple[list[dict], list[dict]]:
    LEFT = img_w * 0.22
    labels, content = [], []
    for b in boxes:
        if b["cx"] < LEFT and _LABEL_RE.search(b["text"]):
            labels.append(b)
        else:
            content.append(b)
    return sorted(labels, key=lambda b: b["cy"]), content


# ── Stage 1-d: cluster content boxes into rows ─────────────────────────────────
def _cluster_rows(content: list[dict], img_h: int) -> list[list[dict]]:
    if not content:
        return []
    heights = sorted(b["h"] for b in content)
    med_h   = heights[len(heights) // 2] or (img_h * 0.03)
    ROW_GAP = max(med_h * 1.6, img_h * 0.025)
    content_sorted = sorted(content, key=lambda b: b["cy"])
    rows: list[list[dict]] = []
    for box in content_sorted:
        placed = False
        for row in rows:
            row_cy = sum(b["cy"] for b in row) / len(row)
            if abs(box["cy"] - row_cy) < ROW_GAP:
                row.append(box)
                placed = True
                break
        if not placed:
            rows.append([box])
    return rows


# ── Stage 1-e: assign hierarchy levels to rows ─────────────────────────────────
def _assign_levels(rows: list[list[dict]], labels: list[dict]) -> list[tuple[int, list[dict]]]:
    def _row_cy(row): return sum(b["cy"] for b in row) / len(row)
    if labels:
        label_ys = [l["cy"] for l in labels]
        def _level(cy): return int(min(range(len(label_ys)), key=lambda i: abs(label_ys[i] - cy)))
        return [(_level(_row_cy(row)), row) for row in rows]
    else:
        return [(i, row) for i, row in enumerate(sorted(rows, key=_row_cy))]


# ── Stage 1-f: build entity list from geometry (pure OCR mode) ─────────────────
def _build_entities(row_levels: list[tuple[int, list[dict]]]) -> list[dict]:
    level_boxes: dict[int, list[dict]] = {}
    for level, row in row_levels:
        level_boxes.setdefault(level, []).extend(row)

    entities: list[dict] = []
    for level, row in sorted(row_levels, key=lambda x: x[0]):
        for box in row:
            parent_name = grand_name = ""
            if level > 0:
                p_lvl = level - 1
                while p_lvl >= 0 and p_lvl not in level_boxes:
                    p_lvl -= 1
                if p_lvl >= 0:
                    def _score(pb):
                        ov   = _x_overlap(box, pb)
                        dist = abs(pb["cx"] - box["cx"]) / (box["x_max"] - box["x_min"] + 1)
                        return ov - dist * 0.1
                    best = max(level_boxes[p_lvl], key=_score)
                    parent_name = best["text"]
                    if p_lvl > 0:
                        gp_lvl = p_lvl - 1
                        while gp_lvl >= 0 and gp_lvl not in level_boxes:
                            gp_lvl -= 1
                        if gp_lvl >= 0:
                            best_gp = max(level_boxes[gp_lvl],
                                          key=lambda pb: _x_overlap(best, pb))
                            grand_name = best_gp["text"]

            # Attempt to split "ROLE | PERSON" produced by merge step
            parts = box["text"].split(" | ", 1)
            role   = parts[0].strip()
            person = parts[1].strip() if len(parts) > 1 else ""
            entities.append(_make_entity(role, person, parent_name, grand_name))

    seen: set[tuple] = set()
    unique: list[dict] = []
    for e in entities:
        k = (e["name"].lower(), e["niveau1"].lower())
        if k not in seen:
            seen.add(k)
            unique.append(e)
    return unique


# ── Public: pure OCR ───────────────────────────────────────────────────────────
def analyze_orgchart_ocr(image_bytes: bytes, use_gpu: bool = True) -> list[dict]:
    if not _HAS_EASYOCR:
        raise RuntimeError("EasyOCR non installé — pip install easyocr")
    boxes, img_w, img_h = _raw_ocr(image_bytes, use_gpu)
    boxes = _merge_fragments(boxes, img_h)
    labels, content = _split_labels(boxes, img_w)
    rows = _cluster_rows(content, img_h)
    if not rows:
        return []
    row_levels = _assign_levels(rows, labels)
    return _build_entities(row_levels)


# ── Public: hybrid (OCR + VLM + reconciliation) ────────────────────────────────
def analyze_orgchart_hybrid(
    image_bytes: bytes,
    model: str,
    timeout: float = 600.0,
    use_gpu: bool = True,
) -> dict:
    """
    3-stage hybrid pipeline.

    Returns
    -------
    {
      "entities": [flat list of entity dicts — backwards-compat for BIA fiches],
      "levels":   [{label, entities:[…]}, …]  — grouped for UI table display
    }
    """
    if not _HAS_EASYOCR:
        raise RuntimeError("EasyOCR non installé — pip install easyocr")

    # ── Stage 1 : OCR ──────────────────────────────────────────────────────────
    boxes, img_w, img_h = _raw_ocr(image_bytes, use_gpu)
    boxes = _merge_fragments(boxes, img_h)
    labels, content = _split_labels(boxes, img_w)
    rows = _cluster_rows(content, img_h)
    row_levels = _assign_levels(rows, labels) if rows else []

    # Build OCR summary: "ROLE | PERSON  (Y=123, Level=Directions centrales)"
    ocr_lines: list[str] = []
    label_texts = [l["text"] for l in labels]
    for lvl, row in sorted(row_levels, key=lambda x: x[0]):
        lvl_name = label_texts[lvl] if lvl < len(label_texts) else f"Niveau {lvl}"
        for box in sorted(row, key=lambda b: b["cx"]):
            ocr_lines.append(f'- [{lvl_name}]  "{box["text"]}"  (Y={int(box["cy"])})')

    level_label_block = "\n".join(
        f'  {i+1}. "{l["text"]}"  (Y={int(l["cy"])})' for i, l in enumerate(labels)
    ) or "  (none detected — infer from Y positions)"

    # ── Stage 2 : VLM independent vision extraction ────────────────────────────
    import base64
    import httpx

    vlm_raw_text = "(VLM unavailable)"
    try:
        from orgchart_vision import _analyze_via_ollama, _resize_image as _rv
        image_small = _rv(image_bytes)
        image_b64   = base64.b64encode(image_small).decode()

        # Ask VLM specifically for role+person structure
        vlm_prompt = (
            "You are analysing an organisational chart image. "
            "Extract EVERY entity visible. For each entity output TWO fields:\n"
            "  • role   : the function/position label (e.g. DIRECTEUR GENERAL, CEO, Head of IT)\n"
            "  • person : the name(s) of the holder(s), empty string if none shown\n"
            "Multiple people in the same box → join with ' / '.\n"
            "Return ONLY a JSON array, no text before or after:\n"
            '[{"role":"...", "person":"..."}, ...]'
        )
        vlm_raw_text = _analyze_via_ollama(
            image_b64, model, timeout=timeout * 0.40,
            prompt_override=vlm_prompt,
        )
    except TypeError:
        # older version without prompt_override — fall back silently
        try:
            from orgchart_vision import _analyze_via_ollama, _resize_image as _rv
            image_small = _rv(image_bytes)
            image_b64   = base64.b64encode(image_small).decode()
            vlm_raw_text = _analyze_via_ollama(image_b64, model, timeout=timeout * 0.40)
        except Exception as exc:
            vlm_raw_text = f"(VLM error: {exc})"
    except Exception as exc:
        vlm_raw_text = f"(VLM error: {exc})"

    # ── Stage 3 : VLM reconciliation — text-only, fast ─────────────────────────
    ocr_block = "\n".join(ocr_lines) or "(no OCR entities)"

    reconcile_prompt = f"""You are reconciling two independent extractions of a hierarchical diagram (org chart, process chart, etc.).

══ STRUCTURAL LEVEL LABELS (from left margin of diagram) ══
{level_label_block}

══ SOURCE A — OCR (geometric reading, may have character errors or split lines) ══
Format: [Level label]  "text"  (Y position — lower Y = higher in diagram)
{ocr_block}

══ SOURCE B — Vision model (may miss entities but reads text more accurately) ══
{vlm_raw_text}

══ YOUR TASK ══
Produce a clean, complete extraction following these rules:

1. SEPARATE role from person
   Each visual box typically contains TWO lines:
   - TOP line  → the ROLE / FUNCTION (e.g. "PRÉSIDENT", "DIRECTEUR GÉNÉRAL", "Head of IT")
   - BOTTOM line → the PERSON NAME(s) (e.g. "MR AHMED BEN MOULEHOM", "WALID SAKKA")
   OCR may have joined them with " | " — split them correctly.
   If only one line exists (no person named), put it in "role" and leave "person" empty.

2. MERGE OCR fragments that belong to the same box
   e.g. role="PRÉSIDENT" person="" merged with role="MR AHMED BEN MOULEHOM" person=""
   → becomes role="PRÉSIDENT" person="MR AHMED BEN MOULEHOM"

3. CORRECT OCR reading errors using Source B as truth
   e.g. "0 R & 4 N I & R 4 M ME" → this is a stylised title, IGNORE IT

4. IGNORE page title, date, page number, legend, watermark, any non-entity text

5. COMPLETE the list: add what A found but B missed, AND what B found but A missed

6. GROUP entities under their structural level label (use the labels detected above;
   if none detected, infer levels from Y position — lower Y = higher level)

7. Multiple people in one box → join with " / " in the person field

Return ONLY this JSON, no text before or after:
{{
  "levels": [
    {{
      "label": "Conseil d'Administration",
      "entities": [
        {{"role": "PRÉSIDENT", "person": "MR AHMED BEN MOULEHOM"}},
        {{"role": "COMITE PERMANENT D'AUDIT", "person": ""}},
        ...
      ]
    }},
    {{
      "label": "Direction Générale",
      "entities": [
        {{"role": "DIRECTEUR GENERAL", "person": "MR WALID SAKKA"}},
        ...
      ]
    }},
    ...
  ]
}}"""

    OLLAMA_BASE = "http://localhost:11434"
    payload = {
        "model":  model,
        "system": (
            "You are a precise data-extraction assistant. "
            "You respond ONLY with valid JSON, no text before or after."
        ),
        "prompt": reconcile_prompt,
        "stream": False,
        "options": {"temperature": 0.0, "num_predict": 8000, "num_ctx": 12000},
    }

    try:
        r = httpx.post(f"{OLLAMA_BASE}/api/generate", json=payload, timeout=timeout * 0.55)
        r.raise_for_status()
        raw_response = r.json().get("response", "")
    except httpx.ConnectError:
        raise RuntimeError("Ollama not available. Run `ollama serve`.")
    except httpx.TimeoutException:
        raise RuntimeError(f"Model {model} timed out ({int(timeout*0.55)}s).")
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Ollama error {e.response.status_code}: {e.response.text[:300]}")

    return _parse_reconciliation(raw_response, row_levels, labels)


# ── Parse reconciliation JSON ──────────────────────────────────────────────────
def _parse_reconciliation(raw: str, row_levels, labels) -> dict:
    """Parse the reconciliation JSON. Falls back to pure OCR geometry on failure."""
    # Strip markdown fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```\s*$",        "", cleaned.strip(), flags=re.MULTILINE)
    # Find first { … }
    m = re.search(r'\{[\s\S]*\}', cleaned)
    if m:
        cleaned = m.group(0)

    levels_out:   list[dict] = []
    entities_flat: list[dict] = []

    try:
        data       = json.loads(cleaned)
        raw_levels = data.get("levels", [])
        if not isinstance(raw_levels, list) or not raw_levels:
            raise ValueError("empty levels")

        for grp in raw_levels:
            label    = str(grp.get("label", "")).strip()
            grp_ents = grp.get("entities", [])
            if not isinstance(grp_ents, list):
                continue
            clean_ents: list[dict] = []
            for e in grp_ents:
                if not isinstance(e, dict):
                    continue
                # Accept both new schema (role/person) and old schema (name)
                role   = str(e.get("role",   e.get("name", "")) or "").strip()
                person = str(e.get("person", "")                 or "").strip()
                if not role and not person:
                    continue
                ent = _make_entity(
                    role, person,
                    str(e.get("niveau1", "") or "").strip(),
                    str(e.get("niveau2", "") or "").strip(),
                    str(e.get("niveau3", "") or "").strip(),
                )
                clean_ents.append(ent)
                entities_flat.append(ent)
            if clean_ents:
                levels_out.append({"label": label, "entities": clean_ents})

        if entities_flat:
            return {"entities": entities_flat, "levels": levels_out}

    except (json.JSONDecodeError, ValueError):
        pass

    # ── Fallback: pure OCR with basic grouping ─────────────────────────────────
    ocr_ents = _build_entities(row_levels) if row_levels else []
    if not ocr_ents:
        return {"entities": [], "levels": []}

    label_texts = [l["text"] for l in labels] if labels else []
    if row_levels and label_texts:
        level_map: dict[int, list[dict]] = {}
        for lvl, row in row_levels:
            for box in row:
                level_map.setdefault(lvl, []).append(box["text"])
        fb_levels: list[dict] = []
        for lvl in sorted(level_map.keys()):
            lname = label_texts[lvl] if lvl < len(label_texts) else f"Level {lvl}"
            ents  = [e for e in ocr_ents if e["name"] in level_map[lvl]
                     or any(t in e["name"] for t in level_map[lvl])]
            if ents:
                fb_levels.append({"label": lname, "entities": ents})
        if fb_levels:
            return {"entities": ocr_ents, "levels": fb_levels}

    return {"entities": ocr_ents, "levels": [{"label": "Entities", "entities": ocr_ents}]}


# ── Public: GLM-OCR via Ollama ─────────────────────────────────────────────────
def glmocr_available() -> bool:
    """Return True if glm-ocr model is loaded in Ollama."""
    try:
        import httpx
        r = httpx.get("http://localhost:11434/api/tags", timeout=3)
        return any("glm-ocr" in m.get("name", "") for m in r.json().get("models", []))
    except Exception:
        return False


def analyze_orgchart_glmocr(
    image_bytes: bytes,
    timeout: float = 600.0,
    vlm_model: str | None = None,
) -> dict:
    """
    3-stage pipeline:
      Stage 1 — GLM-OCR   : accurate text extraction (native "Text recognition:" prompt)
      Stage 2 — text LLM  : structure raw text → role/person/level JSON
      Stage 3 — vision LLM: analyze image + entities → parent-child tree (D3-compatible)

    Returns {"entities":[…], "levels":[…], "tree":{…}}
      tree is None when no vlm_model is available.
    """
    import base64
    import httpx

    import httpx
    OLLAMA_BASE = "http://localhost:11434"

    # ── Stage 1 : GLM-OCR — raw text extraction (native prompt format) ────────
    # GLM-OCR outputs markdown/plain text, not JSON.
    # Use its native "Text recognition:" prompt for maximum accuracy.
    image_small = _resize_image(image_bytes, max_side=896)
    image_b64   = base64.b64encode(image_small).decode()

    # Stage 3 needs a sharper image to trace thin connector lines across many
    # boxes — 896px is fine for text OCR but too coarse for line-tracing.
    image_hires    = _resize_image(image_bytes, max_side=1400)
    image_hires_b64 = base64.b64encode(image_hires).decode()

    stage1_payload = {
        "model": "glm-ocr:latest",
        "prompt": "Text recognition:",
        "images": [image_b64],
        "stream": False,
        "keep_alive": "10m",
        "options": {
            "temperature": 0.0,
            "num_predict": 3000,
            "num_ctx":     4096,
            "num_gpu":     99,
        },
    }

    try:
        r1 = httpx.post(f"{OLLAMA_BASE}/api/generate",
                        json=stage1_payload, timeout=timeout * 0.6)
        r1.raise_for_status()
        ocr_text = r1.json().get("response", "").strip()
    except httpx.ConnectError:
        raise RuntimeError("Ollama non disponible. Lancez `ollama serve`.")
    except httpx.TimeoutException:
        raise RuntimeError(f"GLM-OCR stage 1 a dépassé le timeout ({int(timeout*0.6)}s).")
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Erreur Ollama {e.response.status_code}: {e.response.text[:300]}")

    if not ocr_text:
        raise RuntimeError("GLM-OCR n'a retourné aucun texte.")

    # ── Stage 2 : text-only LLM — structure the raw OCR text into JSON ────────
    # Use the first available Ollama model (any model, no vision needed)
    all_models: list[str] = []
    text_model = None
    _vision_hints = ("vl", "vision", "llava", "bakllava", "moondream",
                     "minicpm", "cogvlm", "phi3-vision", "internvl", "glm-ocr")
    try:
        tags_r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=5)
        tag_models = tags_r.json().get("models", [])
        all_models = [m["name"] for m in tag_models]
        print(f"[GLM-OCR] available models: {all_models}")
        # Prefer small, text-only (non-vision) models for Stage 2 speed.
        # Use the REAL on-disk size from Ollama, not name-guessing — model
        # name tags (e.g. "e2b") don't reliably reflect actual parameter count.
        text_only = [m for m in tag_models
                     if not any(h in m["name"].lower() for h in _vision_hints)]
        text_only.sort(key=lambda m: m.get("size", float("inf")))
        text_model = text_only[0]["name"] if text_only else None
        print(f"[GLM-OCR] Stage2 text_model={text_model!r} "
              f"(sizes: {[(m['name'], round(m.get('size',0)/1e9,1)) for m in text_only]})")
    except Exception as _tag_err:
        print(f"[GLM-OCR] /api/tags failed: {_tag_err}")

    if not text_model:
        print("[GLM-OCR Stage2] no text model — skipping structuring")
        result = _parse_glmocr_raw(ocr_text)
    else:
        structure_prompt = (
            "You received raw OCR text extracted from a hierarchical diagram image.\n"
            "Parse it into structured JSON.\n\n"
            f"RAW OCR TEXT:\n{ocr_text}\n\n"
            "Rules:\n"
            "1. Each entry has a ROLE (function/position) and optionally a PERSON name.\n"
            "   They are often on consecutive lines: role on top, person below.\n"
            "2. Group entries under structural LEVEL labels visible in the diagram.\n"
            "   If no levels visible, use label \"Entités\".\n"
            "3. Multiple people in one box → join with \" / \".\n"
            "4. Ignore: page title, dates, page numbers, decorative text.\n\n"
            "Return ONLY valid JSON, no text before or after:\n"
            "{\"levels\":[{\"label\":\"...\",\"entities\":[{\"role\":\"...\",\"person\":\"...\"}]}]}"
        )
        stage2_payload = {
            "model": text_model,
            "system": "You are a precise data-extraction assistant. Respond ONLY with valid JSON.",
            "prompt": structure_prompt,
            "stream": False,
            "keep_alive": "10m",
            "options": {"temperature": 0.0, "num_predict": 3000, "num_ctx": 6000, "num_gpu": 99},
        }
        structured_raw = ""
        try:
            r2 = httpx.post(f"{OLLAMA_BASE}/api/generate",
                            json=stage2_payload, timeout=timeout * 0.30)
            r2.raise_for_status()
            structured_raw = r2.json().get("response", "")
            print(f"[GLM-OCR Stage2] response (first 300): {structured_raw[:300]!r}")
        except Exception as _s2_err:
            print(f"[GLM-OCR Stage2] FAILED: {_s2_err} — using raw parse fallback")

        if structured_raw:
            result = _parse_reconciliation(structured_raw, [], [])
            if not result.get("entities"):
                print("[GLM-OCR Stage2] no entities from reconciliation — raw fallback")
                result = _parse_glmocr_raw(ocr_text)
        else:
            result = _parse_glmocr_raw(ocr_text)

    # ── Stage 3 : VLM hierarchy — build parent-child tree from image ───────────
    # Unload Stage 1 (GLM-OCR) + Stage 2 text model from VRAM first so vision model has room
    for _unload in ["glm-ocr:latest", text_model]:
        if _unload:
            try:
                httpx.post(f"{OLLAMA_BASE}/api/generate",
                           json={"model": _unload, "keep_alive": "0"},
                           timeout=10)
                print(f"[GLM-OCR Stage3] unloaded {_unload!r} from VRAM")
            except Exception:
                pass

    # Strip entities whose role exactly matches a known level/section label with
    # no attached person — these are section headers that leaked through as
    # fake entities (mainly from the raw-text fallback parser), not real nodes.
    level_labels = {lvl.get("label", "").strip().lower()
                     for lvl in result.get("levels", []) if lvl.get("label")}
    tree_entities = [
        e for e in result.get("entities", [])
        if not (e.get("role", "").strip().lower() in level_labels and not e.get("person", "").strip())
    ]
    if len(tree_entities) != len(result.get("entities", [])):
        print(f"[GLM-OCR Stage3] filtered {len(result['entities']) - len(tree_entities)} "
              f"section-header entities before tree building")

    tree = None
    vision_model = vlm_model or _auto_vision_model(all_models)
    print(f"[GLM-OCR Stage3] vlm_model={vlm_model!r} vision_model={vision_model!r} entities={len(tree_entities)}")
    if vision_model and tree_entities:
        try:
            tree = _build_hierarchy_tree(
                image_hires_b64, tree_entities, vision_model,
                OLLAMA_BASE, timeout=480.0,   # 8-min budget for VLM
            )
        except Exception as _tree_err:
            err_msg = str(_tree_err)
            print(f"[GLM-OCR] Stage 3 hierarchy failed: {err_msg}")
            tree = {"_error": err_msg}  # pass error to frontend for display

    result["tree"] = tree
    return result


def _auto_vision_model(all_models: list[str]) -> str | None:
    """Pick the first Ollama model that supports vision (has a known vision tag)."""
    vision_hints = ("vl", "vision", "llava", "bakllava", "moondream",
                    "minicpm", "cogvlm", "phi3-vision", "internvl")
    for m in all_models:
        ml = m.lower()
        if any(h in ml for h in vision_hints) and "glm-ocr" not in ml:
            return m
    return None


def _build_hierarchy_tree(
    image_b64: str,
    entities: list[dict],
    model: str,
    ollama_base: str,
    timeout: float,
) -> dict | None:
    """
    Send the image + flat entity list to a vision model.
    Ask it to determine parent-child relationships from the diagram's connecting
    lines and visual layout, and return a D3-compatible tree.
    """
    import httpx

    entity_list = "\n".join(
        f'{i+1}. role="{e.get("role","")}" person="{e.get("person","")}"'
        for i, e in enumerate(entities[:30])   # cap at 30 — vision context is limited
    )

    prompt = (
        "You are analyzing an organizational chart image.\n"
        "I have pre-extracted these entities from the chart:\n"
        f"{entity_list}\n\n"
        "IMPORTANT: Some text in the image are SECTION HEADERS / CATEGORY LABELS "
        "(e.g. large titles on the left margin, or row dividers spanning the page) — "
        "they are NOT organizational entities and must NEVER appear as nodes in your tree. "
        "Only the boxed entities from my list above are real nodes.\n\n"
        "Look at the image: identify the connecting LINES/ARROWS between boxes — these define "
        "who reports to whom. Follow the actual line connections, not visual proximity or section grouping.\n"
        "Build the FULL hierarchy depth — if A connects to B connects to C connects to D, "
        "the tree must be A > B > C > D (4 levels), not all of B,C,D flattened under A.\n\n"
        "Return ONLY a JSON object with this exact structure (no text before or after):\n"
        '{"name":"root","role":"root","person":"","children":['
        '{"role":"TOP ROLE","person":"TOP PERSON","children":['
        '{"role":"CHILD ROLE","person":"CHILD PERSON","children":['
        '{"role":"GRANDCHILD ROLE","person":"GRANDCHILD PERSON","children":[]}'
        "]}]}]}\n\n"
        "Use the EXACT role/person strings from my list. Do not add new entities. "
        "Do not include section headers as nodes."
    )

    payload = {
        "model": model,
        "prompt": prompt,
        "images": [image_b64],
        "stream": False,
        "keep_alive": "10m",
        "options": {"temperature": 0.0, "num_predict": 2200, "num_ctx": 6144, "num_gpu": 99},
    }

    import httpx as _hx
    r = _hx.post(f"{ollama_base}/api/generate", json=payload, timeout=timeout)
    r.raise_for_status()
    raw = r.json().get("response", "")
    print(f"[GLM-OCR Stage3] raw VLM response (first 500): {raw[:500]!r}")

    # Parse JSON
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```\s*$", "", cleaned.strip(), flags=re.MULTILINE)
    m = re.search(r'\{[\s\S]*\}', cleaned, re.DOTALL)
    if not m:
        raise ValueError(f"VLM returned no JSON object. Raw (200 chars): {raw[:200]!r}")
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError as je:
        raise ValueError(f"VLM JSON parse error: {je}. Raw snippet: {m.group(0)[:200]!r}") from je

    # Ensure every node has role/person/selected fields
    def _normalise(node: dict) -> dict:
        role   = node.get("role",   node.get("name", "")).strip()
        person = node.get("person", "").strip()
        return {
            "role":     role,
            "person":   person,
            "name":     (role + (" " + person if person else "")).strip(),
            "selected": True,
            "children": [_normalise(c) for c in node.get("children", [])],
        }

    # Build the set of real entities — anything the VLM invents (e.g. section
    # headers it mistook for nodes) gets pruned, its children re-parented up.
    valid_roles = {e.get("role", "").strip().lower() for e in entities if e.get("role")}

    def _prune(node: dict) -> list[dict]:
        """Return a list of nodes to splice in place of `node` (1 if valid, N if pruned)."""
        children: list[dict] = []
        for c in node.get("children", []):
            children.extend(_prune(c))
        role = (node.get("role") or "").strip().lower()
        if role and role not in valid_roles:
            print(f"[GLM-OCR Stage3] pruned hallucinated node: {node.get('role')!r}")
            return children  # drop this node, promote its children
        node["children"] = children
        return [node]

    # Root may be a wrapper node or the first real entity
    if data.get("name") == "root" and data.get("children"):
        pruned_children: list[dict] = []
        for c in data["children"]:
            pruned_children.extend(_prune(c))
        return {"name": "root", "children": [_normalise(c) for c in pruned_children]}

    result = _prune(data)
    return _normalise(result[0]) if result else _normalise(data)


def _parse_glmocr_raw(ocr_text: str) -> dict:
    """
    Last-resort parser: convert plain OCR text into a flat entity list
    when the structuring LLM is unavailable or fails.
    Each non-empty line becomes an entity with role=line, person=''.
    """
    entities: list[dict] = []
    for line in ocr_text.splitlines():
        line = line.strip(" |-•*#\t")
        if not line or _NOISE_RE.match(line):
            continue
        # Try to split "ROLE: PERSON" or "ROLE — PERSON"
        m = re.split(r"\s*[:—–]\s*", line, maxsplit=1)
        role   = m[0].strip()
        person = m[1].strip() if len(m) > 1 else ""
        entities.append(_make_entity(role, person))
    if not entities:
        return {"entities": [], "levels": []}
    return {"entities": entities,
            "levels": [{"label": "Entités extraites", "entities": entities}]}
