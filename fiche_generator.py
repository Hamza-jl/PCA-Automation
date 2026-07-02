"""
fiche_generator.py — Generates one BIA fiche per department/structure
from a 'fiche de recensement des structures' (Suivi projet.xlsx) and a
BIA fiche template (.docx).

Entry point:
    generate_all_fiches(
        xlsx_path, template_path, output_dir,
        version="2.0", openai_api_key=None
    ) -> (list[Path], list[str])
"""

from __future__ import annotations

import base64
import json
import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import openpyxl
from docx import Document


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(value) -> str:
    """Normalise a cell value to a stripped single-line string."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\n", " ")).strip()


def _safe_filename(name: str) -> str:
    """Strip characters illegal in Windows file names."""
    return re.sub(r'[\\/:*?"<>|]', "-", name).strip(" .")


# ---------------------------------------------------------------------------
# Extract ALL images from the recensement Excel (via zip — no BytesIO bugs)
# ---------------------------------------------------------------------------

def _extract_all_images(xlsx_path: Path) -> list[tuple[str, bytes]]:
    """
    Returns [(filename, raw_bytes), ...] for every image in xl/media/.
    Reading from the zip directly avoids the openpyxl BytesIO seek bug.
    """
    try:
        with zipfile.ZipFile(str(xlsx_path), "r") as z:
            media = [
                f for f in z.namelist()
                if re.match(r"xl/media/.*\.(png|jpg|jpeg|gif|bmp)$", f, re.IGNORECASE)
            ]
            return [(f, z.read(f)) for f in sorted(media)]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# OpenAI Vision — detect client name + identify the client logo
# ---------------------------------------------------------------------------

def detect_client_info(
    xlsx_path: Path,
    api_key: str,
) -> tuple[str, Optional[bytes]]:
    """
    Uses GPT-4o Vision to:
      1. Identify which image in the xlsx is the CLIENT logo (not Devoteam).
      2. Extract the client company name from that logo.

    Returns (client_name, logo_bytes).
    Falls back to ("Client", largest_image) if the API call fails.
    """
    images = _extract_all_images(xlsx_path)
    if not images:
        return "Client", None

    # Fallback: largest image (used if API call fails)
    fallback_logo = max(images, key=lambda x: len(x[1]))[1]

    try:
        import openai

        client = openai.OpenAI(api_key=api_key)

        # Build the message: text instruction + one image_url block per image
        content: list[dict] = [
            {
                "type": "text",
                "text": (
                    "The following images are extracted from an Excel file used for a BIA "
                    "(Business Impact Analysis) project managed by Devoteam (a consulting firm).\n\n"
                    "Please analyse every image and:\n"
                    "1. Identify the CLIENT company logo — this is NOT Devoteam's logo.\n"
                    "2. Extract the exact client company name as it appears in the logo.\n\n"
                    "Respond ONLY with a valid JSON object, no markdown, no explanation:\n"
                    '{"client_name": "<name>", "logo_index": <0-based index or null>}'
                ),
            }
        ]

        for fname, data in images:
            ext = fname.rsplit(".", 1)[-1].lower()
            mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
            b64 = base64.b64encode(data).decode()
            content.append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{mime};base64,{b64}",
                        "detail": "low",
                    },
                }
            )

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": content}],
            max_tokens=150,
        )

        raw = response.choices[0].message.content.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("` \n")
        result = json.loads(raw)

        client_name = str(result.get("client_name") or "Client").strip()
        logo_index  = result.get("logo_index")

        logo_bytes: Optional[bytes] = None
        if logo_index is not None and 0 <= int(logo_index) < len(images):
            logo_bytes = images[int(logo_index)][1]
        else:
            logo_bytes = fallback_logo

        return client_name, logo_bytes

    except Exception as exc:
        # Non-fatal — return a safe default so generation still runs
        print(f"[fiche_generator] OpenAI detection failed: {exc}")
        return "Client", fallback_logo


# ---------------------------------------------------------------------------
# Parse the recensement sheet into a list of structures
# ---------------------------------------------------------------------------

# Keywords that identify each conceptual column type in the header row.
# Order matters within each list — more specific keywords first.
# n1 = broadest org level, n3 = most specific.
_HDR_KEYWORDS: dict[str, list[str]] = {
    "n1":   ["niveau 1", "niveau1", "pôle", "pole", "division"],
    "n2":   ["niveau 2", "niveau2", "unité", "unite"],
    "n3":   ["niveau 3", "niveau3", "département", "departement"],
    "vav":  ["vis-à-vis", "vis a vis", "contact", "responsable"],
    "date": ["date"],
}

# Cell values treated as "empty" (no entity)
_EMPTY_MARKERS = {"-", "–", "—", "", "n/a", "na"}


def _is_empty_marker(val: str) -> bool:
    return val.strip().lower() in _EMPTY_MARKERS or not val.strip()


def _detect_header_and_cols(ws) -> tuple[int, dict[str, int]]:
    """
    Scan the worksheet for a header row and return
    (header_row_index, {col_type: col_index}).
    """
    for row_idx, row in enumerate(ws.iter_rows(max_row=30, values_only=True)):
        vals = [_clean(v) for v in row]
        row_lower = " ".join(vals).lower()
        # Header row: contains at least two org-level keywords
        matches = sum(
            1 for kws in _HDR_KEYWORDS.values()
            for kw in kws
            if kw in row_lower
        )
        if matches < 2:
            continue

        col_map: dict[str, int] = {}
        for col_idx, cell_val in enumerate(vals):
            cv = cell_val.lower()
            for col_type, keywords in _HDR_KEYWORDS.items():
                if col_type not in col_map and any(kw in cv for kw in keywords):
                    col_map[col_type] = col_idx
                    break
        if col_map:
            return row_idx, col_map

    return -1, {}


def parse_structures(xlsx_path: Path) -> list[dict]:
    """
    Parse a recensement Excel file and return one dict per meeting/structure row.

    Handles two common formats:
      • "Niveau 1 / Niveau 2 / Niveau 3" columns (Suivi projet style)
      • "Division / Unité / Département"   columns (Recensement style)

    Each returned dict:
        name       – most specific non-empty level name
        vis_a_vis  – contact person (first line only)
        date       – "DD/MM/YYYY" string
        niveau1    – top-level parent (cascaded)
        niveau2    – mid-level parent (cascaded)
        niveau3    – deepest level (may be empty)
    """
    wb = openpyxl.load_workbook(str(xlsx_path), data_only=True)

    # Prefer a sheet whose name suggests a meeting / planning / tracking sheet
    preferred_keywords = ("réunion", "reunion", "planning", "avancement", "suivi")
    sheet_name = next(
        (s for s in wb.sheetnames
         if any(kw in s.lower() for kw in preferred_keywords)),
        wb.sheetnames[0],
    )
    ws = wb[sheet_name]

    header_row_idx, col_map = _detect_header_and_cols(ws)
    if not col_map:
        return []

    # Determine which columns map to the three hierarchy levels
    # Prefer n3 > n2 > n1 as "name"; fall back gracefully
    level_cols: list[int] = []
    for level in ("n1", "n2", "n3"):
        if level in col_map:
            level_cols.append(col_map[level])

    vav_col  = col_map.get("vav")
    date_col = col_map.get("date")

    structures: list[dict] = []
    cascaded: list[str] = [""] * len(level_cols)  # last seen value per level

    for row_idx, row in enumerate(ws.iter_rows(min_row=header_row_idx + 2, values_only=True)):
        vals = list(row)

        # Extract level values
        level_vals: list[str] = []
        for col_i in level_cols:
            raw = _clean(vals[col_i]) if col_i < len(vals) else ""
            level_vals.append(raw)

        # Update cascaded values (carry forward non-empty cells)
        for i, lv in enumerate(level_vals):
            if lv and not _is_empty_marker(lv):
                cascaded[i] = lv
                # Reset deeper levels when a higher level changes
                for j in range(i + 1, len(cascaded)):
                    cascaded[j] = ""

        # Entity name = deepest non-empty, non-marker level value
        name = ""
        for lv in reversed(level_vals):
            if lv and not _is_empty_marker(lv):
                name = lv
                break
        if not name:
            continue

        # Contact person — take first non-empty line from raw (multi-line) value
        vav = ""
        if vav_col is not None and vav_col < len(vals):
            raw_cell = vals[vav_col]
            if raw_cell is not None:
                first_line = str(raw_cell).split("\n")[0].strip()
                first_line = re.sub(r"\s+", " ", first_line).strip()
                if first_line.lower() not in ("à définir", "a definir", "-", "–", ""):
                    vav = first_line

        # Meeting date
        date_str = ""
        if date_col is not None and date_col < len(vals):
            date_raw = vals[date_col]
            if date_raw:
                if isinstance(date_raw, datetime):
                    date_str = date_raw.strftime("%d/%m/%Y")
                else:
                    date_str = str(date_raw)

        structures.append(
            {
                "name":     name,
                "vis_a_vis": vav,
                "date":     date_str,
                "niveau1":  cascaded[0] if len(cascaded) > 0 else "",
                "niveau2":  cascaded[1] if len(cascaded) > 1 else "",
                "niveau3":  cascaded[2] if len(cascaded) > 2 else "",
            }
        )

    return structures


# ---------------------------------------------------------------------------
# Text replacement helpers
# ---------------------------------------------------------------------------

def _replace_in_para(para, old: str, new: str) -> None:
    """
    Replace *old* with *new* in *para* while preserving run formatting.
    Tries run-by-run first; falls back to collapsing all text into run[0].
    """
    if old not in para.text:
        return
    for run in para.runs:
        if old in run.text:
            run.text = run.text.replace(old, new)
            return
    # Fallback: merge all text into first run
    new_full = para.text.replace(old, new)
    if para.runs:
        para.runs[0].text = new_full
        for run in para.runs[1:]:
            run.text = ""


def _replace_in_cell(cell, old: str, new: str) -> None:
    for para in cell.paragraphs:
        _replace_in_para(para, old, new)


def _replace_in_doc(doc: Document, old: str, new: str) -> None:
    """Replace *old* with *new* in every paragraph of the document body."""
    for para in doc.paragraphs:
        _replace_in_para(para, old, new)


# ---------------------------------------------------------------------------
# Logo replacement in header
# ---------------------------------------------------------------------------

def _replace_header_logo(doc: Document, logo_bytes: bytes) -> None:
    """
    Replaces the client logo (rId1) in each non-linked header.
    The Devoteam logo (rId2) is left untouched.

    Confirmed mapping for the STAR template:
      rId1 → image1.png  = client / STAR logo   ← replace
      rId2 → image2.png  = Devoteam logo         ← keep
    """
    for section in doc.sections:
        if section.header.is_linked_to_previous:
            continue
        part = section.header.part
        if "rId1" not in part.rels:
            continue
        # Overwrite blob in-place (content_type is read-only on ImagePart)
        part.rels["rId1"]._target._blob = logo_bytes


# ---------------------------------------------------------------------------
# Generate one fiche
# ---------------------------------------------------------------------------

def generate_fiche(
    template_path: Path,
    structure: dict,
    logo_bytes: Optional[bytes],
    output_path: Path,
    client_name: str = "Client",
    version: str = "2.0",
) -> None:
    """
    Clone the BIA template, fill entity-specific fields, replace the client
    name and logo, and save to *output_path*.
    Blank sections are intentionally left blank.
    """
    doc = Document(str(template_path))

    name     = structure["name"]
    vis_a_vis = structure["vis_a_vis"]
    date_str = structure["date"]
    doc_ref  = f"{client_name} - MCO - BIA - {name} - V{version}"

    # ------------------------------------------------------------------
    # 1. Replace hard-coded client name ("STAR Assurances") everywhere
    # ------------------------------------------------------------------
    _replace_in_doc(doc, "STAR Assurances", client_name)
    _replace_in_doc(doc, "STAR",            client_name)  # catch remaining

    # ------------------------------------------------------------------
    # 2. Cover page paragraph: "Entité : xx"
    # ------------------------------------------------------------------
    for para in doc.paragraphs:
        if "Entité" in para.text and "xx" in para.text:
            _replace_in_para(para, "xx", name)
            break

    # ------------------------------------------------------------------
    # 3. Identity table (Table index 2)
    # ------------------------------------------------------------------
    try:
        t = doc.tables[2]

        # Row 0 — entity name
        _replace_in_cell(t.cell(0, 1), "xx", name)
        _replace_in_cell(t.cell(0, 2), "xx", name)

        # Row 9 — version
        _replace_in_cell(t.cell(9, 1), "2.0", version)
        _replace_in_cell(t.cell(9, 2), "2.0", version)

        # Row 10 — date of last update
        if date_str:
            _replace_in_cell(t.cell(10, 1), "xx", date_str)
            _replace_in_cell(t.cell(10, 2), "xx", date_str)

        # Row 11 — document reference
        _replace_in_cell(t.cell(11, 1), "xx", doc_ref)
        _replace_in_cell(t.cell(11, 2), "xx", doc_ref)

        # Row 2 — first attendee (the client vis-à-vis)
        valid_vav = vis_a_vis and vis_a_vis.lower() not in ("à définir", "a definir", "")
        if valid_vav:
            _replace_in_cell(t.cell(2, 1), "Mr Lazher HEDFI", vis_a_vis)

    except Exception:
        pass

    # ------------------------------------------------------------------
    # 4. "Tableau 6 : xx" caption
    # ------------------------------------------------------------------
    for para in doc.paragraphs:
        if "Tableau 6" in para.text and "xx" in para.text:
            _replace_in_para(para, "xx", name)

    # ------------------------------------------------------------------
    # 5. Replace client logo in every non-linked header
    # ------------------------------------------------------------------
    if logo_bytes:
        _replace_header_logo(doc, logo_bytes)

    doc.save(str(output_path))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_all_fiches(
    xlsx_path: Path,
    template_path: Path,
    output_dir: Path,
    version: str = "2.0",
    openai_api_key: Optional[str] = None,
    client_name: Optional[str] = None,
) -> tuple[list[Path], list[str]]:
    """
    Generate one BIA fiche per structure found in *xlsx_path*.

    Args:
        xlsx_path       – recensement Excel file
        template_path   – blank BIA fiche template (.docx)
        output_dir      – folder to write generated fiches into
        version         – "1.0" or "2.0"
        openai_api_key  – GPT-4o key for logo/client detection; falls back to
                          the OPENAI_API_KEY env var if not provided
        client_name     – if provided, used directly and AI name detection is
                          skipped (logo detection still runs when a key is available)

    Returns:
        (generated_paths, error_strings)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve API key
    api_key = openai_api_key or os.environ.get("OPENAI_API_KEY", "")

    if client_name:
        # Name supplied by user — only detect the logo via AI
        if api_key:
            _, logo_bytes = detect_client_info(xlsx_path, api_key)
        else:
            images = _extract_all_images(xlsx_path)
            logo_bytes = max(images, key=lambda x: len(x[1]))[1] if images else None
    else:
        # No name supplied — use AI to detect both name and logo
        if api_key:
            client_name, logo_bytes = detect_client_info(xlsx_path, api_key)
        else:
            images = _extract_all_images(xlsx_path)
            logo_bytes = max(images, key=lambda x: len(x[1]))[1] if images else None
            client_name = "Client"

    structures = parse_structures(xlsx_path)

    generated: list[Path] = []
    errors: list[str] = []

    for s in structures:
        safe_name = _safe_filename(s["name"])
        safe_client = _safe_filename(client_name)
        out_path = output_dir / f"{safe_client} - MCO - BIA - {safe_name} - V{version}.docx"
        try:
            generate_fiche(
                template_path, s, logo_bytes, out_path,
                client_name=client_name,
                version=version,
            )
            generated.append(out_path)
        except Exception as exc:
            errors.append(f"{s['name']}: {exc}")

    return generated, errors
