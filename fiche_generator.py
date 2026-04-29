"""
fiche_generator.py — Generates one BIA fiche per department/structure
from a 'fiche de recensement des structures' (Suivi projet.xlsx) and a
BIA fiche template (.docx).

Entry point:
    generate_all_fiches(xlsx_path, template_path, output_dir) -> list[Path]
"""

from __future__ import annotations

import io
import re
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
    """Strip characters that are illegal in Windows file names."""
    return re.sub(r'[\\/:*?"<>|]', "-", name).strip(" .")


# ---------------------------------------------------------------------------
# Extract client logo from the recensement Excel
# ---------------------------------------------------------------------------

def extract_client_logo(xlsx_path: Path) -> Optional[bytes]:
    """
    Returns the raw PNG/JPG bytes of the client logo embedded in the
    'Planning des réunions' sheet (largest image anchored at column 0),
    or None if no image is found.
    """
    wb = openpyxl.load_workbook(str(xlsx_path))
    sheet_name = next(
        (s for s in wb.sheetnames
         if "réunion" in s.lower() or "reunion" in s.lower()),
        wb.sheetnames[0],
    )
    ws = wb[sheet_name]
    if not hasattr(ws, "_images") or not ws._images:
        return None

    best_data: Optional[bytes] = None
    best_size = 0
    for img in ws._images:
        try:
            anchor = img.anchor
            col = getattr(getattr(anchor, "_from", None), "col", 99)
            ref = img.ref
            if hasattr(ref, "read"):
                data = ref.read()
            elif hasattr(ref, "getvalue"):
                data = ref.getvalue()
            else:
                continue
            if col == 0 and len(data) > best_size:
                best_data = data
                best_size = len(data)
        except Exception:
            pass
    return best_data


# ---------------------------------------------------------------------------
# Parse the recensement sheet into a list of structures
# ---------------------------------------------------------------------------

def parse_structures(xlsx_path: Path) -> list[dict]:
    """
    Reads the 'Planning des réunions' sheet and returns one dict per row
    that represents a concrete structure (department / sub-department).

    Each dict:
        name       – most specific level name (N3 > N2 > N1)
        vis_a_vis  – contact person name
        date       – meeting date as "DD/MM/YYYY" string
        niveau1    – parent Pôle (cascaded)
        niveau2    – parent Département (cascaded)
        niveau3    – sub-department (may be empty)
    """
    wb = openpyxl.load_workbook(str(xlsx_path), data_only=True)
    sheet_name = next(
        (s for s in wb.sheetnames
         if "réunion" in s.lower() or "reunion" in s.lower()),
        wb.sheetnames[0],
    )
    ws = wb[sheet_name]

    COL_N1, COL_N2, COL_N3 = 0, 1, 2
    COL_VAV, COL_DATE = 3, 4

    # Skip rows until we find the header row that starts with "Niveau 1"
    header_found = False
    structures: list[dict] = []
    current_n1 = ""
    current_n2 = ""

    for row in ws.iter_rows(values_only=True):
        vals = list(row)

        if not header_found:
            if vals[0] and "niveau" in str(vals[0]).lower():
                header_found = True
            continue

        n1 = _clean(vals[COL_N1]) if COL_N1 < len(vals) else ""
        n2 = _clean(vals[COL_N2]) if COL_N2 < len(vals) else ""
        n3 = _clean(vals[COL_N3]) if COL_N3 < len(vals) else ""
        vav = _clean(vals[COL_VAV]) if COL_VAV < len(vals) else ""
        date_raw = vals[COL_DATE] if COL_DATE < len(vals) else None

        # Cascade parent names
        if n1:
            current_n1 = n1
        if n2:
            current_n2 = n2

        # The structure name is the most specific non-empty level
        if n3:
            name = n3
        elif n2:
            name = n2
        elif n1:
            name = n1
        else:
            continue  # blank row

        # Format date
        date_str = ""
        if date_raw:
            if isinstance(date_raw, datetime):
                date_str = date_raw.strftime("%d/%m/%Y")
            else:
                date_str = str(date_raw)

        structures.append(
            {
                "name": name,
                "vis_a_vis": vav,
                "date": date_str,
                "niveau1": current_n1,
                "niveau2": current_n2,
                "niveau3": n3,
            }
        )

    return structures


# ---------------------------------------------------------------------------
# Text replacement helpers
# ---------------------------------------------------------------------------

def _replace_in_para(para, old: str, new: str) -> None:
    """
    Replace *old* with *new* in *para* while preserving run formatting.
    Tries run-by-run first; falls back to collapsing all runs into one.
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


# ---------------------------------------------------------------------------
# Logo replacement in header
# ---------------------------------------------------------------------------

def _replace_header_logo(doc: Document, logo_bytes: bytes) -> None:
    """
    Replaces the client/STAR logo (rId1) in each non-linked header with *logo_bytes*.
    The Devoteam logo (rId2) is left untouched.

    Mapping confirmed from the template:
      rId1 → image1.png (8 790 B)  = STAR / client logo  ← replace this
      rId2 → image2.png (11 955 B) = Devoteam logo        ← keep this
    """
    for section in doc.sections:
        if section.header.is_linked_to_previous:
            continue
        hdr = section.header
        part = hdr.part
        if "rId1" not in part.rels:
            continue
        target_part = part.rels["rId1"]._target
        # Overwrite the embedded image bytes in-place.
        # content_type is read-only on ImagePart, so only replace the blob.
        target_part._blob = logo_bytes


# ---------------------------------------------------------------------------
# Generate one fiche
# ---------------------------------------------------------------------------

def generate_fiche(
    template_path: Path,
    structure: dict,
    logo_bytes: Optional[bytes],
    output_path: Path,
) -> None:
    """
    Clone the BIA template, fill in the entity-specific fields, replace the
    client logo, and save to *output_path*.  All template sections that are
    intentionally left blank remain blank.
    """
    doc = Document(str(template_path))

    name = structure["name"]
    vis_a_vis = structure["vis_a_vis"]
    date_str = structure["date"]

    # ------------------------------------------------------------------
    # 1. Cover page paragraph: "Entité : xx"
    # ------------------------------------------------------------------
    for para in doc.paragraphs:
        if "Entité" in para.text and "xx" in para.text:
            _replace_in_para(para, "xx", name)
            break  # only the first occurrence

    # ------------------------------------------------------------------
    # 2. Identity table (Table index 2 in the template)
    #    Row 0  : Entité | xx | xx
    #    Row 10 : Dernière mise à jour | xx | xx
    #    Row 11 : Référence du document | xx | xx
    #    Rows 2-7: Présents (attendees) — keep existing Devoteam names,
    #              replace only "Mr Lazher HEDFI" placeholder with vis-à-vis
    # ------------------------------------------------------------------
    try:
        t = doc.tables[2]

        # Entity name
        _replace_in_cell(t.cell(0, 1), "xx", name)
        _replace_in_cell(t.cell(0, 2), "xx", name)

        # Date
        if date_str:
            _replace_in_cell(t.cell(10, 1), "xx", date_str)
            _replace_in_cell(t.cell(10, 2), "xx", date_str)

        # Document reference
        ref = f"STAR - MCO - BIA - {name} - V2.0"
        _replace_in_cell(t.cell(11, 1), "xx", ref)
        _replace_in_cell(t.cell(11, 2), "xx", ref)

        # Vis-à-vis attendee: replace the first attendee cell
        valid_vav = (
            vis_a_vis
            and vis_a_vis.lower() not in ("à définir", "a definir", "")
        )
        if valid_vav:
            _replace_in_cell(t.cell(2, 1), "Mr Lazher HEDFI", vis_a_vis)

    except Exception:
        pass  # table index mismatch — skip gracefully

    # ------------------------------------------------------------------
    # 3. "Tableau 6 : xx" caption (Évaluation des impacts section)
    # ------------------------------------------------------------------
    for para in doc.paragraphs:
        if "Tableau 6" in para.text and "xx" in para.text:
            _replace_in_para(para, "xx", name)

    # ------------------------------------------------------------------
    # 4. Replace client logo in every non-linked header
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
) -> tuple[list[Path], list[str]]:
    """
    Generate one BIA fiche per structure found in *xlsx_path*.

    Returns:
        generated – list of successfully created file paths
        errors    – list of human-readable error strings
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    logo_bytes = extract_client_logo(xlsx_path)
    structures = parse_structures(xlsx_path)

    generated: list[Path] = []
    errors: list[str] = []

    for s in structures:
        safe_name = _safe_filename(s["name"])
        out_path = output_dir / f"STAR - MCO - BIA - {safe_name} - V2.0.docx"
        try:
            generate_fiche(template_path, s, logo_bytes, out_path)
            generated.append(out_path)
        except Exception as exc:
            errors.append(f"{s['name']}: {exc}")

    return generated, errors
