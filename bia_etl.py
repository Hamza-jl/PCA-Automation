#!/usr/bin/env python3
"""
BIA ETL Pipeline
================
Extracts structured data from BIA fiche (.docx) files and fills the
Synthèse BIA (.xlsx) template.

WHY NO LLM:
  - All source data lives in Word tables, not free text.
  - Mapping fiche-table → synthesis-sheet is deterministic.
  - Validations are finite lookup dictionaries.
  - LLMs would add hallucination risk, cost, and non-reproducibility
    for a task that is 100% structural/formulaic.

WHAT IS "AI" HERE:
  - rapidfuzz: fuzzy string matching for department name lookup.
    The synthesis template has typos ("ingenieurue") vs the fiche's
    correct spelling. A simple Levenshtein ratio resolves this safely.

USAGE:
  # Single fiche:
  python bia_etl.py --fiche path/to/fiche.docx --synthese path/to/synthese.xlsx

  # Folder of fiches:
  python bia_etl.py --fiches-dir path/to/fiches/ --synthese path/to/synthese.xlsx

  # Dry-run (extract + print, no write):
  python bia_etl.py --fiche path/to/fiche.docx --synthese path/to/synthese.xlsx --dry-run
"""

import argparse
import copy
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from docx import Document
from docx.oxml.ns import qn as _qn
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from rapidfuzz import fuzz, process


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES  (the "schema" of a BIA fiche)
# ─────────────────────────────────────────────────────────────────────────────
# These are plain dataclasses — no ORM, no DB, no serialisation overhead.
# They represent exactly what can be extracted from a fiche word-for-word.

@dataclass
class Activity:
    name: str
    resources: str
    critical_period: str
    criticality: str
    volume: str = ""

@dataclass
class ImpactRow:
    """One activity's impact scores across all dimensions and time scenarios.

    Two fiche formats are supported:
      2-col (STAR): a=scénario A (<1J),  b=scénario B (≥5J)
      4-col (GAT):  a=1H, b=4H, c=1J, d=2-3J  (direct mapping to synthesis columns)
    is_4col=True signals that c/d are populated and should be used directly.
    """
    activity_name: str
    im_a: str = ""; im_b: str = ""   # Image de marque
    di_a: str = ""; di_b: str = ""   # Désorganisation interne
    jr_a: str = ""; jr_b: str = ""   # Juridique / Réglementaire
    fin_a: str = ""; fin_b: str = "" # Financier
    # Extra columns for 4-col (GAT) format
    im_c: str = ""; im_d: str = ""
    di_c: str = ""; di_d: str = ""
    jr_c: str = ""; jr_d: str = ""
    fin_c: str = ""; fin_d: str = ""
    dmia_expressed: str = ""
    is_4col: bool = False
    # Raw column headers from §5.2 (e.g. ["A\nInterruption < 1 jour", "B\nInterruption ≥ 5 jours"])
    # Used to map fiche scenarios to the correct Excel time columns.
    scenario_headers: list = field(default_factory=list)

@dataclass
class Exchange:
    correspondent: str
    ie_type: str        # "I" = interne, "E" = externe
    info_type: str
    criticality: str = ""   # A / B / C / D  (may be absent in some fiche formats)
    tr_type: str = ""       # "T" = transmis, "R" = reçu
    si_resources: str = ""

@dataclass
class RampUpRow:
    """One row of the montée en charge table (Effectif, Positions, Télétravail…)."""
    label: str
    nominal: str
    h0: str = ""; h1: str = ""; h2: str = ""; h4: str = ""
    j1: str = ""; j2: str = ""; j3: str = ""; j4: str = ""; j5: str = ""
    j10: str = ""; j15: str = ""; j30: str = ""
    comments: str = ""
    # When Commentaires has colored text (one color = one structure), stores:
    # {structure_name: {time_col_label: value}}  e.g. {"Gestion des AO": {"H+4": "+2"}}
    per_structure: dict = field(default_factory=dict)

@dataclass
class KeyPerson:
    function: str
    last_name: str
    first_name: str
    position: str = ""
    seniority: str = ""
    replacements: str = ""

@dataclass
class ITApplication:
    name: str
    criticality: str
    dmia: str
    pmdt: str
    workaround: str = ""
    comments: str = ""

@dataclass
class OtherEquipment:
    designation: str
    h0: str = ""; h1: str = ""; h2: str = ""; h4: str = ""
    j1: str = ""; j2: str = ""; j3: str = ""; j4: str = ""; j5: str = ""
    j10: str = ""; j15: str = ""
    comments: str = ""

@dataclass
class CriticalDoc:
    name: str
    storage_type: str
    duplication: str               # "O" or "N" (just the flag)
    duplication_method: str = ""   # where it's duplicated (e.g. "AVERROES")

@dataclass
class BIAFiche:
    """All structured data extracted from one BIA fiche word document."""
    entity_name: str
    division: str = ""
    unite: str = ""
    department: str = ""
    activities: list[Activity] = field(default_factory=list)
    impact_rows: list[ImpactRow] = field(default_factory=list)
    exchanges: list[Exchange] = field(default_factory=list)
    ramp_up: list[RampUpRow] = field(default_factory=list)
    key_people: list[KeyPerson] = field(default_factory=list)
    it_applications: list[ITApplication] = field(default_factory=list)
    other_equipment: list[OtherEquipment] = field(default_factory=list)
    critical_docs: list[CriticalDoc] = field(default_factory=list)
    # Weights read from §5.1 Matrice d'impact (overrides hardcoded defaults)
    impact_weights: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# IMPACT SCORE CALCULATION
# ─────────────────────────────────────────────────────────────────────────────
# Weights are hardcoded from the impact scale table in the fiche:
#   Image de marque = 4, Désorganisation interne = 1, Réglementaire = 2, Financier = 3
# Score = sum(impact_level × weight) for all 4 dimensions.
# The Echelle d'impact sheet then maps the score to Faible/Significatif/Majeur/Catastrophique.

IMPACT_WEIGHTS = {"im": 4, "di": 1, "jr": 2, "fin": 3}

# Excel time horizons in hours (used for scenario mapping)
_EXCEL_COL_HOURS = {"1H": 1.0, "4H": 4.0, "1J": 24.0, "2-3J": 60.0}


def _parse_scenario_hours(header_text: str) -> Optional[float]:
    """
    Parse a fiche §5.2 column header and return the threshold in hours.

    Examples:
      "A\nInterruption < 1 jour"  → 24.0   (upper bound of scenario A)
      "B\nInterruption ≥ 5 jours" → 120.0  (lower bound of scenario B)
      "1H"  → 1.0
      "4H"  → 4.0
      "1J"  → 24.0
      "2-3J"→ 60.0

    Returns the threshold in hours, or None if unparseable.
    """
    h = _strip_accents(header_text.lower().replace("\n", " ").replace(",", "."))
    # Direct Excel column name
    direct = {"1h": 1.0, "4h": 4.0, "1j": 24.0, "2-3j": 60.0, "2j": 48.0, "3j": 72.0}
    for k, v in direct.items():
        if h.strip() == k:
            return v
    # Pattern: "< N jour(s)" / ">= N jours" / "≥ N jours"
    m = re.search(r"(\d+(?:\.\d+)?)\s*(heure|h\b|jour|j\b|semaine)", h)
    if not m:
        return None
    n, unit = float(m.group(1)), m.group(2)
    if "heure" in unit or unit == "h":
        return n
    if "jour" in unit or unit == "j":
        return n * 24.0
    if "semaine" in unit:
        return n * 168.0
    return None


def _map_scenarios_to_excel(scenario_headers: list) -> dict:
    """
    Given the raw column headers from §5.2, return a mapping
    {excel_col_name: scenario_index (0-based)} for 1H, 4H, 1J, 2-3J.

    Logic:
      - Parse the time boundary from each scenario header.
      - For each Excel time horizon, find the scenario whose range it falls into.
      - If no scenario's condition matches exactly (gap between scenarios),
        use the immediately following scenario (conservative: longer outage).

    Example for STAR ["A Interruption < 1 jour", "B Interruption >= 5 jours"]:
      scenario 0 covers < 24h → 1H (1h), 4H (4h)
      scenario 1 covers >= 120h → used for 1J (24h) and 2-3J (60h) since they
        don't satisfy < 24h and there's no other scenario.
    """
    if not scenario_headers:
        # Fallback: 2-col default (A→1H/4H, B→1J/2-3J)
        return {"1H": 0, "4H": 0, "1J": 1, "2-3J": 1}

    n = len(scenario_headers)
    if n >= 4:
        # 4-col format (GAT): direct 1:1 mapping
        order = ["1H", "4H", "1J", "2-3J"]
        return {col: i for i, col in enumerate(order[:n])}

    # Parse threshold from each header
    thresholds = []
    for hdr in scenario_headers:
        # Detect operator: < means "upper bound exclusive", >= / ≥ means "lower bound"
        h_low = hdr.lower().replace("≥", ">=").replace("≤", "<=")
        hours = _parse_scenario_hours(hdr)
        if hours is None:
            thresholds.append(None)
            continue
        op = "<" if "<" in h_low and ">=" not in h_low else ">="
        thresholds.append((op, hours))

    mapping: dict[str, int | None] = {}
    excel_cols = ["1H", "4H", "1J", "2-3J"]

    for col in excel_cols:
        col_hours = _EXCEL_COL_HOURS[col]
        assigned = None   # None = no scenario covers this time → leave empty
        for i, thresh in enumerate(thresholds):
            if thresh is None:
                continue
            op, h = thresh
            if op == "<" and col_hours < h:
                assigned = i
                break
            elif op == ">=" and col_hours >= h:
                assigned = i
                break
        mapping[col] = assigned

    return mapping


def _safe_int(v: str) -> Optional[int]:
    """Convert impact cell value to int, returning None for NA/empty/non-numeric."""
    if not v or v.strip().upper() in ("NA", "-", ""):
        return None
    try:
        return int(v.strip())
    except ValueError:
        return None

def compute_score(im: str, di: str, jr: str, fin: str,
                  weights: Optional[dict] = None) -> str:
    """
    Compute weighted impact score.
    NA / empty values are treated as 0 (not blocking).
    Returns empty string only if ALL dimensions are missing/empty.
    """
    w = weights or IMPACT_WEIGHTS
    vals = [_safe_int(v) for v in (im, di, jr, fin)]
    # If every single dimension is None (all missing/NA), nothing to compute
    if all(v is None for v in vals):
        return ""
    # Treat None (NA/missing) as 0
    ints = [v if v is not None else 0 for v in vals]
    return str(
        ints[0] * w.get("im", 4) +
        ints[1] * w.get("di", 1) +
        ints[2] * w.get("jr", 2) +
        ints[3] * w.get("fin", 3)
    )


def _clean_scenario_label(header_text: str) -> str:
    """
    Extract a short human-readable label from a §5.2 column header.
    "A\\nInterruption < 1 jour"  → "< 1 jour"
    "B\\nInterruption ≥ 5 jours" → "≥ 5 jours"
    "1H"                          → "1H"
    """
    h = header_text.strip()
    # Strip leading letter + newline: "A\n...", "B\n..."
    h = re.sub(r'^[A-Za-z]\s*\n\s*', '', h)
    # Strip "Interruption" prefix (case-insensitive)
    h = re.sub(r'^Interruption\s*', '', h, flags=re.IGNORECASE).strip()
    return h


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT — DOCX PARSER
# ─────────────────────────────────────────────────────────────────────────────
# WHY semantic detection instead of fixed indices:
#   Different fiches have different numbers of activities → different numbers
#   of impact matrix tables → table indices shift. Using column headers as
#   fingerprints makes the parser robust to any fiche in this family.

def _cell(table, row_i: int, col_i: int) -> str:
    """Safe cell accessor — returns empty string on index error."""
    try:
        return table.rows[row_i].cells[col_i].text.strip()
    except IndexError:
        return ""

def _row_texts(row) -> list[str]:
    return [c.text.strip() for c in row.cells]


def _is_subheader_row(cells: list[str]) -> bool:
    """
    Detect a category/subheader row inside a table.

    In some fiches (e.g. GAT Direction Commerciale "Animation du réseau",
    "Marketing", "Développement / Formation"), one row groups the following
    rows under a sub-theme.  Word renders this by horizontally merging all
    cells of that row, but python-docx then returns the same text in every
    cell, e.g.:
        ['Marketing', 'Marketing', 'Marketing', 'Marketing', 'Marketing']

    These rows are NOT data rows and must be filtered out so they don't pollute
    the synthèse.  Heuristic: a row whose non-empty cells are all identical
    AND the row has at least 2 cells (so we don't reject single-column tables).
    """
    if not cells or len(cells) < 2:
        return False
    non_empty = [c for c in cells if c]
    if len(non_empty) < 2:
        return False
    first = non_empty[0]
    # All non-empty cells must be the exact same string
    return all(c == first for c in non_empty)


def _split_duplication(raw: str) -> tuple[str, str]:
    """
    Split a fiche duplication cell into (flag, method).

    Input examples (left column is GAT fiche, right column is GT synthèse output):
        "O (AVERROES)"        → ("O", "AVERROES")
        "Oui (Sur One Drive)" → ("O", "Sur One Drive")
        "O"                   → ("O", "")
        "N"                   → ("N", "")
        "Pas la totalité"     → ("N", "Pas la totalité")
        ""                    → ("", "")
    """
    if not raw:
        return ("", "")
    s = raw.strip()
    low = s.lower()

    # Pattern: "<flag> (<method>)" or "<flag>\n<method>"
    m = re.match(r"^\s*(o(?:ui)?|n(?:on)?)\s*[\(\n/]?\s*(.*?)\s*\)?\s*$",
                 s, re.IGNORECASE | re.DOTALL)
    if m:
        flag_raw, method = m.group(1), m.group(2).strip()
        flag = "O" if flag_raw.lower().startswith("o") else "N"
        # Strip stray closing paren or leading slash
        method = method.strip(" \t/").rstrip(")")
        return (flag, method)

    # Free-form text that doesn't start with O/N
    if "oui" in low or low.startswith("o "):
        return ("O", s)
    if "non" in low or low.startswith("n ") or "pas" in low:
        return ("N", s)
    # Unknown → keep raw in method, leave flag empty
    return ("", s)

def _table_fingerprint(table) -> str:
    """First non-empty cell in row 0 — used as a stable table identifier."""
    for row in table.rows:
        for cell in row.cells:
            text = cell.text.strip()
            if text:
                return text
    return ""

def _is_activity_list_table(table) -> bool:
    if len(table.columns) < 4:
        return False
    headers = _row_texts(table.rows[0])
    return any("Activit" in h for h in headers) and any("Ressource" in h for h in headers)

def _is_impact_matrix_table(table) -> bool:
    """Impact per activity: 3 cols, header row has 'Interruption' in col 1 or 2."""
    if len(table.rows) < 4:
        return False
    headers = _row_texts(table.rows[0])
    return len(headers) >= 3 and "Interruption" in " ".join(headers)

def _is_dmia_table(table) -> bool:
    headers = _row_texts(table.rows[0])
    # Bug fix: GAT uses "Désignation de l'activité" instead of "Processus"
    return any("DMIA" in h for h in headers) and any(
        "Processus" in h or "Process" in h or "activit" in h.lower() or "Désignation" in h
        for h in headers
    )

def _is_exchange_table(table) -> bool:
    headers = _row_texts(table.rows[0])
    return any("Groupes fonctionnels" in h or "Correspondants" in h for h in headers)

def _is_ramp_up_table(table) -> bool:
    if len(table.rows) < 2:
        return False
    headers = _row_texts(table.rows[0])
    return any("Montée en charge" in h for h in headers) and any("Nominal" in h for h in headers)

def _is_key_people_table(table) -> bool:
    headers = _row_texts(table.rows[0])
    return any("Fonction" in h for h in headers) and any("Nom" in h for h in headers) and any("Prénom" in h or "Prenom" in h for h in headers)

def _is_app_table(table) -> bool:
    headers = _row_texts(table.rows[0])
    return any("Application" in h for h in headers) and any("DMIA" in h for h in headers) and any("PMDT" in h for h in headers)

def _is_other_eqt_table(table) -> bool:
    if len(table.rows) < 2:
        return False
    headers = _row_texts(table.rows[0])
    return any("Désignation" in h or "Designation" in h for h in headers) and any("H+" in h or "J+" in h for h in headers)

def _is_doc_table(table) -> bool:
    headers = _row_texts(table.rows[0])
    return any("Documents" in h or "Fichiers" in h for h in headers) and any("stockage" in h.lower() for h in headers)

def _is_identification_table(table) -> bool:
    if len(table.rows) < 2:
        return False
    # Bug fix: GAT first cell = "Date de l'entretien", not "Entité".
    # Scan ALL cells (not just first) so both STAR and GAT formats match.
    for row in table.rows:
        for cell in row.cells:
            text = cell.text.strip()
            if "Entit" in text or "Présents" in text:
                return True
    return False

def extract(docx_path: str | Path, llm_model: str | None = None, verbose: bool = False) -> BIAFiche:
    """
    EXTRACT phase: reads a .docx fiche and returns a BIAFiche dataclass.

    Table detection uses column header fingerprints, not positional indices,
    so it works regardless of how many activities (and thus impact tables) the
    fiche contains.

    llm_model: if provided and Ollama is running with that model, tables that
               the rule-based parser cannot classify are sent to the LLM as a
               fallback.  When llm_model is None (default) or Ollama is not
               reachable, behaviour is identical to the original implementation.
    """
    doc = Document(str(docx_path))
    fiche = BIAFiche(entity_name="")

    # Collect impact matrix tables separately (one per activity, identified together)
    raw_impact_tables = []
    dmia_map: dict[str, str] = {}  # activity_name → dmia_expressed

    # ── §5.1 weight table detection ───────────────────────────────────────────
    # Pre-scan for the Matrice d'impact (§5.1) weight table before the main loop
    # so weights are available when computing scores.
    _dim_kw_map = {
        "im":  ["image", "marque"],
        "di":  ["desorganisat", "desorg", "interne"],
        "jr":  ["reglementaire", "regl", "juridique"],
        "fin": ["financ"],
    }
    for _t in doc.tables:
        if not _t.rows or len(_t.rows) < 2:
            continue
        _h0 = " ".join(_row_texts(_t.rows[0])).lower()
        if "poids" not in _h0:
            continue
        # This is the weight table — read poids per dimension
        _hdr = _row_texts(_t.rows[0])
        _poids_col = next((i for i, h in enumerate(_hdr) if "poids" in h.lower()), None)
        if _poids_col is None:
            continue
        for _row in _t.rows[1:]:
            _cells = _row_texts(_row)
            if not _cells:
                continue
            _dim_raw = _strip_accents(_cells[0].lower())
            _w_raw   = _cells[_poids_col].strip() if _poids_col < len(_cells) else ""
            try:
                _w = int(_w_raw)
            except (ValueError, TypeError):
                continue
            for _key, _kws in _dim_kw_map.items():
                if any(kw in _dim_raw for kw in _kws):
                    fiche.impact_weights[_key] = _w
                    break
        break  # only one weight table per fiche

    for table in doc.tables:
        if _is_identification_table(table) and not fiche.entity_name:
            # Bug fix: GAT has "Entité" label at row 1 col 0 (not row 0 col 0).
            # Scan all rows: find the cell that contains "Entit" and read the adjacent value cell.
            for row in table.rows:
                cells = _row_texts(row)
                for i, c in enumerate(cells):
                    if "Entit" in c and i + 1 < len(cells) and cells[i + 1]:
                        fiche.entity_name = cells[i + 1]
                        break
                if fiche.entity_name:
                    break
            # Fallback: original positional read (STAR format)
            if not fiche.entity_name:
                fiche.entity_name = _cell(table, 0, 1) or _cell(table, 0, 2)

        elif _is_activity_list_table(table):
            for row in table.rows[1:]:
                cells = _row_texts(row)
                if len(cells) >= 4 and cells[0] and not _is_subheader_row(cells):
                    fiche.activities.append(Activity(
                        name=cells[0],
                        resources=cells[1],
                        critical_period=cells[2],
                        criticality=cells[3],
                        volume=cells[4] if len(cells) > 4 else "",
                    ))

        elif _is_impact_matrix_table(table):
            raw_impact_tables.append(table)

        elif _is_dmia_table(table):
            for row in table.rows[1:]:
                cells = _row_texts(row)
                if len(cells) >= 2 and cells[0]:
                    # Store with both original and lowercased key so lookup is
                    # case-insensitive (e.g. "Affaires" vs "affaires" mismatch).
                    key = cells[0].strip()
                    dmia_map[key] = cells[1].strip()
                    dmia_map[key.lower()] = cells[1].strip()

        elif _is_exchange_table(table):
            headers = _row_texts(table.rows[0])
            header_text = " ".join(headers).lower()

            # ── Locate each logical column by header content, not position ────
            # This handles every client format regardless of column order or
            # whether optional columns (I/E, Criticité, Typologie) are present.
            def _find_col(tests):
                """Return index of first header matching any test, else None."""
                for i, h in enumerate(headers):
                    hn = h.strip().lower()
                    if any(t(hn, h) for t in tests):
                        return i
                return None

            ci_correspondent = _find_col([
                lambda n, h: "groupes" in n or "correspondant" in n,
            ]) or 0

            ci_ie = _find_col([
                lambda n, h: h.strip() in ("I/E", "I", "E") or
                             ("interne" in n and "externe" in n),
            ])
            ci_info = _find_col([
                lambda n, h: "type" in n and "inform" in n,
            ])
            ci_crit = _find_col([
                # In externes tables this column carries the Typologie label
                # (Mono / Multi). We capture it in the same field; the transform
                # routes it to either "Niveau de criticité" or "Typologie" based
                # on the table's I/E flag.
                lambda n, h: "critici" in n or "typolog" in n,
            ])
            ci_tr = _find_col([
                lambda n, h: (
                    h.strip() in ("T / R", "T/R") or
                    (set(h.strip().upper().replace(" ", "")) == {"T", "/", "R"}) or
                    # Handles "(T/R)\nTransmis/ Reçu" style headers
                    "t/r" in n or "t / r" in n or
                    ("transmis" in n and "recu" in _strip_accents(n))
                ),
            ])
            ci_resources = _find_col([
                lambda n, h: "ressource" in n,
            ])

            # Infer I/E from table-level header text when no explicit column
            if "externe" in header_text:
                default_ie = "E"
            elif "interne" in header_text:
                default_ie = "I"
            else:
                default_ie = "I"

            for row in table.rows[1:]:
                cells = _row_texts(row)
                if not cells or not cells[0]:
                    continue
                # Skip horizontally-merged category rows like
                # ['Marketing','Marketing','Marketing','Marketing','Marketing']
                if _is_subheader_row(cells):
                    continue

                def _gcell(idx):
                    return cells[idx] if idx is not None and idx < len(cells) else ""

                ie_val = _gcell(ci_ie).strip().upper() if ci_ie is not None else ""
                # Normalize full words → single letter: "INTERNE"→"I", "EXTERNE"→"E"
                if ie_val.startswith("I"):
                    ie_val = "I"
                elif ie_val.startswith("E"):
                    ie_val = "E"
                fiche.exchanges.append(Exchange(
                    correspondent=_gcell(ci_correspondent),
                    ie_type=ie_val or default_ie,
                    info_type=_gcell(ci_info) if ci_info is not None
                              else (cells[1] if len(cells) > 1 else ""),
                    criticality=_gcell(ci_crit),
                    tr_type=_gcell(ci_tr),
                    si_resources=_gcell(ci_resources),
                ))

        elif _is_ramp_up_table(table):
            # Row 0 = header with time horizons
            # Rows 1+ = Effectif, Positions, Télétravail, ...
            headers = _row_texts(table.rows[0])

            def _col_idx(keyword: str) -> int:
                for i, h in enumerate(headers):
                    if keyword in h:
                        return i
                return -1

            # Detect the Commentaires column index (last non-empty header, or explicit)
            _com_idx = _col_idx("Commentaires")
            if _com_idx < 0:
                _com_idx = len(headers) - 1  # last column fallback

            # Build a header-label → column-index map for time columns
            _TIME_KEYWORDS = ["H0", "H+1", "H+2", "H+4", "J+1", "J+2",
                              "J+3", "J+4", "J+5", "J+10", "J+15", "J+30"]
            _time_col_map: dict[str, int] = {}  # "H+4" → col index
            for kw in _TIME_KEYWORDS:
                idx = _col_idx(kw)
                if idx >= 0:
                    _time_col_map[kw] = idx

            def _extract_color_structures(docx_row) -> dict:
                """
                If the Commentaires cell contains colored runs (one color per
                structure), return {structure_name: {time_col: value}} by
                matching each colored cell value to its structure via run color.

                Handles three cases:
                  - Explicit RGB:  w:val="FF0000"
                  - Theme + RGB:   w:val="0090FF" + w:themeColor="accent1"
                    (python-docx returns type=THEME so .rgb raises; we read
                    w:val directly from XML instead)
                  - Auto/black:    no w:color tag → sentinel "__AUTO__"

                Returns an empty dict if no colored structures are found.
                """
                _AUTO = "__AUTO__"

                def _run_hex(run) -> str:
                    """Return uppercase hex color for a run, or _AUTO if none."""
                    rPr = run._r.find(_qn("w:rPr"))
                    if rPr is None:
                        return _AUTO
                    col_elem = rPr.find(_qn("w:color"))
                    if col_elem is None:
                        return _AUTO
                    val = col_elem.get(_qn("w:val"), "auto")
                    if not val or val.lower() == "auto":
                        return _AUTO
                    return val.upper()

                if _com_idx < 0 or _com_idx >= len(docx_row.cells):
                    return {}

                # 1. Build color → [structure_name] from Commentaires cell.
                # Rules:
                #   - Each non-annotation paragraph registers one structure name.
                #   - Annotation paragraphs (start with "(") are skipped.
                #   - Multiple structures may share the same color (e.g. when the
                #     document was uniformly reformatted).  In that case ALL of them
                #     receive matching time values.
                com_cell = docx_row.cells[_com_idx]
                color_map: dict[str, list[str]] = {}   # hex_color → [struct_name, ...]
                for para in com_cell.paragraphs:
                    runs = [r for r in para.runs if r.text.strip()]
                    if not runs:
                        continue
                    full_text = "".join(r.text for r in runs).strip()
                    if not full_text:
                        continue
                    # Skip parenthetical annotations — they qualify a structure,
                    # they are not structure names themselves.
                    if full_text.startswith("("):
                        continue
                    dom_color = _run_hex(runs[0])
                    color_map.setdefault(dom_color, []).append(full_text)

                # Only trigger expansion if at least one non-auto colored structure exists
                has_colored = any(k != _AUTO for k in color_map)
                if not has_colored:
                    return {}   # plain text comments — no structure expansion

                # Helper: Euclidean RGB distance between two hex color strings.
                def _rgb_dist(h1: str, h2: str) -> float:
                    try:
                        r1, g1, b1 = int(h1[0:2],16), int(h1[2:4],16), int(h1[4:6],16)
                        r2, g2, b2 = int(h2[0:2],16), int(h2[2:4],16), int(h2[4:6],16)
                        return ((r1-r2)**2 + (g1-g2)**2 + (b1-b2)**2) ** 0.5
                    except Exception:
                        return 999.0

                # Nearest-color lookup: map a run color to the closest Commentaires
                # color within a tolerance (handles slight shade variations e.g.
                # EE0000 in Commentaires vs FF0000 in time cells — same red, different
                # brightness, Euclidean distance ≈ 17).
                _COLOR_TOLERANCE = 60   # RGB Euclidean distance threshold
                _color_cache: dict[str, str | None] = {}  # memoize lookups

                def _nearest_color(run_hex: str) -> str | None:
                    """Return the best-matching color key from color_map, or None."""
                    if run_hex in _color_cache:
                        return _color_cache[run_hex]
                    # Exact match first
                    if run_hex in color_map:
                        _color_cache[run_hex] = run_hex
                        return run_hex
                    # Auto sentinel: only matches __AUTO__ key
                    if run_hex == _AUTO:
                        result = _AUTO if _AUTO in color_map else None
                        _color_cache[run_hex] = result
                        return result
                    # Approximate: find nearest non-AUTO color within tolerance
                    best_key, best_dist = None, _COLOR_TOLERANCE
                    for ck in color_map:
                        if ck == _AUTO:
                            continue
                        d = _rgb_dist(run_hex, ck)
                        if d < best_dist:
                            best_key, best_dist = ck, d
                    _color_cache[run_hex] = best_key
                    return best_key

                # 2. For each time column, match colored runs to structures
                per_struct: dict[str, dict[str, str]] = {}
                for time_label, col_idx in _time_col_map.items():
                    if col_idx >= len(docx_row.cells):
                        continue
                    data_cell = docx_row.cells[col_idx]
                    for para in data_cell.paragraphs:
                        for run in para.runs:
                            val = run.text.strip()
                            if not val:
                                continue
                            run_color = _run_hex(run)
                            matched_key = _nearest_color(run_color)
                            if matched_key is not None:
                                for struct_name in color_map[matched_key]:
                                    per_struct.setdefault(struct_name, {})
                                    # Accumulate split runs (e.g. "+" and "2")
                                    existing = per_struct[struct_name].get(time_label, "")
                                    per_struct[struct_name][time_label] = existing + val

                return per_struct

            for row in table.rows[1:]:
                cells = _row_texts(row)
                if not cells or not cells[0] or _is_subheader_row(cells):
                    continue

                def _get(keyword: str) -> str:
                    idx = _col_idx(keyword)
                    return cells[idx] if 0 <= idx < len(cells) else ""

                per_structure = _extract_color_structures(row)

                fiche.ramp_up.append(RampUpRow(
                    label=cells[0],
                    nominal=_get("Nominal"),
                    h0=_get("H0"),
                    h1=_get("H+1"),
                    h2=_get("H+2"),
                    h4=_get("H+4"),
                    j1=_get("J+1"),
                    j2=_get("J+2"),
                    j3=_get("J+3"),
                    j4=_get("J+4"),
                    j5=_get("J+5"),
                    j10=_get("J+10"),
                    j15=_get("J+15"),
                    j30=_get("J+30"),
                    comments=cells[_com_idx] if 0 <= _com_idx < len(cells) else "",
                    per_structure=per_structure,
                ))

        elif _is_key_people_table(table):
            # Use header-driven indexing because column order can vary slightly
            # between client templates (some omit "Poste" or "Ancienneté").
            kp_headers = _row_texts(table.rows[0])

            def _kp_idx(*keywords) -> int:
                for i, h in enumerate(kp_headers):
                    hl = h.lower()
                    if any(k in hl for k in keywords):
                        return i
                return -1

            ci_fct  = _kp_idx("fonction")
            ci_nom  = _kp_idx("nom")
            ci_prn  = _kp_idx("prénom", "prenom")
            ci_post = _kp_idx("poste")
            ci_anc  = _kp_idx("ancienneté", "anciennete")
            ci_supp = _kp_idx("suppléant", "suppleant", "rempla")

            for row in table.rows[1:]:
                cells = _row_texts(row)
                if not cells or _is_subheader_row(cells):
                    continue
                # Need at least last_name OR first_name to be meaningful
                last = cells[ci_nom] if 0 <= ci_nom < len(cells) else ""
                first = cells[ci_prn] if 0 <= ci_prn < len(cells) else ""
                if not (last or first):
                    continue
                fiche.key_people.append(KeyPerson(
                    function=    cells[ci_fct]  if 0 <= ci_fct  < len(cells) else "",
                    last_name=   last,
                    first_name=  first,
                    position=    cells[ci_post] if 0 <= ci_post < len(cells) else "",
                    seniority=   cells[ci_anc]  if 0 <= ci_anc  < len(cells) else "",
                    replacements=cells[ci_supp] if 0 <= ci_supp < len(cells) else "",
                ))

        elif _is_app_table(table):
            # Use header-driven indexing — column count varies (5 vs 6) between
            # client templates: sometimes "Contournement" is the last column,
            # sometimes a "Commentaires" column follows it.
            ap_headers = _row_texts(table.rows[0])

            def _ap_idx(*keywords) -> int:
                for i, h in enumerate(ap_headers):
                    hl = h.lower()
                    if any(k in hl for k in keywords):
                        return i
                return -1

            ci_app  = _ap_idx("application")
            ci_crit = _ap_idx("criticit")
            ci_dmia = _ap_idx("dmia")
            ci_pmdt = _ap_idx("pmdt")
            ci_work = _ap_idx("contournement", "workaround", "palliatif")
            ci_com  = _ap_idx("commentaire")

            for row in table.rows[1:]:
                cells = _row_texts(row)
                if not cells or not cells[0] or _is_subheader_row(cells):
                    continue
                fiche.it_applications.append(ITApplication(
                    name=       cells[ci_app]  if 0 <= ci_app  < len(cells) else cells[0],
                    criticality=cells[ci_crit] if 0 <= ci_crit < len(cells) else "",
                    dmia=       cells[ci_dmia] if 0 <= ci_dmia < len(cells) else "",
                    pmdt=       cells[ci_pmdt] if 0 <= ci_pmdt < len(cells) else "",
                    workaround= cells[ci_work] if 0 <= ci_work < len(cells) else "",
                    comments=   cells[ci_com]  if 0 <= ci_com  < len(cells) else "",
                ))

        elif _is_other_eqt_table(table):
            headers = _row_texts(table.rows[0])

            def _eqt_col(keyword: str) -> int:
                """Exact-token match against headers — '1' must not match 'J+10'."""
                kw = keyword.strip().lower().replace(" ", "")
                for i, h in enumerate(headers):
                    if h.strip().lower().replace(" ", "") == kw:
                        return i
                return -1

            ci_com = -1
            for i, h in enumerate(headers):
                if "commentaire" in h.lower():
                    ci_com = i
                    break

            for row in table.rows[1:]:
                cells = _row_texts(row)
                if not cells or not cells[0] or _is_subheader_row(cells):
                    continue

                def _get_eqt(keyword: str) -> str:
                    i = _eqt_col(keyword)
                    return cells[i] if 0 <= i < len(cells) else ""

                fiche.other_equipment.append(OtherEquipment(
                    designation=cells[0],
                    h0= _get_eqt("H0"),
                    h1= _get_eqt("H+1"),
                    h2= _get_eqt("H+2"),
                    h4= _get_eqt("H+4"),
                    j1= _get_eqt("J+1"),
                    j2= _get_eqt("J+2"),
                    j3= _get_eqt("J+3"),
                    j4= _get_eqt("J+4"),
                    j5= _get_eqt("J+5"),
                    j10=_get_eqt("J+10"),
                    j15=_get_eqt("J+15"),
                    comments=cells[ci_com] if 0 <= ci_com < len(cells) else "",
                ))

        elif _is_doc_table(table):
            for row in table.rows[1:]:
                cells = _row_texts(row)
                if len(cells) < 3 or not cells[0] or _is_subheader_row(cells):
                    continue
                # Filter rows like "Rien à signaler" that say nothing useful
                name = cells[0]
                if name.strip().lower() in ("rien à signaler", "rien a signaler", "ras", "n/a", "na"):
                    continue
                # Some templates have a dedicated "Modalité de duplication" 4th col;
                # most GAT fiches pack flag + method into the 3rd cell.
                if len(cells) >= 4 and cells[3]:
                    dup_flag = cells[2]
                    dup_meth = cells[3]
                else:
                    dup_flag, dup_meth = _split_duplication(cells[2])
                fiche.critical_docs.append(CriticalDoc(
                    name=name,
                    storage_type=cells[1],
                    duplication=dup_flag,
                    duplication_method=dup_meth,
                ))

    # Process impact matrices: each table = one (or more) activity's impact scores.
    # WHY we do this after the main loop: we need dmia_map to be populated first.
    #
    # Some fiches (e.g. STAR) use ONE shared impact table for multiple activities:
    #   "Actuariat / Statistique / Data  |  A (<1J)  |  B (≥5J)"
    # We detect this by checking if the table header cell contains more than one
    # activity name (separated by / , ; or newlines) and replicate the ImpactRow
    # for every matched activity rather than just the first one.

    activity_names_lower = {a.name.lower(): a.name for a in fiche.activities}

    def _resolve_activities_for_table(t_idx: int, t) -> list[str]:
        """
        Return the list of activity names this impact table covers.

        Priority:
          1. Header cell contains slash/comma-separated names that match
             known fiche activities → return all matched names.
          2. Exactly one impact table per activity (index-based) → return
             the activity at t_idx.
          3. Fallback: return the raw header text as a single name.
        """
        header_text = _cell(t, 0, 0).strip()

        # Split on common separators: / , ; and newline
        import re as _re_local
        parts = [p.strip() for p in _re_local.split(r"[/,;\n]+", header_text) if p.strip()]

        if len(parts) > 1:
            # Match each part against known activities (case-insensitive, partial)
            matched = []
            for part in parts:
                part_low = part.lower()
                # Exact match first
                if part_low in activity_names_lower:
                    matched.append(activity_names_lower[part_low])
                    continue
                # Substring match: part is contained in an activity name or vice versa
                for act_low, act_name in activity_names_lower.items():
                    if part_low in act_low or act_low in part_low:
                        if act_name not in matched:
                            matched.append(act_name)
                        break
            if matched:
                return matched

        # Index-based fallback
        if t_idx < len(fiche.activities):
            return [fiche.activities[t_idx].name]

        # Last resort: use header text
        return [header_text] if header_text else []

    for t_idx, t in enumerate(raw_impact_tables):
        target_activities = _resolve_activities_for_table(t_idx, t)

        # Bug fix 5: GAT has 4 data columns (1H, 4H, 1J, 2-3J).
        # Detect format from header: count non-label columns.
        header_row = _row_texts(t.rows[0]) if t.rows else []
        data_col_count = max(0, len(header_row) - 1)  # subtract the label/dimension column
        is_4col = data_col_count >= 4
        # Capture raw scenario column headers (all except the label column)
        scenario_headers = header_row[1:] if len(header_row) > 1 else []

        scores: dict[str, dict[str, str]] = {}
        for row_idx, row in enumerate(t.rows[1:], start=1):
            dim_name = _cell(t, row_idx, 0).lower()
            val_a = _cell(t, row_idx, 1)
            val_b = _cell(t, row_idx, 2)
            val_c = _cell(t, row_idx, 3) if is_4col else ""
            val_d = _cell(t, row_idx, 4) if is_4col else ""
            if "image" in dim_name or "marque" in dim_name:
                scores["im"] = {"a": val_a, "b": val_b, "c": val_c, "d": val_d}
            elif "d" in dim_name and ("sorg" in dim_name or "org" in dim_name):
                scores["di"] = {"a": val_a, "b": val_b, "c": val_c, "d": val_d}
            elif "r" in dim_name and ("gl" in dim_name or "juridique" in dim_name or "regl" in dim_name):
                scores["jr"] = {"a": val_a, "b": val_b, "c": val_c, "d": val_d}
            elif "financ" in dim_name:
                scores["fin"] = {"a": val_a, "b": val_b, "c": val_c, "d": val_d}

        def _s(dim: str, scenario: str) -> str:
            return scores.get(dim, {}).get(scenario, "")

        # Create one ImpactRow per matched activity (handles shared tables)
        for activity_name in target_activities:
            fiche.impact_rows.append(ImpactRow(
                activity_name=activity_name,
                im_a=_s("im","a"), im_b=_s("im","b"), im_c=_s("im","c"), im_d=_s("im","d"),
                di_a=_s("di","a"), di_b=_s("di","b"), di_c=_s("di","c"), di_d=_s("di","d"),
                jr_a=_s("jr","a"), jr_b=_s("jr","b"), jr_c=_s("jr","c"), jr_d=_s("jr","d"),
                fin_a=_s("fin","a"), fin_b=_s("fin","b"), fin_c=_s("fin","c"), fin_d=_s("fin","d"),
                dmia_expressed=dmia_map.get(activity_name, "") or dmia_map.get(activity_name.lower(), ""),
                is_4col=is_4col,
                scenario_headers=scenario_headers,
            ))

    # Resolve Division / Unité / Département from entity name.
    # Convention: the fiche typically describes ONE top-level org unit (a
    # Direction). We store its name in `division` so the LOAD phase can place
    # it under "Structure Niveau 1" / column B when auto-appending. Subordinate
    # levels stay empty (the recensement is the authoritative source for the
    # full hierarchy when it is available).
    fiche.division = fiche.entity_name

    # ── Optional LLM fallback ─────────────────────────────────────────────────
    if llm_model:
        try:
            from llm_fallback import enhance_fiche, is_ollama_available
            if is_ollama_available(llm_model):
                fiche = enhance_fiche(fiche, docx_path, model=llm_model, verbose=verbose)
        except ImportError:
            pass  # llm_fallback not installed — silent no-op

    return fiche


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORM — SHEET DATA BUILDERS
# ─────────────────────────────────────────────────────────────────────────────
# Each function takes a BIAFiche and returns a list of dicts, where each dict
# maps column_name → value. The LOAD phase writes these into the XLSX.

def transform_activites(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Activité": a.name,
            "Ressources Utilisées": a.resources,
            "Période critique": a.critical_period,
            "Niveau de criticité": a.criticality,
            "Volume": a.volume,
            "Commentaires": "",
        }
        for a in fiche.activities
    ]

def transform_impact_dmia(fiche: BIAFiche) -> list[dict]:
    """
    Build Impact DMIA rows with DYNAMIC column names derived from the fiche's
    scenario headers (from §5.2 Evaluation des impacts).

    For a 2-scenario fiche (STAR: "< 1 jour" / ">= 5 jours"):
        IM < 1 jour | IM >= 5 jours | DI < 1 jour | DI >= 5 jours | ...
        Score < 1 jour | Score >= 5 jours

    For a 4-scenario fiche (GAT: "1H" / "4H" / "1J" / "2-3J"):
        IM 1H | IM 4H | IM 1J | IM 2-3J | ... | Score 1H | ... | Score 2-3J

    Any number of scenarios is supported automatically.
    """
    w = fiche.impact_weights if fiche.impact_weights else None

    def _sc(im, di, jr, fin):
        return compute_score(im, di, jr, fin, weights=w)

    rows = []
    for ir in fiche.impact_rows:
        if ir.is_4col:
            labels = ["1H", "4H", "1J", "2-3J"]
            dim_vals = {
                "IM":  [ir.im_a,  ir.im_b,  ir.im_c,  ir.im_d],
                "DI":  [ir.di_a,  ir.di_b,  ir.di_c,  ir.di_d],
                "JR":  [ir.jr_a,  ir.jr_b,  ir.jr_c,  ir.jr_d],
                "FIN": [ir.fin_a, ir.fin_b, ir.fin_c, ir.fin_d],
            }
        else:
            labels = [_clean_scenario_label(h) for h in ir.scenario_headers] if ir.scenario_headers else ["A", "B"]
            dim_vals = {
                "IM":  [ir.im_a,  ir.im_b],
                "DI":  [ir.di_a,  ir.di_b],
                "JR":  [ir.jr_a,  ir.jr_b],
                "FIN": [ir.fin_a, ir.fin_b],
            }

        row: dict = {"Activité": ir.activity_name}

        # One column per dimension per scenario
        for dim, vals in dim_vals.items():
            for i, label in enumerate(labels):
                row[f"{dim} {label}"] = vals[i] if i < len(vals) else ""

        # Score per scenario
        for i, label in enumerate(labels):
            im  = dim_vals["IM"][i]  if i < len(dim_vals["IM"])  else ""
            di  = dim_vals["DI"][i]  if i < len(dim_vals["DI"])  else ""
            jr  = dim_vals["JR"][i]  if i < len(dim_vals["JR"])  else ""
            fin = dim_vals["FIN"][i] if i < len(dim_vals["FIN"]) else ""
            row[f"Score {label}"] = _sc(im, di, jr, fin)

        row["DMIA Exprimée"] = ir.dmia_expressed
        row["DMIA Préconisé"] = ""
        row["Commentaires"] = ""
        rows.append(row)

    return rows


def transform_applications_it(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Application": a.name,
            "Niveau de criticité": a.criticality,
            "DMIA": a.dmia,
            "PMDT": a.pmdt,
            "Contournement envisageable": a.workaround,
            "Commentaires": a.comments,
        }
        for a in fiche.it_applications
    ]

def transform_echanges_internes(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Groupes fonctionnels / Correspondants": e.correspondent,
            "Types d'informations": e.info_type,
            "Niveau de criticité": e.criticality,
            "T / R": e.tr_type,
            "Ressources utilisées": e.si_resources,
            "Commentaires": "",
        }
        for e in fiche.exchanges if e.ie_type.strip().upper() == "I"
    ]

def transform_echanges_externes(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Groupes fonctionnels / Correspondants": e.correspondent,
            "Types d'informations": e.info_type,
            # For externes, the "criticité" column in the docx actually carries
            # the typology (Mono/Multi). Map it to "Typologie".
            "Typologie": e.criticality,
            "T / R": e.tr_type,
            "Ressources utilisées": e.si_resources,
            "Commentaires": "",
        }
        for e in fiche.exchanges if e.ie_type.strip().upper() == "E"
    ]

def transform_montee_en_charge(fiche: BIAFiche) -> list[dict]:
    """
    Build Montée en charge rows.

    Normal case (no colored structures):
        One row per primary metric (Effectif / Positions / Télétravail),
        with "Commentaires" column.

    Colored-structure case (per_structure populated):
        One row per structure per primary metric.
        "Commentaires" column is replaced by "Structure".
        Each row has only the values that belong to that structure.
    """
    # Only keep the three base metrics — exclude cumulated/% variants.
    _PRIMARY_LABELS = {"effectif", "position", "positions", "télétravail", "teletravail"}
    _EXCLUDE_KEYWORDS = {"cumul", "%"}

    def _is_primary(label: str) -> bool:
        norm = _strip_accents(label.strip().lower())
        # Reject anything that contains "cumulé/cumulés" or "%"
        if any(kw in norm for kw in _EXCLUDE_KEYWORDS):
            return False
        return (norm in {_strip_accents(p) for p in _PRIMARY_LABELS})

    rows: list[dict] = []
    for r in fiche.ramp_up:
        if not _is_primary(r.label):
            continue

        if r.per_structure:
            # Group structures that share the same time values (= same color in
            # the Word doc) into a single Excel row.  Structures with identical
            # time-value signatures are joined with a newline in the Structure
            # cell — e.g. "Gestion des affaires fac.\nGestion des différents traités"
            # instead of two separate rows with the same "+3" value.
            from collections import defaultdict as _dd
            _groups: dict[frozenset, list[str]] = _dd(list)
            for struct_name, time_vals in r.per_structure.items():
                key = frozenset(
                    (k, v) for k, v in time_vals.items() if v
                )
                _groups[key].append(struct_name)

            for time_key, struct_names in _groups.items():
                time_vals = dict(time_key)
                rows.append({
                    "Montée en charge exprimée": r.label,
                    "Nominal": r.nominal,
                    "Structure": "\n".join(struct_names),
                    "H0":  time_vals.get("H0",  ""),
                    "H+1": time_vals.get("H+1", ""),
                    "H+2": time_vals.get("H+2", ""),
                    "H+4": time_vals.get("H+4", ""),
                    "J+1": time_vals.get("J+1", ""),
                    "J+2": time_vals.get("J+2", ""),
                    "J+3": time_vals.get("J+3", ""),
                    "J+4": time_vals.get("J+4", ""),
                    "J+5": time_vals.get("J+5", ""),
                    "J+10": time_vals.get("J+10", ""),
                    "J+15": time_vals.get("J+15", ""),
                    "J+30": time_vals.get("J+30", ""),
                })
        else:
            # Standard row — leave Commentaires empty for rows that only
            # appear as context (Effectif/Positions whose values are 0 or
            # aggregated across structures already shown individually).
            rows.append({
                "Montée en charge exprimée": r.label,
                "Nominal": r.nominal,
                "H0": r.h0, "H+1": r.h1, "H+2": r.h2, "H+4": r.h4,
                "J+1": r.j1, "J+2": r.j2, "J+3": r.j3, "J+4": r.j4,
                "J+5": r.j5, "J+10": r.j10, "J+15": r.j15, "J+30": r.j30,
                "Commentaires": "",   # suppress raw comment text when structures are present
            })

    return rows

def transform_collaborateurs(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Fonction": p.function,
            "Nom": p.last_name,
            "Prénom": p.first_name,
            "Poste": p.position or p.function,
            "Ancienneté dans le poste": p.seniority,
            "Suppléants possibles": p.replacements,
            "Commentaires": "",
        }
        for p in fiche.key_people
    ]

def transform_autres_eqt(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Désignation": e.designation,
            "H0": e.h0, "H+1": e.h1, "H+2": e.h2, "H+4": e.h4,
            "J+1": e.j1, "J+2": e.j2, "J+3": e.j3, "J+5": e.j5,
            "J+10": e.j10, "J+15": e.j15,
            "Commentaires": e.comments,
        }
        for e in fiche.other_equipment
    ]

def transform_doc_critiques(fiche: BIAFiche) -> list[dict]:
    return [
        {
            "Documents / Fichiers": d.name,
            "Type de stockage\n(Electronique / Papier)": d.storage_type,
            "Duplication \n(O / N)": d.duplication,
            "Modalité de Duplication": d.duplication_method,
            "Commentaires": "",
        }
        for d in fiche.critical_docs
    ]

def _dmia_lot(dmia: str) -> str:
    """Map a DMIA string to its restart lot.

    Lot 1 — H0 to J+1  (0 – 24 h)   → RED
    Lot 2 — J+2 to J+5 (25 – 120 h)  → ORANGE
    Lot 3 — beyond J+5 (> 120 h)     → GREEN

    Handles all DMIA formats: H+4, 4H, J+1, 1J, J1, N.A., etc.
    Uses _dmia_to_hours so the same parsing logic applies everywhere.
    Unknown / N.A. values fall back to Lot 3.
    """
    hours = _dmia_to_hours(dmia or "")
    if hours is None:
        return "Lot 3"
    if hours <= 24:    # H0 → J+1 (inclusive)
        return "Lot 1"
    if hours <= 120:   # J+2 → J+5 (inclusive)
        return "Lot 2"
    return "Lot 3"


def transform_redemarrages(fiche: BIAFiche) -> list[dict]:
    """Build rows for the 'Redémarrages des applications' sheet.

    Each IT application is assigned to a restart Lot based on its DMIA:
      Lot 1 : H0 – J+1
      Lot 2 : J+2 – J+5
      Lot 3 : > J+5

    Rows are sorted so Lot 1 comes first, then Lot 2, then Lot 3.
    """
    rows = []
    for app in fiche.it_applications:
        lot = _dmia_lot(app.dmia)
        rows.append({
            "Lot":       lot,
            "Application": app.name,
            "Structure": fiche.entity_name,
            "DMIA":      app.dmia,
            "PMDT":      app.pmdt,
        })
    # Sort by lot so the sheet is naturally grouped
    lot_order = {"Lot 1": 0, "Lot 2": 1, "Lot 3": 2}
    rows.sort(key=lambda r: lot_order.get(r["Lot"], 3))
    return rows


TRANSFORM_MAP = {
    "Activités":                       transform_activites,
    "Impact DMIA":                     transform_impact_dmia,
    "Applications IT":                 transform_applications_it,
    "Redémarrages des applications":   transform_redemarrages,
    "Echanges I":                      transform_echanges_internes,
    "Echanges E":                      transform_echanges_externes,
    "Montée en charge":                transform_montee_en_charge,
    "Collaborateurs Clés":             transform_collaborateurs,
    "Autres Eqt IT":                   transform_autres_eqt,
    "Doc critiques":                   transform_doc_critiques,
}


# ─────────────────────────────────────────────────────────────────────────────
# BOOTSTRAP — build a pre-seeded synthèse from the fiche de recensement
# ─────────────────────────────────────────────────────────────────────────────

def _clean(v) -> str:
    """Return stripped string; treat '-', 'nan', None as empty."""
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s in ("-", "nan", "NaN", "None") else s


def _recensement_norm(s: str) -> str:
    """Lowercase + strip accents for robust column header comparison."""
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    ).strip()


# Synonym lists — add new names here as new client formats appear.
# Each synonym is a substring: "direction" matches "Direction Générale", etc.
_NIVEAU_SYNONYMS: dict[str, list[str]] = {
    "division": [
        "division", "direction", "direction generale", "direction centrale",
        "pole", "branche", "groupe", "bu", "business unit", "filiere",
        "secteur", "perimetre", "entite mere",
    ],
    "unite": [
        "unite", "service", "sous-direction", "departement central",
        "direction departementale", "sous direction",
    ],
    "departement": [
        "departement", "entite", "equipe", "cellule", "bureau", "team",
        "agence", "etablissement",
        # NOTE: "structure" intentionally excluded — many recensement files have
        # a generic "Structure" label column that is NOT an org-level column.
    ],
}


def detect_recensement_columns(recensement_path: str | Path) -> dict:
    """
    Inspect a recensement Excel file and return the auto-detected column
    mapping together with a confidence rating.

    Returns a dict:
    {
      "header_row": int,          # 1-based row index of the detected header row
      "all_columns": [            # all non-empty header columns (for the UI)
          {"index": int, "name": str}, ...
      ],
      "mapping": {                # auto-detected level → 0-based column index
          "division":    int | null,
          "unite":       int | null,
          "departement": int | null,
      },
      "confidence": "high" | "medium" | "low",
      "matched_names": {          # the actual header text that matched each level
          "division": str | null, ...
      },
    }
    """
    from openpyxl import load_workbook

    wb = load_workbook(str(recensement_path), data_only=True)
    ws = wb.active

    header_row_idx = 1
    best_score = 0
    col_map: dict[str, int | None] = {"division": None, "unite": None, "departement": None}
    matched_names: dict[str, str | None] = {"division": None, "unite": None, "departement": None}
    all_cols: list[dict] = []

    for row in ws.iter_rows(min_row=1, max_row=15):
        candidate_map: dict[str, int | None] = {"division": None, "unite": None, "departement": None}
        candidate_names: dict[str, str | None] = {"division": None, "unite": None, "departement": None}
        score = 0
        row_cols: list[dict] = []
        for i, cell in enumerate(row):
            raw = str(cell.value).strip() if cell.value else ""
            if not raw or raw in ("-", "nan", "None"):
                continue
            row_cols.append({"index": i, "name": raw})
            normed = _recensement_norm(raw)
            for level, synonyms in _NIVEAU_SYNONYMS.items():
                if candidate_map[level] is None and any(syn in normed for syn in synonyms):
                    candidate_map[level] = i
                    candidate_names[level] = raw
                    score += 1
                    break
        if score > best_score:
            best_score = score
            col_map = candidate_map
            matched_names = candidate_names
            header_row_idx = row[0].row
            all_cols = row_cols
        if score >= 2:
            break  # good enough, stop scanning early

    # Determine confidence
    if best_score >= 2:
        confidence = "high"
    elif best_score == 1:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "header_row": header_row_idx,
        "all_columns": all_cols,
        "mapping": col_map,
        "confidence": confidence,
        "matched_names": matched_names,
    }


def read_recensement(
    recensement_path: str | Path,
    custom_mapping: dict | None = None,
) -> list[dict]:
    """
    Parse a fiche de recensement Excel file and return a de-duplicated list of
    entity dicts:
        [{"division": str, "unite": str, "departement": str, "entity_name": str}, ...]

    Different clients use completely different column names for the same concept.
    We handle this via an extended synonym map (see _NIVEAU_SYNONYMS above).

    Args:
        recensement_path: Path to the Excel file.
        custom_mapping: Optional override for the auto-detected column mapping.
                        Format: {"division": int|None, "unite": int|None, "departement": int|None}
                        where each value is the 0-based column index in the header row.
                        When provided, auto-detection is skipped entirely.

    If only a single level is found, all rows are deduplicated on that column
    and it becomes the entity_name directly.
    """
    from openpyxl import load_workbook

    wb = load_workbook(str(recensement_path), data_only=True)
    ws = wb.active

    if custom_mapping is not None:
        # User-provided mapping: skip auto-detection, just find the header row
        header_row_idx = 1
        for row in ws.iter_rows(min_row=1, max_row=15):
            non_empty = sum(1 for c in row if c.value and str(c.value).strip())
            if non_empty >= 2:
                header_row_idx = row[0].row
                break
        col_map = {
            "division":    custom_mapping.get("division"),
            "unite":       custom_mapping.get("unite"),
            "departement": custom_mapping.get("departement"),
        }
    else:
        # Auto-detect columns using synonym matching
        header_row_idx = 1
        best_score = 0
        col_map: dict[str, int | None] = {"division": None, "unite": None, "departement": None}

        for row in ws.iter_rows(min_row=1, max_row=15):
            candidate_map: dict[str, int | None] = {"division": None, "unite": None, "departement": None}
            score = 0
            for i, cell in enumerate(row):
                raw = str(cell.value).strip() if cell.value else ""
                if not raw or raw in ("-", "nan", "None"):
                    continue
                normed = _recensement_norm(raw)
                for level, synonyms in _NIVEAU_SYNONYMS.items():
                    if candidate_map[level] is None and any(syn in normed for syn in synonyms):
                        candidate_map[level] = i
                        score += 1
                        break
            if score > best_score:
                best_score = score
                col_map = candidate_map
                header_row_idx = row[0].row
            if score >= 2:
                break

        # Fallback: use first non-empty column if nothing matched
        if best_score == 0:
            header_row_idx = 1
            col_map["division"] = 0

    # ── Read data rows ─────────────────────────────────────────────────────────
    seen_entities: set[str] = set()
    entities: list[dict] = []

    for row in ws.iter_rows(min_row=header_row_idx + 1):
        def _get(col_idx: int | None) -> str:
            if col_idx is None or col_idx >= len(row):
                return ""
            return _clean(row[col_idx].value)

        div  = _get(col_map["division"])
        unit = _get(col_map["unite"])
        dept = _get(col_map["departement"])

        # Skip fully empty rows
        if not div and not unit and not dept:
            continue

        # entity_name = most specific non-empty level (Département > Unité > Division)
        entity_name = dept or unit or div
        if not entity_name:
            continue

        # Deduplicate — many files repeat the same org unit once per activity row
        key = (div, unit, dept)
        if key in seen_entities:
            continue
        seen_entities.add(key)

        entities.append({
            "division":    div,
            "unite":       unit,
            "departement": dept,
            "entity_name": entity_name,
        })

    return entities


def bootstrap_synthese(
    recensement_path: str | Path,
    master_template_path: str | Path,
    output_path: str | Path,
    verbose: bool = True,
    custom_mapping: dict | None = None,
) -> list[dict]:
    """
    BOOTSTRAP phase — Step 1 of the new two-step workflow.

    Reads the fiche de recensement, extracts the client's org structure
    (Division / Unité / Département), and writes one blank data row per entity
    into every sheet of the master synthèse template.

    Returns the list of entity dicts so the caller can log them.

    Args:
        custom_mapping: Optional column mapping override (see read_recensement).

    WHY: The ETL needs pre-seeded rows to fuzzy-match against.  Previously those
    rows were created manually.  Now they come directly from the recensement,
    which the team always fills in first — so this step is free and automatic.
    """
    import copy
    entities = read_recensement(recensement_path, custom_mapping=custom_mapping)

    if not entities:
        raise ValueError(
            "Aucune entité trouvée dans la fiche de recensement. "
            "Vérifiez que le fichier contient au moins une colonne parmi : "
            "Division, Direction, Unité, Service, Département, Entité, Équipe, etc."
        )

    if verbose:
        print(f"  Recensement: {len(entities)} entit(ies) found")
        for e in entities:
            print(f"    • {e['entity_name']}"
                  + (f"  [div: {e['division']}]" if e['division'] else "")
                  + (f"  [unit: {e['unite']}]"   if e['unite']    else ""))

    wb = load_workbook(str(master_template_path))

    # Sheets where rows are NOT keyed on entity name (Division/Unité/Département)
    # — the entity seed rows do not belong there and must be skipped.
    _NO_ENTITY_SEED = {"Redémarrages des applications"}

    for sheet_name in TRANSFORM_MAP:
        if sheet_name.strip() in _NO_ENTITY_SEED:
            continue   # this sheet is populated entirely from app-level data

        ws = None
        for sn in wb.sheetnames:
            if sn.strip() == sheet_name.strip():
                ws = wb[sn]
                break
        if ws is None:
            if verbose:
                print(f"  [WARN] Sheet '{sheet_name}' not found in template, skipping.")
            continue

        # Scan ALL existing data rows (from DATA_START_ROW until a fully blank row).
        # We replace them wholesale with the entity rows from the recensement.
        first_data_row = DATA_START_ROW
        existing_data_rows = []
        for row in ws.iter_rows(min_row=first_data_row):
            vals = [str(c.value).strip() if c.value else "" for c in row]
            if not any(vals):          # completely blank → end of data block
                break
            existing_data_rows.append(row[0].row)

        n_entities = len(entities)
        n_existing = len(existing_data_rows)
        style_src  = existing_data_rows[0] if existing_data_rows else first_data_row

        # Adjust row count to match entities
        if n_entities > n_existing:
            # Insert extra rows right after the last existing row
            insert_at = (existing_data_rows[-1] + 1
                         if existing_data_rows else first_data_row)
            ws.insert_rows(insert_at, n_entities - n_existing)
            for offset in range(n_entities - n_existing):
                target = insert_at + offset
                for col in range(1, ws.max_column + 1):
                    src_cell = ws.cell(row=style_src, column=col)
                    tgt_cell = ws.cell(row=target, column=col)
                    if src_cell.has_style:
                        tgt_cell._style = copy.copy(src_cell._style)

        elif n_existing > n_entities:
            # Delete the surplus rows from the bottom of the block
            surplus_start = existing_data_rows[n_entities]
            ws.delete_rows(surplus_start, n_existing - n_entities)

        # Clear all data cells in the block, then write entity values
        for i in range(n_entities):
            row_num = first_data_row + i
            # Clear every data cell first (removes old dept names)
            for col in range(2, ws.max_column + 1):
                try:
                    ws.cell(row=row_num, column=col).value = None
                except AttributeError:
                    pass

        # Write Division / Unité / Département
        for i, ent in enumerate(entities):
            row_num = first_data_row + i
            try:
                ws.cell(row=row_num, column=2).value = ent["division"]    or "-"
                ws.cell(row=row_num, column=3).value = ent["unite"]       or "-"
                ws.cell(row=row_num, column=4).value = ent["departement"] or "-"
            except AttributeError:
                pass   # merged cell — skip

        if verbose:
            print(f"  OK [{sheet_name}] -> {n_entities} row(s) seeded")

    wb.save(str(output_path))
    if verbose:
        print(f"\nBootstrapped synthese saved -> {output_path}")

    return entities


# ─────────────────────────────────────────────────────────────────────────────
# ORG TREE — extract hierarchy + DMIA heatmap from synthèse
# ─────────────────────────────────────────────────────────────────────────────

_SKIP_SHEETS_ORG: set[str] = {
    "Synthèse", "A masquer", "Echelle d'impact",
    "Redémarrages des applications", "Redémarrages des applications ",
}

# DMIA string → hours conversion
_DMIA_FIXED: dict[str, float] = {
    "h0": 0, "h+0": 0,
    "h1": 1, "h+1": 1,
    "h2": 2, "h+2": 2,
    "h4": 4, "h+4": 4,
    "h8": 8, "h+8": 8,
    "h12": 12, "h+12": 12,
    "h24": 24, "h+24": 24,
    "j1": 24, "j+1": 24, "j2": 48, "j+2": 48,
    "j3": 72, "j+3": 72, "j4": 96, "j+4": 96,
    "j5": 120, "j+5": 120, "j7": 168, "j+7": 168,
    "j10": 240, "j+10": 240, "j15": 360, "j+15": 360,
    "j30": 720, "j+30": 720,
}


def _dmia_to_hours(s: str) -> float | None:
    """Parse a DMIA expression to hours. Returns None if unrecognised."""
    if not s:
        return None
    norm = s.strip().lower().replace(" ", "").replace("≤", "").replace("<", "")
    if norm in _DMIA_FIXED:
        return _DMIA_FIXED[norm]
    # Generic patterns: h<N>, h+<N>, <N>h, j<N>, <N>j, <N>jours?
    m = re.match(r'h\+?(\d+(?:\.\d+)?)$', norm)
    if m:
        return float(m.group(1))
    m = re.match(r'(\d+(?:\.\d+)?)h$', norm)
    if m:
        return float(m.group(1))
    m = re.match(r'j\+?(\d+(?:\.\d+)?)$', norm)
    if m:
        return float(m.group(1)) * 24
    m = re.match(r'(\d+(?:\.\d+)?)j(?:ours?)?$', norm)
    if m:
        return float(m.group(1)) * 24
    return None


def _dmia_color(hours: float | None) -> str:
    """
    Heatmap color per manager spec:
      Rouge  → H0 → J+2   (≤ 48 h)
      Orange → J+3 → J+5  (72–120 h)
      Vert   → Au-delà    (> 120 h)
      none   → no DMIA data
    """
    if hours is None:
        return "none"
    if hours <= 48:
        return "rouge"
    if hours <= 120:
        return "orange"
    return "vert"


def _cell_val(ws, row: int, col: int) -> str:
    v = ws.cell(row=row, column=col).value
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("-", "nan", "none", "") else s


# ── header-row auto-detection ─────────────────────────────────────────────────
import unicodedata as _udata

def _strip_accents(s: str) -> str:
    # Normalize typographic apostrophes/quotes to straight ASCII before
    # stripping diacritics — Excel files often use U+2019 (right single quote)
    # while web inputs use U+0027 (straight apostrophe), causing dict mismatches.
    s = s.replace("’", "'").replace("‘", "'").replace("ʼ", "'")
    return "".join(
        ch for ch in _udata.normalize("NFD", s)
        if _udata.category(ch) != "Mn"
    )

_HDR_KEYWORDS = frozenset({
    "structure", "niveau", "direction", "departement", "service",
    "division", "unite", "activite", "dmia", "application", "processus",
})

def _find_hdr_row(ws, max_search: int = 10) -> int:
    """Return the row (1-based) that looks most like a header row.
    Scores each row by counting cells whose text contains a keyword.
    Falls back to row 1 if nothing matches."""
    max_col = min(ws.max_column or 15, 30)
    best_row, best_score = 1, 0
    for r in range(1, max_search + 1):
        score = 0
        for c in range(1, max_col + 1):
            v = ws.cell(r, c).value
            if not v:
                continue
            vl = _strip_accents(str(v).strip().lower())
            if any(kw in vl for kw in _HDR_KEYWORDS):
                score += 1
        if score > best_score:
            best_score, best_row = score, r
    return best_row


# Structure-column labels that indicate an organisational hierarchy level
_STRUCT_LABELS = frozenset({
    "dir. centrale", "direction centrale", "direction", "direction generale",
    "dir. generale", "departement", "service", "division", "unite",
})


def _norm_struct_key(vals: list[str], target_len: int) -> tuple:
    """
    Normalise a structure key by stripping leading empty values then
    right-padding with empty strings to ``target_len``.

    Different sheets in the same workbook sometimes place the same entity at
    different column offsets (e.g. Impact DMIA may have col-2 always empty
    while Activités fills from col-2 directly).  Stripping leading empties
    makes keys comparable regardless of sheet-specific offset.
    """
    stripped: list[str] = list(vals)
    while stripped and not stripped[0]:
        stripped.pop(0)
    while len(stripped) < target_len:
        stripped.append("")
    return tuple(stripped[:target_len])


def extract_org_tree(synthese_path: str | Path) -> dict:
    """
    Parse a synthèse BIA xlsx and return a nested org tree.

    Handles multiple file layouts:
      • Classic format: headers at row 5, "Structure Niveau N" columns
      • Compact format: headers at row 1, "Direction"/"Département" columns
      • Merged-cell format: structure values not repeated (carry-forward applied)

    Returns:
      {
        "activities":   <tree>,
        "applications": <tree>,
        "depth":        int,
      }
    """
    wb = load_workbook(str(synthese_path), data_only=True)

    # ── Step 1: detect header row + structure columns ─────────────────────────
    struct_cols: list[int] = []
    main_ws    = None
    hdr_row_main = 1

    for sheet_name in wb.sheetnames:
        if sheet_name.strip() in _SKIP_SHEETS_ORG:
            continue
        ws  = wb[sheet_name]
        hdr = _find_hdr_row(ws)
        potential: list[int] = []
        for col in range(1, min((ws.max_column or 10), 15) + 1):
            v = ws.cell(hdr, col).value
            if not v:
                continue
            vl = _strip_accents(str(v).strip().lower())
            if ("structure" in vl and "niveau" in vl) or vl in _STRUCT_LABELS:
                potential.append(col)
        if potential:
            struct_cols   = potential
            main_ws       = ws
            hdr_row_main  = hdr
            break

    # Fallback: classic layout assumed (cols 2,3,4 with header at row 5)
    if not struct_cols:
        struct_cols  = [2, 3, 4]
        hdr_row_main = 5
        for sheet_name in wb.sheetnames:
            if sheet_name.strip() not in _SKIP_SHEETS_ORG:
                main_ws = wb[sheet_name]
                break

    depth      = len(struct_cols)
    data_start = hdr_row_main + 1

    # ── Step 2: collect unique org entities (with carry-forward) ─────────────
    seen: set[tuple]       = set()
    entities: list[list[str]] = []

    if main_ws is not None:
        prev = [""] * depth
        for row_idx in range(data_start, (main_ws.max_row or data_start) + 1):
            raw = [_cell_val(main_ws, row_idx, c) for c in struct_cols]
            if not any(raw):
                continue
            # Carry-forward: fill empty cells from the row above
            for i, v in enumerate(raw):
                if v:
                    prev[i] = v
            key = _norm_struct_key(list(prev), depth)
            if key in seen:
                continue
            seen.add(key)
            entities.append(list(key))

    # ── Step 3: read DMIA data ────────────────────────────────────────────────
    # Locate relevant sheets (flexible name matching)
    impact_ws    = None   # "Impact DMIA" or similar
    arbit_ws     = None   # "DMIA arbitrée" or similar
    dmia_base_ws = None   # standalone "DMIA" sheet (fallback)

    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "impact" in sl and "dmia" in sl:
            impact_ws = wb[sn]
        elif "arbit" in sl:
            arbit_ws = wb[sn]
        elif "dmia" in sl and impact_ws is None and arbit_ws is None:
            # standalone DMIA sheet — lower priority than "Impact DMIA"
            dmia_base_ws = wb[sn]

    primary_dmia_ws = impact_ws or dmia_base_ws

    _DMIA_COL_PRIORITY = (
        "DMIA Arbitree", "DMIA arbitree",
        "DMIA Preconise", "DMIA Preconisee", "DMIA recommandee", "DMIA Recommandee",
        "DMIA Exprimee", "DMIA exprimee", "DMIA",
    )
    _ACT_COL_LABELS = ("Activite", "Activite/Processus", "Processus", "Activite ")

    def _read_dmia_sheet(ws_src) -> dict:
        """Read DMIA rows with auto header-row detection and carry-forward."""
        if ws_src is None:
            return {}
        hdr      = _find_hdr_row(ws_src)
        d_start  = hdr + 1
        max_col  = min(ws_src.max_column or 40, 60)

        # Build header map (accent-normalised keys)
        hdrs: dict[str, int] = {}
        for c in range(1, max_col + 1):
            v = ws_src.cell(hdr, c).value
            if v:
                norm = _strip_accents(str(v).strip())
                hdrs[norm] = c
                hdrs[str(v).strip()] = c  # also store original

        # Activity column
        act_col = None
        for lbl in _ACT_COL_LABELS:
            if lbl in hdrs:
                act_col = hdrs[lbl]
                break

        # DMIA column (priority order, accent-normalised)
        dmia_col = None
        for lbl in _DMIA_COL_PRIORITY:
            if lbl in hdrs:
                dmia_col = hdrs[lbl]
                break

        fallback_col = hdrs.get("DMIA Exprimee") or hdrs.get("DMIA exprimee")

        if not dmia_col:
            return {}

        result: dict[tuple, list[dict]] = {}
        prev = [""] * depth

        for row_idx in range(d_start, (ws_src.max_row or d_start) + 1):
            raw = [_cell_val(ws_src, row_idx, c) for c in struct_cols]
            if not any(raw):
                continue
            for i, v in enumerate(raw):
                if v:
                    prev[i] = v
            key = _norm_struct_key(list(prev), depth)

            act_name = _cell_val(ws_src, row_idx, act_col) if act_col else ""
            if not act_name:
                continue

            dmia_str = _cell_val(ws_src, row_idx, dmia_col)
            if not dmia_str and fallback_col and fallback_col != dmia_col:
                dmia_str = _cell_val(ws_src, row_idx, fallback_col)

            hours = _dmia_to_hours(dmia_str)
            result.setdefault(key, []).append({
                "activity":   act_name,
                "dmia":       dmia_str or "",
                "hours":      hours,
                "dmia_color": _dmia_color(hours),
            })
        return result

    # Merge: primary (impact/base) → override with arbitrée
    dmia_by_entity: dict[tuple, list[dict]] = _read_dmia_sheet(primary_dmia_ws)
    if arbit_ws:
        for k, v in _read_dmia_sheet(arbit_ws).items():
            dmia_by_entity[k] = v

    # ── Step 4: read Applications IT data ────────────────────────────────────
    apps_by_entity: dict[tuple, list[dict]] = {}

    app_ws = None
    # Prefer "Applications IT" over generic "Applications"
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "application" in sl and "it" in sl:
            app_ws = wb[sn]
            break
    if app_ws is None:
        for sn in wb.sheetnames:
            if "application" in _strip_accents(sn.strip().lower()):
                app_ws = wb[sn]
                break

    _APP_CRIT_COLOR = {
        "v": "rouge", "vital": "rouge",
        "mc": "rouge", "mission critique": "rouge",
        "c*": "orange", "critique": "orange",
        "c": "vert",
        "pc": "vert", "peu critique": "vert",
    }

    if app_ws is not None:
        hdr_app  = _find_hdr_row(app_ws)
        hdrs_app: dict[str, int] = {}
        for c in range(1, min(app_ws.max_column or 20, 30) + 1):
            v = app_ws.cell(hdr_app, c).value
            if v:
                hdrs_app[str(v).strip()] = c
                hdrs_app[_strip_accents(str(v).strip())] = c

        app_name_col = hdrs_app.get("Application")
        crit_col     = (hdrs_app.get("Niveau de criticite")
                        or hdrs_app.get("Niveau de criticité"))
        app_dmia_col = hdrs_app.get("DMIA")
        pmdt_col     = hdrs_app.get("PMDT")
        bypass_col   = (hdrs_app.get("Contournement envisageable")
                        or hdrs_app.get("Contournement"))

        if app_name_col:
            prev_app = [""] * depth
            for row_idx in range(hdr_app + 1, (app_ws.max_row or hdr_app) + 1):
                raw = [_cell_val(app_ws, row_idx, c) for c in struct_cols]
                if not any(raw):
                    continue
                for i, v in enumerate(raw):
                    if v:
                        prev_app[i] = v
                key = _norm_struct_key(list(prev_app), depth)

                app_name = _cell_val(app_ws, row_idx, app_name_col)
                if not app_name:
                    continue
                crit_raw = _cell_val(app_ws, row_idx, crit_col) if crit_col else ""
                dmia_str = _cell_val(app_ws, row_idx, app_dmia_col) if app_dmia_col else ""
                pmdt_str = _cell_val(app_ws, row_idx, pmdt_col) if pmdt_col else ""
                bypass   = _cell_val(app_ws, row_idx, bypass_col) if bypass_col else ""
                hours    = _dmia_to_hours(dmia_str)
                color    = _APP_CRIT_COLOR.get(crit_raw.lower(), None) or _dmia_color(hours)

                apps_by_entity.setdefault(key, []).append({
                    "app":        app_name,
                    "dmia":       dmia_str,
                    "hours":      hours,
                    "criticite":  crit_raw,
                    "pmdt":       pmdt_str,
                    "bypass":     bypass,
                    "dmia_color": color,
                })

    # ── Step 5: build annotated trees ────────────────────────────────────────
    act_tree = _build_dynamic_tree(entities, struct_cols, dmia_by_entity,  "activities",   depth)
    app_tree = _build_dynamic_tree(entities, struct_cols, apps_by_entity, "applications", depth)

    return {"activities": act_tree, "applications": app_tree, "depth": depth}


def extract_montee_en_charge(synthese_path: str | Path) -> dict:
    """
    Parse a synthèse BIA and return flat, sorted lists of activities and
    applications with their DMIA for the Montée en charge diagram.

    Returns:
        {
          "activities":   [{entity, name, dmia, hours, color}, …],
          "applications": [{entity, name, dmia, hours, color, criticite}, …],
        }
    """
    tree = extract_org_tree(synthese_path)

    def _flatten_acts(node: dict, entity: str = "") -> list:
        items: list[dict] = []
        n = node.get("name", "")
        epath = (entity + " › " + n).strip(" › ") if (n and n != "Organisation") else entity
        for child in node.get("children", []):
            items.extend(_flatten_acts(child, epath))
        for act in node.get("_activities", []):
            if act.get("activity"):
                items.append({
                    "entity": epath,
                    "name":   act["activity"],
                    "dmia":   act.get("dmia", ""),
                    "hours":  act.get("hours"),
                    "color":  act.get("dmia_color", "none"),
                })
        return items

    def _flatten_apps(node: dict, entity: str = "") -> list:
        items: list[dict] = []
        n = node.get("name", "")
        epath = (entity + " › " + n).strip(" › ") if (n and n != "Organisation") else entity
        for child in node.get("children", []):
            items.extend(_flatten_apps(child, epath))
        for app in node.get("_applications", []):
            if app.get("app"):
                items.append({
                    "entity":   epath,
                    "name":     app["app"],
                    "dmia":     app.get("dmia", ""),
                    "hours":    app.get("hours"),
                    "color":    app.get("dmia_color", "none"),
                    "criticite": app.get("criticite", ""),
                })
        return items

    def _sort_key(x: dict) -> tuple:
        h = x.get("hours")
        return (0 if h is None else h, x.get("entity", ""), x.get("name", ""))

    acts = sorted(_flatten_acts(tree["activities"]),  key=_sort_key)
    apps = sorted(_flatten_apps(tree["applications"]), key=_sort_key)

    return {"activities": acts, "applications": apps}


def extract_rapport_bia(synthese_path: str | Path) -> dict:
    """
    Parse a synthèse BIA and return all data needed for the rapport BIA charts.

    Data source: the 'Montée en charge arbitrée' (or 'Montée en charge') sheet.
    Structure: each activity has 3 consecutive rows:
        row 1 — Effectif   (col 4 = "Effectif",   cols 5+ = values at each time horizon)
        row 2 — Positions  (col 4 = "Positions")
        row 3 — Télétravail(col 4 = "Télétravail")
    Direction / Département / Activité are in cols 1-3 with carry-forward.

    Returns:
    {
      "time_cols": ["H+0","H+1","H+2","H+4","J+1","J+2","J+3","J+5","J+10","J+15"],
      "activities": [
          {"direction","departement","activity","nominal",
           "positions":[N,...], "teletravail":[N,...], "effectif":[N,...]},
          ...
      ],
      "totals": {                     # aggregated across all activities
          "positions":   [N,...],
          "teletravail": [N,...],
          "operational": [N,...],     # count of activities with pos+tt > 0 at each step
      },
      "recovery_steps": [             # for the cumulative recovery curve
          {"label","hours","count","percent","activities":[...]},
          ...
      ],
      "equipment": [{"name","qty"}, ...],
    }
    """
    wb = load_workbook(str(synthese_path), data_only=True)

    # ── Locate the best Montée en charge sheet ─────────────────────────────────
    # Prefer "arbitrée" (official planned values)
    mec_ws = None
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "mont" in sl and "charg" in sl and "arbit" in sl:
            mec_ws = wb[sn]; break
    if mec_ws is None:
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if "mont" in sl and "charg" in sl:
                mec_ws = wb[sn]; break

    time_cols: list[str] = []
    activities: list[dict] = []

    if mec_ws:
        # ── Header row: find time columns ───────────────────────────────────────
        hdr = _find_hdr_row(mec_ws)
        max_col = min(mec_ws.max_column or 20, 25)
        col_map: dict[str, int] = {}   # "H+4" → column index (1-based)
        TIME_LABELS = ["H+0","H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4",
                       "J+5","J+10","J+15","J+30"]
        for c in range(1, max_col + 1):
            raw = str(mec_ws.cell(hdr, c).value or "").strip()
            if raw in TIME_LABELS:
                # Normalise "H0" → "H+0"
                key = raw if "+" in raw else raw.replace("H0","H+0").replace("J0","J+0")
                col_map[key] = c
                col_map[raw] = c

        TIME_ORDER = ["H+0","H+1","H+2","H+4","J+1","J+2","J+3","J+5","J+10","J+15","J+30"]
        time_cols = [t for t in TIME_ORDER if t in col_map]

        def _pnum(v) -> "float | None":
            if v is None: return None
            try:   return float(str(v).replace(",", ".").strip()) or None
            except: return None

        # ── Auto-detect label columns ────────────────────────────────────────────
        # The sheet may have an empty col A before the real data.
        # We detect the "row type" column by scanning the header row for the
        # first column whose value contains "montee" / "charge" / "effectif" /
        # "position" — that column is col_type. The 3 label cols precede it.
        col_type = 4   # default: Direction(1) Dept(2) Act(3) Type(4)
        for c in range(1, min((mec_ws.max_column or 10), 10) + 1):
            v = _strip_accents(str(mec_ws.cell(hdr, c).value or "").strip().lower())
            if any(kw in v for kw in ("mont", "charg", "effectif", "position")):
                col_type = c; break
        col_dir = col_type - 3
        col_dep = col_type - 2
        col_act = col_type - 1
        col_nominal = col_type + 1

        prev_dir, prev_dep, prev_act = "", "", ""
        current: dict | None = None   # activity being built

        def _flush():
            nonlocal current
            if current:
                # Use direction or département as fallback when activity cell is "-" or empty
                if not current.get("activity"):
                    current["activity"] = (current.get("departement")
                                           or current.get("direction") or "?")
                activities.append(current)
            current = None

        _ROW_TYPES = {"effectif", "positions", "position", "teletravail",
                      "teletravail", "teletravaill"}

        for r in range(hdr + 1, (mec_ws.max_row or hdr) + 1):
            # Read label columns with carry-forward
            dir_v  = _cell_val(mec_ws, r, col_dir)  if col_dir  >= 1 else ""
            dep_v  = _cell_val(mec_ws, r, col_dep)  if col_dep  >= 1 else ""
            act_v  = _cell_val(mec_ws, r, col_act)  if col_act  >= 1 else ""
            row_type = _strip_accents(str(mec_ws.cell(r, col_type).value or "").strip().lower())

            if dir_v: prev_dir = dir_v
            if dep_v: prev_dep = dep_v
            if act_v: prev_act = act_v

            # Skip rows that aren't one of the 3 known types
            if row_type not in _ROW_TYPES:
                continue

            if "telet" in row_type:
                row_type_clean = "teletravail"
            elif "pos" in row_type:
                row_type_clean = "positions"
            else:
                row_type_clean = row_type  # "effectif"

            # Each new "effectif" row = new activity
            if row_type_clean == "effectif":
                _flush()
                nominal = _pnum(mec_ws.cell(r, col_nominal).value)
                current = {
                    "direction":   prev_dir,
                    "departement": prev_dep,
                    "activity":    prev_act,
                    "nominal":     int(nominal) if nominal else None,
                    "effectif":    [None] * len(time_cols),
                    "positions":   [None] * len(time_cols),
                    "teletravail": [None] * len(time_cols),
                }

            if current is None:
                continue

            # Read time values for this row
            vals = []
            for t in time_cols:
                col_idx = col_map.get(t)
                vals.append(_pnum(mec_ws.cell(r, col_idx).value) if col_idx else None)

            current[row_type_clean] = vals

        _flush()  # Don't forget the last activity

    # ── Build aggregated totals ────────────────────────────────────────────────
    n = len(time_cols)
    total_pos = [0.0] * n
    total_tt  = [0.0] * n
    operational_count = [0] * n   # activities with pos+tt > 0 at each step

    for act in activities:
        for i in range(n):
            p = act["positions"][i]  or 0
            t = act["teletravail"][i] or 0
            total_pos[i] += p
            total_tt[i]  += t
            if p + t > 0:
                operational_count[i] += 1

    total_activities = len(activities)

    # ── Build recovery_steps (cumulative, for the curve chart) ────────────────
    # An activity is "operational at step i" if it has pos+tt > 0 at step i
    # OR at any earlier step (cumulative).
    # We compute the FIRST step at which each activity becomes operational.

    first_op: list[int | None] = []   # index of first time_cols where pos+tt > 0
    for act in activities:
        first = None
        for i, t in enumerate(time_cols):
            p = act["positions"][i]   or 0
            tt= act["teletravail"][i] or 0
            if p + tt > 0:
                first = i; break
        first_op.append(first)

    # Build cumulative count at each step
    recovery_steps = [{"label": "Début", "hours": 0, "count": 0,
                       "percent": 0, "activities": []}]

    _HOURS = {"H+0":0,"H+1":1,"H+2":2,"H+4":4,"J+1":24,"J+2":48,"J+3":72,
              "J+5":120,"J+10":240,"J+15":360,"J+30":720}

    for i, t in enumerate(time_cols):
        # activities operational by step i (first_op ≤ i, or None = never → skip)
        ops = []
        for j, act in enumerate(activities):
            fo = first_op[j]
            if fo is not None and fo <= i:
                ops.append({
                    "name":         act["activity"],
                    "entity":       act["departement"] or act["direction"],
                    "direction":    act["direction"],
                    "departement":  act["departement"],
                    "positions":    act["positions"][i],
                    "teletravail":  act["teletravail"][i],
                    "dmia":         t,
                })
        pct = round(len(ops) / total_activities * 100) if total_activities else 0
        recovery_steps.append({
            "label":      t,
            "hours":      _HOURS.get(t, 0),
            "count":      len(ops),
            "percent":    pct,
            "activities": ops,
        })

    # ── Equipment (Logistiques sheet) ──────────────────────────────────────────
    equipment: list[dict] = []
    eqt_ws = None
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "logistique" in sl or ("autre" in sl and ("eqt" in sl or "equip" in sl)):
            eqt_ws = wb[sn]; break

    if eqt_ws:
        hdr_e   = _find_hdr_row(eqt_ws)
        max_c_e = min(eqt_ws.max_column or 15, 15)
        for r in range(hdr_e + 1, (eqt_ws.max_row or hdr_e) + 1):
            name = _cell_val(eqt_ws, r, 1)
            if not name: continue
            qty = 0
            for c in range(2, max_c_e + 1):
                v = _cell_val(eqt_ws, r, c)
                try:
                    nv = float(str(v).replace(",", ".").strip())
                    if nv > qty: qty = nv
                except Exception: pass
            if name and qty > 0:
                equipment.append({"name": name, "qty": int(qty)})

    return {
        "total_activities": total_activities,
        "time_cols":        time_cols,
        "activities":       activities,
        "totals": {
            "positions":   [v or 0 for v in total_pos],
            "teletravail": [v or 0 for v in total_tt],
            "operational": operational_count,
        },
        "recovery_steps": recovery_steps,
        "equipment":      equipment,
    }


# ─────────────────────────────────────────────────────────────────────────────
# BIDIRECTIONAL SYNC  — read / write Impact DMIA & Applications IT
# ─────────────────────────────────────────────────────────────────────────────

_IMPACT_DMIA_COLS = [
    "niv1", "niv2", "niv3", "activite",
    "im_1h","im_4h","im_1j","im_2_3j",
    "di_1h","di_4h","di_1j","di_2_3j",
    "jr_1h","jr_4h","jr_1j","jr_2_3j",
    "fin_1h","fin_4h","fin_1j","fin_2_3j",
    "score_1h","score_4h","score_1j","score_2_3j",
    "dmia_exprimee","dmia_preconise","commentaires",
]

_APPS_IT_COLS = [
    "niv1","niv2","niv3","application",
    "criticite","dmia","pmdt","contournement","commentaires",
]


def read_impact_dmia(synthese_path: str | Path) -> dict:
    """
    Read the Impact DMIA sheet. Supports two formats:
      GAT: sheet 'Impact DMIA' — 4 time columns per dimension (IM 1H/4H/1J/2-3J …)
      AMI: sheet 'Impact'      — single score per dimension (IM, DI, JR, FI)
           supplemented by 'DMIA' sheet for dmia_exprimee
    Returns {"columns": [...], "rows": [{col: val, ...}, ...], "header_row": int}
    """
    wb = load_workbook(str(synthese_path), data_only=True)
    ws = None
    # Priority: sheet with both "impact" and "dmia" (GAT), then just "impact" (AMI)
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "impact" in sl and "dmia" in sl:
            ws = wb[sn]; break
    if ws is None:
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if sl == "impact" or (sl.startswith("impact") and "dmia" not in sl):
                ws = wb[sn]; break
    if not ws:
        return {"columns": _IMPACT_DMIA_COLS, "rows": [], "header_row": 0}

    hdr = _find_hdr_row(ws)
    # Map column index → logical name
    col_map: dict[int, str] = {}
    _HDR_ALIASES = {
        # Structure columns (GAT format)
        "structure niveau 1": "niv1", "structure niveau 2": "niv2",
        "structure niveau 3": "niv3",
        # Direction/Dept columns (AMI format)
        "direction": "niv1", "departement": "niv2", "pole": "niv1",
        # Activity
        "activite": "activite", "activité": "activite",
        # Impact scores — GAT 4-time format
        "im 1h": "im_1h", "im 4h": "im_4h", "im 1j": "im_1j", "im 2-3j": "im_2_3j",
        "di 1h": "di_1h", "di 4h": "di_4h", "di 1j": "di_1j", "di 2-3j": "di_2_3j",
        "jr 1h": "jr_1h", "jr 4h": "jr_4h", "jr 1j": "jr_1j", "jr 2-3j": "jr_2_3j",
        "fin 1h": "fin_1h", "fin 4h": "fin_4h", "fin 1j": "fin_1j", "fin 2-3j": "fin_2_3j",
        "score 1h": "score_1h", "score 4h": "score_4h",
        "score 1j": "score_1j", "score 2-3j": "score_2_3j",
        # Impact scores — AMI single-score format (mapped to _1h slot)
        "im": "im_1h", "di": "di_1h", "jr": "jr_1h", "fi": "fin_1h",
        "score": "score_1h", "niveau d impact": "score_1h",
        # DMIA
        "dmia exprimee": "dmia_exprimee", "dmia exprimée": "dmia_exprimee",
        "dmia exprime": "dmia_exprimee",
        "dmia preconise": "dmia_preconise", "dmia préconisé": "dmia_preconise",
        "dmia recommandee": "dmia_preconise", "dmia recommandée": "dmia_preconise",
        "commentaires": "commentaires",
    }
    for c in range(1, (ws.max_column or 30) + 1):
        raw = _strip_accents(str(ws.cell(hdr, c).value or "").strip().lower())
        if raw in _HDR_ALIASES:
            col_map[c] = _HDR_ALIASES[raw]

    rows = []
    prev = {"niv1": "", "niv2": "", "niv3": ""}
    for r in range(hdr + 1, (ws.max_row or hdr) + 1):
        row: dict = {}
        for c, key in col_map.items():
            v = _cell_val(ws, r, c)
            row[key] = v
        if not row.get("activite"):
            continue
        # carry-forward for structure levels
        for k in ("niv1", "niv2", "niv3"):
            if row.get(k):
                prev[k] = row[k]
            else:
                row[k] = prev[k]
        row["_row"] = r  # preserve Excel row for write-back
        rows.append(row)

    return {"columns": _IMPACT_DMIA_COLS, "rows": rows, "header_row": hdr}


def _write_sheet_dmia(ws, edits: list[dict]) -> int:
    """
    Write DMIA edits to a single worksheet. Returns count of rows updated.
    Handles both GAT (4-col impact) and AMI (single-score) column layouts.
    """
    hdr = _find_hdr_row(ws)
    _WRITE_ALIASES: dict[str, list[str]] = {
        "niv1":          ["structure niveau 1", "direction", "pole"],
        "niv2":          ["structure niveau 2", "departement"],
        "niv3":          ["structure niveau 3"],
        "activite":      ["activite", "activité"],
        "im_1h":         ["im 1h"],
        "im_4h":         ["im 4h"],
        "im_1j":         ["im 1j"],
        "im_2_3j":       ["im 2-3j"],
        "di_1h":         ["di 1h"],
        "di_4h":         ["di 4h"],
        "di_1j":         ["di 1j"],
        "di_2_3j":       ["di 2-3j"],
        "jr_1h":         ["jr 1h"],
        "jr_4h":         ["jr 4h"],
        "jr_1j":         ["jr 1j"],
        "jr_2_3j":       ["jr 2-3j"],
        "fin_1h":        ["fin 1h", "fi"],
        "fin_4h":        ["fin 4h"],
        "fin_1j":        ["fin 1j"],
        "fin_2_3j":      ["fin 2-3j"],
        # Single-score AMI columns
        "im_single":     ["im"],
        "di_single":     ["di"],
        "jr_single":     ["jr"],
        "score_1h":      ["score 1h", "score", "niveau d impact"],
        "dmia_exprimee": ["dmia exprimee", "dmia exprimée", "dmia exprime"],
        "dmia_preconise":["dmia preconise", "dmia préconisé", "dmia recommandee",
                          "dmia recommandée", "dmia arbitree", "dmia arbitrée"],
        "commentaires":  ["premieres actions", "premières actions", "commentaires"],
    }
    col_map: dict[str, int] = {}
    for c in range(1, (ws.max_column or 30) + 1):
        raw = _strip_accents(str(ws.cell(hdr, c).value or "").strip().lower())
        for logical, aliases in _WRITE_ALIASES.items():
            if raw in aliases:
                if logical not in col_map:
                    col_map[logical] = c
                break

    act_col = col_map.get("activite", 0)
    if not act_col:
        return 0

    # Build act_to_rows as a list per activity name — the same activity can appear
    # on multiple rows (e.g. "Interruption < 1" and "Interruption ≥ 5"), and ALL
    # of them must be updated. Using a dict would silently drop all but the last.
    act_to_rows: dict[str, list[int]] = {}
    for r in range(hdr + 1, (ws.max_row or hdr) + 1):
        v = _cell_val(ws, r, act_col)
        if v:
            key = _strip_accents(v.lower().strip())
            act_to_rows.setdefault(key, []).append(r)

    updated = 0
    for edit in edits:
        act = _strip_accents(edit.get("activite", "").lower().strip())
        target_rows = act_to_rows.get(act, [])
        if not target_rows:
            continue
        wrote = False
        for target_row in target_rows:
            for key, col_idx in col_map.items():
                # Map single-score AMI cols
                edit_key = key
                if key == "im_single":   edit_key = "im_1h"
                elif key == "di_single": edit_key = "di_1h"
                elif key == "jr_single": edit_key = "jr_1h"
                if edit_key in edit and edit_key not in ("niv1","niv2","niv3","activite"):
                    val = edit[edit_key]
                    ws.cell(target_row, col_idx).value = val or None
                    wrote = True
        if wrote:
            updated += 1
    return updated


def find_fiche_activity_candidates(fiche_path: str | Path,
                                    synth_activity: str,
                                    threshold: float = 0.15) -> list[dict]:
    """
    Search a fiche .docx for activities similar to synth_activity.
    Returns list of {activity, similarity} sorted by similarity desc.
    """
    doc = Document(str(fiche_path))
    synth_key = _strip_accents(synth_activity.lower().strip())
    synth_words = set(w for w in synth_key.split() if len(w) > 2)
    candidates: list[dict] = []
    seen: set = set()

    for table in doc.tables:
        if not table.rows:
            continue
        h = " ".join(_strip_accents(c.text.lower()) for c in table.rows[0].cells)
        if "dmia" not in h:
            continue
        if not any(kw in h for kw in ("processus", "activit", "designation")):
            continue
        for row in table.rows[1:]:
            act_raw = row.cells[0].text.strip()
            if not act_raw or act_raw in seen:
                continue
            seen.add(act_raw)
            act_key = _strip_accents(act_raw.lower().strip())
            act_words = set(w for w in act_key.split() if len(w) > 2)
            # Exact match
            if act_key == synth_key:
                return [{"activity": act_raw, "similarity": 1.0}]
            # Word overlap
            if act_words and synth_words:
                overlap = len(synth_words & act_words) / max(len(synth_words | act_words), 1)
            else:
                overlap = 0.0
            # Substring bonus
            if synth_key in act_key or act_key in synth_key:
                overlap = max(overlap, 0.5)
            if overlap >= threshold:
                candidates.append({"activity": act_raw, "similarity": round(overlap, 2)})

    candidates.sort(key=lambda x: -x["similarity"])
    return candidates[:5]


def find_unmatched_activities(edits: list[dict],
                               linked_fiches_paths: list[dict]) -> list[dict]:
    """
    For each edit whose activity has no strong match (≥0.45) in any linked fiche,
    return near-match candidates from each fiche.

    linked_fiches_paths: [{id, project_name, path}]

    Returns list of:
    {
      "activite": str,
      "dmia_exprimee": str,
      "candidates": [
          {"fiche_id": int, "fiche_name": str, "activity": str, "similarity": float}
      ]
    }
    """
    # Activities to check — include those with or without DMIA change
    relevant_edits = [e for e in edits if e.get("activite")]
    unmatched = []

    for edit in relevant_edits:
        synth_act = _strip_accents(edit["activite"].lower().strip())
        synth_words = set(w for w in synth_act.split() if len(w) > 2)
        all_candidates = []

        for fiche_info in linked_fiches_paths:
            path = fiche_info.get("path")
            if not path or not Path(path).exists():
                continue
            try:
                fiche_candidates = find_fiche_activity_candidates(Path(path), edit["activite"])
                for c in fiche_candidates:
                    all_candidates.append({
                        "fiche_id":   fiche_info["id"],
                        "fiche_name": fiche_info["project_name"],
                        "activity":   c["activity"],
                        "similarity": c["similarity"],
                    })
            except Exception:
                continue

        # Check if there was a strong match (≥0.45) — if yes, no need to report
        if any(c["similarity"] >= 0.45 for c in all_candidates):
            continue  # already matched automatically

        if all_candidates:
            all_candidates.sort(key=lambda x: -x["similarity"])
            unmatched.append({
                "activite":      edit["activite"],
                "dmia_exprimee": edit["dmia_exprimee"],
                "candidates":    all_candidates[:3],
            })
        else:
            # No candidates at all
            unmatched.append({
                "activite":      edit["activite"],
                "dmia_exprimee": edit["dmia_exprimee"],
                "candidates":    [],
            })

    return unmatched


def write_impact_dmia(synthese_path: str | Path, edits: list[dict]) -> None:
    """
    Apply edits to ALL DMIA-related sheets in-place:
      - 'Impact DMIA' / 'Impact'        (GAT / AMI main impact sheet)
      - 'DMIA'                           (AMI: flat DMIA list)
      - 'DMIA arbitrée'                  (AMI: arbitrated DMIA)
    A single wb.save() call updates all sheets atomically.
    """
    from openpyxl import load_workbook as _lw
    wb = _lw(str(synthese_path))

    def _find_sheet(*keywords_sets):
        """Find first sheet whose name matches ALL keyword sets (OR within each set)."""
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if all(any(kw in sl for kw in kws) for kws in keywords_sets):
                return wb[sn]
        return None

    # Update every DMIA-related sheet in a single wb.save() call
    sheets_to_update = []

    # 1. Main impact+DMIA sheet (GAT: "Impact DMIA", AMI: "Impact")
    s = _find_sheet(["impact"], ["dmia"])
    if s is None:
        s = _find_sheet(["impact"])
    if s:
        sheets_to_update.append(s)

    # 2. Standalone DMIA sheet (AMI: "DMIA")
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if sl == "dmia":
            sheets_to_update.append(wb[sn]); break

    # 3. Arbitrated DMIA sheet (AMI: "DMIA arbitrée")
    s = _find_sheet(["dmia"], ["arbit"])
    if s:
        sheets_to_update.append(s)

    if not sheets_to_update:
        return

    for ws in sheets_to_update:
        _write_sheet_dmia(ws, edits)

    wb.save(str(synthese_path))


def read_applications_it(synthese_path: str | Path) -> dict:
    """Read the 'Applications IT' sheet. Returns {columns, rows}."""
    wb = load_workbook(str(synthese_path), data_only=True)
    ws = None
    # Priority order: "Applications IT" > "Redémmarage des applications" > any "Application*" sheet with DMIA column
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "application" in sl and ("it" in sl.split() or sl == "applications it"):
            ws = wb[sn]; break
    if ws is None:
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if "redemmar" in sl or "redem" in sl or "restart" in sl:
                ws = wb[sn]; break
    if ws is None:
        # Fall back to any sheet named "Applications" that has a DMIA column
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if "application" in sl:
                candidate = wb[sn]
                hdr_c = _find_hdr_row(candidate)
                header_vals = [_strip_accents(str(candidate.cell(hdr_c, c).value or "").lower())
                               for c in range(1, min((candidate.max_column or 10)+1, 15))]
                if any("dmia" in v for v in header_vals):
                    ws = candidate; break
    if not ws:
        return {"columns": _APPS_IT_COLS, "rows": []}

    hdr = _find_hdr_row(ws)
    _APP_ALIASES = {
        "structure niveau 1": "niv1", "structure niveau 2": "niv2",
        "structure niveau 3": "niv3",
        # AMI structure columns
        "pole": "niv1", "direction": "niv1", "departement": "niv2",
        "direction departement": "niv2", "lots": "niv3",
        "application": "application",
        "niveau de criticite": "criticite", "criticite": "criticite", "criticité": "criticite",
        "dmia": "dmia", "pmdt": "pmdt",
        "contournement envisageable": "contournement", "contournement": "contournement",
        "commentaires": "commentaires",
    }
    col_map: dict[int, str] = {}
    for c in range(1, (ws.max_column or 20) + 1):
        raw = _strip_accents(str(ws.cell(hdr, c).value or "").strip().lower())
        if raw in _APP_ALIASES:
            col_map[c] = _APP_ALIASES[raw]

    rows = []
    prev = {"niv1": "", "niv2": "", "niv3": ""}
    for r in range(hdr + 1, (ws.max_row or hdr) + 1):
        row: dict = {}
        for c, key in col_map.items():
            row[key] = _cell_val(ws, r, c)
        if not row.get("application"):
            continue
        for k in ("niv1", "niv2", "niv3"):
            if row.get(k):
                prev[k] = row[k]
            else:
                row[k] = prev[k]
        row["_row"] = r
        rows.append(row)

    return {"columns": _APPS_IT_COLS, "rows": rows}


def write_applications_it(synthese_path: str | Path, edits: list[dict]) -> None:
    """
    Apply edits to ALL application-related sheets:
      - 'Applications IT'                 (GAT format)
      - 'Redémmarage des applications'    (AMI format)
    """
    from openpyxl import load_workbook as _lw
    wb = _lw(str(synthese_path))

    sheets_to_update: list = []

    # 1. "Applications IT" (GAT: sheet with "application" + "it")
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "application" in sl and ("it" in sl.split() or sl == "applications it"):
            sheets_to_update.append(wb[sn]); break

    # 2. "Redémmarage des applications" (AMI) or any app sheet with DMIA column
    for sn in wb.sheetnames:
        sl = _strip_accents(sn.strip().lower())
        if "redemmar" in sl or "redem" in sl or "restart" in sl:
            sheets_to_update.append(wb[sn]); break

    # 3. Fallback: any "application" sheet with a DMIA column not already found
    if not sheets_to_update:
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if "application" in sl:
                candidate = wb[sn]
                hdr_c = _find_hdr_row(candidate)
                header_vals = [_strip_accents(str(candidate.cell(hdr_c, c).value or "").lower())
                               for c in range(1, min((candidate.max_column or 10)+1, 15))]
                if any("dmia" in v for v in header_vals):
                    sheets_to_update.append(candidate); break

    if not sheets_to_update:
        return

    ws = None  # reassigned per sheet below
    if ws is None:
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if "redemmar" in sl or "redem" in sl:
                ws = wb[sn]; break
    if ws is None:
        for sn in wb.sheetnames:
            sl = _strip_accents(sn.strip().lower())
            if "application" in sl:
                candidate = wb[sn]
                hdr_c = _find_hdr_row(candidate)
                header_vals = [_strip_accents(str(candidate.cell(hdr_c, c).value or "").lower())
                               for c in range(1, min((candidate.max_column or 10)+1, 15))]
                if any("dmia" in v for v in header_vals):
                    sheets_to_update.append(candidate); break

    if not sheets_to_update:
        return

    _APP_WRITE = {
        "criticite":     ["niveau de criticite", "criticite", "criticité"],
        "dmia":          ["dmia"],
        "pmdt":          ["pmdt"],
        "contournement": ["contournement envisageable", "contournement"],
        "commentaires":  ["commentaires"],
    }

    for ws in sheets_to_update:
        hdr = _find_hdr_row(ws)
        col_map: dict[str, int] = {}
        for c in range(1, (ws.max_column or 20) + 1):
            raw = _strip_accents(str(ws.cell(hdr, c).value or "").strip().lower())
            # app name column
            if "application" in raw and "application" not in col_map:
                col_map["application"] = c; continue
            for key, aliases in _APP_WRITE.items():
                if any(a in raw for a in aliases):
                    if key not in col_map:
                        col_map[key] = c
                    break

        app_col = col_map.get("application", 0)
        if not app_col:
            continue

        app_to_row: dict[str, int] = {}
        for r in range(hdr + 1, (ws.max_row or hdr) + 1):
            v = _cell_val(ws, r, app_col)
            if v:
                app_to_row[_strip_accents(v.lower().strip())] = r

        for edit in edits:
            # Always match by application name — _row is sheet-specific
            target_row = app_to_row.get(
                _strip_accents(edit.get("application","").lower().strip()))
            if not target_row:
                continue
            for key, col_idx in col_map.items():
                if key in edit and key not in ("application", "niv1", "niv2", "niv3"):
                    ws.cell(target_row, col_idx).value = edit[key] or None

    wb.save(str(synthese_path))


def _set_cell_text(cell, value: str) -> None:
    """Safely replace all text in a docx table cell while preserving formatting."""
    value = (value or "").strip()
    # Clear all runs in all paragraphs
    for para in cell.paragraphs:
        for run in para.runs:
            run.text = ""
    # Write to first paragraph's first run (or add one)
    if cell.paragraphs:
        p = cell.paragraphs[0]
        if p.runs:
            p.runs[0].text = value
        else:
            p.add_run(value)


def update_fiche_dmia_table(fiche_path: str | Path, updates: dict) -> int:
    """
    Write DMIA updates back into the fiche .docx.
    updates: {activity_name_lower_stripped: {"dmia_exprimee": str}}
    Returns count of rows updated.
    """
    doc = Document(str(fiche_path))
    updated = 0

    # Pre-build fuzzy lookup: stripped key → original key in updates
    # Allows partial/fuzzy matching when synthèse uses slightly different names.
    # Strategy (in priority order):
    #   1. Exact match
    #   2. Synthèse name is fully contained in fiche name
    #      e.g. "bureau d'ordre" ⊂ "bureau d'ordre cotunace : boc"  → match
    #   3. Jaccard word-overlap ≥ 0.40
    def _best_match(act_key: str) -> "str | None":
        if act_key in updates:
            return act_key
        act_words = set(w for w in act_key.split() if len(w) > 1)
        best, best_score = None, 0
        for uk in updates:
            uk_words = set(w for w in uk.split() if len(w) > 1)
            if not uk_words:
                continue
            # Strategy 2: all synthèse words appear in the fiche activity name
            if uk_words and uk_words.issubset(act_words):
                return uk
            # Strategy 3: Jaccard overlap (lowered threshold to 0.40)
            overlap = len(act_words & uk_words) / max(len(act_words | uk_words), 1)
            if overlap >= 0.40 and overlap > best_score:
                best, best_score = uk, overlap
        return best

    for table in doc.tables:
        if not table.rows:
            continue
        # Find header row — accepts "Désignation", "Activité", "Processus"
        hdr_row_idx = None
        for ri, row in enumerate(table.rows):
            row_text = " ".join(_strip_accents(c.text.lower()) for c in row.cells)
            if "dmia" in row_text and any(kw in row_text for kw in
                    ("designation", "activit", "processus")):
                hdr_row_idx = ri; break
        if hdr_row_idx is None:
            continue

        hdr_cells = table.rows[hdr_row_idx].cells
        act_col = dmia_col = None
        for ci, cell in enumerate(hdr_cells):
            raw = _strip_accents(cell.text.strip().lower())
            if act_col is None and any(kw in raw for kw in
                    ("designation", "activit", "processus")):
                act_col = ci
            if dmia_col is None and "dmia" in raw and "exprim" in raw:
                dmia_col = ci
            elif dmia_col is None and "dmia" in raw:
                dmia_col = ci

        if act_col is None or dmia_col is None:
            continue

        for row in table.rows[hdr_row_idx + 1:]:
            if act_col >= len(row.cells):
                continue
            act_key = _strip_accents(row.cells[act_col].text.strip().lower())
            matched_key = _best_match(act_key)
            if matched_key:
                upd = updates[matched_key]
                if "dmia_exprimee" in upd and dmia_col < len(row.cells):
                    _set_cell_text(row.cells[dmia_col], upd["dmia_exprimee"])
                    updated += 1

    if updated:
        doc.save(str(fiche_path))
    return updated


def update_fiche_applications_table(fiche_path: str | Path, updates: dict) -> int:
    """
    Write application updates back into the fiche .docx.
    updates: {app_name_lower_stripped: {"criticite": str, "dmia": str, "pmdt": str, "contournement": str}}
    Returns count of rows updated.
    """
    doc = Document(str(fiche_path))
    updated = 0

    def _app_best_match(app_key: str) -> "str | None":
        if app_key in updates:
            return app_key
        # Case-insensitive substring match
        for uk in updates:
            if uk in app_key or app_key in uk:
                return uk
        return None

    for table in doc.tables:
        if not table.rows:
            continue
        hdr_row_idx = None
        for ri, row in enumerate(table.rows):
            row_text = " ".join(_strip_accents(c.text.lower()) for c in row.cells)
            if "application" in row_text and "dmia" in row_text:
                hdr_row_idx = ri; break
        if hdr_row_idx is None:
            continue

        hdr_cells = table.rows[hdr_row_idx].cells
        field_cols: dict[str, int] = {}
        app_col = 0
        for ci, cell in enumerate(hdr_cells):
            raw = _strip_accents(cell.text.strip().lower())
            if "application" in raw and ci == 0:
                app_col = ci
            elif "criticite" in raw or "criticit" in raw:
                field_cols["criticite"] = ci
            elif "dmia" in raw and "pmdt" not in raw:
                field_cols["dmia"] = ci
            elif "pmdt" in raw:
                field_cols["pmdt"] = ci
            elif "contournement" in raw:
                field_cols["contournement"] = ci

        for row in table.rows[hdr_row_idx + 1:]:
            if app_col >= len(row.cells):
                continue
            app_key = _strip_accents(row.cells[app_col].text.strip().lower())
            matched_key = _app_best_match(app_key)
            if matched_key:
                upd = updates[matched_key]
                for field, col_idx in field_cols.items():
                    if field in upd and col_idx < len(row.cells):
                        _set_cell_text(row.cells[col_idx], upd[field])
                updated += 1

    if updated:
        doc.save(str(fiche_path))
    return updated


def extract_fiche_equipment(fiche_path: str | Path) -> dict:
    """
    Extract "Autres équipements et outils de communication" from a BIA fiche (.docx).

    Supports two formats:
      - AMI format: identity in table with "Entité" row (multi-col), equipment table
                    header row = "Désignation | H+2 | H+4 | J+1 | ..."
      - GAT format: identity in table with "Entité" row (2-col), same equipment header

    Returns:
    {
      "entity":     str,
      "department": str,
      "items": [
          {"name": str, "horizons": {"H+4": 2, "J+1": 1, ...}},
          ...
      ]
    }
    """
    doc = Document(str(fiche_path))

    # ── Extract entity + department from identity table ────────────────────────
    entity = ""
    department = ""
    TIME_RE = re.compile(r"^(H\+?\d+|J\+?\d+)$", re.IGNORECASE)

    for table in doc.tables:
        for row in table.rows:
            cells = [c.text.strip() for c in row.cells]
            if cells and _strip_accents(cells[0].lower()) in ("entite", "entité"):
                # Find the non-empty value cell
                for cv in cells[1:]:
                    if cv and cv not in ("", "-"):
                        entity = cv; break
                break

    # Department falls back to entity (for fiches, entity IS the department)
    department = entity

    # ── Find the equipment table (header has "Désignation" + time columns) ─────
    eqt_table = None
    for table in doc.tables:
        if not table.rows:
            continue
        first_row_cells = [_strip_accents(c.text.strip().lower()) for c in table.rows[0].cells]
        if any("designation" in c for c in first_row_cells):
            eqt_table = table
            # Don't break — use the LAST matching table (the actual data one, not TOC)

    items: list[dict] = []
    if eqt_table and len(eqt_table.rows) > 1:
        # Parse header: find time horizon columns
        hdr_cells = [c.text.strip() for c in eqt_table.rows[0].cells]
        time_col_map: dict[str, int] = {}
        for ci, h in enumerate(hdr_cells):
            if TIME_RE.match(h.replace(" ", "")):
                key = h.strip()
                # Normalise "H4" → "H+4", "J1" → "J+1"
                if "+" not in key:
                    key = re.sub(r"([HJ])(\d)", r"\1+\2", key)
                time_col_map[key] = ci

        for row in eqt_table.rows[1:]:
            cells = [c.text.strip() for c in row.cells]
            if not cells:
                continue
            name = cells[0]
            if not name or name in ("-", ""):
                continue
            horizons: dict[str, int] = {}
            for label, ci in time_col_map.items():
                if ci < len(cells):
                    try:
                        v = float(cells[ci].replace(",", ".") or "0")
                        if v > 0:
                            horizons[label] = int(v)
                    except ValueError:
                        pass
            items.append({"name": name, "horizons": horizons})

    return {
        "entity":     entity or Path(fiche_path).stem,
        "department": department or entity,
        "items":      items,
    }


def _build_dynamic_tree(
    entities:    list[list[str]],
    struct_cols: list[int],
    leaf_data:   dict[tuple, list[dict]],
    mode:        str,
    depth:       int,
) -> dict:
    """Build a nested tree from flat entity rows with dynamic depth."""
    _LEVEL_NAMES = ["root", "n1", "n2", "n3", "n4"]

    root = {"name": "Organisation", "_level": "root",
            "_path": (), "children": []}

    def _get_or_create(parent: dict, name: str, level: int, path: tuple) -> dict:
        for child in parent.get("children", []):
            if child["name"] == name:
                return child
        node: dict = {
            "name":     name,
            "_level":   _LEVEL_NAMES[min(level, 4)],
            "_path":    path,
            "children": [],
        }
        parent.setdefault("children", []).append(node)
        return node

    for vals in entities:
        # Build path ignoring empty levels
        node = root
        for lvl_idx, val in enumerate(vals):
            if not val:
                continue
            path = tuple(vals[: lvl_idx + 1])
            node = _get_or_create(node, val, lvl_idx + 1, path)

    # Annotate leaves with DMIA / app data
    def _annotate(node: dict) -> None:
        children = node.get("children", [])
        is_leaf = not children

        if is_leaf:
            # Normalize the path key the same way DMIA data was stored
            norm_key = _norm_struct_key(list(node["_path"]), depth)
            acts = leaf_data.get(norm_key, [])

            if mode == "activities":
                node["_activities"] = acts
                valid_h = [a["hours"] for a in acts if a.get("hours") is not None]
                worst = min(valid_h) if valid_h else None
                node["_dmia_hours"]    = worst
                node["_dmia_color"]    = _dmia_color(worst)
                worst_act = min((a for a in acts if a.get("hours") is not None),
                                key=lambda x: x["hours"], default=None)
                node["_dmia_critical"] = worst_act["dmia"] if worst_act else ""
                node["_dmia_label"]    = worst_act["dmia"] if worst_act else ""
            else:
                node["_applications"] = acts
                valid_h = [a["hours"] for a in acts if a.get("hours") is not None]
                worst = min(valid_h) if valid_h else None
                node["_dmia_hours"]    = worst
                # Use worst criticality color if hours unavailable
                crit_order = {"rouge": 0, "orange": 1, "vert": 2, "none": 3}
                worst_color = min((a.get("dmia_color", "none") for a in acts),
                                  key=lambda c: crit_order.get(c, 3), default="none")
                node["_dmia_color"]    = worst_color if not valid_h else _dmia_color(worst)
                worst_app = (min((a for a in acts if a.get("hours") is not None),
                                 key=lambda x: x["hours"], default=None)
                             or (acts[0] if acts else None))
                node["_dmia_critical"] = worst_app["dmia"] if worst_app else ""
        else:
            for child in children:
                _annotate(child)
            child_hours = [c["_dmia_hours"] for c in children if c.get("_dmia_hours") is not None]
            worst = min(child_hours) if child_hours else None
            node["_dmia_hours"]    = worst
            node["_dmia_color"]    = _dmia_color(worst)
            node["_dmia_critical"] = ""
            if mode == "activities":
                all_acts: list[dict] = []
                for child in children:
                    all_acts.extend(child.get("_activities", []))
                node["_activities"] = all_acts
            else:
                all_apps: list[dict] = []
                for child in children:
                    all_apps.extend(child.get("_applications", []))
                # Use worst color from children if no hours
                crit_order = {"rouge": 0, "orange": 1, "vert": 2, "none": 3}
                worst_color = min((c.get("_dmia_color", "none") for c in children),
                                  key=lambda c: crit_order.get(c, 3), default="none")
                if not child_hours:
                    node["_dmia_color"] = worst_color
                node["_applications"] = all_apps

    _annotate(root)
    return root


def _build_org_tree(
    entities: list[dict],
    dmia_by_entity: dict | None = None,
) -> dict:
    """
    Convert flat entity list → nested tree dict for D3 hierarchy,
    annotated with DMIA heatmap colours.
    """
    root_children: list[dict] = []
    div_index: dict[str, dict] = {}

    for e in entities:
        div  = e["division"]    or ""
        unit = e["unite"]       or ""
        dept = e["departement"] or ""

        # Division node
        if div not in div_index:
            node: dict = {
                "name": div or "—", "_level": "division",
                "_div": div, "_unit": "", "_dept": "", "children": [],
            }
            div_index[div] = node
            root_children.append(node)
        div_node = div_index[div]

        if not unit and not dept:
            continue

        # Unité node
        unit_node = next((c for c in div_node["children"] if c.get("_unit") == unit), None)
        if unit_node is None:
            unit_node = {
                "name": unit or "—", "_level": "unite",
                "_div": div, "_unit": unit, "_dept": "", "children": [],
            }
            div_node["children"].append(unit_node)

        if not dept:
            continue

        # Département node (leaf)
        if not any(c.get("_dept") == dept for c in unit_node["children"]):
            unit_node["children"].append({
                "name": dept, "_level": "departement",
                "_div": div, "_unit": unit, "_dept": dept,
            })

    # ── DMIA annotation pass (bottom-up) ──────────────────────────────────────
    def _annotate(node: dict) -> None:
        level    = node.get("_level")
        children = node.get("children", [])

        # Treat as a leaf if it IS a département, OR if it has no children
        # (e.g. a Division or Unité that appears alone in the template because
        #  the client stores all its activities at the division level with no
        #  sub-units — Audit, RH, Réassurance, Juridique, etc.).
        is_leaf = (level == "departement") or not children

        if is_leaf:
            # Direct DMIA lookup: key matches how extract_org_tree reads cols 2,3,4
            key = (node.get("_div", ""), node.get("_unit", ""), node.get("_dept", ""))
            acts = (dmia_by_entity or {}).get(key, [])
            node["_activities"] = acts
            valid_hours = [a["hours"] for a in acts if a["hours"] is not None]
            worst = min(valid_hours) if valid_hours else None
            node["_dmia_hours"]    = worst
            node["_dmia_color"]    = _dmia_color(worst)
            worst_act = min(
                (a for a in acts if a["hours"] is not None),
                key=lambda x: x["hours"], default=None,
            )
            node["_dmia_critical"] = worst_act["dmia"] if worst_act else ""

        else:
            for child in children:
                _annotate(child)
            child_hours = [c["_dmia_hours"] for c in children if c.get("_dmia_hours") is not None]
            worst = min(child_hours) if child_hours else None
            node["_dmia_hours"]    = worst
            node["_dmia_color"]    = _dmia_color(worst)
            node["_dmia_critical"] = ""
            # Aggregate activities for info panel / heatmap
            all_acts: list[dict] = []
            for child in children:
                all_acts.extend(child.get("_activities", []))
            node["_activities"] = all_acts

    root_node: dict = {
        "name": "Organisation", "_level": "root",
        "_div": "", "_unit": "", "_dept": "",
        "children": root_children,
    }
    _annotate(root_node)

    # Stats
    stats: dict[str, int] = {"division": 0, "unite": 0, "departement": 0,
                              "rouge": 0, "orange": 0, "vert": 0, "none": 0}

    def _count(nodes: list[dict]) -> None:
        for n in nodes:
            lvl = n.get("_level", "")
            if lvl in stats:
                stats[lvl] += 1
            col = n.get("_dmia_color", "none")
            if col in stats:
                stats[col] += 1
            if "children" in n:
                _count(n["children"])

    _count(root_children)
    stats["total"] = stats["division"] + stats["unite"] + stats["departement"]
    root_node["_stats"] = stats
    return root_node


# ─────────────────────────────────────────────────────────────────────────────
# LOAD — XLSX WRITER
# ─────────────────────────────────────────────────────────────────────────────

HEADER_ROW = 5   # Row index (1-based) where column headers live in each sheet
DATA_START_ROW = 6  # First data row (1-based)

# ── Flat-sheet colour rules ───────────────────────────────────────────────────
# Some sheets (e.g. "Redémarrages des applications") are NOT keyed on entity
# rows — every data row is written directly.  For these sheets load() calls
# _write_flat_rows() instead of the entity-matching path.
#
# Each entry maps: sheet_name → {col_header → {cell_value → (bg_hex, fg_hex)}}
# Any cell whose value matches a key gets a solid fill + bold white text.

from openpyxl.styles import PatternFill as _PatternFill, Font as _Font

_FLAT_SHEETS: dict[str, dict] = {
    "Redémarrages des applications": {
        "Lot": {
            "Lot 1": ("FF0000", "FFFFFF"),   # bright red    / white
            "Lot 2": ("FF8C00", "FFFFFF"),   # orange        / white
            "Lot 3": ("00B050", "FFFFFF"),   # green         / white
        }
    }
}

def _write_flat_rows(ws, data_rows: list[dict], colour_rules: dict) -> None:
    """Write `data_rows` into a flat (non-entity-keyed) sheet, globally sorted
    by the first colour-coded column (e.g. "Lot").

    Strategy: read-merge-sort-delete-rewrite
    ----------------------------------------
    On every call we:
      1. Read ALL existing data rows currently in the sheet.
      2. Merge them with the incoming `data_rows`.
      3. Sort the combined list by lot rank (Lot 1 → Lot 2 → Lot 3).
      4. Delete every data row from DATA_START_ROW downward.
      5. Write the sorted combined list back from DATA_START_ROW, with colours.

    This guarantees a globally sorted sheet regardless of the order in which
    individual fiche calls arrive.
    """
    if not data_rows:
        return

    # ── Build header→col-index map ────────────────────────────────────────────
    col_map: dict[str, int] = {}
    for cell in ws[HEADER_ROW]:
        if cell.value and str(cell.value).strip():
            col_map[str(cell.value).strip()] = cell.column

    if not col_map:
        return

    # Identify the "sort key" column (first column that has colour rules)
    sort_col_header = next(iter(colour_rules), None)        # e.g. "Lot"
    sort_col_idx    = col_map.get(sort_col_header)          # 1-based col index
    lot_order: list[str] = list(colour_rules.get(sort_col_header, {}).keys())
    # e.g. ["Lot 1", "Lot 2", "Lot 3"]
    lot_rank: dict[str, int] = {lot: i for i, lot in enumerate(lot_order)}

    # Ordered list of all column headers (left-to-right) for read/write
    ordered_headers: list[tuple[str, int]] = sorted(col_map.items(), key=lambda kv: kv[1])

    def _resolve_col(header: str) -> int | None:
        idx = col_map.get(header)
        if idx is not None:
            return idx
        for h, ci in col_map.items():
            if h.strip().lower() == header.strip().lower():
                return ci
        return None

    # ── Step 1: read all existing data rows ───────────────────────────────────
    placeholder_values: set[str] = set(lot_order)
    existing_rows: list[dict] = []
    max_r = ws.max_row or (DATA_START_ROW - 1)
    for r in range(DATA_START_ROW, max_r + 1):
        row_dict: dict = {}
        for hdr, col_idx in ordered_headers:
            v = ws.cell(row=r, column=col_idx).value
            if v is not None:
                row_dict[hdr] = v
        if not row_dict:
            continue  # truly blank row — skip
        # Skip placeholder-only rows (e.g. a row whose sort-col = "Lot 1" but
        # all other cells are empty — these were seeded by the template).
        sort_val = str(row_dict.get(sort_col_header, "")).strip()
        if sort_val in placeholder_values:
            other_vals = [v for h, v in row_dict.items() if h != sort_col_header]
            if not any(str(v).strip() for v in other_vals):
                continue  # placeholder — do not preserve
        existing_rows.append(row_dict)

    # ── Step 2: merge ─────────────────────────────────────────────────────────
    combined: list[dict] = existing_rows + data_rows

    # ── Step 3: sort by lot rank ──────────────────────────────────────────────
    combined.sort(key=lambda rd: lot_rank.get(
        str(rd.get(sort_col_header, "")).strip(), len(lot_order)))

    # ── Step 4: delete all data rows ─────────────────────────────────────────
    current_max = ws.max_row or (DATA_START_ROW - 1)
    if current_max >= DATA_START_ROW:
        ws.delete_rows(DATA_START_ROW, current_max - DATA_START_ROW + 1)

    # ── Step 5: write sorted rows back ───────────────────────────────────────
    for i, row_dict in enumerate(combined):
        target_row = DATA_START_ROW + i
        for col_header, value in row_dict.items():
            col_idx = _resolve_col(col_header)
            if col_idx is None:
                continue
            cell = ws.cell(row=target_row, column=col_idx)
            cell.value = value
            # Apply colour to the sort-key column (e.g. "Lot")
            if sort_col_header and col_idx == sort_col_idx:
                rule = colour_rules[sort_col_header].get(str(value or "").strip())
                if rule:
                    bg_hex, fg_hex = rule
                    cell.fill = _PatternFill("solid", fgColor=bg_hex)
                    cell.font = _Font(bold=True, color=fg_hex,
                                      name=(cell.font.name or "Calibri"),
                                      size=(cell.font.size or 11))

# Columns B, C, D hold Division / Unité / Département — used for row matching.
# We scan all three and pick the best fuzzy match.
DEPT_COLUMNS = [2, 3, 4]  # 1-based column indices (B=2, C=3, D=4)

# Words that are column header labels, NOT actual org-unit names.
# Some templates accidentally leave these as placeholder text in data rows.
_HEADER_PLACEHOLDER_WORDS: frozenset[str] = frozenset({
    "division", "unité", "unite", "département", "departement",
    "entité", "entite", "structure", "activité", "activite",
    "xx", "x",
})

# Regex to strip leading org-level prefix words from entity names.
# e.g. "Département Achat" → "Achat", "Division Réassurance" → "Réassurance"
_ORG_PREFIX_RE = re.compile(
    r"^(?:département|departement|division|direction\s+de\s+la|direction\s+du|"
    r"direction\s+des|direction|service|unité|unite|pôle|pole|dept\.?)\s+",
    re.IGNORECASE,
)


def _strip_org_prefix(name: str) -> str:
    """Remove org-level prefix ('Département', 'Division', etc.) from entity name."""
    return _ORG_PREFIX_RE.sub("", name).strip()


def _find_department_row(ws, dept_name: str) -> Optional[int]:
    """
    Find which row in the sheet corresponds to dept_name.
    Uses rapidfuzz for fuzzy matching because the synthesis template has typos
    (e.g. "ingenieurue" instead of "ingénierie") and accent variations.
    Returns the 1-based row index, or None if not found.

    Key improvements over the naïve approach:
      1. Placeholder rows (where the cell value IS a column header word like
         "Division", "Département") are excluded so they can never win a match.
      2. We try both the full entity name AND the prefix-stripped version
         (e.g. "Département Achat" → also try "Achat") so the actual dept row
         beats the placeholder words.
      3. Accent-normalised comparison handles é/e mismatches between fiches
         (filled with accents) and templates (sometimes accent-free).
      4. token_set_ratio handles subset relationships ("Risk Management" ⊂
         "Risk management et sécurité") alongside token_sort_ratio.
    """
    import unicodedata

    def _norm(s: str) -> str:
        """Lowercase + strip accents for robust fuzzy matching."""
        return "".join(
            c for c in unicodedata.normalize("NFD", s.lower())
            if unicodedata.category(c) != "Mn"
        )

    candidates: dict[int, str] = {}      # row_num → original cell text
    norm_candidates: dict[int, str] = {} # row_num → normalised cell text
    for row in ws.iter_rows(min_row=DATA_START_ROW):
        best_val: str | None = None
        # Iterate cols B→C→D; later col overwrites so Département (col D) wins
        # when it has a real value — that is the most specific org level.
        for col_idx in DEPT_COLUMNS:
            cell = ws.cell(row=row[0].row, column=col_idx)
            v = str(cell.value).strip() if cell.value else ""
            if v and v not in ("-", "") and v.lower() not in _HEADER_PLACEHOLDER_WORDS:
                best_val = v
        if best_val:
            rn = row[0].row
            candidates[rn] = best_val
            norm_candidates[rn] = _norm(best_val)

    if not candidates:
        return None

    # Build query variants: prefix-stripped first (more precise), then full name.
    stripped = _strip_org_prefix(dept_name)
    queries = [stripped, dept_name] if stripped != dept_name else [dept_name]

    # We track scores from BOTH scorers separately.
    # token_set_ratio is very permissive (subsets score 100) so we use it only
    # as a secondary signal; the primary gate is token_sort_ratio.
    sort_best_score = 0
    sort_best_key: int | None = None
    set_best_score  = 0
    set_best_key:  int | None = None

    for q in queries:
        q_norm = _norm(q)
        r_sort = process.extractOne(q_norm, norm_candidates, scorer=fuzz.token_sort_ratio)
        if r_sort and r_sort[1] > sort_best_score:
            sort_best_score = r_sort[1]
            sort_best_key   = r_sort[2]
        r_set = process.extractOne(q_norm, norm_candidates, scorer=fuzz.token_set_ratio)
        if r_set and r_set[1] > set_best_score:
            set_best_score = r_set[1]
            set_best_key   = r_set[2]

    # A match is accepted only when BOTH scorers agree on the same row AND
    # token_sort_ratio (the stricter one) is above the threshold.
    # This prevents false positives like "Direction GAT Invest" → "Direction GAT VIE"
    # (token_set_ratio=87, token_sort_ratio=72 → rejected) while still accepting
    # legitimate near-matches caused by minor wording differences.
    SORT_THRESHOLD = 88   # strict primary gate — prevents "GAT Invest"→"GAT VIE" (86.5)
    SET_THRESHOLD  = 60   # loose secondary confirmation

    if (sort_best_score >= SORT_THRESHOLD
            and set_best_score >= SET_THRESHOLD
            and sort_best_key == set_best_key):
        return sort_best_key

    # Fallback: if only token_sort_ratio is very high (>= 90), accept it even
    # without the set-ratio agreement (handles rare edge cases).
    if sort_best_score >= 90:
        return sort_best_key

    return None


def _get_column_map(ws) -> dict[str, int]:
    """Read header row → return {column_name: column_index (1-based)}."""
    col_map = {}
    for cell in ws[HEADER_ROW]:
        if cell.value:
            col_map[str(cell.value).strip()] = cell.column
    return col_map


def _copy_row_style(ws, source_row: int, target_row: int):
    """Copy cell styles from source_row to target_row (Division/Unité/Département cols)."""
    for col in range(1, ws.max_column + 1):
        src = ws.cell(row=source_row, column=col)
        tgt = ws.cell(row=target_row, column=col)
        if src.has_style:
            tgt._style = copy.copy(src._style)


def _resolved_cell_value(ws, row: int, col: int):
    """
    Return the effective value of a cell, even when it is a non-top-left cell
    inside a merged range (those cells hold None; we look up the master cell).
    """
    cell = ws.cell(row=row, column=col)
    if cell.value is not None:
        return cell.value
    # Search merged ranges to find if this cell is a secondary member
    for mr in ws.merged_cells.ranges:
        if row in range(mr.min_row, mr.max_row + 1) and col in range(mr.min_col, mr.max_col + 1):
            return ws.cell(row=mr.min_row, column=mr.min_col).value
    return None


def _clear_existing_rows(ws, first_row: int) -> None:
    """
    Delete all rows that belong to the same entity as first_row, except first_row
    itself (which is kept as the template row to write new data into).

    "Belongs to the same entity" means: the DEPT_COLUMNS in that row are either
    identical to first_row's values OR completely empty (a continuation row
    written by a previous `load()` call that copied the org-unit identifiers).

    We stop as soon as we see a row whose DEPT_COLUMNS differ AND are non-empty
    — that row belongs to a different entity.
    """
    import unicodedata

    def _norm(s: str) -> str:
        return "".join(
            c for c in unicodedata.normalize("NFD", s.lower())
            if unicodedata.category(c) != "Mn"
        )

    # Capture the entity key from first_row
    ref_vals = []
    for col in DEPT_COLUMNS:
        v = _resolved_cell_value(ws, first_row, col)
        ref_vals.append(str(v).strip() if v else "")
    ref_norm = _norm(" ".join(v for v in ref_vals if v))

    # Collect extra rows to delete (everything after first_row that still
    # matches this entity)
    rows_to_delete: list[int] = []
    row_idx = first_row + 1
    max_row = ws.max_row or first_row
    while row_idx <= max_row:
        row_vals = []
        for col in DEPT_COLUMNS:
            v = _resolved_cell_value(ws, row_idx, col)
            row_vals.append(str(v).strip() if v else "")
        row_text = " ".join(v for v in row_vals if v)
        # Empty org columns → continuation row from a previous run → delete it
        if not row_text:
            rows_to_delete.append(row_idx)
            row_idx += 1
            continue
        # Different entity → stop
        if _norm(row_text) != ref_norm:
            break
        # Same entity → mark for deletion
        rows_to_delete.append(row_idx)
        row_idx += 1

    # Unmerge any merged ranges that overlap the rows we are about to delete,
    # otherwise openpyxl raises an error or corrupts the sheet structure.
    if rows_to_delete:
        delete_set = set(rows_to_delete)
        ranges_to_unmerge = [
            mr for mr in list(ws.merged_cells.ranges)
            if any(r in range(mr.min_row, mr.max_row + 1) for r in delete_set)
        ]
        for mr in ranges_to_unmerge:
            ws.unmerge_cells(str(mr))

    # Delete collected rows in reverse order so row numbers stay valid
    for r in reversed(rows_to_delete):
        ws.delete_rows(r)


def _last_data_row(ws) -> int:
    """
    Return the 1-based index of the last existing data row in `ws`.

    Walks downward from DATA_START_ROW until it finds a stretch of fully blank
    rows in the org columns (B/C/D). Returns the row right before that.
    Used to append entities that are present in the fiches but absent from the
    pre-seeded recensement.
    """
    last = DATA_START_ROW - 1
    blank_streak = 0
    max_row = ws.max_row or DATA_START_ROW
    for r in range(DATA_START_ROW, max_row + 1):
        vals = [_resolved_cell_value(ws, r, c) for c in DEPT_COLUMNS]
        if any(v and str(v).strip() and str(v).strip() != "-" for v in vals):
            last = r
            blank_streak = 0
        else:
            blank_streak += 1
            if blank_streak > 3:
                break
    return last


def _append_entity_row(ws, fiche: "BIAFiche") -> int:
    """
    Create a brand-new row for `fiche` at the bottom of the data block of `ws`,
    copying the style of the row above so the synthèse remains visually
    consistent.  Returns the 1-based row index of the newly created row.

    Used when an entity is present in a fiche but missing from the recensement.
    Rather than dropping its data, we extend the synthèse with a fresh row so
    nothing is lost.
    """
    last = _last_data_row(ws)
    new_row = last + 1
    # Copy style from `last` (or from HEADER_ROW+1 if the sheet is empty)
    style_src = last if last >= DATA_START_ROW else HEADER_ROW + 1
    for col in range(1, (ws.max_column or 10) + 1):
        try:
            src = ws.cell(row=style_src, column=col)
            tgt = ws.cell(row=new_row, column=col)
            if src.has_style:
                tgt._style = copy.copy(src._style)
        except Exception:
            pass

    # Populate Division / Unité / Département from the fiche
    # Convention: entity_name lives in column D (Département / Niveau 3).
    # Fall back to placing it in column B if no recensement context exists.
    try:
        ws.cell(row=new_row, column=2).value = fiche.division or fiche.entity_name or "-"
        ws.cell(row=new_row, column=3).value = fiche.unite or "-"
        ws.cell(row=new_row, column=4).value = fiche.department or "-"
    except AttributeError:
        pass
    return new_row


def _merge_identity_columns(ws) -> None:
    """
    In every data sheet, merge consecutive cells that share the same value in
    the Division / Unité / Département columns (the first DEPT_COLUMNS columns).
    The merge is vertical only — it makes repeated entity labels appear as one
    tall cell instead of the same text repeated row after row.

    Only merges cells BELOW the header row; never touches title / header rows.
    Only merges when a run of 2+ adjacent rows have identical, non-empty values.
    """
    from openpyxl.styles import Alignment

    hdr = _find_hdr_row(ws)
    data_start = hdr + 1
    max_row = ws.max_row or 1
    if data_start > max_row:
        return

    # Detect which columns hold Division / Unité / Département by scanning the
    # header row — these are the columns whose header is one of these three
    # labels. Fall back to columns 1-3 if none are found.
    _ID_LABELS = {"division", "unite", "unité", "departement", "département"}
    identity_cols = [
        c for c in range(1, (ws.max_column or 10) + 1)
        if _strip_accents(str(ws.cell(hdr, c).value or "").strip().lower()) in _ID_LABELS
    ]
    if not identity_cols:
        identity_cols = [1, 2, 3]

    for col in identity_cols:
        run_start = data_start
        run_val   = ws.cell(data_start, col).value

        for r in range(data_start + 1, max_row + 2):  # +2 to flush the last run
            cur_val = ws.cell(r, col).value if r <= max_row else object()  # sentinel
            if cur_val == run_val and run_val not in (None, "", "-"):
                continue  # still in the same run
            # Flush the run
            if r - run_start >= 2 and run_val not in (None, ""):
                ws.merge_cells(
                    start_row=run_start, start_column=col,
                    end_row=r - 1,       end_column=col,
                )
                # Centre vertically inside the merged cell
                ws.cell(run_start, col).alignment = Alignment(
                    vertical="center", wrap_text=True
                )
            run_start = r
            run_val   = cur_val


def _remove_empty_rows_cols(ws) -> None:
    """
    Delete rows in the DATA AREA (below the header) that are completely empty
    (all cells None or '').  Rows with any value including '-' are kept.
    Rows above the header are never touched to avoid disrupting merged title cells.
    """
    def _cell_empty(v) -> bool:
        if v is None: return True
        return str(v).strip() == ""

    hdr = _find_hdr_row(ws)
    max_row = ws.max_row or 1
    max_col = ws.max_column or 1

    # Delete empty rows BELOW the header only (bottom-up)
    for r in range(max_row, hdr, -1):
        if all(_cell_empty(ws.cell(r, c).value) for c in range(1, max_col + 1)):
            ws.delete_rows(r)


def _rename_montee_commentaires_to_structure(ws, fiche: BIAFiche) -> None:
    """
    If any ramp_up row has per_structure data, rename the "Commentaires"
    column header in the Montée en charge sheet to "Structure".
    """
    if not any(r.per_structure for r in fiche.ramp_up):
        return
    hdr_row = _find_hdr_row(ws)
    for col in range(1, (ws.max_column or 30) + 1):
        v = str(ws.cell(hdr_row, col).value or "").strip()
        if _strip_accents(v.lower()) == "commentaires":
            ws.cell(hdr_row, col).value = "Structure"
            return


def _restructure_impact_dmia_sheet(ws, fiche: BIAFiche) -> None:
    """
    Rename the dynamic columns of the Impact DMIA sheet to match the actual
    scenario labels from the fiche's §5.2 table.

    OLD (fixed template): IM 1H | IM 4H | IM 1J | IM 2-3J | ... | Score 2-3J
    NEW (STAR 2-scenario): IM < 1 jour | IM ≥ 5 jours | ... | Score ≥ 5 jours

    - Columns that correspond to a scenario are renamed.
    - Extra old columns (beyond the number of scenarios) have their headers cleared
      so no stale labels remain.
    - For GAT 4-col fiches the column structure already matches — no change needed.
    """
    if not fiche.impact_rows:
        return

    ir = fiche.impact_rows[0]

    # GAT 4-col: already matches the template structure → nothing to do
    if ir.is_4col:
        return

    if not ir.scenario_headers:
        return

    labels = [_clean_scenario_label(h) for h in ir.scenario_headers]
    n = len(labels)

    # ── Locate the header row and the first "IM" column ──────────────────────
    hdr_row = _find_hdr_row(ws)
    max_col = ws.max_column or 30

    _DIM_PREFIXES = ("im", "di", "jr", "fin", "score")
    _STATIC_AFTER = ("dmia", "commentaires")   # columns that come after the dynamic block

    first_dynamic = None
    last_dynamic  = None

    for col in range(1, max_col + 1):
        raw = _strip_accents(str(ws.cell(hdr_row, col).value or "").strip().lower())
        prefix = raw.split()[0] if raw.split() else ""
        if prefix in _DIM_PREFIXES:
            if first_dynamic is None:
                first_dynamic = col
            last_dynamic = col
        elif first_dynamic is not None and any(k in raw for k in _STATIC_AFTER):
            break  # past the dynamic block

    if first_dynamic is None:
        return   # couldn't locate dynamic columns

    # ── Build new headers ──────────────────────────────────────────────────────
    DIMS = ["IM", "DI", "JR", "FIN"]
    new_headers: list[str] = []
    for dim in DIMS:
        for label in labels:
            new_headers.append(f"{dim} {label}")
    for label in labels:
        new_headers.append(f"Score {label}")

    # ── Overwrite headers in-place ─────────────────────────────────────────────
    for i, header in enumerate(new_headers):
        ws.cell(hdr_row, first_dynamic + i).value = header

    # Delete the surplus old columns that are no longer needed.
    # Go right-to-left so indices don't shift as we delete.
    n_new = len(new_headers)
    n_old = last_dynamic - first_dynamic + 1
    surplus = n_old - n_new
    if surplus > 0:
        # Delete from last surplus column down to first surplus column
        delete_start = first_dynamic + n_new   # 1-based col index of first surplus col
        ws.delete_cols(delete_start, surplus)


def load(synthesis_path: str | Path, fiche: BIAFiche, dry_run: bool = False,
         output_path: str | Path = None, verbose: bool = True,
         unmatched: list | None = None):
    """
    LOAD phase: for each sheet in TRANSFORM_MAP, find the department row,
    insert extra rows if the fiche has multiple items (multiple activities,
    multiple apps, etc.), and fill in the data.

    Row insertion strategy:
      1. Find the template row for this department.
      2. If N items to insert > 1, insert (N-1) blank rows directly below,
         copying the row style so the sheet looks consistent.
      3. Fill all N rows with data starting from the template row.

    WHY insert instead of overwrite:
      The synthesis has one pre-existing row per department. When a department
      has 5 activities, we need 5 rows. We cannot merge into one cell because
      that breaks the tabular contract of the synthesis.

    Args:
        unmatched: optional list that will be appended with the entity_name
                   when NO matching row is found in the first sheet checked.
                   Callers can surface this as a user-visible warning.
    """
    wb = load_workbook(str(synthesis_path))
    _entity_not_found_reported = False   # report only once per fiche

    for sheet_name, transform_fn in TRANSFORM_MAP.items():
        # Normalize sheet name — the xlsx may have trailing spaces
        ws = None
        for sn in wb.sheetnames:
            if sn.strip() == sheet_name.strip():
                ws = wb[sn]
                break
        if ws is None:
            if verbose: print(f"  [WARN] Sheet '{sheet_name}' not found, skipping.")
            continue

        # For Impact DMIA: rename column headers to match the fiche's scenario labels
        # before computing col_map so the fuzzy matcher finds them correctly.
        if sheet_name.strip() == "Impact DMIA" and not dry_run:
            _restructure_impact_dmia_sheet(ws, fiche)

        # For Montée en charge: rename "Commentaires" → "Structure" when the
        # fiche uses per-structure colored breakdown.
        if sheet_name.strip() == "Montée en charge" and not dry_run:
            _rename_montee_commentaires_to_structure(ws, fiche)

        data_rows = transform_fn(fiche)
        if not data_rows:
            if verbose: print(f"  [INFO] No data for sheet '{sheet_name}' from this fiche.")
            continue

        # ── Flat sheets: bypass entity-row matching entirely ──────────────────
        if sheet_name.strip() in _FLAT_SHEETS:
            if not dry_run:
                colour_rules = _FLAT_SHEETS[sheet_name.strip()]
                _write_flat_rows(ws, data_rows, colour_rules)
                if verbose:
                    print(f"  OK [{sheet_name}] -> {len(data_rows)} row(s) appended (flat)")
            continue
        # ─────────────────────────────────────────────────────────────────────

        col_map = _get_column_map(ws)
        dept_row = _find_department_row(ws, fiche.entity_name)

        if dept_row is None:
            # Entity is absent from the recensement: rather than dropping the
            # fiche's data, append a fresh row at the bottom of the data block.
            # This keeps the synthèse complete even when the recensement was
            # only partially filled by the client.
            if dry_run:
                if verbose:
                    print(f"  [DRY] Would auto-append row for '{fiche.entity_name}' "
                          f"in sheet '{sheet_name}'.")
                if not _entity_not_found_reported and unmatched is not None:
                    unmatched.append(fiche.entity_name)
                    _entity_not_found_reported = True
                continue
            dept_row = _append_entity_row(ws, fiche)
            if verbose:
                print(f"  [INFO] '{fiche.entity_name}' not in recensement — "
                      f"auto-appended at row {dept_row} of '{sheet_name}'.")
            # Refresh col_map after potential structural change
            col_map = _get_column_map(ws)

        n = len(data_rows)

        if not dry_run:
            # ── Clear any pre-existing rows for this entity ──────────────────
            # If the input synthèse was already filled (e.g. a previous run),
            # extra rows from the old data must be removed first so they don't
            # accumulate alongside the freshly-extracted rows.
            _clear_existing_rows(ws, dept_row)

            # Insert extra rows if needed (N-1 because row dept_row already exists)
            if n > 1:
                ws.insert_rows(dept_row + 1, n - 1)
                for i in range(1, n):
                    _copy_row_style(ws, dept_row, dept_row + i)
                    # Copy Division/Unité/Département identifiers
                    # Include col 1 (Division) even though it is not in DEPT_COLUMNS
                    # so that _merge_identity_columns can detect the full run.
                    for col in ([1] + DEPT_COLUMNS):
                        ws.cell(row=dept_row + i, column=col).value = \
                            ws.cell(row=dept_row, column=col).value

            # Write data
            for i, data_row in enumerate(data_rows):
                target_row = dept_row + i
                for col_name, value in data_row.items():
                    # Fuzzy match column headers (handles minor label variations)
                    best_col_name = _fuzzy_col(col_name, col_map)
                    if best_col_name:
                        col_idx = col_map[best_col_name]
                        try:
                            ws.cell(row=target_row, column=col_idx).value = value
                        except AttributeError:
                            # MergedCell secondary cells are read-only; skip gracefully.
                            # The master (top-left) cell of the merged range will be
                            # written when col_idx hits it directly.
                            pass

            # ── Merge identity columns for this entity block ──────────────────
            # We know exactly which rows belong to this entity (dept_row..dept_row+n-1).
            # Merge each identity column over that range so repeated labels appear
            # as one tall cell.  We do this here (not as post-processing) so we
            # don't accidentally bleed into template filler rows that share the
            # same "-" value.
            if n > 1:
                from openpyxl.styles import Alignment as _Align
                _ID_LABELS_LOAD = {"division", "unite", "departement", "nominal"}
                hdr_row_load = _find_hdr_row(ws)
                for col in range(1, (ws.max_column or 10) + 1):
                    hdr_label = _strip_accents(
                        str(ws.cell(hdr_row_load, col).value or "").strip().lower()
                    )
                    if hdr_label not in _ID_LABELS_LOAD:
                        continue
                    # All n rows share the same value — merge them.
                    cell_val = ws.cell(dept_row, col).value
                    if cell_val is None:
                        continue
                    ws.merge_cells(
                        start_row=dept_row, start_column=col,
                        end_row=dept_row + n - 1, end_column=col,
                    )
                    ws.cell(dept_row, col).alignment = _Align(
                        vertical="center", wrap_text=True
                    )

        if verbose: print(f"  OK [{sheet_name}] -> {n} row(s) written for '{fiche.entity_name}'")

    if not dry_run:
        # ── Remove empty rows and columns from every sheet ────────────────────
        # "Empty" = every cell is None or "". "-" counts as non-empty.
        for sn in wb.sheetnames:
            _remove_empty_rows_cols(wb[sn])

        out_path = Path(output_path) if output_path else Path(synthesis_path).with_stem(
            Path(synthesis_path).stem + "_filled"
        )
        wb.save(str(out_path))
        if verbose: print(f"\nSaved: {out_path}")
    else:
        if verbose: print("\n[DRY RUN] No file written.")


def _fuzzy_col(col_name: str, col_map: dict[str, int]) -> Optional[str]:
    """Find the best matching column header in col_map using fuzzy matching."""
    if col_name in col_map:
        return col_name
    if not col_map:
        return None
    result = process.extractOne(col_name, list(col_map.keys()), scorer=fuzz.token_sort_ratio)
    if result and result[1] >= 70:
        return result[0]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def _fiche_is_empty(fiche: BIAFiche) -> bool:
    """Return True when the fiche contains no usable data at all."""
    return (
        not fiche.entity_name
        and not fiche.activities
        and not fiche.impact_rows
        and not fiche.exchanges
        and not fiche.it_applications
        and not fiche.key_people
        and not fiche.ramp_up
        and not fiche.other_equipment
        and not fiche.critical_docs
    )


def process_fiche(docx_path: Path, synthesis_path: Path, dry_run: bool,
                  output_path: Path | None = None) -> bool:
    """
    Extract + load one fiche.  Returns True if data was written, False if skipped.
    A fiche is skipped (not an error) when it is blank or cannot be parsed —
    the synthèse is left untouched rather than silently written back unchanged.

    When ``output_path`` is provided, the filled workbook is written there
    instead of the default "<stem>_filled.xlsx" sibling of ``synthesis_path``.
    Callers processing a folder pass the same path for every fiche AND set
    ``synthesis_path`` to point at the previous output so successive fiches
    accumulate their data in the same file instead of clobbering each other.
    """
    print(f"\n{'='*60}")
    print(f"Processing: {docx_path.name}")
    fiche = extract(docx_path)

    # ── Guard: empty fiche ────────────────────────────────────────────────────
    if _fiche_is_empty(fiche):
        print(f"  [SKIP] Fiche appears to be empty or could not be parsed.")
        print(f"         No data extracted — synthèse left unchanged.")
        return False

    if not fiche.entity_name:
        print(f"  [WARN] Entity name not found — department matching may fail.")

    print(f"  Entity        : {fiche.entity_name}")
    print(f"  Activities    : {len(fiche.activities)}")
    print(f"  Impact rows   : {len(fiche.impact_rows)}")
    print(f"  Exchanges     : {len(fiche.exchanges)}")
    print(f"  IT Apps       : {len(fiche.it_applications)}")
    print(f"  Key people    : {len(fiche.key_people)}")
    print(f"  Ramp-up rows  : {len(fiche.ramp_up)}")
    print(f"  Other eqt     : {len(fiche.other_equipment)}")
    print(f"  Critical docs : {len(fiche.critical_docs)}")

    load(synthesis_path, fiche, dry_run=dry_run, output_path=output_path)
    return True


def main():
    parser = argparse.ArgumentParser(description="BIA ETL: fiches DOCX → synthèse XLSX")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fiche", type=Path, help="Single BIA fiche .docx")
    group.add_argument("--fiches-dir", type=Path, help="Folder containing all BIA fiche .docx files")
    parser.add_argument("--synthese", type=Path, required=True, help="Synthèse BIA .xlsx template (master blank)")
    parser.add_argument("--recensement", type=Path, default=None,
                        help="Fiche de recensement .xlsx — seeds the synthèse with the client's org structure before filling")
    parser.add_argument("--dry-run", action="store_true", help="Extract and print only, do not write")
    args = parser.parse_args()

    if not args.synthese.exists():
        sys.exit(f"ERROR: Synthèse file not found: {args.synthese}")

    # ── Step 1 (optional): bootstrap synthèse from recensement ────────────────
    # The recensement seeds the org structure (Division/Unité/Département rows)
    # into every sheet, so Step 2 fuzzy-matching always finds a row to write to.
    active_synthese = args.synthese
    if args.recensement:
        if not args.recensement.exists():
            sys.exit(f"ERROR: Recensement file not found: {args.recensement}")
        bootstrapped = args.synthese.with_stem(args.synthese.stem + "_bootstrapped")
        print(f"\n{'='*60}")
        print(f"STEP 1 — Bootstrap synthèse from recensement")
        print(f"  Source : {args.recensement.name}")
        bootstrap_synthese(args.recensement, args.synthese, bootstrapped, verbose=True)
        active_synthese = bootstrapped

    # ── Step 2: fill with fiche data ──────────────────────────────────────────
    if args.recensement:
        print(f"\n{'='*60}")
        print(f"STEP 2 — Fill synthèse from BIA fiches")

    if args.fiche:
        if not args.fiche.exists():
            sys.exit(f"ERROR: Fiche not found: {args.fiche}")
        process_fiche(args.fiche, active_synthese, args.dry_run)
    else:
        docx_files = sorted(args.fiches_dir.glob("*.docx"))
        if not docx_files:
            sys.exit(f"ERROR: No .docx files found in {args.fiches_dir}")
        print(f"Found {len(docx_files)} fiche(s) in {args.fiches_dir}")

        # Accumulate every fiche into a single output workbook by feeding the
        # output of fiche N as the input of fiche N+1.  Without this each
        # iteration would re-read the bootstrapped file and clobber prior data.
        accumulated_out = args.synthese.with_stem(args.synthese.stem + "_filled")
        current_input = active_synthese

        skipped, processed, errors = [], [], []
        for f in docx_files:
            try:
                ok = process_fiche(f, current_input, args.dry_run,
                                   output_path=accumulated_out)
                if ok and not args.dry_run:
                    current_input = accumulated_out
                (processed if ok else skipped).append(f.name)
            except Exception as e:
                print(f"  [ERROR] {f.name}: {e}")
                errors.append(f.name)

        # ── Final summary ──────────────────────────────────────────────────
        print(f"\n{'='*60}")
        print(f"SUMMARY: {len(processed)} processed, {len(skipped)} skipped, {len(errors)} errors")
        if skipped:
            print(f"\nSkipped (empty / unreadable fiches):")
            for name in skipped:
                print(f"  • {name}")
        if errors:
            print(f"\nErrors:")
            for name in errors:
                print(f"  • {name}")


if __name__ == "__main__":
    main()
