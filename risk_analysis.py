"""
risk_analysis.py — Analyse des risques module

Parses the risk Excel file, stores it in the DB, calls Ollama for AI suggestions,
and exports the modified file back to Excel.
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
import zipfile
import xml.etree.ElementTree as _ET
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

import httpx
import openpyxl

import risk_scoring as _scoring

_NS_SS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_ET.register_namespace("", _NS_SS)
_ET.register_namespace("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships")
_ET.register_namespace("x14ac", "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac")

DB_PATH = Path(__file__).parent / "projects.db"

# ── Header → row key rules ───────────────────────────────────────────────────
# (field, exact aliases, substring fallbacks) — all compared on the normalised
# header text.  Exact aliases are tried across the whole row first, because the
# discriminating pairs in these sheets differ only by a prefix: "Niveau du
# Risque Brut" (a 1-16 score) sits next to "Risque brut" (its band), and plain
# substring matching hands both to whichever rule is checked first.  For the
# same reason "Coût" is matched exactly — as a substring, "co" also matches
# "Constats" and "Efficacité du contrôle".
_HEADER_RULES = [
    ("id",          ["i.d", "i d", "id", "n", "n°", "no"],                ["i.d"]),
    ("scenario",    ["scenario de risque", "scenario"],                   ["scenario"]),
    ("constats",    ["constats", "constat"],                              ["constat"]),
    ("site",        ["site"],                                             ["site"]),
    ("prob",        ["probabilite"],                                      ["probabilit"]),
    ("impact",      ["impact"],                                           ["impact"]),
    ("nrb",         ["niveau du risque brut", "niveau risque brut"],      ["niveau du risque brut", "niveau risque brut"]),
    ("risque_brut", ["risque brut"],                                      []),
    ("maitrise",    ["elements de maitrise", "element de maitrise"],      ["ments de ma", "maitrise"]),
    ("efficacite",  ["efficacite du controle", "efficacite"],             ["efficacit"]),
    ("nrn",         ["niveau du risque net", "niveau risque net"],        ["niveau du risque net", "niveau risque net"]),
    ("risque_net",  ["risque net"],                                       []),
    ("option",      ["option de traitement", "option"],                   ["option"]),
    ("plan",        ["plan d actions", "plan d action", "plan"],          ["plan d"]),
    ("cout",        ["cout"],                                             []),
    ("complexite",  ["complexite"],                                       ["complexit"]),
    ("priorite",    ["priorite"],                                         ["priorit"]),
]

def _normalize(s: str) -> str:
    """Lowercase + strip accents for fuzzy header matching."""
    import unicodedata
    return unicodedata.normalize("NFKD", s.lower()).encode("ascii", "ignore").decode()


def _match_headers(cells: dict) -> dict:
    """
    Map {column: normalised header} onto {field: column}.

    Exact aliases win across the entire row before any substring rule is
    considered, so "Risque brut" claims its own column even though "Niveau du
    Risque Brut" also contains that text.  A column is consumed once matched,
    which stops one header satisfying two fields.
    """
    field_col: dict[str, int] = {}
    used_cols: set = set()

    for field, exacts, _subs in _HEADER_RULES:
        for col, text in sorted(cells.items()):
            if col in used_cols or field in field_col:
                continue
            if text in exacts:
                field_col[field] = col
                used_cols.add(col)

    for field, _exacts, subs in _HEADER_RULES:
        if field in field_col or not subs:
            continue
        # Longest pattern first: the more specific the match, the likelier it
        # is the header actually meant that field.
        for pat in sorted(subs, key=len, reverse=True):
            hit = next((c for c, t in sorted(cells.items())
                        if c not in used_cols and pat in t), None)
            if hit is not None:
                field_col[field] = hit
                used_cols.add(hit)
                break

    return field_col


def _detect_columns(ws) -> tuple[int, int, dict]:
    """
    Scan the worksheet for the header row.
    Returns (header_row, data_start_row, col_map) where col_map is
    key→1-based column index (e.g. {'id': 2, 'scenario': 3, 'plan': 14, ...}).
    """
    col_map: dict[str, int] = {}
    header_row = None

    for row in range(1, 20):
        cells = {}
        for col in range(1, 30):
            v = ws.cell(row=row, column=col).value
            if v is None or str(v).strip() == "":
                continue
            cells[col] = _normalize(str(v)).strip()
        if not cells:
            continue
        row_map = _match_headers(cells)
        # If this row has at least 4 known headers, treat it as the header row
        if len(row_map) >= 4:
            col_map = row_map
            header_row = row
            break

    if header_row is None:
        # Fallback: assume structure of DSI file (header row 6)
        header_row = 6
        col_map = {
            "id": 2, "scenario": 3, "constats": 4,
            "prob": 5, "impact": 6, "nrb": 7, "risque_brut": 8,
            "maitrise": 9, "efficacite": 10, "risque_net": 11,
            "option": 12, "plan": 13, "cout": 14,
            "complexite": 15, "priorite": 16,
        }

    return header_row, header_row + 1, col_map

OLLAMA_BASE = "http://localhost:11434"

PCA_CONTEXT = """Tu es un expert en gestion des risques et continuité d'activité (PCA/BCP) travaillant pour des organisations du secteur financier (banques, assurances) en Tunisie.

Contexte métier :
Les crises systémiques et les aléas majeurs font partie du paysage des organisations. Un plan de continuité d'activité (PCA) permet de structurer la capacité à tenir dans la durée, en articulant pilotage, gestion des risques et responsabilité des dirigeants. La réglementation tunisienne et les normes internationales (ISO 22301, NIS 2) imposent une analyse rigoureuse des risques et des plans d'actions actionnables.

Ta mission : À partir d'un constat de risque identifié lors d'une visite de site, et des éléments de maîtrise déjà en place, tu dois proposer un Plan d'actions concis, précis et immédiatement actionnable pour réduire ce risque résiduel.

Règles de réponse :
- Propose 2 à 4 actions numérotées, concrètes et réalistes
- Chaque action doit être directement applicable par une équipe opérationnelle
- Utilise un langage professionnel en français
- Sois concis : 1 à 2 phrases maximum par action
- Ne répète pas les éléments de maîtrise déjà en place
- Ne commence pas par des formules de politesse ou introductions
"""


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS risk_files (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            sector      TEXT DEFAULT '',
            client      TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now')),
            rows_json   TEXT NOT NULL,
            source_sheet TEXT DEFAULT 'Analyse des risques DVT-Sites'
        )
    """)
    # The scale is per-file (each client may use its own Echelle), so it has to
    # travel with the rows rather than be re-derived from a source workbook the
    # server may no longer hold.
    cols = {r[1] for r in con.execute("PRAGMA table_info(risk_files)")}
    if "scale_json" not in cols:
        con.execute("ALTER TABLE risk_files ADD COLUMN scale_json TEXT DEFAULT ''")
    con.commit()
    con.close()


def save_risk_file(name: str, sector: str, client: str,
                   rows: list[dict], sheet: str = "Analyse des risques DVT-Sites",
                   scale: dict | None = None) -> int:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        "INSERT INTO risk_files (name, sector, client, rows_json, source_sheet, scale_json) "
        "VALUES (?,?,?,?,?,?)",
        (name, sector, client, json.dumps(rows, ensure_ascii=False), sheet,
         json.dumps(scale or {}, ensure_ascii=False))
    )
    file_id = cur.lastrowid
    con.commit()
    con.close()
    return file_id


def update_risk_file(file_id: int, rows: list[dict]):
    _init_db()
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE risk_files SET rows_json=?, updated_at=datetime('now') WHERE id=?",
        (json.dumps(rows, ensure_ascii=False), file_id)
    )
    con.commit()
    con.close()


def list_risk_files() -> list[dict]:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT id, name, sector, client, created_at, updated_at, source_sheet FROM risk_files ORDER BY updated_at DESC"
    ).fetchall()
    con.close()
    return [
        {"id": r[0], "name": r[1], "sector": r[2], "client": r[3],
         "created_at": r[4], "updated_at": r[5], "sheet": r[6]}
        for r in rows
    ]


def load_risk_file(file_id: int) -> dict | None:
    _init_db()
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT id, name, sector, client, rows_json, source_sheet, scale_json "
        "FROM risk_files WHERE id=?",
        (file_id,)
    ).fetchone()
    con.close()
    if not row:
        return None
    try:
        scale = json.loads(row[6]) if row[6] else {}
    except (TypeError, ValueError):
        scale = {}
    return {
        "id": row[0], "name": row[1], "sector": row[2], "client": row[3],
        "rows": json.loads(row[4]), "sheet": row[5],
        "scale": scale or _scoring.DEFAULT_SCALE.to_dict(),
    }


def delete_risk_file(file_id: int):
    _init_db()
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM risk_files WHERE id=?", (file_id,))
    con.commit()
    con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Excel parsing
# ─────────────────────────────────────────────────────────────────────────────

def _cell(ws, row: int, col: int):
    v = ws.cell(row=row, column=col).value
    if v is None:
        return ""
    if isinstance(v, float) and v == int(v):
        return int(v)
    return v


def parse_risk_excel(
    file_bytes: bytes, sheet_name: str | None = None
) -> tuple[list[dict], str, list[str], dict]:
    """
    Parse an Excel risk file.
    Returns (rows, detected_sheet_name, available_sheets, scale_dict).

    Column positions are detected dynamically from the header row, and the
    Probabilité × Impact → Risque brut → Risque net chain is recomputed rather
    than read from the cached cell values.  Recomputing is not optional: these
    workbooks routinely ship with the chain pointing at an external file on the
    author's machine, so the cached values are frozen at whatever they were
    when that link last resolved.  Rows whose stored values disagree with the
    recomputation carry a `diverged` map of what the file had said.
    """
    wb = openpyxl.load_workbook(BytesIO(file_bytes), data_only=True)
    sheets = wb.sheetnames
    try:
        scale = _scoring.read_scale(wb)
    except Exception:
        scale = _scoring.DEFAULT_SCALE

    # Auto-detect main sheet if not specified
    if sheet_name is None:
        for s in sheets:
            if "risque" in s.lower():
                sheet_name = s
                break
        if sheet_name is None:
            sheet_name = sheets[0]

    ws = wb[sheet_name]
    _hrow, data_start, col_map = _detect_columns(ws)

    def g(r: int, field: str):
        col = col_map.get(field)
        return _cell(ws, r, col) if col else ""

    rows = []
    for r in range(data_start, (ws.max_row or data_start) + 1):
        rid = g(r, "id")
        if not rid or str(rid).strip() == "":
            continue
        row = {
            "row_num":    r,
            "id":         str(rid),
            "scenario":   str(g(r, "scenario") or ""),
            "constats":   str(g(r, "constats") or ""),
            "site":       str(g(r, "site") or ""),
            "prob":       g(r, "prob"),
            "impact":     g(r, "impact"),
            "nrb":        g(r, "nrb"),
            "risque_brut":str(g(r, "risque_brut") or ""),
            "maitrise":   str(g(r, "maitrise") or ""),
            "efficacite": str(g(r, "efficacite") or ""),
            "nrn":        g(r, "nrn"),
            "risque_net": str(g(r, "risque_net") or ""),
            "option":     str(g(r, "option") or ""),
            "plan":       str(g(r, "plan") or ""),
            "cout":       str(g(r, "cout") or ""),
            "complexite": str(g(r, "complexite") or ""),
            "priorite":   str(g(r, "priorite") or ""),
        }
        apply_scoring(row, scale)
        rows.append(row)

    return rows, sheet_name, sheets, scale.to_dict()


def apply_scoring(row: dict, scale=None) -> dict:
    """
    Recompute one row in place and record where the source disagreed.

    Kept separate from parsing so re-scoring after an edit runs through exactly
    the same code path as the initial import.
    """
    sc = scale or _scoring.DEFAULT_SCALE
    computed = _scoring.score(row.get("prob"), row.get("impact"),
                              row.get("efficacite"), sc)
    diverged = _scoring.diff_against(row, computed)

    row["nrb"] = computed["nrb"]
    row["risque_brut"] = computed["risque_brut"]
    row["risque_net"] = computed["risque_net"]
    if computed["efficacite_canon"]:
        row["efficacite"] = computed["efficacite_canon"]
    row["diverged"] = diverged
    return row


# ─────────────────────────────────────────────────────────────────────────────
# Excel export
# ─────────────────────────────────────────────────────────────────────────────

def _col_letter(n: int) -> str:
    """Convert 1-based column index to Excel letter(s): 16 → 'P'."""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _find_sheet_xml_path(zf: zipfile.ZipFile, sheet_name: str) -> str:
    """Resolve the zip entry path for the named worksheet."""
    # Parse workbook.xml to get sheet rId
    wb_xml = _ET.fromstring(zf.read("xl/workbook.xml"))
    ns = {"x": _NS_SS}
    sheet_rId = None
    for sh in wb_xml.findall(".//x:sheet", ns):
        if sh.get("name") == sheet_name:
            sheet_rId = sh.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            break
    if sheet_rId is None:
        # Fallback: first sheet
        sh = wb_xml.find(".//x:sheet", ns)
        sheet_rId = sh.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")

    # Parse xl/_rels/workbook.xml.rels to get the actual file path.
    # Per OPC, a Target starting with "/" is package-root-relative (openpyxl
    # writes worksheet targets this way, e.g. "/xl/worksheets/sheet1.xml");
    # anything else is relative to the .rels file's own folder, i.e. "xl/"
    # (e.g. "worksheets/sheet1.xml" or "styles.xml").
    rels_xml = _ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    for rel in rels_xml:
        if rel.get("Id") == sheet_rId:
            target = rel.get("Target")
            if target.startswith("/"):
                target = target.lstrip("/")
            else:
                target = "xl/" + target
            return target

    raise ValueError(f"Cannot find sheet '{sheet_name}' in workbook relationships")


def _detect_plan_col(original_bytes: bytes, sheet_path: str) -> int:
    """
    Read the sheet XML directly to find which column has 'Plan d'actions' header.
    Returns the 1-based column index, defaulting to 13 if not found.
    """
    with zipfile.ZipFile(BytesIO(original_bytes)) as zf:
        sheet_xml = zf.read(sheet_path)

    # Look for the Plan d'actions cell in the header area (first 20 rows)
    # Cell values may be shared strings — we need shared strings too
    with zipfile.ZipFile(BytesIO(original_bytes)) as zf:
        try:
            ss_xml = zf.read("xl/sharedStrings.xml")
            ss_root = _ET.fromstring(ss_xml)
            ns = {"x": _NS_SS}
            shared_strings = [
                "".join(t.text or "" for t in si.iter("{%s}t" % _NS_SS))
                for si in ss_root.findall("x:si", ns)
            ]
        except Exception:
            shared_strings = []

    root = _ET.fromstring(sheet_xml)
    ns = {"x": _NS_SS}
    for row_el in root.findall(".//x:row", ns):
        rnum = int(row_el.get("r", 9999))
        if rnum > 20:
            break
        for c_el in row_el.findall("x:c", ns):
            ref = c_el.get("r", "")
            t   = c_el.get("t", "")
            v_el = c_el.find("x:v", ns)
            if v_el is None:
                continue
            if t == "s":
                try:
                    val = shared_strings[int(v_el.text)]
                except Exception:
                    continue
            elif t in ("str", "inlineStr"):
                is_el = c_el.find(".//x:t", ns)
                val = is_el.text if is_el is not None else ""
            else:
                val = v_el.text or ""

            if "plan" in val.lower() and "action" in val.lower():
                # Extract column letter(s) from ref like "M6" or "N7"
                col_str = re.match(r'([A-Z]+)', ref)
                if col_str:
                    letters = col_str.group(1)
                    col_idx = 0
                    for ch in letters:
                        col_idx = col_idx * 26 + (ord(ch) - 64)
                    return col_idx

    return 13  # fallback: column M


# Columns written back verbatim as text. Every one of these is editable in the
# Cartographie table, so all of them have to survive the export — writing only
# the scored columns would silently drop the user's edits to Constats, Éléments
# de maîtrise and the rest.
_TEXT_FIELDS = ("id", "scenario", "constats", "site", "maitrise",
                "efficacite", "option", "plan")


def _xml_escape(text: str) -> str:
    return (str(text).replace("&", "&amp;")
                     .replace("<", "&lt;")
                     .replace(">", "&gt;"))


def _find_echelle_range(original_bytes: bytes) -> Optional[tuple]:
    """
    Locate the workbook's own risk-scale lookup table.

    Returns (sheet_name, "$C$6:$D$21") for the contiguous (score, label) block,
    or None when the workbook has no such sheet — in which case the exported
    Risque brut formula falls back to a self-contained nested IF.
    """
    try:
        wb = openpyxl.load_workbook(BytesIO(original_bytes), data_only=True)
    except Exception:
        return None

    for name in wb.sheetnames:
        if "echelle" not in _scoring._norm(name):
            continue
        ws = wb[name]
        hits = []
        for row in ws.iter_rows():
            score_cell = label_cell = None
            for cell in row:
                v = cell.value
                if v is None or v == "":
                    continue
                if score_cell is None and isinstance(v, (int, float)) and not isinstance(v, bool):
                    score_cell = cell
                elif isinstance(v, str) and v.strip():
                    label_cell = cell
            if score_cell is not None and label_cell is not None:
                hits.append((score_cell, label_cell))
        if len(hits) >= 2:
            first_s, first_l = hits[0]
            last_s, _last_l = hits[-1]
            return (name,
                    f"${_col_letter(first_s.column)}${first_s.row}:"
                    f"${_col_letter(first_l.column)}${last_s.row}")
    return None


def _brut_formula(nrb_ref: str, scale, echelle: Optional[tuple]) -> str:
    """Risque brut: VLOOKUP against the workbook's Echelle when it has one."""
    if echelle:
        sheet, rng = echelle
        quoted = f"'{sheet}'" if re.search(r"[^A-Za-z0-9_]", sheet) else sheet
        return f'IFERROR(VLOOKUP({nrb_ref},{quoted}!{rng},2,0),0)'
    # No lookup sheet — inline the bands so the formula cannot break.
    expr = '""'
    for upper, label in reversed(scale.bands):
        expr = f'IF({nrb_ref}<={upper},"{label}",{expr})'
    return expr


def _net_formula(brut_ref: str, eff_ref: str, scale) -> str:
    """
    Risque net: the (risque brut × efficacité) matrix as a nested IF.

    Generated from the Scale rather than hardcoded, so a client on their own
    bands or vocabulary exports a formula that matches what the app displayed.
    Efficacités sharing an outcome are collapsed into one OR(), which keeps the
    shape of the formula Devoteam already writes by hand.
    """
    from collections import OrderedDict

    band_expr = '""'
    for _upper, band in reversed(scale.bands):
        by_outcome: "OrderedDict[str, list]" = OrderedDict()
        for eff in scale.efficacite_vocab:
            out = scale.net_for(band, eff)
            if out:
                by_outcome.setdefault(out, []).append(eff)

        if not by_outcome:
            continue

        # A band that resolves the same way regardless of the controls (Faible
        # is always Accepté) needs no inner test at all.
        if len(by_outcome) == 1:
            only = next(iter(by_outcome))
            inner = f'"{only}"'
        else:
            inner = '""'
            for out, effs in reversed(list(by_outcome.items())):
                if len(effs) == 1:
                    cond = f'{eff_ref}="{effs[0]}"'
                else:
                    cond = "OR(" + ",".join(f'{eff_ref}="{e}"' for e in effs) + ")"
                inner = f'IF({cond},"{out}",{inner})'

        band_expr = f'IF({brut_ref}="{band}",{inner},{band_expr})'

    return band_expr


def export_risk_excel(original_bytes: bytes, rows: list[dict], sheet_name: str,
                      scale: dict | None = None) -> bytes:
    """
    Write the edited rows back into the original workbook by ZIP-level XML
    patching, which preserves the charts, drawings and external-link parts that
    openpyxl would drop.

    Plan d'actions, Probabilité, Impact and Efficacité are written as values;
    Niveau du Risque Brut, Risque brut and Risque net are written as live Excel
    formulas carrying their computed result as the cached value, so the sheet
    both opens correct and recalculates when someone edits a driver. The
    formulas are emitted against the workbook's own Echelle sheet, which also
    replaces any `[1]Echelle!` reference pointing at a workbook that only ever
    existed on the original author's machine.
    """
    if not rows:
        return original_bytes

    sc = _scoring.DEFAULT_SCALE
    if scale:
        try:
            sc = _scale_from_dict(scale)
        except Exception:
            sc = _scoring.DEFAULT_SCALE

    with zipfile.ZipFile(BytesIO(original_bytes), "r") as zf:
        try:
            sheet_path = _find_sheet_xml_path(zf, sheet_name)
        except Exception:
            sheet_path = "xl/worksheets/sheet1.xml"
        sheet_text = zf.read(sheet_path).decode("utf-8")

    try:
        wb = openpyxl.load_workbook(BytesIO(original_bytes), data_only=True)
        ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.worksheets[0]
        _hdr, _ds, col_map = _detect_columns(ws)
    except Exception:
        col_map = {}
    if "plan" not in col_map:
        col_map["plan"] = _detect_plan_col(original_bytes, sheet_path)

    echelle = _find_echelle_range(original_bytes)
    L = {f: _col_letter(c) for f, c in col_map.items()}

    # field → (row → payload). Value fields carry text; formula fields carry
    # (formula, cached result).
    by_row = {r["row_num"]: r for r in rows if r.get("row_num")}
    plans = {rn: r.get("plan", "") for rn, r in by_row.items()}

    def _cell_xml(ref: str, style: str, row: dict, field: str) -> Optional[str]:
        s_attr = f' s="{style}"' if style else ""
        rn = row["row_num"]

        if field in _TEXT_FIELDS:
            txt = plans.get(rn, "") if field == "plan" else (row.get(field) or "")
            if txt == "":
                return f'<c r="{ref}"{s_attr}/>'
            return (f'<c r="{ref}"{s_attr} t="inlineStr">'
                    f'<is><t xml:space="preserve">{_xml_escape(txt)}</t></is></c>')

        if field in ("prob", "impact"):
            v = row.get(field)
            n = _scoring._num(v)
            if n is None:
                return f'<c r="{ref}"{s_attr}/>'
            n = int(n) if float(n).is_integer() else n
            return f'<c r="{ref}"{s_attr}><v>{n}</v></c>'

        if field == "nrb":
            if "prob" not in L or "impact" not in L:
                return None
            f = f'{L["impact"]}{rn}*{L["prob"]}{rn}'
            cached = row.get("nrb")
            if cached in (None, ""):
                return f'<c r="{ref}"{s_attr}><f>{_xml_escape(f)}</f></c>'
            return (f'<c r="{ref}"{s_attr}><f>{_xml_escape(f)}</f>'
                    f'<v>{cached}</v></c>')

        if field == "risque_brut":
            if "nrb" not in L:
                return None
            f = _brut_formula(f'{L["nrb"]}{rn}', sc, echelle)
            cached = row.get("risque_brut") or ""
            return (f'<c r="{ref}"{s_attr} t="str"><f>{_xml_escape(f)}</f>'
                    f'<v>{_xml_escape(cached)}</v></c>')

        if field == "risque_net":
            if "risque_brut" not in L or "efficacite" not in L:
                return None
            f = _net_formula(f'{L["risque_brut"]}{rn}', f'{L["efficacite"]}{rn}', sc)
            cached = row.get("risque_net") or ""
            return (f'<c r="{ref}"{s_attr} t="str"><f>{_xml_escape(f)}</f>'
                    f'<v>{_xml_escape(cached)}</v></c>')

        return None

    letter_to_field = {
        L[f]: f
        for f in (*_TEXT_FIELDS, "prob", "impact", "nrb",
                  "risque_brut", "risque_net")
        if f in L
    }
    if not letter_to_field:
        return original_bytes

    cell_re = re.compile(
        r'<c\s+r="([A-Z]+)(\d+)"([^>]*?)(/>|>.*?</c>)', re.DOTALL
    )

    def _replace(m: re.Match) -> str:
        col, rn_s, attrs, _tail = m.group(1), m.group(2), m.group(3), m.group(4)
        field = letter_to_field.get(col)
        if not field:
            return m.group(0)
        rn = int(rn_s)
        row = by_row.get(rn)
        if row is None:
            return m.group(0)
        style_m = re.search(r's="([^"]*)"', attrs)
        style = style_m.group(1) if style_m else ""
        out = _cell_xml(f"{col}{rn}", style, row, field)
        return out if out else m.group(0)

    patched = cell_re.sub(_replace, sheet_text)

    # Any surviving [n]Echelle! reference still points at the absent external
    # workbook; repoint it at the local sheet of the same name.
    patched = re.sub(r'\[\d+\](?=[A-Za-z\'])', '', patched)

    # With every [n] reference rewritten, the external-link parts are orphaned.
    # Leaving them in place would still make Excel raise its "this workbook
    # contains links to other data sources" banner on open, which is precisely
    # the prompt this rewrite exists to remove — so drop the declaration, the
    # relationship, the content-type override and the parts themselves.
    drop_prefixes = ("xl/externalLinks/",)

    with zipfile.ZipFile(BytesIO(original_bytes), "r") as zf:
        names = set(zf.namelist())
        wb_xml = zf.read("xl/workbook.xml").decode("utf-8")
        rels_xml = zf.read("xl/_rels/workbook.xml.rels").decode("utf-8")
        ct_xml = zf.read("[Content_Types].xml").decode("utf-8")

    has_external = any(n.startswith(drop_prefixes) for n in names)
    if has_external:
        wb_xml = re.sub(r"<externalReferences>.*?</externalReferences>", "",
                        wb_xml, flags=re.DOTALL)
        rels_xml = re.sub(
            r'<Relationship[^>]*Type="[^"]*externalLink"[^>]*/>', "", rels_xml)
        ct_xml = re.sub(
            r'<Override[^>]*PartName="/xl/externalLinks/[^"]*"[^>]*/>', "", ct_xml)

    out = BytesIO()
    with zipfile.ZipFile(BytesIO(original_bytes), "r") as zf_in, \
         zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf_out:
        for item in zf_in.infolist():
            name = item.filename
            if name == sheet_path:
                zf_out.writestr(item, patched.encode("utf-8"))
            elif name == "xl/calcChain.xml":
                # The chain no longer describes the formulas just written;
                # Excel rebuilds it silently when absent, but repairs loudly
                # when it is stale.
                continue
            elif has_external and name.startswith(drop_prefixes):
                continue
            elif has_external and name == "xl/workbook.xml":
                zf_out.writestr(item, wb_xml.encode("utf-8"))
            elif has_external and name == "xl/_rels/workbook.xml.rels":
                zf_out.writestr(item, rels_xml.encode("utf-8"))
            elif has_external and name == "[Content_Types].xml":
                zf_out.writestr(item, ct_xml.encode("utf-8"))
            else:
                zf_out.writestr(item, zf_in.read(name))

    return out.getvalue()


def _scale_from_dict(d: dict):
    """Rebuild a Scale from its serialised form."""
    bands = [(int(u), str(l)) for u, l in d.get("bands", [])]
    matrix = {
        (_scoring._norm(e["brut"]), _scoring._norm(e["efficacite"])): e["net"]
        for e in d.get("matrix", [])
    }
    return _scoring.Scale(
        bands=bands or list(_scoring.DEFAULT_BANDS),
        matrix=matrix or _scoring._build_matrix(_scoring._DEFAULT_MATRIX_ROWS),
        efficacite_vocab=d.get("efficacite_vocab") or list(_scoring.DEFAULT_EFFICACITE),
        source=d.get("source", "default"),
        matrix_conflicts=d.get("matrix_conflicts", []),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ollama AI suggestion
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(row: dict) -> str:
    return f"""Constat de risque :
{row['constats']}

Scénario de risque : {row['scenario']}
Site concerné : {row['site']}
Niveau de risque brut : {row.get('risque_brut') or 'non évalué'} (score {row.get('nrb') or '—'} = probabilité {row.get('prob') or '—'} × impact {row.get('impact') or '—'})

Éléments de maîtrise déjà en place :
{row['maitrise']}

Efficacité des contrôles actuels : {row['efficacite']}
Risque net après contrôles : {row.get('risque_net') or 'non évalué'}
Option de traitement retenue : {row['option']}

Propose un Plan d'actions pour traiter ce constat."""


def _looks_degenerate(text: str) -> bool:
    """
    Small models occasionally echo their own system prompt back instead of
    answering it, or fall into a loop repeating the same sentence until the
    token budget runs out. Both failure modes leave a tell: the same
    non-trivial line appears several times. Catch that rather than shipping
    it into a client's action-plan spreadsheet.
    """
    lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 15]
    if not lines:
        return False
    from collections import Counter
    counts = Counter(lines)
    most_common, n = counts.most_common(1)[0]
    return n >= 3


def stream_suggestion(
    row: dict,
    model: str,
    timeout: float = 120.0,
) -> Generator[str, None, None]:
    """
    Stream an AI suggestion for a risk row via Ollama.
    Yields SSE-formatted strings: data lines + final timing line.

    Uses /api/chat with the instructions on the "system" role and the risk
    constat on "user", rather than /api/generate's flat system+prompt
    concatenation — small instruct models follow the former far more
    reliably and are much less prone to echoing the instructions back as
    if they were the answer. repeat_penalty is set explicitly (Ollama's
    default is too weak to reliably stop a small model from looping on the
    same sentence).
    """
    prompt = build_prompt(row)

    def _payload(repeat_penalty: float, temperature: float) -> dict:
        return {
            "model": model,
            "messages": [
                {"role": "system", "content": PCA_CONTEXT},
                {"role": "user", "content": prompt},
            ],
            "stream": True,
            "options": {
                "temperature": temperature,
                "top_p": 0.9,
                "repeat_penalty": repeat_penalty,
                "num_predict": 400,
            }
        }

    def _run(client: httpx.Client, repeat_penalty: float, temperature: float,
             live: bool):
        """One generation attempt. If live, tokens are yielded as they
        arrive (normal streaming feel); if not, they're only accumulated —
        used for the silent retry so a bad first attempt is never shown."""
        text = ""
        first_tok = None
        t0 = time.perf_counter()
        with client.stream("POST", f"{OLLAMA_BASE}/api/chat",
                           json=_payload(repeat_penalty, temperature)) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                except Exception:
                    continue
                token = chunk.get("message", {}).get("content", "")
                if token:
                    if first_tok is None:
                        first_tok = time.perf_counter() - t0
                    text += token
                    if live:
                        yield f"data: {json.dumps({'type':'token','text':token})}\n\n"
                if chunk.get("done"):
                    break
        return text, (first_tok or (time.perf_counter() - t0)), time.perf_counter() - t0

    start = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout) as client:
            full_text, first_token_time, _ = yield from _run(
                client, repeat_penalty=1.3, temperature=0.3, live=True)

            if _looks_degenerate(full_text):
                # First attempt looped or echoed the prompt. Retry once,
                # silently (no tokens sent to the client until we have a
                # clean result) with a stronger repeat penalty.
                retry_text, _, _ = yield from _run(client, repeat_penalty=1.6,
                                                   temperature=0.5, live=False)
                if retry_text.strip() and not _looks_degenerate(retry_text):
                    full_text = retry_text
                    for chunk_text in [full_text[i:i+40] for i in range(0, len(full_text), 40)]:
                        yield f"data: {json.dumps({'type':'token','text':chunk_text})}\n\n"
                # If the retry is also degenerate, ship the original first
                # attempt rather than nothing — it's visibly wrong in the
                # editor and the user can hit "regenerate" themselves.

            elapsed = time.perf_counter() - start
            yield f"data: {json.dumps({'type':'done','elapsed':round(elapsed,2),'ttft':round(first_token_time or elapsed,2),'model':model,'text':full_text})}\n\n"
    except httpx.ConnectError:
        yield f"data: {json.dumps({'type':'error','message':'Ollama non disponible — lancez `ollama serve` sur ce PC.'})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"


def list_ollama_models() -> list[str]:
    """Return list of locally available Ollama models."""
    try:
        r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=5)
        data = r.json()
        return [m["name"] for m in data.get("models", [])]
    except Exception:
        return []
