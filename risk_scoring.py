"""
risk_scoring.py — the Probabilité × Impact → Risque brut → Risque net chain.

Devoteam's risk workbooks encode this as three spreadsheet formulas:

    Niveau du Risque Brut  =  Impact * Probabilité
    Risque brut            =  VLOOKUP(niveau, Echelle!$C:$D, 2, 0)
    Risque net             =  a 4×5 nested IF over (risque brut, efficacité)

Recomputing them here, rather than trusting the cached cell values, matters
because the source workbooks routinely ship with the chain broken: the
AL BARAKA file resolves its first 29 VLOOKUPs against a `Z.xlsx` sitting on
the original author's Downloads folder, so those cells cannot recalculate on
any other machine.  The values still *display* — Excel caches them — which is
exactly what makes the breakage easy to miss.

The scale and the matrix are data, not code.  read_scale() lifts them from
whichever workbook was uploaded so a client working on a 5×5 scale is scored
on their own scale, and DEFAULT_SCALE covers files that ship without the
reference sheets.
"""
from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass, field
from typing import Optional


def _norm(s) -> str:
    """
    Fold to a comparison key: lowercase, accents stripped, runs of any
    non-alphanumeric collapsed to a single space.

    The workbooks are inconsistent in ways that all have to compare equal:
    `Echelle` emits "Extrême" while `Matrice Risque-Maitrise` heads the same
    column "Extreme", and the efficacité dropdown offers
    "Incomplet/inefficace" where the formula tests "Incomplet/Inefficace".
    """
    if s is None:
        return ""
    txt = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    out = []
    prev_sep = False
    for ch in txt.lower():
        if ch.isalnum():
            out.append(ch)
            prev_sep = False
        elif not prev_sep:
            out.append(" ")
            prev_sep = True
    return "".join(out).strip()


ACCEPTED = "Accepté"

# Canonical efficacité vocabulary, weakest control first.  Taken from the
# data-validation list on the "Efficacité du contrôle" column.
DEFAULT_EFFICACITE = [
    "Inexistant",
    "Incomplet/inefficace",
    "Assez Satisfaisant",
    "Satisfaisant",
    "Très Satisfaisant",
]

# Devoteam standard bands: (inclusive upper bound, label).
DEFAULT_BANDS = [(3, "Faible"), (8, "Moyen"), (11, "Fort"), (16, "Extrême")]

# Devoteam standard mitigation matrix, keyed (risque brut, efficacité).
# Mirrors the "Matrice Risque-Maitrise" sheet and the nested IF verbatim.
_DEFAULT_MATRIX_ROWS = {
    "Très Satisfaisant":    {"Faible": ACCEPTED, "Moyen": ACCEPTED, "Fort": "Faible", "Extrême": "Faible"},
    "Satisfaisant":         {"Faible": ACCEPTED, "Moyen": ACCEPTED, "Fort": "Faible", "Extrême": "Moyen"},
    "Assez Satisfaisant":   {"Faible": ACCEPTED, "Moyen": "Faible", "Fort": "Moyen",  "Extrême": "Fort"},
    "Incomplet/inefficace": {"Faible": ACCEPTED, "Moyen": "Moyen",  "Fort": "Fort",   "Extrême": "Extrême"},
    "Inexistant":           {"Faible": ACCEPTED, "Moyen": "Moyen",  "Fort": "Fort",   "Extrême": "Extrême"},
}


def _build_matrix(rows: dict) -> dict:
    """Flatten {efficacité: {brut: net}} into {(norm brut, norm eff): net}."""
    return {
        (_norm(brut), _norm(eff)): net
        for eff, per_brut in rows.items()
        for brut, net in per_brut.items()
    }


@dataclass
class Scale:
    """A workbook's risk-scoring configuration."""
    bands: list = field(default_factory=lambda: list(DEFAULT_BANDS))
    matrix: dict = field(default_factory=lambda: _build_matrix(_DEFAULT_MATRIX_ROWS))
    efficacite_vocab: list = field(default_factory=lambda: list(DEFAULT_EFFICACITE))
    source: str = "default"          # "default" | "workbook"
    # Pairs where the workbook's Matrice sheet disagrees with the formula the
    # workbook actually computes with.  Surfaced, never silently applied.
    matrix_conflicts: list = field(default_factory=list)

    # ── derived ──────────────────────────────────────────────────────────────

    @property
    def max_score(self) -> int:
        return self.bands[-1][0] if self.bands else 16

    @property
    def axis_max(self) -> int:
        """
        Highest value offered in the Probabilité / Impact dropdowns.

        These matrices are square, so the axis is the square root of the top
        band — 16 → 4, 25 → 5.  Rounded up so a non-square top bound still
        offers enough room rather than silently truncating the choices.
        """
        return max(2, math.ceil(math.sqrt(self.max_score)))

    def band_for(self, score) -> str:
        """Map a raw Impact×Probabilité score to its band label."""
        try:
            n = float(score)
        except (TypeError, ValueError):
            return ""
        if n <= 0:
            return ""
        for upper, label in self.bands:
            if n <= upper:
                return label
        # Above the top band — the workbook's own VLOOKUP would return 0 here,
        # but clamping to the worst band is the honest reading of the intent.
        return self.bands[-1][1] if self.bands else ""

    def canonical_efficacite(self, value) -> str:
        """Resolve a free-text efficacité to the vocabulary entry it means."""
        key = _norm(value)
        if not key:
            return ""
        for v in self.efficacite_vocab:
            if _norm(v) == key:
                return v
        # Prefix match catches "Incomplet" against "Incomplet/inefficace".
        for v in self.efficacite_vocab:
            nv = _norm(v)
            if nv.startswith(key) or key.startswith(nv):
                return v
        return ""

    def net_for(self, risque_brut, efficacite) -> str:
        """Look up the residual risk for a (brut, efficacité) pair."""
        nb, ne = _norm(risque_brut), _norm(efficacite)
        if not nb:
            return ""
        # Every matrix row accepts a Faible brut outright; the nested IF short
        # -circuits on it before efficacité is even consulted.
        if nb == _norm("Faible"):
            return ACCEPTED
        if not ne:
            return ""
        return self.matrix.get((nb, ne), "")

    def to_dict(self) -> dict:
        """Serialise for the API so the browser scores against the same data."""
        return {
            "bands": [[u, l] for u, l in self.bands],
            "matrix": [
                {"brut": b, "efficacite": e, "net": n}
                for (b, e), n in self.matrix.items()
            ],
            "efficacite_vocab": list(self.efficacite_vocab),
            "axis_max": self.axis_max,
            "max_score": self.max_score,
            "accepted_label": ACCEPTED,
            "source": self.source,
            "matrix_conflicts": list(self.matrix_conflicts),
        }


DEFAULT_SCALE = Scale()


# ─────────────────────────────────────────────────────────────────────────────
# Reading a scale out of an uploaded workbook
# ─────────────────────────────────────────────────────────────────────────────

def _find_sheet(wb, *keywords) -> Optional[str]:
    """First sheet whose normalised name contains all keywords."""
    for name in wb.sheetnames:
        n = _norm(name)
        if all(k in n for k in keywords):
            return name
    return None


def _read_bands(wb) -> Optional[list]:
    """
    Build bands from an "Echelle" sheet laid out as (score, label) pairs.

    Scanned rather than read at fixed coordinates because the two known
    layouts differ: `Echelle` puts the pairs in C:D from row 6, `Echelle de
    risque` in B:C from row 4.
    """
    name = _find_sheet(wb, "echelle")
    if not name:
        return None
    ws = wb[name]

    pairs: list[tuple[int, str]] = []
    for row in ws.iter_rows():
        score = label = None
        for cell in row:
            v = cell.value
            if v is None or v == "":
                continue
            if score is None and isinstance(v, (int, float)) and not isinstance(v, bool):
                score = int(v)
            elif isinstance(v, str) and v.strip():
                label = v.strip()
        if score is not None and label:
            pairs.append((score, label))

    if len(pairs) < 2:
        return None

    # Collapse the score→label listing into (upper bound, label) bands.
    pairs.sort(key=lambda p: p[0])
    bands: list[tuple[int, str]] = []
    for score, label in pairs:
        if bands and _norm(bands[-1][1]) == _norm(label):
            bands[-1] = (score, bands[-1][1])
        else:
            bands.append((score, label))
    return bands


def _canon_in(vocab: list, value) -> str:
    """Resolve a label onto a vocabulary, tolerating prefix abbreviations."""
    key = _norm(value)
    if not key:
        return ""
    for v in vocab:
        if _norm(v) == key:
            return v
    for v in vocab:
        nv = _norm(v)
        if nv.startswith(key) or key.startswith(nv):
            return v
    return str(value).strip()


def _read_matrix(wb, bands, vocab) -> Optional[dict]:
    """
    Build the mitigation matrix from a "Matrice Risque-Maitrise" grid: band
    labels across the header row, efficacité labels down the first column.

    Both axes are canonicalised onto the workbook's own vocabularies — the
    sheet heads its worst column "Extreme" where the formulas test "Extrême",
    and labels its fourth row "Incomplet" where the dropdown offers
    "Incomplet/inefficace".
    """
    name = _find_sheet(wb, "matrice")
    if not name:
        return None
    ws = wb[name]

    band_labels = [b[1] for b in bands]
    band_keys = {_norm(l) for l in band_labels}

    header_row = header_cols = None
    for row in ws.iter_rows():
        hits = {c.column: c.value for c in row
                if isinstance(c.value, str) and _norm(c.value) in band_keys}
        if len(hits) >= 2:
            header_row, header_cols = row[0].row, hits
            break
    if not header_cols:
        return None

    matrix: dict = {}
    for row in ws.iter_rows(min_row=header_row + 1):
        eff = next((c.value for c in row
                    if isinstance(c.value, str) and c.value.strip()), None)
        if not eff:
            continue
        eff_canon = _canon_in(vocab, eff)
        for col, band in header_cols.items():
            cell = ws.cell(row=row[0].row, column=col).value
            if cell is None or str(cell).strip() == "":
                continue
            out = _canon_in(band_labels + [ACCEPTED], cell)
            matrix[(_norm(_canon_in(band_labels, band)), _norm(eff_canon))] = out

    return matrix or None


def _read_efficacite_vocab(wb) -> Optional[list]:
    """Recover the efficacité vocabulary from the column's dropdown list."""
    for ws in wb.worksheets:
        for dv in getattr(ws, "data_validations", None).dataValidation if getattr(ws, "data_validations", None) else []:
            f1 = (dv.formula1 or "").strip()
            if not (f1.startswith('"') and f1.endswith('"')):
                continue
            opts = [o.strip() for o in f1[1:-1].split(",") if o.strip()]
            if any("satisfaisant" in _norm(o) for o in opts) and len(opts) >= 3:
                return opts
    return None


def read_scale(wb) -> Scale:
    """
    Derive a Scale from an uploaded workbook, falling back per-part.

    Bands and vocabulary are taken from the file; the matrix is not.  In the
    shipped Devoteam template the "Matrice Risque-Maitrise" sheet contradicts
    the nested IF it supposedly documents — the sheet grades
    Satisfaisant × Moyen as "Faible" where the formula returns "Accepté", and
    it is the formula that produced every cached value in the workbook.  So
    the formula's logic governs, and the sheet is consulted only to fill pairs
    the formula leaves undefined and to report the disagreement.
    """
    bands = _read_bands(wb)
    from_workbook = bands is not None
    bands = bands or list(DEFAULT_BANDS)

    try:
        vocab = _read_efficacite_vocab(wb)
    except Exception:
        vocab = None
    if vocab:
        from_workbook = True
    else:
        vocab = list(DEFAULT_EFFICACITE)

    matrix = _build_matrix(_DEFAULT_MATRIX_ROWS)
    conflicts: list = []

    try:
        sheet_matrix = _read_matrix(wb, bands, vocab)
    except Exception:
        sheet_matrix = None

    if sheet_matrix:
        for key, sheet_val in sheet_matrix.items():
            current = matrix.get(key)
            if current is None:
                matrix[key] = sheet_val          # a pair the formula omits
            elif _norm(current) != _norm(sheet_val):
                brut_key, eff_key = key
                conflicts.append({
                    "risque_brut": brut_key.title(),
                    "efficacite": _canon_in(vocab, eff_key),
                    "formula": current,
                    "sheet": sheet_val,
                })

    return Scale(
        bands=bands,
        matrix=matrix,
        efficacite_vocab=vocab,
        source="workbook" if from_workbook else "default",
        matrix_conflicts=conflicts,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

def _num(v):
    """Coerce a cell to a number; risk sheets store 3 and '3' interchangeably."""
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return v
    try:
        return float(str(v).strip().replace(",", "."))
    except ValueError:
        return None


def score(prob, impact, efficacite, scale: Scale | None = None) -> dict:
    """
    Run the full chain for one row.

    Returns the three derived fields plus the efficacité resolved onto the
    vocabulary, so callers can show what a free-text entry was understood as.
    """
    sc = scale or DEFAULT_SCALE
    p, i = _num(prob), _num(impact)

    nrb = ""
    if p is not None and i is not None:
        raw = p * i
        nrb = int(raw) if float(raw).is_integer() else raw

    risque_brut = scale_brut = sc.band_for(nrb) if nrb != "" else ""
    eff = sc.canonical_efficacite(efficacite)
    risque_net = sc.net_for(scale_brut, eff) if scale_brut else ""

    return {
        "nrb": nrb,
        "risque_brut": risque_brut,
        "risque_net": risque_net,
        "efficacite_canon": eff,
    }


def diff_against(row: dict, computed: dict) -> dict:
    """
    Report where an uploaded row disagrees with the recomputed values.

    Only meaningful differences are reported: a blank cell in the source is an
    absence, not a conflict, and "Extreme"/"Extrême" is the same answer.
    """
    diverged: dict = {}
    for field_name in ("nrb", "risque_brut", "risque_net"):
        original = row.get(field_name)
        if original is None or str(original).strip() == "":
            continue
        new = computed.get(field_name)
        if str(new).strip() == "":
            continue
        if field_name == "nrb":
            a, b = _num(original), _num(new)
            same = a is not None and b is not None and abs(a - b) < 1e-9
        else:
            same = _norm(original) == _norm(new)
        if not same:
            diverged[field_name] = str(original).strip()
    return diverged
