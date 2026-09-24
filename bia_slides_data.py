"""
bia_slides_data.py — Extract all data needed to render the BIA Rapport slide gallery.
Returns a single JSON-serialisable dict consumed by the frontend.
"""
from __future__ import annotations
import io, math
from typing import Any, Optional

import re
import pandas as pd

DMIA_ORDER = ["H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4","J+5","J+10","J+15","J+30","Au-delà"]
TIME_COLS  = ["H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4","J+5","J+10","J+15"]


def _normalize_dmia(v: str) -> str:
    """
    Normalize raw DMIA strings to DMIA_ORDER format: '3J'→'J+3', '4H'→'H+4', '0H'/'H0'→'H0'.

    Handles the real-world variants found in these fiches beyond the clean
    "3J"/"4H" case, all confirmed present in real workbooks:
      - a space between the number and the unit ("1 J", "2 H") — common when
        the cell was typed rather than copied from a template
      - the unit written before the number ("H8" instead of "8H")
      - a multi-value cell carrying a primary duration plus a footnoted
        exception on a second line ("5J*\\n1J**") — the first value is taken
        as the primary commitment; the footnote qualifies it, it doesn't
        replace it
    """
    if not v:
        return v
    v = v.strip()
    if re.match(r'^[HhJj]\+\d', v):   # already J+N or H+N
        return v.upper().replace('J+', 'J+').replace('H+', 'H+')
    if re.match(r'^[Hh]0$', v):        # H0 already
        return "H0"

    # Multi-value cell: take the first line as the primary duration, with any
    # trailing footnote marker (*, °) stripped before pattern matching.
    first = re.split(r'[\n;]', v)[0].strip()
    first = re.sub(r'[*°]+$', '', first).strip()
    candidate = first if first else v

    m = re.match(r'^(\d+)\s*[Jj]$', candidate)   # NJ / N J → J+N
    if m:
        return f"J+{m.group(1)}"
    m = re.match(r'^(\d+)\s*[Hh]$', candidate)   # NH / N H → H+N
    if m:
        n = int(m.group(1))
        return "H0" if n == 0 else f"H+{n}"
    m = re.match(r'^[Jj]\s*(\d+)$', candidate)   # JN (inverted) → J+N
    if m:
        return f"J+{m.group(1)}"
    m = re.match(r'^[Hh]\s*(\d+)$', candidate)   # HN (inverted) → H+N
    if m:
        n = int(m.group(1))
        return "H0" if n == 0 else f"H+{n}"

    return v


def _dmia_days(v: str) -> Optional[float]:
    """
    DMIA expressed in days, or None when it carries no duration.

    Accepts both spellings found in these workbooks — the normalised "J+3" /
    "H+4" / "H0" and the raw "3J" / "4H" / "0H" — by normalising first. Values
    like "Au-delà" or "-" mean "no committed deadline" and return None.
    """
    s = _normalize_dmia(_safe(v))
    if not s:
        return None
    m = re.match(r'^[Hh]\+(\d+(?:[.,]\d+)?)$', s)
    if m:
        return float(m.group(1).replace(",", ".")) / 24.0
    if re.match(r'^[Hh]0$', s):
        return 0.0
    m = re.match(r'^[Jj]\+(\d+(?:[.,]\d+)?)$', s)
    if m:
        return float(m.group(1).replace(",", "."))
    return None


_DMIA_ORDER_DAYS: Optional[dict] = None   # lazily built cache; DMIA_ORDER is fixed


def _dmia_order_days() -> dict:
    days = {}
    for lbl in DMIA_ORDER:
        if lbl == "Au-delà":
            days[lbl] = float("inf")
        elif lbl == "H0":
            days[lbl] = 0.0
        else:
            days[lbl] = _dmia_days(lbl)
    return days


def _normalize_and_bucket_dmia(v: Any) -> str:
    """
    Full pipeline for one raw "DMIA Exprimée" cell: normalize the formatting,
    then snap onto the fixed DMIA_ORDER scale if it parses to a real duration
    that just isn't one of the standard horizons — e.g. "J+7" or "J+20",
    values a department wrote that don't fall on the official H0/H+1/.../J+30
    scale everyone else uses.

    Snapping always rounds UP to the next bucket, never down. DMIA is a
    maximum acceptable outage — an activity due back by day 7 is covered by
    (must not exceed) the J+10 horizon; rounding it down to J+5 would
    understate how urgent it actually is.

    A cell that doesn't parse to any duration at all (blank, "-", free text)
    is returned as "" — deliberately NOT forced into a bucket, since that
    would misrepresent "no DMIA was ever committed" as "committed, just far
    out". Those stay excluded from DMIA_ORDER and are reported separately.
    """
    global _DMIA_ORDER_DAYS
    if pd.isna(v):
        return ""
    normalized = _normalize_dmia(_safe(v))
    if not normalized:
        return ""
    if normalized in DMIA_ORDER:
        return normalized
    days = _dmia_days(normalized)
    if days is None:
        return normalized   # unparseable text — leave as-is, stays excluded from DMIA_ORDER
    if _DMIA_ORDER_DAYS is None:
        _DMIA_ORDER_DAYS = _dmia_order_days()
    for lbl in DMIA_ORDER:
        if _DMIA_ORDER_DAYS[lbl] >= days:
            return lbl
    return "Au-delà"


def _safe(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v).strip()


# Quantity columns in these workbooks are not reliably numeric. Alongside counts,
# contributors mark a requirement with an "X" — "this equipment is needed at this
# horizon" without committing to a number. A single such cell turns the whole
# pandas column to object dtype, and .sum() then raises
# "unsupported operand type(s) for +: 'float' and 'str'".
_PRESENCE_MARKS = {"x", "✓", "✔", "o", "oui", "yes"}


def _numeric_col(series: "pd.Series") -> "pd.Series":
    """
    Coerce a quantity column to numbers.

    Three shapes have to survive, because all three appear in real workbooks:
      "+6"   an increment, which pandas already parses
      "13*"  a count carrying a footnote marker — the 13 is the datum and
             dropping it would silently understate a whole division
      "X"    a requirement recorded without a count

    Presence marks become 1 rather than 0: they denote something real, and
    zeroing them both understates the total and drops the row from any
    "total > 0" filter downstream.
    """
    txt = series.astype(str).str.strip()
    # Leading number, ignoring any trailing footnote marker or unit.
    lead = txt.str.extract(r'^([+-]?\d+(?:[.,]\d+)?)', expand=False).str.replace(",", ".")
    num = pd.to_numeric(lead, errors="coerce")
    marks = txt.str.lower().isin(_PRESENCE_MARKS)
    return num.where(~marks, 1).fillna(0)


def _read(xl: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    if sheet not in xl.sheet_names:
        return pd.DataFrame()
    df = pd.read_excel(xl, sheet_name=sheet, header=4)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    return df.dropna(how="all").reset_index(drop=True)


def _struct(row: pd.Series) -> str:
    for col in ["Structure Niveau 2", "Structure Niveau 1",
                "Département", "Unité", "Division", "Direction", "Service", "Entité"]:
        v = _safe(row.get(col, ""))
        if v and v not in ("-", "nan"):
            return v
    return ""


def _detect_company(xl: pd.ExcelFile) -> str:
    import re
    for sname in xl.sheet_names[1:4]:
        raw = pd.read_excel(xl, sheet_name=sname, header=None, nrows=3)
        for ri in range(3):
            for ci in range(raw.shape[1]):
                v = _safe(raw.iloc[ri, ci])
                m = re.search(r'([A-ZÉÀÂÎÔÙÜ][A-ZÉÀÂÎÔÙÜa-zéàâîôùü\s\-]+(?:ASSURANCES?|BANK|BANQUE|INSURANCE)\s*\w*)', v, re.I)
                if m:
                    return m.group(1).strip().title()
    return "Client"


def extract_slides_data(xlsx_bytes: bytes) -> dict:
    xl = pd.ExcelFile(io.BytesIO(xlsx_bytes))

    imp_df  = _read(xl, "Impact DMIA")
    mc_df   = _read(xl, "Montée en charge")
    apps_df = _read(xl, "Applications IT")
    col_df  = _read(xl, "Collaborateurs Clés")
    doc_df  = _read(xl, "Doc critiques")
    eq_df   = _read(xl, "Autres Eqt IT")

    # Forward-fill structure/hierarchy columns that use merged cells.
    # Every sheet needs this, not just Impact DMIA: the identity columns are
    # vertically merged across each entity's block, so pandas sees the name on
    # the block's first row and NaN on all the rest. Applying it to Impact DMIA
    # alone left the Applications table with a Structure on 22 rows out of 133
    # and blank everywhere else.
    _ID_COLS = ["Structure Niveau 1", "Structure Niveau 2",
                "Division", "Direction", "Unité", "Département", "Service", "Entité"]
    for _df in (imp_df, mc_df, apps_df, col_df, doc_df, eq_df):
        if _df.empty:
            continue
        for col in _ID_COLS:
            if col in _df.columns:
                _df[col] = _df[col].ffill()

    # Normalize DMIA values to standard format (e.g. "3J"→"J+3", "4H"→"H+4")
    # and snap any off-scale-but-valid value (e.g. "J+7") onto the fixed
    # DMIA_ORDER horizon scale. See _normalize_and_bucket_dmia for why this is
    # not just a straight normalize.
    if not imp_df.empty and "DMIA Exprimée" in imp_df.columns:
        imp_df["DMIA Exprimée"] = imp_df["DMIA Exprimée"].apply(_normalize_and_bucket_dmia)

    company = _detect_company(xl)

    # ── 1. Meta ──────────────────────────────────────────────────────────────
    from datetime import date
    meta = {"company": company, "date": date.today().strftime("%B %Y").capitalize()}

    # ── 2. Résumé Exécutif ───────────────────────────────────────────────────
    resume: dict = {}
    if not imp_df.empty and "DMIA Exprimée" in imp_df.columns:
        total = len(imp_df)
        counts = imp_df["DMIA Exprimée"].value_counts().to_dict()
        ordered_dmia = [d for d in DMIA_ORDER if d in counts]
        ordered_counts = [int(counts[d]) for d in ordered_dmia]

        urgent = {"H0","H+1","H+2","H+4","J+1"}
        urgent_n = sum(counts.get(d, 0) for d in urgent)

        structs: list[dict] = []
        grp_s = imp_df[imp_df["DMIA Exprimée"].isin(urgent)].groupby(
            imp_df.apply(lambda r: _struct(r), axis=1)
        )
        for s, g in grp_s:
            if not s:
                continue
            dmia_idxs = [DMIA_ORDER.index(d) for d in g["DMIA Exprimée"] if d in DMIA_ORDER]
            structs.append({
                "structure": s,
                "count": int(len(g)),
                "dmia_min": DMIA_ORDER[min(dmia_idxs)] if dmia_idxs else "?"
            })
        structs.sort(key=lambda x: x["count"], reverse=True)

        resume = {
            "total": total,
            "urgent_n": int(urgent_n),
            "urgent_pct": int(urgent_n / total * 100) if total else 0,
            "dmia_labels": ordered_dmia,
            "dmia_counts": ordered_counts,
            "critical_structures": structs,
        }

    # ── 3. Reprise RH (montée en charge) ─────────────────────────────────────
    reprise_rh: dict = {}
    if not mc_df.empty:
        avail_tc = [c for c in TIME_COLS if c in mc_df.columns]

        # Matched on a prefix, not equality. The sheet writes the metric as
        # "Positions" while this code asked for "Position", so the whole series
        # silently aggregated to zero and the Position-vs-Télétravail chart drew
        # a flat line. Accents and case vary between clients too.
        _labels = (mc_df["Montée en charge exprimée"].astype(str)
                   .str.strip().str.lower()
                   .str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("ascii"))

        def _rows_for(prefix: str) -> "pd.DataFrame":
            return mc_df[_labels.str.startswith(prefix)]

        # Nominal is the division's headcount, written once per division block
        # and vertically merged across it — so it lands on whichever metric row
        # happens to come first, which is Télétravail or Positions as often as
        # Effectif. Summing only the Effectif rows therefore recovered a
        # fraction of the workforce (41 of 267 on a real file), and every
        # percentage computed against it pinned to 100%.
        nominal_total = 0
        if "Nominal" in mc_df.columns:
            # Counted once per contiguous block rather than per row. Two layouts
            # exist in the wild and this handles both: one writes the identity
            # and headcount once and merges them down the block (so the repeats
            # arrive as NaN and are forward-filled), the other repeats them on
            # every row. Splitting on a *change* in either identity or headcount
            # also keeps divisions that legitimately span two blocks with
            # different headcounts from collapsing into one.
            nom = pd.to_numeric(mc_df["Nominal"], errors="coerce").ffill()
            id_cols = [c for c in ("Division", "Unité", "Département") if c in mc_df.columns]
            if id_cols:
                ident = mc_df[id_cols].ffill().astype(str).agg(" | ".join, axis=1)
            else:
                ident = pd.Series([""] * len(mc_df), index=mc_df.index)
            key = ident + "||" + nom.astype(str)
            blocks = (key != key.shift()).cumsum()
            nominal_total = int(nom.groupby(blocks).first().fillna(0).sum())

        def _agg(prefix: str) -> list[int]:
            rows = _rows_for(prefix)
            if rows.empty:
                return [0] * len(avail_tc)
            return [int(_numeric_col(rows[c]).sum()) for c in avail_tc]

        reprise_rh = {
            "time_cols": avail_tc,
            "nominal": nominal_total,
            "effectif": _agg("effectif"),
            "position": _agg("position"),
            "teletravail": _agg("teletravail"),
        }

    # Helper: try multiple column name variants, return first non-empty value
    def _get(row: pd.Series, *keys: str) -> str:
        for k in keys:
            v = _safe(row.get(k, ""))
            if v:
                return v
        return ""

    # ── 4. Zoom sur la Reprise (per DMIA) ────────────────────────────────────
    zoom: list[dict] = []
    if not imp_df.empty and "DMIA Exprimée" in imp_df.columns:
        for dmia in DMIA_ORDER:
            grp = imp_df[imp_df["DMIA Exprimée"] == dmia]
            if grp.empty:
                continue
            structs_list = grp.apply(lambda r: _struct(r), axis=1).unique().tolist()
            activities = []
            for _, row in grp.iterrows():
                activities.append({
                    "structure": _struct(row),
                    "activite": _safe(row.get("Activité", "")),
                    "im_4h": _get(row, "IM 4H", "IM < 1 jour"),
                    "score": _get(row, "Score 2-3J", "Score ≥ 5 jours", "Score < 1 jour"),
                })
            zoom.append({
                "dmia": dmia,
                "nb_structures": len([s for s in structs_list if s]),
                "nb_activities": len(activities),
                "activities": activities,
            })

    # ── 5. Tableau des Impacts (per DMIA) ────────────────────────────────────
    impacts: list[dict] = []
    if not imp_df.empty and "DMIA Exprimée" in imp_df.columns:
        for dmia in DMIA_ORDER:
            grp = imp_df[imp_df["DMIA Exprimée"] == dmia]
            if grp.empty:
                continue
            rows = []
            for _, row in grp.iterrows():
                rows.append({
                    "structure": _struct(row),
                    "activite": _safe(row.get("Activité", "")),
                    # Image / Réputation
                    "im_1h":   _get(row, "IM 1H",   "IM < 1 jour",  "IM 1h"),
                    "im_4h":   _get(row, "IM 4H",   "IM < 1 jour",  "IM 4h"),
                    "im_1j":   _get(row, "IM 1J",   "IM < 1 jour",  "IM 1j"),
                    "im_2_3j": _get(row, "IM 2-3J", "IM ≥ 5 jours", "IM 2-3j"),
                    # Désorganisation interne
                    "di_1h":   _get(row, "DI 1H",   "DI < 1 jour",  "DI 1h"),
                    "di_4h":   _get(row, "DI 4H",   "DI < 1 jour",  "DI 4h"),
                    "di_1j":   _get(row, "DI 1J",   "DI < 1 jour",  "DI 1j"),
                    "di_2_3j": _get(row, "DI 2-3J", "DI ≥ 5 jours", "DI 2-3j"),
                    # Juridique / Réglementaire
                    "jr_1h":   _get(row, "JR 1H",   "JR < 1 jour",  "JR 1h"),
                    "jr_4h":   _get(row, "JR 4H",   "JR < 1 jour",  "JR 4h"),
                    "jr_1j":   _get(row, "JR 1J",   "JR < 1 jour",  "JR 1j"),
                    "jr_2_3j": _get(row, "JR 2-3J", "JR ≥ 5 jours", "JR 2-3j"),
                    # Financier
                    "fin_1h":  _get(row, "FIN 1H",  "FIN < 1 jour", "FIN 1h"),
                    "fin_4h":  _get(row, "FIN 4H",  "FIN < 1 jour", "FIN 4h"),
                    "fin_1j":  _get(row, "FIN 1J",  "FIN < 1 jour", "FIN 1j"),
                    "fin_2_3j":_get(row, "FIN 2-3J","FIN ≥ 5 jours","FIN 2-3j"),
                    "score":   _get(row, "Score 2-3J", "Score ≥ 5 jours", "Score < 1 jour"),
                    "commentaire": _safe(row.get("Commentaires", "")),
                })
            impacts.append({"dmia": dmia, "rows": rows})

    # ── 6. Applications (per lot) ─────────────────────────────────────────────
    def _lot(dmia: str) -> int:
        """
        Lot 1 ≤ 1 jour · Lot 2 de 2 à 5 jours · Lot 3 au-delà (ou inconnu).

        Classified on the parsed duration rather than by matching a fixed list
        of spellings. The previous version compared against "J+1", "J+2"… but
        these workbooks overwhelmingly write the raw form — "3J", "4H", "10J" —
        so all but a handful of rows fell through to Lot 3: a real file put 130
        applications in Lot 3 and left Lot 2 empty, with 3J entries sitting
        under "Au-delà de 5J".
        """
        days = _dmia_days(dmia)
        if days is None:
            return 3          # "Au-delà", "-", or unparseable
        if days <= 1:
            return 1
        if days <= 5:
            return 2
        return 3

    apps: list[dict] = [{"lot": 1, "label": "Reprise ≤ J+1", "rows": []},
                         {"lot": 2, "label": "Reprise J+2 à J+5", "rows": []},
                         {"lot": 3, "label": "Reprise au-delà de J+5", "rows": []}]
    if not apps_df.empty:
        apps_df = apps_df.copy()
        apps_df["DMIA"] = apps_df["DMIA"].fillna("Au-delà")
        for _, row in apps_df.iterrows():
            dmia = _safe(row.get("DMIA", "Au-delà")) or "Au-delà"
            lot  = _lot(dmia)
            apps[lot - 1]["rows"].append({
                "structure":     _struct(row),
                "application":   _safe(row.get("Application", "")),
                "dmia":          dmia,
                "criticite":     _safe(row.get("Niveau de criticité", "")),
                "contournement": _safe(row.get("Contournement envisageable", ""))[:120],
            })

        # Ascending by real delay within each lot. Sorting on the DMIA text
        # would put "J+10" before "J+2" (comparing '1' against '2' character
        # by character); rows without a committed deadline sort last.
        for bucket in apps:
            bucket["rows"].sort(
                key=lambda r: (_dmia_days(r["dmia"]) if _dmia_days(r["dmia"]) is not None
                               else float("inf")))

    # ── 7. Collaborateurs clés ────────────────────────────────────────────────
    collaborateurs: dict = {"pct_with_supp": 0, "total": 0, "rows": []}
    if not col_df.empty:
        total_c = len(col_df)
        with_s  = col_df["Suppléants possibles"].notna().sum()
        rows_c  = []
        for _, row in col_df.iterrows():
            rows_c.append({
                "structure":  _struct(row),
                "nom":        _safe(row.get("Nom", "")),
                "prenom":     _safe(row.get("Prénom", "")),
                "fonction":   _safe(row.get("Fonction", "")),
                "poste":      _safe(row.get("Poste", "")),
                "anciennete": _safe(row.get("Ancienneté dans le poste", "")),
                "suppleant":  _safe(row.get("Suppléants possibles", "")),
            })
        collaborateurs = {
            "pct_with_supp": int(with_s / total_c * 100) if total_c else 0,
            "total": int(total_c),
            "rows": rows_c,
        }

    # ── 8. Documents critiques ────────────────────────────────────────────────
    documents: dict = {"dupliques": [], "non_dupliques": []}
    if not doc_df.empty and "Duplication \n(O / N)" in doc_df.columns:
        for _, row in doc_df.iterrows():
            d = _safe(row.get("Duplication \n(O / N)", ""))
            entry = {
                "structure":  _struct(row),
                "document":   _safe(row.get("Documents / Fichiers", "")),
                "type":       _safe(row.get("Type de stockage\n(Electronique / Papier)", "")),
                "modalite":   _safe(row.get("Modalité de Duplication", "")),
            }
            if d == "O":
                documents["dupliques"].append(entry)
            else:
                documents["non_dupliques"].append(entry)

    # ── 9. Équipements ────────────────────────────────────────────────────────
    equipements: list[dict] = []
    if not eq_df.empty and "Désignation" in eq_df.columns:
        avail_tc = [c for c in TIME_COLS if c in eq_df.columns]
        eq_num = eq_df[["Désignation"]].copy()
        for c in avail_tc:
            eq_num[c] = _numeric_col(eq_df[c])
        grp = eq_num.groupby("Désignation")[avail_tc].sum()
        grp["Total"] = grp[avail_tc].sum(axis=1)
        grp = grp[grp["Total"] > 0].reset_index()
        for _, row in grp.iterrows():
            entry: dict = {"designation": _safe(row["Désignation"]), "total": int(row["Total"])}
            for tc in avail_tc:
                entry[tc] = int(row[tc]) if row[tc] > 0 else 0
            equipements.append(entry)
        equipements.sort(key=lambda x: x["total"], reverse=True)

    return {
        "meta":           meta,
        "resume":         resume,
        "reprise_rh":     reprise_rh,
        "zoom":           zoom,
        "impacts":        impacts,
        "apps":           apps,
        "collaborateurs": collaborateurs,
        "documents":      documents,
        "equipements":    equipements,
        "time_cols":      TIME_COLS,
    }
