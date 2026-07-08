"""
bia_slides_data.py — Extract all data needed to render the BIA Rapport slide gallery.
Returns a single JSON-serialisable dict consumed by the frontend.
"""
from __future__ import annotations
import io, math
from typing import Any

import pandas as pd

DMIA_ORDER = ["H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4","J+5","J+10","J+15","J+30","Au-delà"]
TIME_COLS  = ["H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4","J+5","J+10","J+15"]


def _safe(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v).strip()


def _read(xl: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    if sheet not in xl.sheet_names:
        return pd.DataFrame()
    df = pd.read_excel(xl, sheet_name=sheet, header=4)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
    return df.dropna(how="all").reset_index(drop=True)


def _struct(row: pd.Series) -> str:
    for col in ["Structure Niveau 2", "Structure Niveau 1"]:
        v = _safe(row.get(col, ""))
        if v and v != "-":
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
        nominal_total = 0
        effectif_rows = mc_df[mc_df["Montée en charge exprimée"] == "Effectif"]
        if not effectif_rows.empty:
            nominal_total = int(effectif_rows["Nominal"].sum(skipna=True))

        def _agg(row_type: str) -> list[int]:
            rows = mc_df[mc_df["Montée en charge exprimée"] == row_type]
            return [int(rows[c].sum(skipna=True)) for c in avail_tc]

        reprise_rh = {
            "time_cols": avail_tc,
            "nominal": nominal_total,
            "effectif": _agg("Effectif"),
            "position": _agg("Position"),
            "teletravail": _agg("Télétravail"),
        }

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
                    "im_4h": _safe(row.get("IM 4H", "")),
                    "score": _safe(row.get("Score 2-3J", "")),
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
                    "im_1h":  _safe(row.get("IM 1H", "")),
                    "im_4h":  _safe(row.get("IM 4H", "")),
                    "im_1j":  _safe(row.get("IM 1J", "")),
                    "im_2_3j":_safe(row.get("IM 2-3J", "")),
                    "score":  _safe(row.get("Score 2-3J", "")),
                    "commentaire": _safe(row.get("Commentaires", "")),
                })
            impacts.append({"dmia": dmia, "rows": rows})

    # ── 6. Applications (per lot) ─────────────────────────────────────────────
    def _lot(dmia: str) -> int:
        if dmia in ("H0","H+1","H+2","H+4","J+1"):
            return 1
        if dmia in ("J+2","J+3","J+4","J+5"):
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
        grp = eq_df.groupby("Désignation")[avail_tc].sum()
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
