"""
bia_report_pptx.py — Generate a BIA Rapport PPTX from a Synthèse BIA xlsx.
Style: Devoteam  (#E8394C red, #1A1814 dark, white / light-gray content slides)
Dimensions: 20" × 11.25"  (same widescreen as the GAT template)
"""
from __future__ import annotations
import io, math
from datetime import date
from typing import Any

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Inches, Pt
from lxml import etree

from bia_etl import _dmia_to_hours


# ── palette ────────────────────────────────────────────────────────────────
RED   = RGBColor(0xE8, 0x39, 0x4C)
DARK  = RGBColor(0x1A, 0x18, 0x14)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
LGRAY = RGBColor(0xF4, 0xF3, 0xF0)
MGRAY = RGBColor(0xD0, 0xCE, 0xC8)
DKGR  = RGBColor(0x5A, 0x58, 0x52)
GRGRN = RGBColor(0x10, 0xB9, 0x81)  # green for "ok"
AMBER = RGBColor(0xF5, 0x9E, 0x0B)  # amber

# slide size (20" × 11.25")
SW = Inches(20)
SH = Inches(11.25)

# DMIA ordering
DMIA_ORDER = ["H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4","J+5","J+10","J+15","J+30","Au-delà"]


# ── helpers ────────────────────────────────────────────────────────────────
def _rgb(r: RGBColor) -> str:
    return f"{r[0]:02X}{r[1]:02X}{r[2]:02X}"

def _new_prs() -> Presentation:
    prs = Presentation()
    prs.slide_width  = SW
    prs.slide_height = SH
    return prs

def _blank_layout(prs: Presentation):
    return prs.slide_layouts[6]  # blank

def _add_rect(slide, x, y, w, h, fill: RGBColor | None = None, line: RGBColor | None = None):
    shp = slide.shapes.add_shape(1, Inches(x), Inches(y), Inches(w), Inches(h))
    shp.line.fill.background()
    if fill:
        shp.fill.solid()
        shp.fill.fore_color.rgb = fill
    else:
        shp.fill.background()
    if line:
        shp.line.color.rgb = line
    else:
        shp.line.fill.background()
    return shp

def _add_text(slide, text: str, x, y, w, h,
              size=14, bold=False, color: RGBColor = DARK,
              align=PP_ALIGN.LEFT, wrap=True):
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = wrap
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.color.rgb = color
    run.font.name = "Calibri"
    return tb

def _add_multiline(slide, lines: list[tuple[str,int,bool,RGBColor]], x, y, w, h, align=PP_ALIGN.LEFT):
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = True
    for i, (text, size, bold, color) in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        run = p.add_run()
        run.text = text
        run.font.size = Pt(size)
        run.font.bold = bold
        run.font.color.rgb = color
        run.font.name = "Calibri"
    return tb

def _img_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight",
                facecolor="none", transparent=True)
    buf.seek(0)
    return buf.read()

def _safe(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(v).strip()


# ── slide factories ─────────────────────────────────────────────────────────

def _slide_cover(prs: Presentation, company: str, report_date: str):
    sl = prs.slides.add_slide(_blank_layout(prs))
    # full dark background
    _add_rect(sl, 0, 0, 20, 11.25, fill=DARK)
    # red accent bar bottom
    _add_rect(sl, 0, 10.5, 20, 0.75, fill=RED)
    # Devoteam label
    _add_text(sl, "Devoteam | AI-driven tech consulting",
              1, 0.6, 12, 0.5, size=11, color=RGBColor(0xB0,0xAE,0xA8))
    # Main title
    _add_text(sl, "Actualisation et Maintien du Plan de Continuité d'Activité",
              1, 1.8, 14, 1.2, size=28, bold=True, color=WHITE)
    _add_text(sl, f"de {company}",
              1, 3.0, 14, 0.8, size=24, bold=False, color=RGBColor(0xCC,0xCA,0xC4))
    # Rapport BIA badge
    _add_rect(sl, 1, 4.2, 3, 0.65, fill=RED)
    _add_text(sl, "Rapport BIA", 1, 4.2, 3, 0.65, size=16, bold=True,
              color=WHITE, align=PP_ALIGN.CENTER)
    _add_text(sl, report_date, 1, 5.1, 6, 0.5, size=14, color=RGBColor(0xCC,0xCA,0xC4))


def _slide_section_divider(prs: Presentation, title: str, num: str):
    sl = prs.slides.add_slide(_blank_layout(prs))
    _add_rect(sl, 0, 0, 20, 11.25, fill=DARK)
    _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
    _add_text(sl, "AI-driven tech consulting",
              1, 0.4, 10, 0.4, size=11, color=RGBColor(0xB0,0xAE,0xA8))
    _add_text(sl, title, 1, 3.5, 16, 2, size=36, bold=True, color=WHITE)
    _add_text(sl, num, 18.5, 9.8, 1.2, 0.5, size=11,
              color=RGBColor(0x55,0x53,0x4E), align=PP_ALIGN.RIGHT)


def _slide_agenda(prs: Presentation, sections: list[str]):
    sl = prs.slides.add_slide(_blank_layout(prs))
    _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
    _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
    _add_text(sl, "Agenda", 1, 0.5, 14, 0.7, size=28, bold=True, color=DARK)
    for i, s in enumerate(sections):
        y = 1.6 + i * 0.75
        _add_rect(sl, 1, y + 0.1, 0.35, 0.35, fill=RED)
        _add_text(sl, s, 1.6, y, 14, 0.6, size=14, bold=False, color=DARK)


def _slide_resume_activites(prs: Presentation, mc_df: pd.DataFrame, imp_df: pd.DataFrame):
    """Summary slide: recovery curve stats."""
    sl = prs.slides.add_slide(_blank_layout(prs))
    _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
    _add_rect(sl, 0, 0, 20, 0.06, fill=RED)

    # title
    _add_text(sl, "Reprise des Activités Exprimées", 1, 0.4, 16, 0.7,
              size=22, bold=True, color=DARK)

    # compute recovery stats from Impact DMIA
    total = len(imp_df)
    if total == 0:
        return
    dmia_counts: dict[str, int] = imp_df["DMIA Exprimée"].value_counts().to_dict()

    # build a horizontal bar chart image
    ordered = [d for d in DMIA_ORDER if d in dmia_counts]
    counts  = [dmia_counts[d] for d in ordered]

    fig, ax = plt.subplots(figsize=(7, 3.5), facecolor="none")
    bars = ax.barh(ordered, counts, color="#E8394C", height=0.55)
    ax.set_xlabel("Nombre d'activités", fontsize=9, color="#5A5852")
    ax.tick_params(colors="#1A1814", labelsize=9)
    ax.spines[["top","right","left"]].set_visible(False)
    ax.spines["bottom"].set_color("#D0CEC8")
    for bar, cnt in zip(bars, counts):
        ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2,
                str(cnt), va="center", fontsize=8, color="#1A1814")
    fig.tight_layout(pad=0.5)
    img = _img_bytes(fig)
    plt.close(fig)

    sl.shapes.add_picture(io.BytesIO(img), Inches(1), Inches(1.3), Inches(9), Inches(5))

    # stats box right side
    dmia_h4 = dmia_counts.get("H+4", 0) + dmia_counts.get("H+2", 0) + dmia_counts.get("H0", 0)
    dmia_j1 = dmia_counts.get("J+1", 0)
    pct_j1  = int((dmia_h4 + dmia_j1) / total * 100) if total else 0
    _add_rect(sl, 11, 1.3, 7.5, 2.5, fill=WHITE)
    _add_text(sl, "Activités nécessitant une reprise à J+1 ou avant :",
              11.3, 1.4, 7, 0.5, size=11, color=DKGR)
    _add_text(sl, f"{dmia_h4 + dmia_j1}", 11.3, 1.9, 3, 1,
              size=48, bold=True, color=RED, align=PP_ALIGN.LEFT)
    _add_text(sl, f"soit {pct_j1} % du total", 11.3, 2.9, 7, 0.4, size=11, color=DKGR)

    _add_text(sl, f"Total activités recensées : {total}", 11.3, 3.5, 7, 0.5, size=11, color=DARK)

    # recovery breakdown table
    _add_text(sl, "Répartition par DMIA", 1, 7.0, 18, 0.5, size=13, bold=True, color=DARK)
    cols = ["DMIA", "Nb activités", "% du total"]
    col_w = [3, 3, 3]
    _draw_table_header(sl, cols, col_w, x=1, y=7.5)
    rows_data = [(d, dmia_counts[d], f"{int(dmia_counts[d]/total*100)} %") for d in ordered]
    for ri, row in enumerate(rows_data):
        _draw_table_row(sl, [str(v) for v in row], col_w, x=1, y=7.5 + 0.4 + ri*0.38, alt=ri%2==0)


def _slide_structures_critiques(prs: Presentation, imp_df: pd.DataFrame):
    """Structures needing recovery at H0-J+1."""
    sl = prs.slides.add_slide(_blank_layout(prs))
    _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
    _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
    _add_text(sl, "Zoom sur les structures et applications critiques",
              1, 0.4, 18, 0.7, size=22, bold=True, color=DARK)

    urgent = imp_df[imp_df["DMIA Exprimée"].isin(["H0","H+1","H+2","H+4","J+1"])].copy()
    structs = urgent.groupby("Structure Niveau 2")["Activité"].count().sort_values(ascending=False)

    _add_text(sl, f"Structures critiques (H0 – J+1) : {len(structs)} structures, {len(urgent)} activités",
              1, 1.2, 18, 0.5, size=13, bold=True, color=DARK)

    cols = ["Structure", "Nb activités", "DMIA min"]
    col_w = [10, 2.5, 2.5]
    _draw_table_header(sl, cols, col_w, x=1, y=1.9)
    for ri, (struct, cnt) in enumerate(structs.items()):
        dmia_min_idx = min(
            DMIA_ORDER.index(d) if d in DMIA_ORDER else 99
            for d in urgent[urgent["Structure Niveau 2"] == struct]["DMIA Exprimée"]
        )
        dmia_min = DMIA_ORDER[dmia_min_idx] if dmia_min_idx < len(DMIA_ORDER) else "?"
        _draw_table_row(sl, [_safe(struct), str(cnt), dmia_min],
                        col_w, x=1, y=1.9 + 0.4 + ri*0.38, alt=ri%2==0)


def _slides_zoom_reprise(prs: Presentation, imp_df: pd.DataFrame):
    """One slide per DMIA timeframe."""
    groups = imp_df.groupby("DMIA Exprimée")
    slide_num = 1
    for dmia in DMIA_ORDER:
        if dmia not in groups.groups:
            continue
        grp = groups.get_group(dmia).reset_index(drop=True)
        structs = grp["Structure Niveau 2"].fillna(grp["Structure Niveau 1"]).unique()

        sl = prs.slides.add_slide(_blank_layout(prs))
        _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
        _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
        _add_rect(sl, 1, 0.4, 2.8, 0.65, fill=RED)
        _add_text(sl, dmia, 1, 0.4, 2.8, 0.65, size=20, bold=True,
                  color=WHITE, align=PP_ALIGN.CENTER)
        _add_text(sl, "Zoom sur la Reprise des Activités 2026",
                  4.2, 0.48, 14, 0.5, size=13, bold=False, color=DKGR)
        _add_text(sl, f"{len(structs)} Structures  ·  {len(grp)} Activités",
                  1, 1.2, 18, 0.5, size=14, bold=True, color=DARK)

        cols = ["Structure", "Activité", "Impact Image", "Score max"]
        col_w = [6.5, 7, 2.5, 2]
        _draw_table_header(sl, cols, col_w, x=1, y=1.9)
        for ri, row in grp.iterrows():
            s1 = _safe(row.get("Structure Niveau 2") or row.get("Structure Niveau 1"))
            act = _safe(row.get("Activité"))
            im  = str(row.get("IM 4H",""))
            sc  = str(row.get("Score 2-3J",""))
            _draw_table_row(sl, [s1, act, im, sc],
                            col_w, x=1, y=1.9 + 0.4 + ri*0.38, alt=ri%2==0)
        slide_num += 1


def _slides_tableau_impacts(prs: Presentation, imp_df: pd.DataFrame, max_rows=16):
    """Tables of impacts grouped by DMIA."""
    imp_cols = ["IM 1H","IM 4H","IM 1J","IM 2-3J"]
    score_col = "Score 2-3J"

    for dmia in DMIA_ORDER:
        grp = imp_df[imp_df["DMIA Exprimée"] == dmia].reset_index(drop=True)
        if grp.empty:
            continue
        pages = math.ceil(len(grp) / max_rows)
        for page in range(pages):
            chunk = grp.iloc[page*max_rows:(page+1)*max_rows]
            sl = prs.slides.add_slide(_blank_layout(prs))
            _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
            _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
            suffix = f" ({page+1}/{pages})" if pages > 1 else ""
            _add_rect(sl, 1, 0.35, 2.5, 0.55, fill=DARK)
            _add_text(sl, f"DMIA  {dmia}", 1, 0.35, 2.5, 0.55, size=13, bold=True,
                      color=WHITE, align=PP_ALIGN.CENTER)
            _add_text(sl, f"Tableau des Impacts{suffix}", 4, 0.42, 13, 0.45, size=13, bold=False, color=DKGR)

            cols = ["Structure", "Activité", "IM 1H","IM 4H","IM 1J","IM 2-3J","Score"]
            col_w = [5.5, 6.5, 1.2, 1.2, 1.2, 1.2, 1.2]
            _draw_table_header(sl, cols, col_w, x=1, y=1.05)
            for ri, (_, row) in enumerate(chunk.iterrows()):
                s1 = _safe(row.get("Structure Niveau 2") or row.get("Structure Niveau 1"))
                act = _safe(row.get("Activité"))
                vals = [s1, act] + [str(int(row[c])) if not pd.isna(row[c]) else "" for c in imp_cols] + [str(int(row[score_col])) if not pd.isna(row.get(score_col)) else ""]
                _draw_table_row(sl, vals, col_w, x=1, y=1.05 + 0.38 + ri*0.38, alt=ri%2==0)


def _slides_applications(prs: Presentation, apps_df: pd.DataFrame, max_rows=16):
    """Applications grouped by lot (DMIA)."""
    apps_df = apps_df.copy()
    apps_df["DMIA"] = apps_df["DMIA"].fillna("Au-delà")

    # Parse each DMIA to a duration in hours once, then use it for both the
    # lot split and the ordering. Unparseable values ("Au-delà", "-", blank)
    # carry no committed deadline: they land in Lot 3 and sort last.
    def _hours(dmia: str) -> float:
        h = _dmia_to_hours(dmia)
        return h if h is not None else float("inf")

    def _lot(hours: float) -> str:
        # Classified on the parsed duration, not on a fixed list of
        # spellings. These workbooks overwhelmingly write the raw form —
        # "3J", "4H", "10J" — so matching literal "J+3" strings dropped
        # nearly everything into Lot 3 and left Lot 2 empty.
        if hours <= 24:
            return "Lot 1 — Reprise ≤ J+1"
        if hours <= 120:
            return "Lot 2 — Reprise J+2 à J+5"
        return "Lot 3 — Reprise au-delà de J+5"

    apps_df["_dmia_hours"] = apps_df["DMIA"].apply(_hours)
    apps_df["Lot"] = apps_df["_dmia_hours"].apply(_lot)
    lot_order = ["Lot 1 — Reprise ≤ J+1","Lot 2 — Reprise J+2 à J+5","Lot 3 — Reprise au-delà de J+5"]

    for lot in lot_order:
        # Ascending by real delay, not by DMIA text — a plain string sort
        # puts "J+10" before "J+2", comparing '1' against '2' character by
        # character. Stable, so equal DMIAs keep their source order.
        grp = (apps_df[apps_df["Lot"] == lot]
              .sort_values("_dmia_hours", kind="stable")
              .drop(columns="_dmia_hours")
              .reset_index(drop=True))
        if grp.empty:
            continue
        pages = math.ceil(len(grp) / max_rows)
        for page in range(pages):
            chunk = grp.iloc[page*max_rows:(page+1)*max_rows]
            sl = prs.slides.add_slide(_blank_layout(prs))
            _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
            _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
            suffix = f" ({page+1}/{pages})" if pages > 1 else ""
            _add_text(sl, f"Reprise actualisée des applications{suffix}",
                      1, 0.4, 18, 0.6, size=22, bold=True, color=DARK)
            _add_rect(sl, 1, 1.1, 0.18, 0.32, fill=RED)
            _add_text(sl, f"{lot}  ·  {len(grp)} applications",
                      1.4, 1.08, 17, 0.4, size=12, bold=True, color=DARK)

            cols = ["Structure", "Application", "DMIA", "Criticité", "Contournement"]
            col_w = [5.5, 3.5, 1.5, 1.5, 6]
            _draw_table_header(sl, cols, col_w, x=1, y=1.65)
            for ri, (_, row) in enumerate(chunk.iterrows()):
                s1 = _safe(row.get("Structure Niveau 2") or row.get("Structure Niveau 1"))
                app= _safe(row.get("Application"))
                dmia= _safe(row.get("DMIA"))
                crit= _safe(row.get("Niveau de criticité"))
                cont= _safe(row.get("Contournement envisageable",""))[:80]
                _draw_table_row(sl, [s1, app, dmia, crit, cont],
                                col_w, x=1, y=1.65 + 0.38 + ri*0.38, alt=ri%2==0)


def _slide_collaborateurs_analyse(prs: Presentation, col_df: pd.DataFrame):
    """Analysis summary: % with supplement."""
    sl = prs.slides.add_slide(_blank_layout(prs))
    _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
    _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
    _add_text(sl, "Analyse des collaborateurs clés",
              1, 0.4, 18, 0.6, size=22, bold=True, color=DARK)

    with_supp = col_df["Suppléants possibles"].notna().sum()
    total = len(col_df)
    pct   = int(with_supp / total * 100) if total else 0

    # donut chart
    fig, ax = plt.subplots(figsize=(4.5, 3.5), facecolor="none")
    vals = [pct, 100-pct]
    colors = ["#E8394C", "#D0CEC8"]
    wedges, _ = ax.pie(vals, colors=colors, startangle=90,
                       wedgeprops=dict(width=0.5))
    ax.text(0, 0, f"{pct}%", ha="center", va="center", fontsize=22, fontweight="bold", color="#1A1814")
    ax.axis("equal")
    fig.tight_layout(pad=0)
    img = _img_bytes(fig)
    plt.close(fig)

    sl.shapes.add_picture(io.BytesIO(img), Inches(1), Inches(1.5), Inches(5), Inches(4))
    _add_text(sl, f"des collaborateurs clés disposent\nd'au moins un suppléant",
              6.5, 2.5, 9, 1.2, size=16, bold=False, color=DARK)
    _add_text(sl, f"Total collaborateurs clés : {total}",
              6.5, 4.0, 9, 0.5, size=13, bold=True, color=DKGR)


def _slides_collaborateurs(prs: Presentation, col_df: pd.DataFrame, max_rows=16):
    pages = math.ceil(len(col_df) / max_rows)
    for page in range(pages):
        chunk = col_df.iloc[page*max_rows:(page+1)*max_rows].reset_index(drop=True)
        sl = prs.slides.add_slide(_blank_layout(prs))
        _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
        _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
        suffix = f" ({page+1}/{pages})" if pages > 1 else ""
        _add_text(sl, f"Liste des Collaborateurs Clés par structure{suffix}",
                  1, 0.4, 18, 0.6, size=20, bold=True, color=DARK)
        cols  = ["Structure", "Nom", "Prénom", "Fonction", "Poste", "Ancienneté", "Suppléant"]
        col_w = [6, 2, 2, 2.5, 2, 1.8, 2.7]
        _draw_table_header(sl, cols, col_w, x=1, y=1.2)
        for ri, (_, row) in enumerate(chunk.iterrows()):
            s1 = _safe(row.get("Structure Niveau 2") or row.get("Structure Niveau 1"))
            _draw_table_row(sl, [
                s1,
                _safe(row.get("Nom")),
                _safe(row.get("Prénom")),
                _safe(row.get("Fonction")),
                _safe(row.get("Poste")),
                _safe(row.get("Ancienneté dans le poste")),
                _safe(row.get("Suppléants possibles")),
            ], col_w, x=1, y=1.2 + 0.38 + ri*0.38, alt=ri%2==0)


def _slides_documents(prs: Presentation, doc_df: pd.DataFrame, max_rows=16):
    for dupl_val, title in [("O", "Documents critiques dupliqués"), ("N", "Documents critiques non dupliqués")]:
        grp = doc_df[doc_df["Duplication \n(O / N)"] == dupl_val].reset_index(drop=True)
        if grp.empty:
            continue
        pages = math.ceil(len(grp) / max_rows)
        for page in range(pages):
            chunk = grp.iloc[page*max_rows:(page+1)*max_rows].reset_index(drop=True)
            sl = prs.slides.add_slide(_blank_layout(prs))
            _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
            _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
            suffix = f" ({page+1}/{pages})" if pages > 1 else ""
            _add_text(sl, f"{title}{suffix}", 1, 0.4, 18, 0.6, size=20, bold=True, color=DARK)
            cols  = ["Structure", "Document / Fichier", "Type stockage", "Modalité duplication"]
            col_w = [6.5, 5.5, 2.5, 4]
            _draw_table_header(sl, cols, col_w, x=1, y=1.2)
            for ri, (_, row) in enumerate(chunk.iterrows()):
                s1 = _safe(row.get("Structure Niveau 2") or row.get("Structure Niveau 1"))
                _draw_table_row(sl, [
                    s1,
                    _safe(row.get("Documents / Fichiers")),
                    _safe(row.get("Type de stockage\n(Electronique / Papier)")),
                    _safe(row.get("Modalité de Duplication")),
                ], col_w, x=1, y=1.2 + 0.38 + ri*0.38, alt=ri%2==0)


def _slides_equipements(prs: Presentation, eq_df: pd.DataFrame, max_rows=20):
    timeframes = ["H0","H+1","H+2","H+4","J+1","J+2","J+3","J+4","J+5","J+10","J+15"]
    avail = [t for t in timeframes if t in eq_df.columns]
    grp = eq_df.groupby("Désignation")[avail].sum().reset_index()
    # Total column
    grp["Total"] = grp[avail].sum(axis=1)
    grp = grp[grp["Total"] > 0].reset_index(drop=True)

    pages = math.ceil(len(grp) / max_rows)
    for page in range(pages):
        chunk = grp.iloc[page*max_rows:(page+1)*max_rows].reset_index(drop=True)
        sl = prs.slides.add_slide(_blank_layout(prs))
        _add_rect(sl, 0, 0, 20, 11.25, fill=LGRAY)
        _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
        suffix = f" ({page+1}/{pages})" if pages > 1 else ""
        _add_text(sl, f"Équipements nécessaires à la reprise{suffix}",
                  1, 0.4, 18, 0.6, size=20, bold=True, color=DARK)
        disp_cols = ["Désignation"] + avail + ["Total"]
        disp_w    = [5] + [0.9]*len(avail) + [1.2]
        _draw_table_header(sl, disp_cols, disp_w, x=1, y=1.2)
        for ri, (_, row) in enumerate(chunk.iterrows()):
            vals = [_safe(row["Désignation"])] + [str(int(row[t])) if row[t] else "" for t in avail] + [str(int(row["Total"]))]
            _draw_table_row(sl, vals, disp_w, x=1, y=1.2 + 0.38 + ri*0.35, alt=ri%2==0)


def _slide_thank_you(prs: Presentation):
    sl = prs.slides.add_slide(_blank_layout(prs))
    _add_rect(sl, 0, 0, 20, 11.25, fill=DARK)
    _add_rect(sl, 0, 0, 20, 0.06, fill=RED)
    _add_text(sl, "Thank you!", 1, 3.5, 18, 2, size=48, bold=True,
              color=WHITE, align=PP_ALIGN.CENTER)
    _add_text(sl, "AI-driven tech consulting", 1, 5.8, 18, 0.5,
              size=14, color=RGBColor(0xB0,0xAE,0xA8), align=PP_ALIGN.CENTER)
    _add_text(sl, "devoteam.com", 1, 9.8, 18, 0.5, size=11,
              color=RGBColor(0x88,0x86,0x80), align=PP_ALIGN.CENTER)


# ── table helpers ───────────────────────────────────────────────────────────
ROW_H = 0.38

def _draw_table_header(slide, cols: list[str], widths: list[float], x: float, y: float):
    cur_x = x
    for col, w in zip(cols, widths):
        _add_rect(slide, cur_x, y, w - 0.02, ROW_H, fill=DARK)
        _add_text(slide, col, cur_x + 0.05, y + 0.02, w - 0.12, ROW_H - 0.04,
                  size=9, bold=True, color=WHITE, align=PP_ALIGN.LEFT)
        cur_x += w

def _draw_table_row(slide, vals: list[str], widths: list[float],
                    x: float, y: float, alt: bool = False):
    bg = RGBColor(0xFF,0xFF,0xFF) if not alt else LGRAY
    cur_x = x
    for val, w in zip(vals, widths):
        _add_rect(slide, cur_x, y, w - 0.02, ROW_H - 0.02, fill=bg, line=MGRAY)
        _add_text(slide, val[:80], cur_x + 0.06, y + 0.02, w - 0.14, ROW_H - 0.06,
                  size=8, bold=False, color=DARK, align=PP_ALIGN.LEFT)
        cur_x += w


# ── main entry point ────────────────────────────────────────────────────────
def generate_pptx(xlsx_bytes: bytes) -> bytes:
    """Parse Synthèse BIA xlsx → return PPTX bytes."""
    xl = pd.ExcelFile(io.BytesIO(xlsx_bytes))

    def _read(sheet, skip=4):
        if sheet not in xl.sheet_names:
            return pd.DataFrame()
        df = pd.read_excel(xl, sheet_name=sheet, header=skip)
        # drop unnamed first col and fully empty rows
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        return df.dropna(how="all").reset_index(drop=True)

    imp_df  = _read("Impact DMIA")
    mc_df   = _read("Montée en charge")
    apps_df = _read("Applications IT")
    col_df  = _read("Collaborateurs Clés")
    doc_df  = _read("Doc critiques")
    eq_df   = _read("Autres Eqt IT")

    # detect company name from any sheet header
    raw = pd.read_excel(xl, sheet_name=xl.sheet_names[1], header=None, nrows=3)
    company = "GAT Assurances"
    for ri in range(3):
        for ci in range(raw.shape[1]):
            v = str(raw.iloc[ri, ci]) if not pd.isna(raw.iloc[ri, ci]) else ""
            if "ASSURANCES" in v.upper() or "BANK" in v.upper() or "BANQUE" in v.upper():
                # extract company part
                import re
                m = re.search(r'([A-ZÉÀÂÎÔÙ\s]+(?:ASSURANCES?|BANK|BANQUE)\s*\w*)', v, re.I)
                if m:
                    company = m.group(1).strip().title()
                break

    report_date = date.today().strftime("%B %Y").capitalize()

    prs = _new_prs()

    _slide_cover(prs, company, report_date)
    _slide_agenda(prs, [
        "Résumé Exécutif",
        "Zoom sur la Reprise des Activités",
        "Tableau des Impacts",
        "Reprise actualisée des applications",
        "Collaborateurs clés",
        "Documents critiques",
        "Équipements nécessaires à la reprise",
    ])

    # ── Section 1: Résumé Exécutif ──────────────────────────────────────────
    _slide_section_divider(prs, "Résumé Exécutif", "01")
    if not imp_df.empty:
        _slide_resume_activites(prs, mc_df, imp_df)
        _slide_structures_critiques(prs, imp_df)

    # ── Section 2: Zoom sur la Reprise ─────────────────────────────────────
    _slide_section_divider(prs, "Zoom sur la Reprise des Activités", "02")
    if not imp_df.empty:
        _slides_zoom_reprise(prs, imp_df)

    # ── Section 3: Tableau des Impacts ─────────────────────────────────────
    _slide_section_divider(prs, "Tableau des Impacts", "03")
    if not imp_df.empty:
        _slides_tableau_impacts(prs, imp_df)

    # ── Section 4: Applications ────────────────────────────────────────────
    _slide_section_divider(prs, "Reprise actualisée des applications", "04")
    if not apps_df.empty:
        _slides_applications(prs, apps_df)

    # ── Section 5: Collaborateurs clés ─────────────────────────────────────
    _slide_section_divider(prs, "Collaborateurs clés", "05")
    if not col_df.empty:
        _slide_collaborateurs_analyse(prs, col_df)
        _slides_collaborateurs(prs, col_df)

    # ── Section 6: Documents critiques ─────────────────────────────────────
    _slide_section_divider(prs, "Documents critiques", "06")
    if not doc_df.empty:
        _slides_documents(prs, doc_df)

    # ── Section 7: Équipements ─────────────────────────────────────────────
    _slide_section_divider(prs, "Équipements nécessaires à la reprise", "07")
    if not eq_df.empty:
        _slides_equipements(prs, eq_df)

    _slide_thank_you(prs)

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()
