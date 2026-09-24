"""
build_project_card.py
Rebuild the BIA Project Card from the original template with:
  - Enhanced slide 1 (cover + key stats)
  - Enhanced slide 2 (challenge / solution / results — more detail)
  - NEW slide 3 — Key Features (6 feature cards, 2-column grid)
  - Enhanced slide 4 (roadmap — was slide 3)
"""

import copy
from pptx import Presentation
from pptx.util import Emu, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.oxml.ns import qn
from lxml import etree
from pptx.oxml import parse_xml

# ─── Constants ────────────────────────────────────────────────────────────────
TMPL   = "C:/Users/jelassi.hamza/Downloads/project card template v1 (1).pptx"
OUT    = "C:/Users/jelassi.hamza/Desktop/Devoteam/BIA_Implementation/BIA_Project_Card.pptx"

RED    = RGBColor(0xEF, 0x4A, 0x60)
WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
GRAY   = RGBColor(0xCC, 0xCC, 0xCC)
DARK   = RGBColor(0x1A, 0x1A, 0x2E)
CARD_BG = RGBColor(0x12, 0x12, 0x22)

W = 9144000   # slide width  (EMU)
H = 5143500   # slide height (EMU)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _set_run(run, text, bold=None, size_pt=None, color=None):
    run.text = text
    if bold is not None:
        run.font.bold = bold
    if size_pt:
        run.font.size = Pt(size_pt)
    if color:
        run.font.color.rgb = color


def _set_tf(shape, lines):
    """lines = list of (text, bold, size_pt, color, align)"""
    tf = shape.text_frame
    tf.word_wrap = True
    for i, (text, bold, size_pt, color, align) in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        if align:
            p.alignment = align
        run = p.add_run()
        _set_run(run, text, bold, size_pt, color)


def add_textbox(slide, x, y, w, h, lines, word_wrap=True):
    """Add a text box; lines same format as _set_tf."""
    txBox = slide.shapes.add_textbox(Emu(x), Emu(y), Emu(w), Emu(h))
    tf = txBox.text_frame
    tf.word_wrap = word_wrap
    for i, (text, bold, size_pt, color, align) in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        if align:
            p.alignment = align
        if text:
            run = p.add_run()
            _set_run(run, text, bold, size_pt, color)
    return txBox


def add_rect(slide, x, y, w, h, fill_color, line_color=None):
    """Add a filled rectangle."""
    shape = slide.shapes.add_shape(
        1,  # MSO_SHAPE_TYPE.RECTANGLE
        Emu(x), Emu(y), Emu(w), Emu(h)
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    if line_color:
        shape.line.color.rgb = line_color
        shape.line.width = Emu(12700)
    else:
        shape.line.fill.background()
    return shape


def move_slide_to(prs, from_idx, to_idx):
    """Move slide at from_idx to to_idx (0-based)."""
    xml_slides = prs.slides._sldIdLst
    slides = list(xml_slides)
    el = slides[from_idx]
    xml_slides.remove(el)
    xml_slides.insert(to_idx, el)


# ─── Load template ────────────────────────────────────────────────────────────
prs = Presentation(TMPL)

# ═══════════════════════════════════════════════════════════════════════════════
# SLIDE 1 — Enhanced Cover
# ═══════════════════════════════════════════════════════════════════════════════
s1 = prs.slides[0]

for shape in s1.shapes:
    name = shape.name
    if not shape.has_text_frame:
        continue
    tf = shape.text_frame

    if name == "Google Shape;149;p29":          # ── Project title
        tf.paragraphs[0].runs[0].text = "BIA Automation Tool"

    elif name == "Google Shape;146;p29":        # ── "Description" label — keep
        pass

    elif name == "Google Shape;145;p29":        # ── Description body
        tf.paragraphs[0].runs[0].text = (
            "Application web qui automatise entièrement la production des documents "
            "Business Impact Analysis (BIA) — de la génération des fiches par département "
            "jusqu'à la consolidation de la Synthèse BIA — en remplaçant des heures de "
            "travail manuel par quelques secondes de traitement automatisé."
        )

    elif name == "Google Shape;148;p29":        # ── "Progress" label — keep
        pass

    elif name == "Google Shape;147;p29":        # ── Progress body
        tf.paragraphs[0].runs[0].text = (
            "MVP livré et validé sur un projet client réel (STAR Assurances). "
            "Les deux modules sont pleinement opérationnels : génération automatique "
            "de fiches BIA par département et consolidation de la Synthèse BIA. "
            "Prêt à être déployé sur l'ensemble des missions BIA Devoteam MCO."
        )

# Add key-stats strip (3 accent boxes at the bottom of the right panel)
STAT_Y = 4500000
STAT_H = 500000
stats = [
    ("< 1 min",         "pour 30 départements"),
    ("0 erreur",        "de formatage ou de données"),
    ("100%",            "réutilisable — tout client"),
]
stat_w = 1480000
stat_gap = 40000
stat_x_start = 3750000

for i, (big, small) in enumerate(stats):
    bx = stat_x_start + i * (stat_w + stat_gap)
    # Accent bar top
    add_rect(s1, bx, STAT_Y, stat_w, 8000, RED)
    # Dark card
    add_rect(s1, bx, STAT_Y + 8000, stat_w, STAT_H - 8000, RGBColor(0x18, 0x18, 0x28))
    # Big number
    add_textbox(s1, bx + 30000, STAT_Y + 30000, stat_w - 60000, 220000,
                [(big, True, 18, RED, PP_ALIGN.LEFT)])
    # Sub-label
    add_textbox(s1, bx + 30000, STAT_Y + 260000, stat_w - 60000, 200000,
                [(small, False, 9, GRAY, PP_ALIGN.LEFT)])


# ═══════════════════════════════════════════════════════════════════════════════
# SLIDE 2 — Enhanced Challenge / Solution / Results
# ═══════════════════════════════════════════════════════════════════════════════
s2 = prs.slides[1]

for shape in s2.shapes:
    name = shape.name
    if not shape.has_text_frame:
        continue
    tf = shape.text_frame

    if name == "Google Shape;158;p30":          # ── Challenge block
        paras = tf.paragraphs
        paras[0].runs[0].text = "Challenge"
        paras[1].runs[0].text = (
            "Créer une fiche BIA par département imposait du copier-coller manuel depuis "
            "un template Word — plusieurs heures de travail répétitif pour 20+ départements."
        )
        paras[2].runs[0].text = (
            "La consolidation des fiches dans la Synthèse BIA se faisait cellule par cellule, "
            "avec un risque élevé d'erreur et d'incohérence entre consultants."
        )
        paras[3].runs[0].text = (
            "Chaque nouveau client nécessitait une mise à jour manuelle du nom et du logo "
            "dans l'ensemble des documents — source d'erreurs et de perte de temps."
        )
        # Add 4th bullet
        if len(paras) < 5:
            new_p = copy.deepcopy(paras[3]._p)
            paras[3]._p.addnext(new_p)
            tf.paragraphs[4].runs[0].text = (
                "Aucun outil standardisé → qualité des livrables variable selon le consultant "
                "et le temps disponible."
            )

    elif name == "Google Shape;164;p30":        # ── Client block
        tf.paragraphs[0].runs[0].text = "Devoteam"
        tf.paragraphs[0].runs[1].text = "BIA Automation Tool"
        tf.paragraphs[1].runs[0].text = "(Outil interne — MVP)"
        if len(tf.paragraphs) > 2:
            tf.paragraphs[2].runs[0].text = ""

    elif name == "Google Shape;165;p30":        # ── Solution + Results block
        paras = tf.paragraphs
        paras[0].runs[0].text = "Solution"
        paras[1].runs[0].text = (
            "Upload de 2 fichiers (Excel de recensement + template Word) → génération "
            "automatique d'une fiche BIA complète par département en quelques secondes."
        )
        paras[2].runs[0].text = (
            "Détection IA (GPT-4o Vision) du nom et logo client directement depuis le fichier "
            "Excel uploadé — aucune saisie manuelle nécessaire."
        )
        paras[3].runs[0].text = "Résultats"
        paras[4].runs[0].text = (
            "Temps de production des fiches : de plusieurs heures à moins d'1 minute "
            "pour 30+ départements."
        )
        paras[5].runs[0].text = (
            "Zéro erreur de formatage, de nommage ou de données dans les documents générés."
        )
        paras[6].runs[0].text = (
            "Réutilisable à l'identique pour tout client Devoteam — validé sur STAR Assurances."
        )

    elif name == "Google Shape;169;p30":        # ── Future clients
        tf.paragraphs[0].runs[0].text = "Tous les clients BIA Devoteam"


# ═══════════════════════════════════════════════════════════════════════════════
# NEW SLIDE 3 — Key Features  (cloned from slide 2 background + new content)
# ═══════════════════════════════════════════════════════════════════════════════
layout_custom = prs.slide_layouts[11]   # CUSTOM_29_1 — same dark background as slide 2
feat_slide = prs.slides.add_slide(layout_custom)

# ── Red vertical accent bar (matches slide 2 style) ──────────────────────────
add_rect(feat_slide, 0, 0, 220000, H, RED)

# ── Slide title ───────────────────────────────────────────────────────────────
add_textbox(feat_slide, 310000, 60000, 8600000, 480000,
            [("Fonctionnalités Clés", True, 28, WHITE, PP_ALIGN.LEFT)])

# Thin red separator line under title
add_rect(feat_slide, 310000, 540000, 8600000, 14000, RED)

# ── 6 Feature cards  (2 cols × 3 rows) ───────────────────────────────────────
FEATURES = [
    (
        "Génération automatique des fiches BIA",
        "Crée un document Word complet par département à partir du fichier de recensement Excel "
        "et du template BIA. Nom du département, contact, date, référence : tout est rempli automatiquement.",
    ),
    (
        "Détection intelligente du client (IA)",
        "GPT-4o Vision analyse les images du fichier Excel pour identifier le nom exact et le logo "
        "du client. Les documents sont personnalisés sans aucune saisie manuelle.",
    ),
    (
        "Consolidation de la Synthèse BIA",
        "Lit toutes les fiches individuelles et remplit le tableau Synthèse BIA Excel avec les données "
        "clés (processus, criticité, RTO, RPO, dépendances) — prêt pour livraison en moins de 30 s.",
    ),
    (
        "Interface web — aucune installation",
        "Accessible depuis n'importe quel navigateur sur le réseau de l'entreprise. "
        "Les consultants n'ont rien à installer ni à configurer.",
    ),
    (
        "Export packagé en un clic",
        "Toutes les fiches générées sont regroupées dans un fichier ZIP téléchargeable immédiatement. "
        "La Synthèse BIA est téléchargeable en un seul fichier Excel.",
    ),
    (
        "Réutilisable pour tout client",
        "Fonctionne pour n'importe quel engagement BIA Devoteam sans modification du code. "
        "Il suffit de changer les fichiers d'entrée pour chaque nouveau projet.",
    ),
]

CARD_W    = 4040000
CARD_H    = 1190000
COL1_X    = 310000
COL2_X    = 4650000
ROW_Y     = [620000, 1910000, 3200000]
CARD_GAP  = 30000

for idx, (title, desc) in enumerate(FEATURES):
    col = idx % 2
    row = idx // 2
    cx  = COL1_X if col == 0 else COL2_X
    cy  = ROW_Y[row]

    # Card background
    add_rect(feat_slide, cx, cy, CARD_W, CARD_H, RGBColor(0x16, 0x16, 0x28))
    # Left red accent border
    add_rect(feat_slide, cx, cy, 14000, CARD_H, RED)
    # Feature number badge
    add_textbox(feat_slide,
                cx + CARD_W - 320000, cy + 30000, 290000, 260000,
                [(f"0{idx+1}", True, 22, RGBColor(0x40, 0x40, 0x55), PP_ALIGN.RIGHT)])
    # Feature title
    add_textbox(feat_slide,
                cx + 60000, cy + 50000, CARD_W - 380000, 280000,
                [(title, True, 12, WHITE, PP_ALIGN.LEFT)])
    # Feature description
    add_textbox(feat_slide,
                cx + 60000, cy + 340000, CARD_W - 120000, 820000,
                [(desc, False, 9, GRAY, PP_ALIGN.LEFT)])

# Move the new slide to position 2 (index 2 = after slide 2, before current slide 3)
move_slide_to(prs, len(prs.slides) - 1, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# SLIDE 4 — Enhanced Roadmap  (was slide 3, now index 3 after insertion)
# ═══════════════════════════════════════════════════════════════════════════════
s4 = prs.slides[3]

for shape in s4.shapes:
    name = shape.name
    if not shape.has_text_frame:
        continue
    tf = shape.text_frame

    if name == "Google Shape;174;p31":          # ── Realisation header + body
        paras = tf.paragraphs
        paras[0].runs[0].text = "R"
        paras[0].runs[1].text = "éalisation"
        if len(paras[0].runs) > 2:
            paras[0].runs[2].text = " — "
        paras[0].runs[3].text = (
            "Application web FastAPI complète avec interface navigateur permettant deux workflows "
            "automatisés : (1) Génération de fiches BIA par département depuis un Excel recensement "
            "+ template Word, avec détection IA du logo/nom client via GPT-4o Vision ; "
            "(2) Consolidation de toutes les fiches individuelles en une Synthèse BIA Excel prête "
            "pour livraison client. Validé sur STAR Assurances."
        )
        if len(paras[0].runs) > 4:
            paras[0].runs[4].text = ""

    elif name == "Google Shape;199;p31":        # ── Milestone 1 label
        tf.paragraphs[0].runs[0].text = "ETL Core Engine"

    elif name == "Google Shape;197;p31":        # ── Milestone 1 duration
        tf.paragraphs[0].runs[0].text = "2 semaines"

    elif name == "Google Shape;202;p31":        # ── Milestone 2 label
        tf.paragraphs[0].runs[0].text = "Générateur Fiches + IA"

    elif name == "Google Shape;200;p31":        # ── Milestone 2 duration
        for run in tf.paragraphs[0].runs:
            run.text = ""
        tf.paragraphs[0].runs[0].text = "2 semaines"

    elif name == "Google Shape;208;p31":        # ── Milestone 3 label
        tf.paragraphs[0].runs[0].text = "Interface Web + Intégration"

# Add milestone detail boxes below the timeline
MILE_DETAILS = [
    "Extraction données fiches → remplissage Synthèse Excel. Validation mapping sur templates STAR Assurances.",
    "Génération Word par département + détection IA logo/nom via GPT-4o Vision. Tests qualité documents.",
    "UI navigateur + API REST reliant les deux modules. Tests end-to-end sur fichiers projets réels.",
]
mile_x = [1578169, 1578169, 1578169]
# Place detail text under each milestone row
for i, detail in enumerate(MILE_DETAILS):
    # Position roughly under each milestone bar
    dy = 1980000 + i * 820000 - 180000
    dx = 1680000 + i * 0
    add_textbox(s4, 1700000, dy, 7200000, 220000,
                [(detail, False, 8, GRAY, PP_ALIGN.LEFT)])


# ─── Save ─────────────────────────────────────────────────────────────────────
prs.save(OUT)
print(f"Saved: {OUT}")
print(f"Total slides: {len(prs.slides)}")
for i, sl in enumerate(prs.slides):
    print(f"  Slide {i+1}: {sl.slide_layout.name}")
