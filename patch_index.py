#!/usr/bin/env python3
"""Patch script: restructure index.html with new accordion sidebar + app-page system."""
import sys, re
sys.stdout.reconfigure(encoding='utf-8')

with open('static/index.html', encoding='utf-8') as f:
    content = f.read()

# ─────────────────────────────────────────────────────────────────────────────
# 1. INJECT CSS (before last </style> in head)
# ─────────────────────────────────────────────────────────────────────────────
NEW_CSS = """
    /* ══════════════════════════════════════
       ACCORDION SIDEBAR + APP PAGES
    ══════════════════════════════════════ */
    .sb-section-label {
      font-size:0.5rem;letter-spacing:0.35em;text-transform:uppercase;
      color:rgba(26,24,20,0.38);padding:0.55rem 0.7rem 0.4rem;margin-top:0.3rem;
    }
    .sb-group { margin-bottom:0.15rem; }
    .sb-group-header {
      display:flex;align-items:center;gap:0.6rem;padding:0.68rem 0.7rem;border-radius:0.6rem;
      cursor:pointer;border:1px solid transparent;transition:background 0.2s,border-color 0.2s;user-select:none;
    }
    .sb-group-header:hover { background:rgba(26,24,20,0.04);border-color:rgba(26,24,20,0.08); }
    .sb-group.open .sb-group-header { background:rgba(242,72,94,0.07);border-color:rgba(242,72,94,0.14); }
    .sb-group-icon {
      width:28px;height:28px;border-radius:7px;flex-shrink:0;display:flex;align-items:center;justify-content:center;
      background:rgba(26,24,20,0.04);border:1px solid rgba(26,24,20,0.08);color:rgba(26,24,20,0.38);
      font-size:0.72rem;transition:background 0.2s,color 0.2s;
    }
    .sb-group.open .sb-group-icon { background:var(--poppy);color:#fff;border-color:var(--poppy); }
    .sb-group-label { flex:1;font-size:0.72rem;font-weight:600;color:rgba(26,24,20,0.60);line-height:1.3; }
    .sb-group.open .sb-group-label { color:rgba(26,24,20,0.95); }
    .sb-phase-tag {
      font-size:0.46rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;
      padding:1px 5px;border-radius:3px;flex-shrink:0;
    }
    .sb-phase-tag.p1 { background:rgba(242,72,94,0.10);color:var(--poppy);border:1px solid rgba(242,72,94,0.18); }
    .sb-chevron { font-size:0.55rem;color:rgba(26,24,20,0.38);flex-shrink:0;transition:transform 0.25s,color 0.2s; }
    .sb-group.open .sb-chevron { transform:rotate(180deg);color:var(--poppy); }
    .sb-children { overflow:hidden;max-height:0;transition:max-height 0.32s cubic-bezier(0.4,0,0.2,1); }
    .sb-group.open .sb-children { max-height:600px; }
    .sb-child {
      display:flex;align-items:center;gap:0.55rem;padding:0.5rem 0.7rem 0.5rem 2.35rem;
      border-radius:0.5rem;cursor:pointer;border:1px solid transparent;
      font-size:0.69rem;font-weight:500;color:rgba(26,24,20,0.60);
      transition:background 0.18s,border-color 0.18s,color 0.18s;position:relative;margin:0.08rem 0;
    }
    .sb-child::before {
      content:'';position:absolute;left:1.15rem;top:50%;transform:translateY(-50%);
      width:5px;height:5px;border-radius:50%;background:rgba(26,24,20,0.38);transition:background 0.2s;
    }
    .sb-child:hover { background:rgba(26,24,20,0.04);color:rgba(26,24,20,0.95);border-color:rgba(26,24,20,0.08); }
    .sb-child:hover::before { background:var(--poppy); }
    .sb-child.active { background:rgba(242,72,94,0.13);color:var(--poppy);border-color:rgba(242,72,94,0.18);font-weight:600; }
    .sb-child.active::before { background:var(--poppy); }
    .sb-child-step {
      font-size:0.48rem;font-weight:700;letter-spacing:0.06em;color:rgba(26,24,20,0.38);
      background:rgba(26,24,20,0.05);border:1px solid rgba(26,24,20,0.08);border-radius:3px;padding:1px 4px;
      flex-shrink:0;transition:background 0.2s,color 0.2s;
    }
    .sb-child.active .sb-child-step { background:rgba(242,72,94,0.10);color:var(--poppy);border-color:rgba(242,72,94,0.20); }
    .sb-child-locked { opacity:0.38;cursor:not-allowed;pointer-events:none; }
    .sb-direct {
      display:flex;align-items:center;gap:0.6rem;padding:0.62rem 0.7rem;border-radius:0.6rem;
      cursor:pointer;border:1px solid transparent;transition:background 0.2s,border-color 0.2s;
      text-decoration:none;margin-bottom:0.12rem;
    }
    .sb-direct:hover { background:rgba(26,24,20,0.04);border-color:rgba(26,24,20,0.08); }
    .sb-direct-icon {
      width:28px;height:28px;border-radius:7px;flex-shrink:0;display:flex;align-items:center;justify-content:center;
      background:rgba(26,24,20,0.04);border:1px solid rgba(26,24,20,0.08);
      font-size:0.72rem;color:rgba(26,24,20,0.38);transition:background 0.2s,color 0.2s;
    }
    .sb-direct:hover .sb-direct-icon { background:rgba(242,72,94,0.08);color:var(--poppy); }
    .sb-direct-label { font-size:0.72rem;font-weight:600;color:rgba(26,24,20,0.60); }
    .sb-direct:hover .sb-direct-label { color:rgba(26,24,20,0.95); }
    /* App topbar */
    #topbar-app {
      position:fixed;top:0;left:var(--sb-w);right:0;height:58px;z-index:200;
      background:rgba(250,248,244,0.95);backdrop-filter:blur(20px);
      border-bottom:1px solid rgba(26,24,20,0.08);
      display:flex;align-items:center;justify-content:space-between;padding:0 2rem;
    }
    .tb-breadcrumb { display:flex;align-items:center;gap:0.5rem;font-size:0.7rem; }
    .tb-crumb-group { color:rgba(26,24,20,0.38);font-weight:500; }
    .tb-crumb-sep { color:rgba(26,24,20,0.3);font-size:0.55rem;margin:0 0.1rem; }
    .tb-crumb-page { color:rgba(26,24,20,0.90);font-weight:600; }
    /* App page system */
    #main-app {
      position:fixed;top:58px;left:var(--sb-w);right:0;bottom:0;overflow-y:auto;background:#FAF8F4;
    }
    .app-page { display:none;padding:2rem 2.5rem 4rem;min-height:100%; }
    .app-page.active { display:block; }
    .pg-header { margin-bottom:1.75rem; }
    .pg-label {
      font-size:0.58rem;font-weight:700;letter-spacing:0.4em;text-transform:uppercase;
      color:rgba(242,72,94,0.65);margin-bottom:0.55rem;display:flex;align-items:center;gap:0.5rem;
    }
    .pg-label::after { content:'';flex:1;height:1px;background:rgba(242,72,94,0.12);max-width:60px; }
    .pg-title { font-size:clamp(1.35rem,2.5vw,1.85rem);font-weight:300;line-height:1.2;margin-bottom:0.5rem; }
    .pg-title strong { font-weight:700; }
    .pg-desc { font-size:0.78rem;color:rgba(26,24,20,0.55);line-height:1.75;max-width:640px; }
    .app-card { background:#fff;border:1px solid rgba(26,24,20,0.08);border-radius:0.875rem;padding:1.5rem; }
    .app-card + .app-card { margin-top:1.25rem; }
    .app-card-title {
      font-size:0.72rem;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;
      color:rgba(26,24,20,0.55);margin-bottom:1rem;display:flex;align-items:center;gap:0.5rem;
    }
    .app-card-title i { color:var(--poppy); }
    .placeholder-page {
      display:flex;flex-direction:column;align-items:center;justify-content:center;
      min-height:55vh;text-align:center;gap:1.2rem;
    }
    .placeholder-icon {
      width:76px;height:76px;border-radius:18px;
      background:rgba(242,72,94,0.06);border:1px solid rgba(242,72,94,0.14);
      display:flex;align-items:center;justify-content:center;color:var(--poppy);font-size:1.9rem;
    }
    /* Sync mismatch items */
    .sync-mismatch-item {
      display:flex;align-items:center;gap:0.75rem;padding:0.85rem 1rem;
      background:#fff;border:1px solid rgba(26,24,20,0.08);border-radius:0.65rem;
      cursor:pointer;transition:border-color 0.18s,background 0.18s;margin-bottom:0.5rem;
    }
    .sync-mismatch-item:hover { border-color:rgba(242,72,94,0.25);background:rgba(242,72,94,0.015); }
    .sync-mismatch-item.resolved { opacity:0.38;pointer-events:none; }
    .smi-entity { font-size:0.62rem;font-weight:700;color:rgba(26,24,20,0.45);min-width:110px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }
    .smi-act { flex:1;font-size:0.73rem;font-weight:600;color:rgba(26,24,20,0.85);min-width:0; }
    .smi-badge { font-size:0.62rem;padding:0.18rem 0.55rem;border-radius:3px;white-space:nowrap; }
    .smi-synth { background:rgba(22,163,74,0.08);color:#15803d;border:1px solid rgba(22,163,74,0.2); }
    .smi-fiche { background:rgba(239,68,68,0.07);color:#dc2626;border:1px solid rgba(239,68,68,0.18); }
    .smi-arrow { font-size:0.6rem;color:rgba(234,88,12,0.7); }
    .smi-open { font-size:0.62rem;color:rgba(26,24,20,0.28);flex-shrink:0; }
    /* Step unlocked indicator */
    .step-unlock-bar {
      display:flex;align-items:center;gap:0.5rem;
      padding:0.65rem 1rem;background:rgba(22,163,74,0.06);border:1px solid rgba(22,163,74,0.18);
      border-radius:0.5rem;font-size:0.72rem;color:#15803d;font-weight:500;margin-bottom:1rem;
    }
    @media (max-width:768px) { #main-app { left:0; } #topbar-app { left:0; } }
"""

style_close_pos = content.rfind('</style>', 0, content.find('<body>'))
content = content[:style_close_pos] + NEW_CSS + content[style_close_pos:]
print("Step 1 done: CSS added")

# ─────────────────────────────────────────────────────────────────────────────
# 2. REPLACE SIDEBAR HTML
# ─────────────────────────────────────────────────────────────────────────────
SIDEBAR_OLD_START = '<!-- ═══════════════════════════════════════\n     SIDEBAR — PCA Navigation\n ═══════════════════════════════════════ -->'
sidebar_start = content.find(SIDEBAR_OLD_START)
sidebar_end   = content.find('</aside>', sidebar_start) + len('</aside>')

logo_line = content[sidebar_start:sidebar_end]
# Extract the logo img line from existing sidebar
logo_match = re.search(r'<img class="sb-logo-img"[^>]+>', logo_line)
logo_img = logo_match.group(0) if logo_match else '<div class="sb-logo-mark">B</div>'

NEW_SIDEBAR = f"""<!-- ═══════════════════════════════════════
     SIDEBAR — Accordion Navigation
 ═══════════════════════════════════════ -->
<aside id="sidebar" role="navigation" aria-label="BIA Navigation">

  <!-- Header -->
  <div id="sb-header">
    <div id="sb-logo">
      {logo_img}
      <div class="sb-logo-text">
        <span class="sb-brand">Devoteam</span>
        <span class="sb-context">Résilience 360°</span>
      </div>
    </div>
    <button id="sb-close" onclick="sbClose()" aria-label="Fermer">
      <i class="fa-solid fa-xmark"></i>
    </button>
  </div>

  <!-- Nav -->
  <nav id="sb-nav">
    <div class="sb-section-label">Menu principal</div>

    <!-- Base de projets -->
    <a href="/projects" class="sb-direct" id="sbp-projets" target="_blank">
      <div class="sb-direct-icon"><i class="fa-solid fa-database"></i></div>
      <span class="sb-direct-label">Base de projets</span>
      <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.5rem;color:rgba(26,24,20,0.28);margin-left:auto;"></i>
    </a>

    <!-- Éditeur de fiches -->
    <a href="/fiche-selector" class="sb-direct" id="sbp-editor" target="_blank">
      <div class="sb-direct-icon"><i class="fa-solid fa-pen-to-square"></i></div>
      <span class="sb-direct-label">Éditeur de fiches</span>
      <i class="fa-solid fa-arrow-up-right-from-square" style="font-size:0.5rem;color:rgba(26,24,20,0.28);margin-left:auto;"></i>
    </a>

    <div class="sb-divider"></div>

    <!-- Synthèse BIA — accordion -->
    <div class="sb-group open" id="grp-synthese">
      <div class="sb-group-header" onclick="toggleGroup('grp-synthese')">
        <div class="sb-group-icon"><i class="fa-regular fa-table-cells"></i></div>
        <div class="sb-group-label">Synthèse BIA</div>
        <span class="sb-phase-tag p1">Ph.1</span>
        <i class="fa-solid fa-chevron-down sb-chevron"></i>
      </div>
      <div class="sb-children">
        <div class="sb-child active" id="child-charger"
             onclick="navigatePage('page-charger-fiches','Synthèse BIA','Charger les fiches BIA','child-charger')">
          <span class="sb-child-step">01</span>Charger les fiches BIA
        </div>
        <div class="sb-child sb-child-locked" id="child-generer"
             onclick="navigatePage('page-generer-synthese','Synthèse BIA','Générer la synthèse BIA','child-generer')">
          <span class="sb-child-step">02</span>Générer la synthèse BIA
        </div>
        <div class="sb-child sb-child-locked" id="child-sync"
             onclick="navigatePage('page-sync','Synthèse BIA','Synchronisation bidirectionnelle','child-sync')">
          <span class="sb-child-step">03</span>Synchronisation bidirectionnelle
        </div>
        <div class="sb-child" id="child-org"
             onclick="navigatePage('page-organigramme','Synthèse BIA','Génération de l\\'organigramme','child-org')">
          <span class="sb-child-step">04</span>Génération de l'organigramme
        </div>
      </div>
    </div>

    <!-- Générer rapport BIA — placeholder -->
    <div class="sb-group" id="grp-rapport">
      <div class="sb-group-header" onclick="toggleGroup('grp-rapport')">
        <div class="sb-group-icon"><i class="fa-regular fa-file-pdf"></i></div>
        <div class="sb-group-label">Générer rapport BIA</div>
        <span class="sb-phase-tag p1">Ph.1</span>
        <i class="fa-solid fa-chevron-down sb-chevron"></i>
      </div>
      <div class="sb-children">
        <div class="sb-child sb-child-locked">
          <span class="sb-child-step">01</span>Rapport de synthèse
        </div>
      </div>
    </div>

    <!-- Gestion des risques — placeholder -->
    <div class="sb-group" id="grp-risques">
      <div class="sb-group-header" onclick="toggleGroup('grp-risques')">
        <div class="sb-group-icon"><i class="fa-solid fa-shield-halved"></i></div>
        <div class="sb-group-label">Gestion des risques</div>
        <span class="sb-phase-tag p1">Ph.1</span>
        <i class="fa-solid fa-chevron-down sb-chevron"></i>
      </div>
      <div class="sb-children">
        <div class="sb-child sb-child-locked">
          <span class="sb-child-step">01</span>Cartographie des risques
        </div>
      </div>
    </div>

    <div class="sb-divider"></div>

    <!-- Locked phases -->
    <div class="sb-phase sb-locked" aria-disabled="true">
      <div class="sb-badge">02</div>
      <div class="sb-phase-body">
        <div class="sb-phase-name">Stratégie de continuité</div>
        <div class="sb-phase-desc">Scénarios · Plans de contournement</div>
      </div>
      <span class="sb-chip soon-chip">Bientôt</span>
    </div>
    <div class="sb-phase sb-locked" aria-disabled="true">
      <div class="sb-badge">03</div>
      <div class="sb-phase-body">
        <div class="sb-phase-name">Mise en place du PCA</div>
        <div class="sb-phase-desc">Corpus normatif · PSI · Plan de crise</div>
      </div>
      <span class="sb-chip soon-chip">Bientôt</span>
    </div>
    <div class="sb-phase sb-locked" aria-disabled="true">
      <div class="sb-badge">04</div>
      <div class="sb-phase-body">
        <div class="sb-phase-name">Tests &amp; MCO</div>
        <div class="sb-phase-desc">Simulations · Acculturation · Transfert</div>
      </div>
      <span class="sb-chip soon-chip">Bientôt</span>
    </div>
  </nav>

  <!-- Footer -->
  <div id="sb-footer">
    <div class="sb-status-row">
      <span class="sb-pulse"></span>
      <span class="sb-status-text">Phase 1 active</span>
    </div>
    <p class="sb-version">BIA Automation Tool — Devoteam MCO</p>
  </div>

</aside>"""

content = content[:sidebar_start] + NEW_SIDEBAR + content[sidebar_end:]
print("Step 2 done: Sidebar replaced")

# ─────────────────────────────────────────────────────────────────────────────
# 3. REPLACE NAVBAR + HERO + FEATURES + TOOL + FOOTER  →  TOPBAR + MAIN-APP
# ─────────────────────────────────────────────────────────────────────────────
NAVBAR_MARKER    = '<!-- NAVBAR -->'
HERO_MARKER      = '<!-- HERO -->'
FOOTER_END_MARK  = '</footer>'

nav_pos    = content.find(NAVBAR_MARKER)
footer_pos = content.rfind(FOOTER_END_MARK) + len(FOOTER_END_MARK)

# Content to keep before navbar: loader, orbs, mobile overlay (they stay)
# Content between nav_pos and footer_pos: navbar + hero + features + tool + footer → REPLACE

# Extract the existing panels content (we need their IDs to remain)
# panel-synthese content
def extract_id_block(html, div_id):
    """Extract the full <div id="X">...</div> block."""
    marker = f'id="{div_id}"'
    start = html.find(marker)
    if start == -1:
        return ""
    # Back up to find the <div
    div_start = html.rfind('<div', 0, start)
    # Find matching close
    depth = 0
    pos = div_start
    while pos < len(html):
        if html[pos:pos+4] == '<div':
            depth += 1
        elif html[pos:pos+6] == '</div>':
            depth -= 1
            if depth == 0:
                return html[div_start:pos+6]
        pos += 1
    return ""

panel_synthese = extract_id_block(content, 'panel-synthese')
panel_orgchart = extract_id_block(content, 'panel-orgchart')
# Remove the outer wf-panel wrapper divs from the panels since we'll put them in pages
def unwrap_panel(panel_html):
    """Remove the outer <div class=\"wf-panel\"...> wrapper."""
    first_end = panel_html.find('>')
    inner_start = first_end + 1
    inner_end = panel_html.rfind('</div>')
    return panel_html[inner_start:inner_end].strip()

synthese_inner = unwrap_panel(panel_synthese) if panel_synthese else "<!-- panel-synthese not found -->"
orgchart_inner = unwrap_panel(panel_orgchart) if panel_orgchart else "<!-- panel-orgchart not found -->"

NEW_MAIN = f"""<!-- TOPBAR APP -->
<header id="topbar-app">
  <div class="tb-breadcrumb">
    <span class="tb-crumb-group" id="tb-group">Synthèse BIA</span>
    <i class="fa-solid fa-chevron-right tb-crumb-sep" id="tb-sep"></i>
    <span class="tb-crumb-page" id="tb-page">Charger les fiches BIA</span>
  </div>
  <div style="display:flex;align-items:center;gap:0.75rem;">
    <a href="/fiche-selector" target="_blank"
       style="font-size:0.67rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;
              padding:0.45rem 0.95rem;border:1px solid rgba(26,24,20,0.10);border-radius:0.45rem;
              color:rgba(26,24,20,0.55);text-decoration:none;transition:all 0.2s;"
       onmouseover="this.style.borderColor='rgba(242,72,94,0.3)';this.style.color='var(--poppy)'"
       onmouseout="this.style.borderColor='rgba(26,24,20,0.10)';this.style.color='rgba(26,24,20,0.55)'">
      <i class="fa-solid fa-pen-to-square" style="font-size:0.65rem;margin-right:0.35rem;"></i>Éditeur de fiche
    </a>
    <button id="sb-nav-toggle" onclick="toggleSidebar()" aria-label="Menu" style="display:flex;flex-direction:column;gap:5px;cursor:pointer;background:none;border:none;padding:.35rem .4rem;border-radius:.4rem;transition:background .2s;" onmouseover="this.style.background='rgba(26,24,20,0.06)'" onmouseout="this.style.background='none'">
      <span style="display:block;width:20px;height:1.5px;background:rgba(26,24,20,0.55);border-radius:1px;"></span>
      <span style="display:block;width:20px;height:1.5px;background:rgba(26,24,20,0.55);border-radius:1px;"></span>
      <span style="display:block;width:20px;height:1.5px;background:rgba(26,24,20,0.55);border-radius:1px;"></span>
    </button>
  </div>
</header>

<!-- MAIN APP PAGES -->
<main id="main-app">

  <!-- ══ PAGE 1: Charger les fiches BIA ══ -->
  <div class="app-page active" id="page-charger-fiches">
    <div class="pg-header">
      <div class="pg-label"><i class="fa-solid fa-folder-open"></i> Étape 01</div>
      <div class="pg-title"><strong>Charger</strong> les fiches BIA</div>
      <div class="pg-desc">Importez vos fiches BIA (.docx) depuis la base de projets ou depuis votre ordinateur. Au moins une fiche est requise pour générer la synthèse.</div>
    </div>

    <!-- Unlock bar (hidden until ≥1 fiche) -->
    <div class="step-unlock-bar" id="charger-unlock-bar" style="display:none;">
      <i class="fa-solid fa-circle-check"></i>
      <span id="charger-unlock-text">Fiche(s) chargée(s) — vous pouvez maintenant générer la synthèse.</span>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;max-width:900px;">
      <!-- Upload from disk -->
      <div class="app-card">
        <div class="app-card-title"><i class="fa-solid fa-upload"></i> Importer des fichiers</div>
        <div class="drop-zone text-center" id="cf-zone-fiches" role="button">
          <div class="drop-zone-icon"><i class="fa-regular fa-file-word"></i></div>
          <div class="drop-zone-title">Fiches BIA</div>
          <div class="drop-zone-sub">.docx — fichiers multiples</div>
          <p class="drop-zone-instruction">Glissez vos fiches ici<br>ou cliquez pour sélectionner</p>
          <div class="file-list" id="cf-file-list"></div>
        </div>
      </div>

      <!-- Load from DB -->
      <div class="app-card">
        <div class="app-card-title"><i class="fa-solid fa-database"></i> Charger depuis la base</div>
        <div style="display:flex;gap:0.5rem;margin-bottom:0.75rem;">
          <select id="cf-db-sector" onchange="cfLoadDB()" style="flex:1;padding:0.45rem 0.65rem;border:1px solid rgba(26,24,20,0.1);border-radius:0.45rem;font-size:0.72rem;background:#FAF8F4;outline:none;">
            <option value="">— Tous secteurs —</option>
            <option value="banques">Banques</option>
            <option value="assurances">Assurances</option>
          </select>
          <button onclick="cfLoadDB()" style="padding:0.45rem 0.85rem;border:1px solid rgba(26,24,20,0.1);border-radius:0.45rem;font-size:0.68rem;background:#FAF8F4;cursor:pointer;transition:all 0.2s;" onmouseover="this.style.borderColor='rgba(242,72,94,0.3)'" onmouseout="this.style.borderColor='rgba(26,24,20,0.1)'">
            <i class="fa-solid fa-magnifying-glass"></i>
          </button>
        </div>
        <input type="text" id="cf-db-search" placeholder="Rechercher client / fichier…"
               style="width:100%;padding:0.45rem 0.65rem;border:1px solid rgba(26,24,20,0.1);border-radius:0.45rem;font-size:0.72rem;background:#FAF8F4;outline:none;margin-bottom:0.65rem;"
               oninput="cfFilterDB(this.value)">
        <div id="cf-db-list" style="max-height:220px;overflow-y:auto;display:flex;flex-direction:column;gap:0.3rem;">
          <div style="font-size:0.68rem;color:rgba(26,24,20,0.35);text-align:center;padding:1rem 0;">
            Choisissez un secteur pour afficher les fiches disponibles.
          </div>
        </div>
      </div>
    </div>

    <!-- Loaded fiches summary -->
    <div class="app-card" id="cf-loaded-card" style="margin-top:1.5rem;max-width:900px;display:none;">
      <div class="app-card-title"><i class="fa-solid fa-list-check"></i> Fiches chargées <span id="cf-loaded-count" style="font-size:0.65rem;background:rgba(242,72,94,0.1);color:var(--poppy);padding:1px 7px;border-radius:9999px;margin-left:0.4rem;">0</span></div>
      <div id="cf-loaded-list" style="display:flex;flex-direction:column;gap:0.35rem;"></div>
      <div style="margin-top:1.25rem;display:flex;gap:0.75rem;flex-wrap:wrap;">
        <button class="btn-gold" onclick="navigatePage('page-generer-synthese','Synthèse BIA','Générer la synthèse BIA','child-generer')">
          <i class="fa-solid fa-arrow-right"></i> Générer la synthèse BIA
        </button>
        <button onclick="cfClearAll()" style="font-size:0.68rem;padding:0.5rem 0.95rem;border:1px solid rgba(26,24,20,0.1);border-radius:0.45rem;cursor:pointer;background:transparent;color:rgba(26,24,20,0.55);">
          <i class="fa-solid fa-trash-can"></i> Vider
        </button>
      </div>
    </div>
  </div>
  <!-- /page-charger-fiches -->

  <!-- ══ PAGE 2: Générer la synthèse BIA ══ -->
  <div class="app-page" id="page-generer-synthese">
    <div class="pg-header">
      <div class="pg-label"><i class="fa-solid fa-bolt-lightning"></i> Étape 02</div>
      <div class="pg-title"><strong>Générer</strong> la synthèse BIA</div>
      <div class="pg-desc">Injectez les fiches BIA dans le modèle de synthèse pour obtenir un tableau consolidé (.xlsx).</div>
    </div>
    <!-- Pre-loaded fiches indicator -->
    <div class="step-unlock-bar" id="gen-loaded-bar" style="display:none;">
      <i class="fa-solid fa-circle-check"></i>
      <span id="gen-loaded-text">0 fiche(s) pré-chargée(s) depuis l'étape précédente.</span>
    </div>
    <!-- Existing synthese panel content embedded here -->
    {synthese_inner}
  </div>
  <!-- /page-generer-synthese -->

  <!-- ══ PAGE 3: Synchronisation bidirectionnelle ══ -->
  <div class="app-page" id="page-sync">
    <div class="pg-header">
      <div class="pg-label"><i class="fa-solid fa-code-compare"></i> Étape 03</div>
      <div class="pg-title"><strong>Synchronisation</strong> bidirectionnelle</div>
      <div class="pg-desc">Comparez les DMIA de la synthèse BIA avec celles des fiches. Les écarts sont listés — cliquez sur un écart pour ouvrir la fiche dans l'éditeur et corriger.</div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;max-width:900px;margin-bottom:1.5rem;">
      <!-- Synthèse upload -->
      <div class="app-card">
        <div class="app-card-title"><i class="fa-regular fa-file-excel"></i> Synthèse BIA</div>
        <div class="drop-zone text-center" id="sync-zone-synthese" role="button">
          <div class="drop-zone-icon"><i class="fa-regular fa-file-excel"></i></div>
          <div class="drop-zone-title">Synthèse BIA</div>
          <div class="drop-zone-sub">.xlsx — avec DMIA arbitrée / préconisée</div>
          <p class="drop-zone-instruction">Glissez la synthèse ici<br>ou cliquez pour sélectionner</p>
          <div class="file-list" id="sync-synthese-list"></div>
        </div>
      </div>
      <!-- Fiches source -->
      <div class="app-card">
        <div class="app-card-title"><i class="fa-regular fa-file-word"></i> Fiches BIA</div>
        <div id="sync-fiches-info" style="font-size:0.72rem;color:rgba(26,24,20,0.55);padding:0.5rem 0 0.75rem;">
          Les fiches chargées à l'étape 1 sont utilisées automatiquement.
        </div>
        <div class="drop-zone text-center" id="sync-zone-fiches" role="button">
          <div class="drop-zone-icon"><i class="fa-regular fa-file-word"></i></div>
          <div class="drop-zone-title">Fiches supplémentaires</div>
          <div class="drop-zone-sub">.docx — optionnel</div>
          <p class="drop-zone-instruction">Glissez des fiches additionnelles ici</p>
          <div class="file-list" id="sync-fiches-list"></div>
        </div>
      </div>
    </div>

    <div style="max-width:900px;margin-bottom:1.5rem;">
      <button class="btn-gold" id="sync-btn" disabled onclick="runSyncCheck()">
        <i class="fa-solid fa-code-compare"></i> Comparer les DMIAs
      </button>
    </div>

    <!-- Results -->
    <div id="sync-results" style="display:none;max-width:900px;">
      <div class="app-card">
        <div class="app-card-title">
          <i class="fa-solid fa-triangle-exclamation"></i>
          Écarts détectés
          <span id="sync-count-badge" style="font-size:0.62rem;background:rgba(242,72,94,0.1);color:var(--poppy);padding:1px 7px;border-radius:9999px;margin-left:0.4rem;">0</span>
        </div>
        <div id="sync-empty" style="font-size:0.73rem;color:rgba(26,24,20,0.45);text-align:center;padding:1.5rem 0;display:none;">
          <i class="fa-solid fa-circle-check" style="color:#16a34a;margin-right:0.4rem;"></i>Aucun écart de DMIA détecté — toutes les fiches sont synchronisées.
        </div>
        <div id="sync-mismatch-list"></div>
        <div id="sync-resolved-count" style="display:none;margin-top:0.75rem;font-size:0.68rem;color:#15803d;">
          <i class="fa-solid fa-circle-check"></i> <span id="sync-resolved-num">0</span> fiche(s) corrigée(s) et retirée(s).
        </div>
      </div>
    </div>

    <!-- Processing spinner -->
    <div id="sync-processing" style="display:none;max-width:900px;">
      <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:3rem;gap:1.25rem;">
        <div class="spinner-rings" style="width:60px;height:60px;">
          <div class="ring1"></div><div class="ring2"></div><div class="ring3"></div>
          <i class="fa-solid fa-code-compare ring-icon" style="font-size:0.7rem;"></i>
        </div>
        <p style="font-size:0.78rem;color:rgba(26,24,20,0.55);letter-spacing:0.12em;text-transform:uppercase;">Comparaison en cours…</p>
      </div>
    </div>
  </div>
  <!-- /page-sync -->

  <!-- ══ PAGE 4: Organigramme ══ -->
  <div class="app-page" id="page-organigramme">
    <div class="pg-header">
      <div class="pg-label"><i class="fa-solid fa-sitemap"></i> Étape 04</div>
      <div class="pg-title"><strong>Génération</strong> de l'organigramme</div>
      <div class="pg-desc">Importez une synthèse BIA (.xlsx) pour visualiser la structure organisationnelle du client sous forme d'arbre interactif.</div>
    </div>
    {orgchart_inner}
  </div>
  <!-- /page-organigramme -->

  <!-- ══ PAGE 5: Rapport BIA (placeholder) ══ -->
  <div class="app-page" id="page-rapport">
    <div class="placeholder-page">
      <div class="placeholder-icon"><i class="fa-regular fa-file-pdf"></i></div>
      <div>
        <div style="font-size:1.4rem;font-weight:300;margin-bottom:0.5rem;">Générer le <strong>rapport BIA</strong></div>
        <div style="font-size:0.78rem;color:rgba(26,24,20,0.50);max-width:380px;line-height:1.75;">
          Cette fonctionnalité est en cours de développement. Elle permettra de produire un rapport BIA complet au format PDF ou Word.
        </div>
      </div>
      <span style="font-size:0.6rem;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;padding:0.3rem 0.9rem;border-radius:9999px;background:rgba(234,88,12,0.08);color:#ea580c;border:1px solid rgba(234,88,12,0.18);">Disponible prochainement</span>
    </div>
  </div>

  <!-- ══ PAGE 6: Gestion des risques (placeholder) ══ -->
  <div class="app-page" id="page-risques">
    <div class="placeholder-page">
      <div class="placeholder-icon"><i class="fa-solid fa-shield-halved"></i></div>
      <div>
        <div style="font-size:1.4rem;font-weight:300;margin-bottom:0.5rem;">Gestion des <strong>risques</strong></div>
        <div style="font-size:0.78rem;color:rgba(26,24,20,0.50);max-width:380px;line-height:1.75;">
          Cartographie des risques, analyse de criticité et plans de mitigation — disponibles dans la prochaine version.
        </div>
      </div>
      <span style="font-size:0.6rem;font-weight:700;letter-spacing:0.2em;text-transform:uppercase;padding:0.3rem 0.9rem;border-radius:9999px;background:rgba(234,88,12,0.08);color:#ea580c;border:1px solid rgba(234,88,12,0.18);">Disponible prochainement</span>
    </div>
  </div>

</main>
"""

content = content[:nav_pos] + NEW_MAIN + content[footer_pos:]
print("Step 3 done: Main content replaced")

# ─────────────────────────────────────────────────────────────────────────────
# 4. ADD NEW JAVASCRIPT before </script>
# ─────────────────────────────────────────────────────────────────────────────
NEW_JS = """

/* ══════════════════════════════════════════════════════════
   ACCORDION GROUP TOGGLE
   ══════════════════════════════════════════════════════════ */
window.toggleGroup = function(id) {
  const g = document.getElementById(id);
  if (!g) return;
  g.classList.toggle('open');
};

/* ══════════════════════════════════════════════════════════
   PAGE NAVIGATION
   ══════════════════════════════════════════════════════════ */
window.navigatePage = function(pageId, group, pageName, childId) {
  // Hide all pages
  document.querySelectorAll('.app-page').forEach(p => p.classList.remove('active'));
  // Show target
  const pg = document.getElementById(pageId);
  if (pg) { pg.classList.add('active'); }
  // Breadcrumb
  const tbg = document.getElementById('tb-group');
  const tbp = document.getElementById('tb-page');
  if (tbg) tbg.textContent = group;
  if (tbp) tbp.textContent = pageName;
  // Sidebar child active state
  document.querySelectorAll('.sb-child').forEach(c => c.classList.remove('active'));
  if (childId) {
    const ch = document.getElementById(childId);
    if (ch) ch.classList.add('active');
  }
  // Sync shared state to target page on navigate
  if (pageId === 'page-generer-synthese') _syncFichesToSynthesePage();
  sbClose();
};

/* ══════════════════════════════════════════════════════════
   SHARED FICHES STATE (between Charger → Générer → Sync)
   ══════════════════════════════════════════════════════════ */
window._sharedFiches    = [];   // File objects from upload
window._sharedFicheIds  = [];   // project_id from DB

function _refreshSharedUI() {
  const total = _sharedFiches.length + _sharedFicheIds.length;
  const card  = document.getElementById('cf-loaded-card');
  const bar   = document.getElementById('charger-unlock-bar');
  const list  = document.getElementById('cf-loaded-list');
  const cnt   = document.getElementById('cf-loaded-count');
  const genBar= document.getElementById('gen-loaded-bar');
  const genTxt= document.getElementById('gen-loaded-text');
  const genEl = document.getElementById('child-generer');
  const syncEl= document.getElementById('child-sync');

  if (card)  card.style.display  = total > 0 ? '' : 'none';
  if (bar)   bar.style.display   = total > 0 ? '' : 'none';
  if (cnt)   cnt.textContent     = total;

  if (genBar) genBar.style.display = total > 0 ? '' : 'none';
  if (genTxt) genTxt.textContent   = total + ' fiche(s) pré-chargée(s) depuis l\\'étape précédente.';

  // Unlock step 02 and 03
  if (genEl)  genEl.classList.toggle('sb-child-locked', total === 0);
  if (syncEl) syncEl.classList.toggle('sb-child-locked', total === 0);

  // Render loaded list
  if (list) {
    const rows = [];
    _sharedFiches.forEach((f, i) => {
      rows.push(`<div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0.6rem;background:rgba(242,72,94,0.03);border:1px solid rgba(242,72,94,0.08);border-radius:0.4rem;font-size:0.68rem;">
        <i class="fa-regular fa-file-word" style="color:var(--poppy);font-size:0.7rem;flex-shrink:0;"></i>
        <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${f.name}</span>
        <button onclick="_removeSharedFiche(${i},-1)" style="background:none;border:none;cursor:pointer;color:rgba(26,24,20,0.3);padding:0;font-size:0.65rem;" title="Retirer">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>`);
    });
    _sharedFicheIds.forEach((pid, i) => {
      rows.push(`<div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0.6rem;background:rgba(21,101,192,0.03);border:1px solid rgba(21,101,192,0.1);border-radius:0.4rem;font-size:0.68rem;">
        <i class="fa-solid fa-database" style="color:#1565C0;font-size:0.65rem;flex-shrink:0;"></i>
        <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${pid.name}</span>
        <button onclick="_removeSharedFiche(-1,${i})" style="background:none;border:none;cursor:pointer;color:rgba(26,24,20,0.3);padding:0;font-size:0.65rem;" title="Retirer">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>`);
    });
    list.innerHTML = rows.join('');
  }
  _syncFichesToSynthesePage();
}

window._removeSharedFiche = function(fileIdx, dbIdx) {
  if (fileIdx >= 0) _sharedFiches.splice(fileIdx, 1);
  if (dbIdx   >= 0) _sharedFicheIds.splice(dbIdx, 1);
  _refreshSharedUI();
};

window.cfClearAll = function() {
  _sharedFiches = []; _sharedFicheIds = [];
  document.getElementById('cf-file-list').innerHTML = '';
  document.getElementById('cf-zone-fiches').classList.remove('has-files');
  _refreshSharedUI();
};

function _syncFichesToSynthesePage() {
  // Pre-populate s1-file-list with shared fiches
  if (typeof wf !== 'undefined') {
    wf.s1.fiches = [..._sharedFiches];
    const el = document.getElementById('s1-file-list');
    if (el) {
      el.innerHTML = '';
      wf.s1.fiches.forEach((f, i) => {
        const d = document.createElement('div');
        d.className = 'file-item';
        d.innerHTML = `<i class="fa-solid fa-file text-xs"></i>
          <span class="fi-name">${f.name.length>26?f.name.slice(0,23)+'...':f.name}</span>
          <span class="fi-size">${f.size<1048576?(f.size/1024).toFixed(1)+' KB':(f.size/1048576).toFixed(1)+' MB'}</span>
          <button class="fi-rm" onclick="event.stopPropagation();rmFile('s1-file-list',${i})"><i class="fa-solid fa-xmark"></i></button>`;
        el.appendChild(d);
      });
      if (wf.s1.fiches.length) document.getElementById('s1-zone-fiches')?.classList.add('has-files');
    }
    if (typeof updS1 === 'function') updS1();
  }
}

/* ── Charger: drop zone for fiches ── */
(function initCFZone() {
  const zone = document.getElementById('cf-zone-fiches');
  if (!zone) return;
  const inp = document.createElement('input');
  inp.type = 'file'; inp.accept = '.docx'; inp.multiple = true; inp.style.display = 'none';
  document.body.appendChild(inp);
  inp.onchange = () => { addFilesToShared(Array.from(inp.files)); inp.value = ''; };
  zone.addEventListener('click', () => inp.click());
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('drag-over');
    addFilesToShared(Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.docx')));
  });
})();

function addFilesToShared(files) {
  files.forEach(f => {
    if (!_sharedFiches.find(x => x.name === f.name)) _sharedFiches.push(f);
  });
  const list = document.getElementById('cf-file-list');
  if (list) {
    list.innerHTML = _sharedFiches.map((f,i) =>
      `<div class="file-item"><i class="fa-solid fa-file"></i>
       <span class="fi-name">${f.name.length>26?f.name.slice(0,23)+'...':f.name}</span>
       <span class="fi-size">${f.size<1048576?(f.size/1024).toFixed(1)+' KB':(f.size/1048576).toFixed(1)+' MB'}</span>
       <button class="fi-rm" onclick="event.stopPropagation();_removeSharedFiche(${i},-1)"><i class="fa-solid fa-xmark"></i></button>
       </div>`).join('');
  }
  if (_sharedFiches.length) document.getElementById('cf-zone-fiches')?.classList.add('has-files');
  _refreshSharedUI();
}

/* ── Charger: DB browser ── */
let _cfDbAll = [];
window.cfLoadDB = async function() {
  const sector = document.getElementById('cf-db-sector')?.value || '';
  const list   = document.getElementById('cf-db-list');
  if (!list) return;
  list.innerHTML = '<div style="font-size:0.68rem;color:rgba(26,24,20,0.38);text-align:center;padding:0.75rem 0;">Chargement…</div>';
  try {
    const r = await fetch('/api/fiche-projects' + (sector ? '?sector=' + encodeURIComponent(sector) : ''));
    const data = await r.json();
    _cfDbAll = data;
    cfFilterDB(document.getElementById('cf-db-search')?.value || '');
  } catch {
    list.innerHTML = '<div style="font-size:0.68rem;color:#dc2626;text-align:center;padding:0.75rem 0;">Erreur de chargement.</div>';
  }
};

window.cfFilterDB = function(q) {
  const ql  = q.toLowerCase().trim();
  const list = document.getElementById('cf-db-list');
  if (!list) return;
  const filtered = ql ? _cfDbAll.filter(p =>
    p.client_name?.toLowerCase().includes(ql) ||
    p.project_name?.toLowerCase().includes(ql) ||
    p.client?.toLowerCase().includes(ql)
  ) : _cfDbAll;
  if (!filtered.length) {
    list.innerHTML = '<div style="font-size:0.68rem;color:rgba(26,24,20,0.35);text-align:center;padding:0.75rem 0;">Aucun résultat.</div>';
    return;
  }
  list.innerHTML = filtered.slice(0, 40).map(p => {
    const alreadyLoaded = _sharedFicheIds.find(x => x.id === p.id);
    return `<div style="display:flex;align-items:center;gap:0.5rem;padding:0.38rem 0.6rem;border:1px solid rgba(26,24,20,0.07);border-radius:0.4rem;font-size:0.68rem;${alreadyLoaded?'opacity:0.45;':'cursor:pointer;'}" ${alreadyLoaded?'':'onclick="cfAddDbFiche('+p.id+','+JSON.stringify((p.project_name||p.client_name||'Projet '+p.id).replace(/"/g,'&quot;'))+')"'}>
      <i class="fa-solid fa-database" style="color:#1565C0;font-size:0.62rem;flex-shrink:0;"></i>
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${p.project_name||p.client_name||'Projet '+p.id}</div>
        <div style="color:rgba(26,24,20,0.4);font-size:0.6rem;">${p.client||p.sector||''}</div>
      </div>
      ${alreadyLoaded ? '<i class="fa-solid fa-check" style="color:#16a34a;flex-shrink:0;font-size:0.6rem;"></i>' : '<i class="fa-solid fa-plus" style="color:rgba(26,24,20,0.28);flex-shrink:0;font-size:0.6rem;"></i>'}
    </div>`;
  }).join('');
};

window.cfAddDbFiche = function(id, name) {
  if (!_sharedFicheIds.find(x => x.id === id)) {
    _sharedFicheIds.push({ id, name });
    _refreshSharedUI();
    cfFilterDB(document.getElementById('cf-db-search')?.value || '');
  }
};

/* ══════════════════════════════════════════════════════════
   SYNCHRONISATION BIDIRECTIONNELLE
   ══════════════════════════════════════════════════════════ */
(function initSyncZones() {
  function mkZone(zoneId, listId, cb) {
    const zone = document.getElementById(zoneId);
    if (!zone) return;
    const inp = document.createElement('input');
    inp.type = 'file'; inp.style.display = 'none';
    inp.accept = zoneId.includes('synthese') ? '.xlsx' : '.docx';
    inp.multiple = !zoneId.includes('synthese');
    document.body.appendChild(inp);
    inp.onchange = () => { cb(Array.from(inp.files)); inp.value = ''; };
    zone.addEventListener('click', () => inp.click());
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
      e.preventDefault(); zone.classList.remove('drag-over');
      cb(Array.from(e.dataTransfer.files));
    });
  }
  let _syncSyntheseFile = null;
  let _syncExtraFiches  = [];

  function updSyncBtn() {
    const btn = document.getElementById('sync-btn');
    if (btn) btn.disabled = !_syncSyntheseFile;
  }

  mkZone('sync-zone-synthese', 'sync-synthese-list', files => {
    if (!files.length) return;
    _syncSyntheseFile = files[0];
    const list = document.getElementById('sync-synthese-list');
    if (list) list.innerHTML = `<div class="file-item"><i class="fa-solid fa-file-excel"></i><span class="fi-name">${_syncSyntheseFile.name}</span></div>`;
    document.getElementById('sync-zone-synthese')?.classList.add('has-files');
    updSyncBtn();
  });
  mkZone('sync-zone-fiches', 'sync-fiches-list', files => {
    const docxFiles = files.filter(f => f.name.endsWith('.docx'));
    _syncExtraFiches = [..._syncExtraFiches, ...docxFiles];
    const list = document.getElementById('sync-fiches-list');
    if (list) list.innerHTML = _syncExtraFiches.map(f =>
      `<div class="file-item"><i class="fa-solid fa-file-word"></i><span class="fi-name">${f.name}</span></div>`
    ).join('');
    if (_syncExtraFiches.length) document.getElementById('sync-zone-fiches')?.classList.add('has-files');
  });

  let _syncMismatches = [];
  let _syncResolved   = 0;

  window.runSyncCheck = async function() {
    if (!_syncSyntheseFile) return;
    document.getElementById('sync-results').style.display    = 'none';
    document.getElementById('sync-processing').style.display = '';

    const form = new FormData();
    form.append('synthese', _syncSyntheseFile, _syncSyntheseFile.name);
    // Add extra fiches
    _syncExtraFiches.forEach(f => form.append('fiches', f, f.name));
    // Also add shared fiches from step 1
    _sharedFiches.forEach(f => form.append('fiches', f, f.name));
    // Add DB IDs
    const ids = _sharedFicheIds.map(x => x.id);
    form.append('project_ids', JSON.stringify(ids));

    try {
      const r = await fetch('/api/sync-check', { method: 'POST', body: form });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      _syncMismatches = data.mismatches || [];
      _syncResolved   = 0;
      renderSyncResults();
    } catch (err) {
      toast('Erreur sync : ' + err.message, 'error');
    } finally {
      document.getElementById('sync-processing').style.display = 'none';
      document.getElementById('sync-results').style.display    = '';
    }
  };

  function renderSyncResults() {
    const list  = document.getElementById('sync-mismatch-list');
    const empty = document.getElementById('sync-empty');
    const badge = document.getElementById('sync-count-badge');
    const resCnt= document.getElementById('sync-resolved-count');
    const resNum= document.getElementById('sync-resolved-num');

    const active = _syncMismatches.filter(m => !m.resolved);
    if (badge)  badge.textContent  = active.length;
    if (resNum) resNum.textContent = _syncResolved;
    if (resCnt) resCnt.style.display = _syncResolved > 0 ? '' : 'none';
    if (empty)  empty.style.display  = active.length === 0 ? '' : 'none';

    if (!list) return;
    list.innerHTML = _syncMismatches.map((m, i) => `
      <div class="sync-mismatch-item ${m.resolved ? 'resolved' : ''}" onclick="openSyncFiche(${i})">
        <div class="smi-entity" title="${m.entity}">${m.entity || m.fiche_source || '—'}</div>
        <div class="smi-act">${m.activity || '—'}</div>
        <span class="smi-badge smi-synth" title="DMIA dans la synthèse">${m.dmia_synthese || '—'}</span>
        <i class="fa-solid fa-arrow-right smi-arrow"></i>
        <span class="smi-badge smi-fiche" title="DMIA dans la fiche">${m.dmia_fiche || '—'}</span>
        <i class="fa-solid fa-arrow-up-right-from-square smi-open"></i>
      </div>`).join('');
  }

  window.openSyncFiche = function(idx) {
    const m = _syncMismatches[idx];
    if (!m || m.resolved) return;
    const pid = m.project_id;
    // Build editor URL
    let url = '/fiche-editor';
    if (pid) url += '?project_id=' + pid;
    // Pass a hint about what to fix via sessionStorage
    sessionStorage.setItem('sync_hint', JSON.stringify({
      activity: m.activity,
      dmia_target: m.dmia_synthese,
      dmia_current: m.dmia_fiche,
    }));
    window.open(url, '_blank');
    // Mark as resolved (user opened the editor)
    _syncMismatches[idx].resolved = true;
    _syncResolved++;
    renderSyncResults();
  };
})();

/* ── Init default page ── */
(function initApp() {
  navigatePage('page-charger-fiches', 'Synthèse BIA', 'Charger les fiches BIA', 'child-charger');
})();
"""

script_close = content.rfind('</script>')
content = content[:script_close] + NEW_JS + '\n' + content[script_close:]
print("Step 4 done: JS added")

# ─────────────────────────────────────────────────────────────────────────────
# 5. WRITE
# ─────────────────────────────────────────────────────────────────────────────
with open('static/index.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Done! New index.html written.")
print(f"Final size: {len(content)} chars, {content.count(chr(10))} lines")
