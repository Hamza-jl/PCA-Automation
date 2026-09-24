import sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', encoding='utf-8') as f:
    content = f.read()

# ══════════════════════════════════════════════════════════════════════
# 1. ADD CSS for workflow stepper + tableau de bord
# ══════════════════════════════════════════════════════════════════════
CSS_NEW = """    /* ── Workflow stepper (below topbar) ── */
    #wf-stepper {
      position:fixed;top:58px;left:var(--sb-w);right:0;z-index:190;
      background:rgba(250,248,244,0.97);backdrop-filter:blur(16px);
      border-bottom:1px solid rgba(26,24,20,0.07);
      display:flex;align-items:center;justify-content:center;
      padding:0.55rem 2rem;gap:0;
    }
    .wf-step {
      display:flex;align-items:center;gap:0.45rem;padding:0.28rem 0.9rem;
      border-radius:99px;font-size:0.62rem;font-weight:600;letter-spacing:0.04em;
      color:rgba(26,24,20,0.32);transition:all 0.25s;white-space:nowrap;
      cursor:default;
    }
    .wf-step.active {
      background:rgba(242,72,94,0.10);color:var(--poppy);
      box-shadow:0 0 0 1px rgba(242,72,94,0.20);
    }
    .wf-step.done {
      color:rgba(26,24,20,0.55);
    }
    .wf-step-num {
      width:18px;height:18px;border-radius:50%;border:1.5px solid currentColor;
      display:flex;align-items:center;justify-content:center;
      font-size:0.55rem;font-weight:800;flex-shrink:0;transition:all 0.25s;
    }
    .wf-step.active .wf-step-num { background:var(--poppy);color:#fff;border-color:var(--poppy); }
    .wf-step.done   .wf-step-num { background:rgba(26,24,20,0.08);border-color:rgba(26,24,20,0.20); }
    .wf-sep { width:28px;height:1px;background:rgba(26,24,20,0.12);flex-shrink:0; }
    /* push main content below both topbar + stepper */
    #main-app { margin-top:calc(58px + 40px); }
    @media (max-width:768px) { #wf-stepper { left:0; } #main-app { left:0; } }

    /* ── Tableau de bord card ── */
    .tdb-card {
      background:linear-gradient(135deg,rgba(242,72,94,0.06) 0%,rgba(26,24,20,0.03) 100%);
      border:1px solid rgba(242,72,94,0.12);border-radius:0.9rem;
      padding:1.5rem 1.75rem;margin-bottom:2rem;
      display:grid;grid-template-columns:auto 1fr;gap:1.25rem;align-items:start;
    }
    .tdb-icon-wrap {
      width:48px;height:48px;border-radius:12px;
      background:var(--poppy);color:#fff;
      display:flex;align-items:center;justify-content:center;font-size:1.15rem;
      flex-shrink:0;box-shadow:0 4px 14px rgba(242,72,94,0.30);
    }
    .tdb-title { font-size:0.95rem;font-weight:800;color:rgba(26,24,20,0.90);margin-bottom:0.2rem; }
    .tdb-desc  { font-size:0.72rem;color:rgba(26,24,20,0.52);line-height:1.65;margin-bottom:0.65rem; }
    .tdb-tags  { display:flex;flex-wrap:wrap;gap:0.35rem; }
    .tdb-tag {
      font-size:0.6rem;font-weight:700;letter-spacing:0.05em;text-transform:uppercase;
      padding:0.22rem 0.65rem;border-radius:99px;
      background:rgba(242,72,94,0.08);color:var(--poppy);border:1px solid rgba(242,72,94,0.18);
    }
    .tdb-tag.ok   { background:rgba(22,163,74,0.08);color:#15803d;border-color:rgba(22,163,74,0.22); }
    .tdb-tag.info { background:rgba(21,101,192,0.08);color:#1565C0;border-color:rgba(21,101,192,0.22); }
    .tdb-tag.warn { background:rgba(234,88,12,0.08);color:#c2410c;border-color:rgba(234,88,12,0.22); }
"""

ANCHOR_CSS = '    /* DB browser rows */'
content = content.replace(ANCHOR_CSS, CSS_NEW + '\n' + ANCHOR_CSS, 1)
print('Step 1 CSS done:', ANCHOR_CSS in content)


# ══════════════════════════════════════════════════════════════════════
# 2. ADD WORKFLOW STEPPER HTML (after topbar-app closing tag)
# ══════════════════════════════════════════════════════════════════════
TOPBAR_END = '</header>\n\n<!-- MAIN APP PAGES -->'
STEPPER_HTML = """</header>

<!-- WORKFLOW STEPPER -->
<div id="wf-stepper">
  <div class="wf-step active" id="wfs-1" onclick="navigatePage('page-charger-fiches','Synthèse BIA','Charger les fiches BIA','child-charger')">
    <span class="wf-step-num">01</span>Charger les fiches BIA
  </div>
  <div class="wf-sep"></div>
  <div class="wf-step" id="wfs-2" onclick="navigatePage('page-generer-synthese','Synthèse BIA','Générer la synthèse BIA','child-generer')">
    <span class="wf-step-num">02</span>Générer la synthèse BIA
  </div>
  <div class="wf-sep"></div>
  <div class="wf-step" id="wfs-3" onclick="navigatePage('page-sync','Synthèse BIA','Synchronisation bidirectionnelle','child-sync')">
    <span class="wf-step-num">03</span>Synchronisation
  </div>
  <div class="wf-sep"></div>
  <div class="wf-step" id="wfs-4" onclick="navigatePage('page-organigramme','Synthèse BIA','Génération organigramme','child-org')">
    <span class="wf-step-num">04</span>Organigramme
  </div>
</div>

<!-- MAIN APP PAGES -->"""

content = content.replace(TOPBAR_END, STEPPER_HTML, 1)
print('Step 2 stepper HTML done:', 'wf-stepper' in content)


# ══════════════════════════════════════════════════════════════════════
# 3. ADD TABLEAU DE BORD to each page (after pg-header)
# ══════════════════════════════════════════════════════════════════════

# Page 1 — Charger les fiches BIA
TDB_P1 = """    </div>
    <!-- Tableau de bord -->
    <div class="tdb-card">
      <div class="tdb-icon-wrap"><i class="fa-solid fa-folder-open"></i></div>
      <div>
        <div class="tdb-title">Tableau de bord — Chargement des fiches</div>
        <div class="tdb-desc">
          Cette étape centralise toutes les fiches BIA (.docx) à analyser. Importez-les depuis la base de données existante ou téléversez-les manuellement. Au moins une fiche est requise pour débloquer les étapes suivantes. Les fiches chargées ici sont automatiquement transmises aux étapes 02 et 03.
        </div>
        <div class="tdb-tags">
          <span class="tdb-tag">📂 Source : base DB ou upload manuel</span>
          <span class="tdb-tag info">🔓 Débloque : Générer la synthèse (02)</span>
          <span class="tdb-tag ok" id="tdb1-count-tag">0 fiche(s) chargée(s)</span>
        </div>
      </div>
    </div>"""

OLD_P1 = """    </div>
    <!-- Pre-loaded fiches indicator -->
    <div class="step-unlock-bar" id="gen-loaded-bar" style="display:none;">"""

# Insert after pg-header closing div in page-charger-fiches
OLD_CF_HEADER_END = """    </div>
    <!-- Load from DB -->"""
NEW_CF_HEADER_END = """    </div>
    <!-- Tableau de bord -->
    <div class="tdb-card">
      <div class="tdb-icon-wrap"><i class="fa-solid fa-folder-open"></i></div>
      <div>
        <div class="tdb-title">Tableau de bord — Chargement des fiches</div>
        <div class="tdb-desc">
          Centralisez ici toutes les fiches BIA (.docx) à analyser. Importez-les depuis la base de données ou téléversez-les manuellement. Au moins une fiche est requise pour débloquer l'étape suivante. Les fiches chargées sont automatiquement transmises aux étapes 02 et 03.
        </div>
        <div class="tdb-tags">
          <span class="tdb-tag">📂 Source : base DB ou upload</span>
          <span class="tdb-tag info">🔓 Débloque étape 02</span>
          <span class="tdb-tag ok" id="tdb1-count-tag">0 fiche(s) chargée(s)</span>
        </div>
      </div>
    </div>
    <!-- Load from DB -->"""

content = content.replace(OLD_CF_HEADER_END, NEW_CF_HEADER_END, 1)
print('Step 3a TDB page1 done:', 'tdb1-count-tag' in content)

# Page 2 — Générer la synthèse BIA: insert TDB after pg-header, before gen-loaded-bar
OLD_GEN_AFTER_HDR = """    <!-- Pre-loaded fiches indicator -->
    <div class="step-unlock-bar" id="gen-loaded-bar" style="display:none;">"""
NEW_GEN_AFTER_HDR = """    <!-- Tableau de bord -->
    <div class="tdb-card">
      <div class="tdb-icon-wrap"><i class="fa-solid fa-bolt-lightning"></i></div>
      <div>
        <div class="tdb-title">Tableau de bord — Génération de la synthèse BIA</div>
        <div class="tdb-desc">
          Injectez les fiches BIA chargées à l'étape précédente dans le modèle de synthèse Excel (.xlsx). Le système consolide automatiquement toutes les données d'impact, DMIA et ressources dans un tableau structuré prêt à l'emploi.
        </div>
        <div class="tdb-tags">
          <span class="tdb-tag">📊 Sortie : fichier .xlsx consolidé</span>
          <span class="tdb-tag info">📥 Entrée : fiches de l'étape 01</span>
          <span class="tdb-tag warn">⚡ Action requise : template synthèse vierge</span>
        </div>
      </div>
    </div>
    <!-- Pre-loaded fiches indicator -->
    <div class="step-unlock-bar" id="gen-loaded-bar" style="display:none;">"""

content = content.replace(OLD_GEN_AFTER_HDR, NEW_GEN_AFTER_HDR, 1)
print('Step 3b TDB page2 done:', 'Génération de la synthèse BIA' in content)

# Page 3 — Synchronisation: find pg-header end in page-sync
OLD_SYNC_HDR = """    </div>
    <div style="max-width:900px;margin-bottom:1.5rem;">
      <button class="btn-gold" id="sync-btn" disabled onclick="runSyncCheck()">"""
NEW_SYNC_HDR = """    </div>
    <!-- Tableau de bord -->
    <div class="tdb-card">
      <div class="tdb-icon-wrap"><i class="fa-solid fa-code-compare"></i></div>
      <div>
        <div class="tdb-title">Tableau de bord — Synchronisation bidirectionnelle</div>
        <div class="tdb-desc">
          Chargez la synthèse BIA (.xlsx) générée et les fiches individuelles (.docx) pour détecter les écarts de DMIA entre les deux sources. Les incohérences sont listées et chaque clic ouvre directement la fiche concernée dans l'éditeur pour correction.
        </div>
        <div class="tdb-tags">
          <span class="tdb-tag">🔍 Détection d'écarts DMIA</span>
          <span class="tdb-tag info">↔ Synthèse .xlsx ↔ Fiches .docx</span>
          <span class="tdb-tag ok">✏️ Correction directe dans l'éditeur</span>
        </div>
      </div>
    </div>
    <div style="max-width:900px;margin-bottom:1.5rem;">
      <button class="btn-gold" id="sync-btn" disabled onclick="runSyncCheck()">"""

content = content.replace(OLD_SYNC_HDR, NEW_SYNC_HDR, 1)
print('Step 3c TDB page3 done:', 'Synchronisation bidirectionnelle' in content and 'tdb-card' in content)

# Page 4 — Organigramme: find pg-header end in page-organigramme
OLD_ORG_HDR = """    </div>
    <div class="glass-g"""
# Use more specific anchor since this pattern might repeat
idx_org = content.find('id="page-organigramme"')
idx_glass = content.find('    <div class="glass-g', idx_org)
ORG_BEFORE = content[idx_glass:idx_glass+30]
print('Org glass anchor:', repr(ORG_BEFORE))


# ══════════════════════════════════════════════════════════════════════
# 4. REMOVE "Fiches BIA" and "Fiche de recensement" drop zones from page 02
# ══════════════════════════════════════════════════════════════════════

# Replace the 3-column grid with just the synthèse zone
OLD_GRID = """            <div class="grid md:grid-cols-3 gap-5 mb-8">

              <div class="drop-zone text-center" id="s1-zone-fiches" role="button">
                <div class="drop-zone-icon"><i class="fa-regular fa-file-word"></i></div>
                <div class="drop-zone-title">Fiches BIA</div>
                <div class="drop-zone-sub">.docx — fichiers multiples</div>
                <p class="drop-zone-instruction">Glissez vos fiches ici<br>ou cliquez pour sélectionner</p>
                <div class="file-list" id="s1-file-list"></div>
              </div>

              <div class="drop-zone text-center" id="s1-zone-synthese" role="button">
                <div class="drop-zone-icon"><i class="fa-regular fa-file-excel"></i></div>
                <div class="drop-zone-title">Synthèse BIA</div>
                <div class="drop-zone-sub">.xlsx — template vierge</div>
                <p class="drop-zone-instruction">Glissez votre synthèse ici<br>ou cliquez pour sélectionner</p>
                <div class="file-list" id="s1-synthese-list"></div>
              </div>

              <div class="drop-zone text-center" id="s1-zone-recens" role="button">
                <div class="drop-zone-icon"><i class="fa-regular fa-rectangle-list"></i></div>
                <div class="drop-zone-title">Fiche de recensement</div>
                <div class="drop-zone-sub">.xlsx — <span class="text-gold/70 font-semibold">optionnel</span></div>
                <p class="drop-zone-instruction">Structure auto-détectée<br>depuis le suivi projet</p>
                <div class="file-list" id="s1-recens-list"></div>
              </div>
            </div>"""

NEW_GRID = """            <div class="flex justify-center mb-8">
              <div class="drop-zone text-center" id="s1-zone-synthese" role="button" style="max-width:340px;width:100%;">
                <div class="drop-zone-icon"><i class="fa-regular fa-file-excel"></i></div>
                <div class="drop-zone-title">Synthèse BIA</div>
                <div class="drop-zone-sub">.xlsx — template vierge</div>
                <p class="drop-zone-instruction">Glissez votre synthèse ici<br>ou cliquez pour sélectionner</p>
                <div class="file-list" id="s1-synthese-list"></div>
              </div>
            </div>
            <!-- hidden file-list kept for JS compatibility -->
            <div id="s1-file-list" style="display:none;"></div>
            <div id="s1-recens-list" style="display:none;"></div>"""

content = content.replace(OLD_GRID, NEW_GRID, 1)
print('Step 4 grid replaced:', 's1-zone-fiches' not in content or 'display:none' in content)


# ══════════════════════════════════════════════════════════════════════
# 5. UPDATE navigatePage JS to sync the wf-stepper
# ══════════════════════════════════════════════════════════════════════
OLD_NAV_FN = """window.navigatePage = function(pageId, group, pageName, childId) {
  // Hide all pages
  document.querySelectorAll('.app-page').forEach(p => p.classList.remove('active'));
  // Show target"""

NEW_NAV_FN = """window.navigatePage = function(pageId, group, pageName, childId) {
  // Sync workflow stepper
  const stepMap = {
    'page-charger-fiches':   'wfs-1',
    'page-generer-synthese': 'wfs-2',
    'page-sync':             'wfs-3',
    'page-organigramme':     'wfs-4',
  };
  const stepOrder = ['wfs-1','wfs-2','wfs-3','wfs-4'];
  const activeStep = stepMap[pageId];
  if (activeStep) {
    const activeIdx = stepOrder.indexOf(activeStep);
    stepOrder.forEach((sid, i) => {
      const el = document.getElementById(sid);
      if (!el) return;
      el.classList.remove('active','done');
      if (sid === activeStep) el.classList.add('active');
      else if (i < activeIdx)  el.classList.add('done');
    });
  }
  // Update tdb1 count tag when leaving/entering charger page
  const tdb1 = document.getElementById('tdb1-count-tag');
  if (tdb1) {
    const total = (window._sharedFiches||[]).length + (window._sharedFicheIds||[]).length;
    tdb1.textContent = total + ' fiche(s) chargée(s)';
    tdb1.className = 'tdb-tag ' + (total > 0 ? 'ok' : '');
  }
  // Hide all pages
  document.querySelectorAll('.app-page').forEach(p => p.classList.remove('active'));
  // Show target"""

content = content.replace(OLD_NAV_FN, NEW_NAV_FN, 1)
print('Step 5 navigatePage updated:', 'stepMap' in content)


# ══════════════════════════════════════════════════════════════════════
# 6. Add TDB for organigramme page (after pg-header)
# ══════════════════════════════════════════════════════════════════════
idx_org2 = content.find('id="page-organigramme"')
idx_glass2 = content.find('    <div class="glass-g', idx_org2)
# insert tdb before the glass div
ORG_CONTENT_START = content[idx_glass2:idx_glass2+60]
TDB_ORG = """    <!-- Tableau de bord -->
    <div class="tdb-card">
      <div class="tdb-icon-wrap"><i class="fa-solid fa-sitemap"></i></div>
      <div>
        <div class="tdb-title">Tableau de bord — Génération de l'organigramme</div>
        <div class="tdb-desc">
          Importez la synthèse BIA (.xlsx) pour visualiser automatiquement la structure organisationnelle du client sous forme d'arbre interactif hiérarchique. Chaque nœud représente une entité avec ses activités critiques associées.
        </div>
        <div class="tdb-tags">
          <span class="tdb-tag">🌳 Visualisation hiérarchique interactive</span>
          <span class="tdb-tag info">📥 Entrée : synthèse .xlsx</span>
          <span class="tdb-tag ok">🖨️ Export : image / PDF</span>
        </div>
      </div>
    </div>\n    """ + content[idx_glass2:idx_glass2+60]

content = content[:idx_glass2] + TDB_ORG[len("    <!-- Tableau de bord -->\n"):]
# Oops, let me redo this properly
# Reset to before this change
with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', encoding='utf-8') as f:
    pass  # already have content in memory, just fix the org insertion

# Find the exact glass-g div that follows the organigramme pg-header
idx_org3 = content.find('id="page-organigramme"')
idx_phdr_end = content.find('</div>\n    <div class="glass-g', idx_org3)
ORG_INSERT_POINT = content[idx_phdr_end:idx_phdr_end+35]
print('Org insert point:', repr(ORG_INSERT_POINT))

TDB_ORG_BLOCK = """</div>
    <!-- Tableau de bord -->
    <div class="tdb-card">
      <div class="tdb-icon-wrap"><i class="fa-solid fa-sitemap"></i></div>
      <div>
        <div class="tdb-title">Tableau de bord — Génération de l'organigramme</div>
        <div class="tdb-desc">
          Importez la synthèse BIA (.xlsx) pour visualiser la structure organisationnelle du client sous forme d'arbre interactif. Chaque nœud représente une entité avec ses activités critiques et DMIAs associées.
        </div>
        <div class="tdb-tags">
          <span class="tdb-tag">🌳 Visualisation hiérarchique</span>
          <span class="tdb-tag info">📥 Entrée : synthèse .xlsx</span>
          <span class="tdb-tag ok">🖨️ Export image / PDF</span>
        </div>
      </div>
    </div>
    <div class="glass-g"""

content = content[:idx_phdr_end] + TDB_ORG_BLOCK + content[idx_phdr_end + len('</div>\n    <div class="glass-g'):]
print('Step 6 TDB org done:', 'Génération de l\'organigramme' in content)


# ══════════════════════════════════════════════════════════════════════
# WRITE
# ══════════════════════════════════════════════════════════════════════
with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print(f'\nDone! Lines: {content.count(chr(10))}, Chars: {len(content)}')
