"""
Rebuild index.html from salvageable parts after corruption.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', encoding='utf-8') as f:
    content = f.read()

# ── 1. Take good HTML up to end of org TDB card ──
GOOD_HTML = content[:116583]   # ends right after TDB card for org page

# ── 2. Append org panel content + remaining pages + JS ──
TAIL = """
    <!-- Org panel: upload zone + visualization -->
    <div class="glass-gold px-4 py-3 mb-6 rounded-xl text-xs text-neutral-900/50 flex items-center gap-2">
      <i class="fa-solid fa-circle-info text-gold/60"></i>
      Importez votre synthèse BIA (.xlsx) pour générer l'organigramme interactif de l'entité.
    </div>

    <div class="grid md:grid-cols-2 gap-5 mb-8">
      <div class="drop-zone text-center" id="org-zone-synthese" role="button">
        <div class="drop-zone-icon"><i class="fa-regular fa-file-excel"></i></div>
        <div class="drop-zone-title">Synthèse BIA</div>
        <div class="drop-zone-sub">.xlsx</div>
        <p class="drop-zone-instruction">Glissez votre synthèse ici<br>ou cliquez pour sélectionner</p>
        <div class="file-list" id="org-file-list"></div>
      </div>
      <div style="display:flex;flex-direction:column;gap:1rem;">
        <button class="btn-gold" id="org-btn-generate" disabled onclick="generateOrgchart()" style="width:100%;">
          <i class="fa-solid fa-sitemap"></i> Générer l'organigramme
        </button>
        <button id="org-btn-export" disabled onclick="exportOrgchart()"
                style="width:100%;padding:0.6rem;border:1px solid rgba(26,24,20,0.12);border-radius:0.5rem;
                       background:transparent;font-family:inherit;font-size:0.72rem;font-weight:600;
                       color:rgba(26,24,20,0.55);cursor:not-allowed;transition:all 0.2s;">
          <i class="fa-solid fa-image"></i> Exporter en image
        </button>
      </div>
    </div>

    <!-- D3 Tree visualization area -->
    <div id="org-tree-wrap" style="display:none;width:100%;overflow:auto;border:1px solid rgba(26,24,20,0.08);
         border-radius:0.75rem;background:#fff;min-height:400px;position:relative;">
      <div id="org-processing" style="display:none;position:absolute;inset:0;display:flex;align-items:center;
           justify-content:center;flex-direction:column;gap:0.75rem;background:rgba(250,248,244,0.9);">
        <div class="spinner-rings" style="width:48px;height:48px;">
          <div class="ring1"></div><div class="ring2"></div><div class="ring3"></div>
          <i class="fa-solid fa-sitemap ring-icon" style="font-size:0.65rem;"></i>
        </div>
        <p style="font-size:0.72rem;color:rgba(26,24,20,0.45);letter-spacing:0.1em;text-transform:uppercase;">Génération en cours…</p>
      </div>
      <svg id="org-svg" style="width:100%;min-height:400px;"></svg>
    </div>

  </div>
  <!-- /page-organigramme -->

  <!-- ══ PAGE 5: Rapport BIA (placeholder) ══ -->
  <div class="app-page" id="page-rapport">
    <div class="pg-header">
      <div class="pg-label"><i class="fa-solid fa-file-chart-column"></i> Rapport BIA</div>
      <div class="pg-title"><strong>Générer</strong> le rapport BIA</div>
      <div class="pg-desc">Fonctionnalité en cours de développement — disponible prochainement.</div>
    </div>
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:4rem 2rem;gap:1rem;color:rgba(26,24,20,0.35);">
      <i class="fa-solid fa-hammer" style="font-size:2.5rem;"></i>
      <p style="font-size:0.85rem;font-weight:600;">En cours de développement</p>
      <p style="font-size:0.72rem;">Cette section sera disponible dans une prochaine version.</p>
    </div>
  </div>
  <!-- /page-rapport -->

  <!-- ══ PAGE 6: Gestion des risques (placeholder) ══ -->
  <div class="app-page" id="page-risques">
    <div class="pg-header">
      <div class="pg-label"><i class="fa-solid fa-triangle-exclamation"></i> Gestion des risques</div>
      <div class="pg-title"><strong>Gestion</strong> des risques</div>
      <div class="pg-desc">Fonctionnalité en cours de développement — disponible prochainement.</div>
    </div>
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:4rem 2rem;gap:1rem;color:rgba(26,24,20,0.35);">
      <i class="fa-solid fa-hammer" style="font-size:2.5rem;"></i>
      <p style="font-size:0.85rem;font-weight:600;">En cours de développement</p>
      <p style="font-size:0.72rem;">Cette section sera disponible dans une prochaine version.</p>
    </div>
  </div>
  <!-- /page-risques -->

</main>

<!-- Mobile overlay -->
<div id="sb-overlay" onclick="sbClose()"></div>
<button id="sb-fab" onclick="sbOpen()" aria-label="Ouvrir la navigation">
  <i class="fa-solid fa-bars-staggered"></i>
</button>

<div id="toast-app" style="position:fixed;bottom:26px;left:50%;transform:translateX(-50%);
  background:#1A1814;color:#fff;padding:10px 20px;border-radius:99px;font-size:12.5px;font-weight:500;
  box-shadow:0 4px 16px rgba(0,0,0,.22);opacity:0;transition:opacity .2s;pointer-events:none;
  z-index:8000;white-space:nowrap;font-family:'Montserrat',sans-serif;"></div>

<script>
/* ════════════════════════════════════════════════════════════
   TOAST UTILITY
════════════════════════════════════════════════════════════ */
function toast(msg, type) {
  const el = document.getElementById('toast-app');
  el.textContent = msg;
  el.style.background = type==='error' ? '#F2485E' : type==='ok' ? '#2E7D32' : '#1A1814';
  el.style.opacity = '1';
  clearTimeout(window._toastTimer);
  window._toastTimer = setTimeout(() => el.style.opacity = '0', 3200);
}

/* ════════════════════════════════════════════════════════════
   SIDEBAR TOGGLE
════════════════════════════════════════════════════════════ */
function sbOpen()  { document.getElementById('sidebar').classList.add('open');    document.getElementById('sb-overlay').classList.add('visible'); }
function sbClose() { document.getElementById('sidebar').classList.remove('open'); document.getElementById('sb-overlay').classList.remove('visible'); }
function toggleSidebar() { document.getElementById('sidebar').classList.contains('open') ? sbClose() : sbOpen(); }

/* ════════════════════════════════════════════════════════════
   ACCORDION GROUP TOGGLE
════════════════════════════════════════════════════════════ */
window.toggleGroup = function(id) {
  const g = document.getElementById(id);
  if (!g) return;
  g.classList.toggle('open');
  const children = document.getElementById(id + '-children');
  if (children) children.style.maxHeight = g.classList.contains('open') ? '600px' : '0';
};

/* ════════════════════════════════════════════════════════════
   PAGE NAVIGATION
════════════════════════════════════════════════════════════ */
window.navigatePage = function(pageId, group, pageName, childId) {
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
  // Update tdb1 count tag
  const tdb1 = document.getElementById('tdb1-count-tag');
  if (tdb1) {
    const total = (window._sharedFiches||[]).length + (window._sharedFicheIds||[]).length;
    tdb1.textContent = total + ' fiche(s) chargée(s)';
    tdb1.className = 'tdb-tag ' + (total > 0 ? 'ok' : '');
  }
  // Hide all pages
  document.querySelectorAll('.app-page').forEach(p => p.classList.remove('active'));
  // Show target
  const page = document.getElementById(pageId);
  if (page) page.classList.add('active');
  // Update breadcrumb
  const grpEl  = document.getElementById('tb-group');
  const pageEl = document.getElementById('tb-page');
  const sepEl  = document.getElementById('tb-sep');
  if (grpEl)  grpEl.textContent  = group || '';
  if (pageEl) pageEl.textContent = pageName || '';
  if (sepEl)  sepEl.style.display = (group && pageName) ? '' : 'none';
  // Update sidebar active child
  document.querySelectorAll('.sb-child').forEach(c => c.classList.remove('active'));
  if (childId) { const ch = document.getElementById(childId); if (ch) ch.classList.add('active'); }
  // Pre-populate synthese page fiches
  if (pageId === 'page-generer-synthese') _syncFichesToSynthesePage();
  sbClose();
};

/* ════════════════════════════════════════════════════════════
   SHARED FICHES STATE (between Charger → Générer → Sync)
════════════════════════════════════════════════════════════ */
window._sharedFiches    = [];   // File objects from upload
window._sharedFicheIds  = [];   // {id, name} from DB

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
  if (genTxt) genTxt.textContent   = total + ' fiche(s) pré-chargée(s) depuis l\'étape précédente.';

  // Unlock step 02 only (sync always accessible)
  if (genEl)  genEl.classList.toggle('sb-child-locked', total === 0);

  // Sidebar loaded count badge
  const sbBadge = document.getElementById('sb-loaded-count');
  if (sbBadge) {
    sbBadge.textContent   = total;
    sbBadge.style.display = total > 0 ? '' : 'none';
  }
  const lockG = document.getElementById('lock-generer');
  if (lockG) lockG.style.display = total > 0 ? 'none' : '';

  // Render loaded list
  if (list) {
    const rows = [];
    _sharedFiches.forEach((f, i) => {
      rows.push(`<div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0.6rem;
        background:rgba(242,72,94,0.03);border:1px solid rgba(242,72,94,0.08);border-radius:0.4rem;font-size:0.68rem;">
        <i class="fa-regular fa-file-word" style="color:var(--poppy);font-size:0.7rem;flex-shrink:0;"></i>
        <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${f.name}</span>
        <button onclick="event.stopPropagation();_removeSharedFiche(${i},-1)"
          style="background:none;border:none;cursor:pointer;color:rgba(26,24,20,0.35);font-size:0.65rem;padding:2px 4px;">
          <i class="fa-solid fa-xmark"></i></button>
        </div>`);
    });
    _sharedFicheIds.forEach((p, i) => {
      rows.push(`<div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0.6rem;
        background:rgba(21,101,192,0.04);border:1px solid rgba(21,101,192,0.10);border-radius:0.4rem;font-size:0.68rem;">
        <i class="fa-solid fa-database" style="color:#1565C0;font-size:0.65rem;flex-shrink:0;"></i>
        <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${p.name}</span>
        <button onclick="event.stopPropagation();_removeSharedFiche(-1,${i})"
          style="background:none;border:none;cursor:pointer;color:rgba(26,24,20,0.35);font-size:0.65rem;padding:2px 4px;">
          <i class="fa-solid fa-xmark"></i></button>
        </div>`);
    });
    list.innerHTML = rows.join('');
  }
}

window._removeSharedFiche = function(fileIdx, dbIdx) {
  if (fileIdx >= 0) _sharedFiches.splice(fileIdx, 1);
  if (dbIdx   >= 0) _sharedFicheIds.splice(dbIdx, 1);
  _refreshSharedUI();
};

window.cfClearAll = function() {
  _sharedFiches = []; _sharedFicheIds = [];
  document.getElementById('cf-file-list').innerHTML = '';
  document.getElementById('cf-zone-fiches')?.classList.remove('has-files');
  _refreshSharedUI();
};

function _syncFichesToSynthesePage() {
  // Pre-populate s1-file-list on the generer page with shared fiches
  const fileList = document.getElementById('s1-file-list');
  if (!fileList) return;
  if (_sharedFiches.length) {
    fileList.innerHTML = _sharedFiches.map(f =>
      `<div style="font-size:0.62rem;color:rgba(26,24,20,0.6);padding:2px 0;">${f.name}</div>`
    ).join('');
  }
  // Mark zone
  const zone = document.getElementById('s1-zone-fiches');
  if (zone && _sharedFiches.length) zone.classList.add('ok');
  // Unlock process button if synthese also selected
  _checkS1Ready();
}

function _checkS1Ready() {
  const btn = document.getElementById('s1-btn-process');
  if (!btn) return;
  const hasSynthese = document.getElementById('s1-zone-synthese')?.classList.contains('ok');
  const hasFiches   = _sharedFiches.length > 0 || _sharedFicheIds.length > 0;
  btn.disabled = !(hasSynthese && hasFiches);
}

/* ════════════════════════════════════════════════════════════
   CHARGER PAGE: Drop zone + DB browser
════════════════════════════════════════════════════════════ */
function addFilesToShared(files) {
  Array.from(files).filter(f => f.name.endsWith('.docx')).forEach(f => {
    if (!_sharedFiches.find(x => x.name === f.name)) _sharedFiches.push(f);
  });
  _refreshSharedUI();
}

(function initChargerZone() {
  const zone = document.getElementById('cf-zone-fiches');
  const inp  = document.createElement('input');
  inp.type = 'file'; inp.multiple = true; inp.accept = '.docx'; inp.style.display = 'none';
  document.body.appendChild(inp);

  if (zone) {
    zone.addEventListener('click', () => inp.click());
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag'));
    zone.addEventListener('drop', e => {
      e.preventDefault(); zone.classList.remove('drag');
      addFilesToShared(e.dataTransfer.files);
    });
  }
  inp.addEventListener('change', () => { addFilesToShared(inp.files); inp.value = ''; });
})();

/* ── DB browser ── */
let _cfDbAll = [];
window.cfLoadDB = async function() {
  const sector = document.getElementById('cf-db-sector')?.value || '';
  const list   = document.getElementById('cf-db-list');
  if (!list) return;
  list.innerHTML = '<div style="font-size:0.68rem;color:rgba(26,24,20,0.38);text-align:center;padding:0.75rem 0;">Chargement…</div>';
  try {
    const r = await fetch('/api/fiche-projects' + (sector ? '?sector=' + encodeURIComponent(sector) : ''));
    const groups = await r.json();
    _cfDbAll = groups.flatMap(g => g.projects.map(p => ({...p, client: g.client})));
    cfFilterDB(document.getElementById('cf-db-search')?.value || '');
  } catch {
    list.innerHTML = '<div style="font-size:0.68rem;color:#dc2626;text-align:center;padding:0.75rem 0;">Erreur de chargement.</div>';
  }
};

window.cfFilterDB = function(q) {
  const ql   = (q || '').toLowerCase().trim();
  const list = document.getElementById('cf-db-list');
  if (!list) return;
  const filtered = ql ? _cfDbAll.filter(p =>
    (p.project_name || '').toLowerCase().includes(ql) ||
    (p.client       || '').toLowerCase().includes(ql)
  ) : _cfDbAll;

  if (!filtered.length) {
    list.innerHTML = '<div style="font-size:0.68rem;color:rgba(26,24,20,0.35);text-align:center;padding:0.75rem 0;">Aucun résultat.</div>';
    return;
  }
  list.innerHTML = filtered.slice(0, 40).map(p => {
    const loaded = !!_sharedFicheIds.find(x => x.id === p.id);
    const label  = p.project_name || p.client || ('Fiche #' + p.id);
    const sub    = [p.client, p.sector].filter(Boolean).join(' · ');
    return `<div class="cf-db-row${loaded ? ' cf-db-loaded' : ''}"
                 style="display:flex;align-items:center;gap:0.5rem;padding:0.38rem 0.6rem;
                        border:1px solid rgba(26,24,20,0.07);border-radius:0.4rem;font-size:0.68rem;
                        ${loaded ? 'opacity:0.45;' : 'cursor:pointer;'}"
                 data-pid="${p.id}"
                 data-pname="${label.replace(/"/g, '"')}">
      <i class="fa-solid fa-database" style="color:#1565C0;font-size:0.62rem;flex-shrink:0;"></i>
      <div style="flex:1;min-width:0;overflow:hidden;">
        <div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${label}</div>
        <div style="color:rgba(26,24,20,0.4);font-size:0.6rem;">${sub}</div>
      </div>
      ${loaded
        ? '<i class="fa-solid fa-check" style="color:#16a34a;flex-shrink:0;font-size:0.6rem;"></i>'
        : '<i class="fa-solid fa-plus"  style="color:rgba(26,24,20,0.28);flex-shrink:0;font-size:0.6rem;"></i>'}
    </div>`;
  }).join('');

  list.querySelectorAll('.cf-db-row:not(.cf-db-loaded)').forEach(row => {
    row.addEventListener('click', () => {
      cfAddDbFiche(parseInt(row.dataset.pid, 10), row.dataset.pname);
    });
  });
};

window.cfAddDbFiche = function(id, name) {
  if (!_sharedFicheIds.find(x => x.id === id)) {
    _sharedFicheIds.push({ id, name });
    _refreshSharedUI();
    cfFilterDB(document.getElementById('cf-db-search')?.value || '');
  }
};

/* ════════════════════════════════════════════════════════════
   GENERER PAGE: Synthese drop zone
════════════════════════════════════════════════════════════ */
(function initGenererZone() {
  // Synthese xlsx zone
  const zone = document.getElementById('s1-zone-synthese');
  const inp  = document.createElement('input');
  inp.type = 'file'; inp.accept = '.xlsx'; inp.style.display = 'none';
  document.body.appendChild(inp);
  window._syntheseFile = null;

  function onSyntheseFile(file) {
    if (!file || !file.name.endsWith('.xlsx')) return;
    window._syntheseFile = file;
    if (zone) {
      zone.classList.add('ok');
      const fl = document.getElementById('s1-synthese-list');
      if (fl) fl.innerHTML = `<div style="font-size:0.62rem;color:#2E7D32;margin-top:4px;">${file.name}</div>`;
    }
    _checkS1Ready();
  }
  if (zone) {
    zone.addEventListener('click', () => inp.click());
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag'));
    zone.addEventListener('drop', e => { e.preventDefault(); zone.classList.remove('drag'); onSyntheseFile(e.dataTransfer.files[0]); });
  }
  inp.addEventListener('change', () => { onSyntheseFile(inp.files[0]); inp.value = ''; });
})();

async function processSynthese() {
  const btn = document.getElementById('s1-btn-process');
  if (!window._syntheseFile) { toast('Uploadez un template synthèse (.xlsx)', 'error'); return; }
  if (!_sharedFiches.length && !_sharedFicheIds.length) { toast('Chargez au moins une fiche BIA à l\'étape 01', 'error'); return; }

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-sm"></span> Traitement…';

  const fd = new FormData();
  fd.append('synthese', window._syntheseFile);

  // Append file-based fiches
  _sharedFiches.forEach(f => fd.append('fiches', f));
  // Append DB-based fiche IDs
  if (_sharedFicheIds.length) fd.append('project_ids', JSON.stringify(_sharedFicheIds.map(x => x.id)));

  // LLM options
  const llmEnabled = document.getElementById('s1-llm-toggle')?.checked;
  fd.append('use_llm', llmEnabled ? '1' : '0');
  if (llmEnabled) fd.append('llm_model', document.getElementById('s1-llm-model')?.value || 'qwen2.5:3b');

  try {
    // Step dots UI
    const dot1 = document.getElementById('s1-dot1');
    const dot2 = document.getElementById('s1-dot2');
    const dot3 = document.getElementById('s1-dot3');
    if (dot1) dot1.classList.add('done'); if (dot2) { dot2.classList.remove('active'); setTimeout(()=>dot2.classList.add('active'),200); }

    const r = await fetch('/api/fill-synthese', { method: 'POST', body: fd });
    if (!r.ok) {
      const j = await r.json().catch(() => ({ detail: r.statusText }));
      throw new Error(Array.isArray(j.detail) ? j.detail.join('\n') : (j.detail || r.statusText));
    }
    if (dot2) dot2.classList.add('done'); if (dot3) { dot3.classList.remove('active'); setTimeout(()=>dot3.classList.add('active'),200); }

    const blob = await r.blob();
    const cd   = r.headers.get('Content-Disposition') || '';
    const m    = cd.match(/filename="?([^";]+)"?/);
    const fname = m ? m[1] : 'synthese_BIA.xlsx';
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a'); a.href = url; a.download = fname; a.click(); URL.revokeObjectURL(url);
    if (dot3) dot3.classList.add('done');
    toast(`Synthèse générée : ${fname}`, 'ok');
  } catch (e) {
    toast(e.message || 'Erreur serveur', 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="fa-solid fa-bolt-lightning"></i> Générer la synthèse';
    _checkS1Ready();
  }
}

function toggleLLM() {
  const on = document.getElementById('s1-llm-toggle')?.checked;
  const opts = document.getElementById('s1-llm-options');
  if (opts) opts.classList.toggle('hidden', !on);
}

/* ════════════════════════════════════════════════════════════
   SYNC PAGE: Check DMIAs
════════════════════════════════════════════════════════════ */
let _syncSyntheseFile = null;
let _syncMismatches   = [];
let _syncResolved     = 0;

(function initSyncZones() {
  // Synthese xlsx zone
  const zs  = document.getElementById('sync-zone-synthese');
  const inp = document.createElement('input');
  inp.type = 'file'; inp.accept = '.xlsx'; inp.style.display = 'none';
  document.body.appendChild(inp);

  function onSyntheseSync(file) {
    if (!file || !file.name.endsWith('.xlsx')) return;
    _syncSyntheseFile = file;
    if (zs) {
      zs.classList.add('ok');
      const fl = zs.querySelector('.file-list');
      if (fl) fl.innerHTML = `<div style="font-size:0.62rem;color:#2E7D32;margin-top:4px;">${file.name}</div>`;
    }
    _checkSyncReady();
  }
  if (zs) {
    zs.addEventListener('click', () => inp.click());
    zs.addEventListener('dragover', e => { e.preventDefault(); zs.classList.add('drag'); });
    zs.addEventListener('dragleave', () => zs.classList.remove('drag'));
    zs.addEventListener('drop', e => { e.preventDefault(); zs.classList.remove('drag'); onSyntheseSync(e.dataTransfer.files[0]); });
  }
  inp.addEventListener('change', () => { onSyntheseSync(inp.files[0]); inp.value = ''; });

  // Fiches zone
  const zf   = document.getElementById('sync-zone-fiches');
  const inp2 = document.createElement('input');
  inp2.type = 'file'; inp2.multiple = true; inp2.accept = '.docx'; inp2.style.display = 'none';
  document.body.appendChild(inp2);
  window._syncExtraFiches = [];

  function onFichesSync(files) {
    Array.from(files).filter(f => f.name.endsWith('.docx')).forEach(f => {
      if (!window._syncExtraFiches.find(x => x.name === f.name)) window._syncExtraFiches.push(f);
    });
    if (zf) {
      zf.classList.add('ok');
      const fl = zf.querySelector('.file-list');
      if (fl) fl.innerHTML = window._syncExtraFiches.map(f =>
        `<div style="font-size:0.62rem;color:#2E7D32;">${f.name}</div>`).join('');
    }
    _checkSyncReady();
  }
  if (zf) {
    zf.addEventListener('click', () => inp2.click());
    zf.addEventListener('dragover', e => { e.preventDefault(); zf.classList.add('drag'); });
    zf.addEventListener('dragleave', () => zf.classList.remove('drag'));
    zf.addEventListener('drop', e => { e.preventDefault(); zf.classList.remove('drag'); onFichesSync(e.dataTransfer.files); });
  }
  inp2.addEventListener('change', () => { onFichesSync(inp2.files); inp2.value = ''; });
})();

function _checkSyncReady() {
  const btn = document.getElementById('sync-btn');
  if (btn) btn.disabled = !_syncSyntheseFile;
}

window.runSyncCheck = async function() {
  if (!_syncSyntheseFile) return;
  document.getElementById('sync-results').style.display    = 'none';
  document.getElementById('sync-processing').style.display = '';

  const fd = new FormData();
  fd.append('synthese', _syncSyntheseFile);

  // Add extra uploaded fiches
  (window._syncExtraFiches || []).forEach(f => fd.append('fiches', f));
  // Add shared fiches from step 1
  _sharedFiches.forEach(f => fd.append('fiches', f));
  if (_sharedFicheIds.length) fd.append('project_ids', JSON.stringify(_sharedFicheIds.map(x => x.id)));

  try {
    const r = await fetch('/api/sync-check', { method: 'POST', body: fd });
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
  const active = _syncMismatches.filter(m => !m.resolved);
  if (badge)  badge.textContent  = active.length;
  if (resCnt) resCnt.textContent = _syncResolved + ' corrigée(s) et retirée(s).';
  if (empty)  empty.style.display  = active.length === 0 ? '' : 'none';
  if (!list) return;
  list.innerHTML = _syncMismatches.map((m, i) => `
    <div class="sync-mismatch-item ${m.resolved ? 'resolved' : ''}" onclick="openSyncFiche(${i})">
      <div class="smi-entity" title="${m.entity}">${m.entity || m.fiche_source || '—'}</div>
      <div class="smi-act">${m.activity || '—'}</div>
      <span class="smi-badge smi-synth" title="DMIA dans la synthèse">${m.dmia_synthese || '—'}</span>
      <i class="fa-solid fa-arrow-right smi-arrow"></i>
      <span class="smi-badge smi-fiche"  title="DMIA dans la fiche">${m.dmia_fiche || '—'}</span>
      <i class="fa-solid fa-arrow-up-right-from-square smi-open"></i>
    </div>`).join('');
}

window.openSyncFiche = function(idx) {
  const m = _syncMismatches[idx];
  if (!m || m.resolved) return;
  const pid = m.project_id;
  let url = '/fiche-editor';
  if (pid) url += '?project_id=' + pid;
  sessionStorage.setItem('sync_hint', JSON.stringify({
    activity: m.activity,
    dmia_target: m.dmia_synthese,
    dmia_current: m.dmia_fiche,
  }));
  window.open(url, '_blank');
  _syncMismatches[idx].resolved = true;
  _syncResolved++;
  renderSyncResults();
};

/* ════════════════════════════════════════════════════════════
   ORGANIGRAMME PAGE
════════════════════════════════════════════════════════════ */
(function initOrgZone() {
  const zone = document.getElementById('org-zone-synthese');
  const inp  = document.createElement('input');
  inp.type = 'file'; inp.accept = '.xlsx'; inp.style.display = 'none';
  document.body.appendChild(inp);
  window._orgSyntheseFile = null;

  function onOrgFile(file) {
    if (!file || !file.name.endsWith('.xlsx')) return;
    window._orgSyntheseFile = file;
    if (zone) {
      zone.classList.add('ok');
      const fl = document.getElementById('org-file-list');
      if (fl) fl.innerHTML = `<div style="font-size:0.62rem;color:#2E7D32;margin-top:4px;">${file.name}</div>`;
    }
    const btn = document.getElementById('org-btn-generate');
    if (btn) { btn.disabled = false; btn.style.cursor = 'pointer'; }
  }
  if (zone) {
    zone.addEventListener('click', () => inp.click());
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag'));
    zone.addEventListener('drop', e => { e.preventDefault(); zone.classList.remove('drag'); onOrgFile(e.dataTransfer.files[0]); });
  }
  inp.addEventListener('change', () => { onOrgFile(inp.files[0]); inp.value = ''; });
})();

async function generateOrgchart() {
  if (!window._orgSyntheseFile) return;
  const btn = document.getElementById('org-btn-generate');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-sm"></span> Génération…';
  document.getElementById('org-tree-wrap').style.display = '';
  document.getElementById('org-processing').style.display = 'flex';

  const fd = new FormData();
  fd.append('synthese', window._orgSyntheseFile);

  try {
    const r = await fetch('/api/orgchart-data', { method: 'POST', body: fd });
    if (!r.ok) throw new Error((await r.json().catch(() => ({detail:r.statusText}))).detail);
    const data = await r.json();
    renderOrgTree(data);
    document.getElementById('org-btn-export').disabled = false;
    document.getElementById('org-btn-export').style.cursor = 'pointer';
    toast('Organigramme généré', 'ok');
  } catch (e) {
    toast(e.message || 'Erreur génération', 'error');
  } finally {
    document.getElementById('org-processing').style.display = 'none';
    btn.disabled = false;
    btn.innerHTML = '<i class="fa-solid fa-sitemap"></i> Régénérer';
  }
}

function renderOrgTree(data) {
  const svg = d3.select('#org-svg');
  svg.selectAll('*').remove();
  if (!data || !data.name) { svg.append('text').attr('x',20).attr('y',40).text('Aucune donnée'); return; }

  const W = document.getElementById('org-svg').clientWidth || 900;
  const root = d3.hierarchy(data);
  const treeLayout = d3.tree().size([W - 80, (root.height + 1) * 90]);
  treeLayout(root);

  const g = svg.append('g').attr('transform','translate(40,40)');
  svg.attr('height', (root.height + 1) * 90 + 80);

  // Links
  g.selectAll('.link').data(root.links()).enter().append('path')
    .attr('class','link')
    .attr('fill','none').attr('stroke','rgba(26,24,20,0.15)').attr('stroke-width',1.5)
    .attr('d', d3.linkVertical().x(d=>d.x).y(d=>d.y));

  // Nodes
  const node = g.selectAll('.node').data(root.descendants()).enter().append('g')
    .attr('transform', d => `translate(${d.x},${d.y})`);

  node.append('rect')
    .attr('x',-60).attr('y',-18).attr('width',120).attr('height',36)
    .attr('rx',7).attr('ry',7)
    .attr('fill', d => d.depth===0 ? '#F2485E' : d.depth===1 ? '#1565C0' : '#FAF8F4')
    .attr('stroke', d => d.depth===0 ? '#d63448' : d.depth===1 ? '#1040A0' : 'rgba(26,24,20,0.12)')
    .attr('stroke-width',1.5);

  node.append('text')
    .attr('text-anchor','middle').attr('dy','0.35em')
    .attr('font-size', d => d.depth===0 ? '11px' : '10px')
    .attr('font-weight', d => d.depth<=1 ? '700' : '500')
    .attr('fill', d => d.depth<=1 ? '#fff' : 'rgba(26,24,20,0.8)')
    .attr('font-family',"'Montserrat',sans-serif")
    .text(d => {
      const t = d.data.name || '';
      return t.length > 16 ? t.slice(0,15)+'…' : t;
    });
}

function exportOrgchart() {
  const svg = document.getElementById('org-svg');
  if (!svg) return;
  const blob = new Blob([svg.outerHTML], { type: 'image/svg+xml' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = 'organigramme.svg'; a.click();
  URL.revokeObjectURL(url);
}

/* ════════════════════════════════════════════════════════════
   INIT DEFAULT PAGE
════════════════════════════════════════════════════════════ */
(function initApp() {
  navigatePage('page-charger-fiches', 'Synthèse BIA', 'Charger les fiches BIA', 'child-charger');
})();
</script>
</body>
</html>
"""

new_content = GOOD_HTML + TAIL

with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', 'w', encoding='utf-8') as f:
    f.write(new_content)

print(f'Rebuilt! Lines: {new_content.count(chr(10))}, Chars: {len(new_content)}')
# Verify key elements
for kw in ['wf-stepper', 'tdb-card', 'navigatePage', 'toggleGroup', '_sharedFiches', 'cfLoadDB', 'cfFilterDB', 'runSyncCheck', 'renderOrgTree', 'initApp']:
    print(f'  {"OK" if kw in new_content else "MISSING"} {kw}')
