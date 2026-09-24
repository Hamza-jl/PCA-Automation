
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
    'page-generer-fiches':   'wfs-0',
    'page-charger-fiches':   'wfs-1',
    'page-generer-synthese': 'wfs-2',
    'page-sync':             'wfs-3',
    'page-organigramme':     'wfs-4',
  };
  const stepOrder = ['wfs-0','wfs-1','wfs-2','wfs-3','wfs-4'];
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
   GÉNÉRER FICHES PAGE (step 00)
════════════════════════════════════════════════════════════ */
window._gfState = {
  recFile:   null,   // recensement xlsx File
  tmplFile:  null,   // template docx File
  zipBlob:   null,   // ZIP blob from server
  fileNames: [],     // docx names inside the ZIP
};

(function initGfZones() {
  // ── Recensement drop zone ──────────────────────────────
  const recZone = document.getElementById('gf-zone-rec');
  const recInp  = document.createElement('input');
  recInp.type = 'file'; recInp.accept = '.xlsx'; recInp.style.display = 'none';
  document.body.appendChild(recInp);
  if (recZone) {
    recZone.addEventListener('click', () => recInp.click());
    recZone.addEventListener('dragover',  e => { e.preventDefault(); recZone.classList.add('drag'); });
    recZone.addEventListener('dragleave', () => recZone.classList.remove('drag'));
    recZone.addEventListener('drop', e => {
      e.preventDefault(); recZone.classList.remove('drag');
      const f = e.dataTransfer.files[0];
      if (f && f.name.toLowerCase().endsWith('.xlsx')) _gfSetRec(f);
    });
  }
  recInp.addEventListener('change', () => { if (recInp.files[0]) _gfSetRec(recInp.files[0]); recInp.value = ''; });

  // ── Template drop zone ─────────────────────────────────
  const tmplZone = document.getElementById('gf-zone-tmpl');
  const tmplInp  = document.createElement('input');
  tmplInp.type = 'file'; tmplInp.accept = '.docx'; tmplInp.style.display = 'none';
  document.body.appendChild(tmplInp);
  if (tmplZone) {
    tmplZone.addEventListener('click', () => tmplInp.click());
    tmplZone.addEventListener('dragover',  e => { e.preventDefault(); tmplZone.classList.add('drag'); });
    tmplZone.addEventListener('dragleave', () => tmplZone.classList.remove('drag'));
    tmplZone.addEventListener('drop', e => {
      e.preventDefault(); tmplZone.classList.remove('drag');
      const f = e.dataTransfer.files[0];
      if (f && f.name.toLowerCase().endsWith('.docx')) _gfSetTmpl(f);
    });
  }
  tmplInp.addEventListener('change', () => { if (tmplInp.files[0]) _gfSetTmpl(tmplInp.files[0]); tmplInp.value = ''; });
})();

function _gfSetRec(file) {
  window._gfState.recFile = file;
  const list = document.getElementById('gf-rec-list');
  const zone = document.getElementById('gf-zone-rec');
  if (list) list.innerHTML = `<div style="font-size:0.62rem;color:rgba(26,24,20,0.7);padding:2px 0;">
    <i class="fa-solid fa-check" style="color:#16a34a;margin-right:0.3rem;"></i>${file.name}</div>`;
  if (zone) zone.classList.add('ok');
  _gfCheckReady();
}

function _gfSetTmpl(file) {
  window._gfState.tmplFile = file;
  const list = document.getElementById('gf-tmpl-list');
  const zone = document.getElementById('gf-zone-tmpl');
  if (list) list.innerHTML = `<div style="font-size:0.62rem;color:rgba(26,24,20,0.7);padding:2px 0;">
    <i class="fa-solid fa-check" style="color:#1565C0;margin-right:0.3rem;"></i>${file.name}</div>`;
  if (zone) zone.classList.add('ok');
  _gfCheckReady();
}

function _gfCheckReady() {
  const btn = document.getElementById('gf-btn-generate');
  if (btn) btn.disabled = !(window._gfState.recFile && window._gfState.tmplFile);
}

async function gfGenerate() {
  const s = window._gfState;
  if (!s.recFile || !s.tmplFile) return;

  const panels = {
    upload:   document.getElementById('gf-upload-panel'),
    proc:     document.getElementById('gf-processing'),
    success:  document.getElementById('gf-success'),
    error:    document.getElementById('gf-error'),
  };
  panels.upload.style.display  = 'none';
  panels.proc.style.display    = '';
  panels.success.style.display = 'none';
  panels.error.style.display   = 'none';

  const fd = new FormData();
  fd.append('recensement', s.recFile);
  fd.append('template',    s.tmplFile);
  fd.append('version',     document.getElementById('gf-version')?.value || '2.0');
  const cn = (document.getElementById('gf-client-name')?.value || '').trim();
  const ok = (document.getElementById('gf-openai-key')?.value  || '').trim();
  if (cn) fd.append('client_name', cn);
  if (ok) fd.append('openai_key',  ok);

  try {
    const resp = await fetch('/api/generate-fiches', { method: 'POST', body: fd });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: 'Erreur serveur' }));
      throw new Error(typeof err.detail === 'string' ? err.detail : JSON.stringify(err.detail));
    }

    const zipBlob      = await resp.blob();
    const count        = parseInt(resp.headers.get('X-Generated-Count') || '0', 10);
    const errHdr       = resp.headers.get('X-Errors') || '';
    const detectedName = resp.headers.get('X-Client-Name') || '';

    s.zipBlob   = zipBlob;
    s.fileNames = [];

    // Read file names from ZIP (JSZip)
    if (typeof JSZip !== 'undefined') {
      try {
        const zip = await JSZip.loadAsync(zipBlob);
        zip.forEach(rel => { if (rel.toLowerCase().endsWith('.docx')) s.fileNames.push(rel); });
      } catch (_) {}
    }

    panels.proc.style.display    = 'none';
    panels.success.style.display = '';

    document.getElementById('gf-count').textContent = count || s.fileNames.length || '?';

    const detEl  = document.getElementById('gf-client-detected');
    const nameEl = document.getElementById('gf-client-name-detected');
    if (detectedName && detEl && nameEl) {
      nameEl.textContent = detectedName;
      detEl.style.display = '';
      const dbCl = document.getElementById('gf-db-client');
      if (dbCl && !dbCl.value) dbCl.value = detectedName;
    }

    const filesList = document.getElementById('gf-files-list');
    if (filesList) {
      filesList.innerHTML = s.fileNames.length
        ? s.fileNames.map(n => `
            <div style="display:flex;align-items:center;gap:0.5rem;padding:0.35rem 0.6rem;
                        background:rgba(21,101,192,0.03);border:1px solid rgba(21,101,192,0.1);
                        border-radius:0.4rem;font-size:0.68rem;">
              <i class="fa-regular fa-file-word" style="color:#1565C0;font-size:0.7rem;flex-shrink:0;"></i>
              <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${n}</span>
            </div>`).join('')
        : `<div style="font-size:0.68rem;color:rgba(26,24,20,0.45);padding:0.5rem;">${count} fichier(s) (liste non disponible)</div>`;
    }

    if (errHdr) toast('Avertissements : ' + errHdr, 'warn');

  } catch (e) {
    panels.proc.style.display  = 'none';
    panels.error.style.display = '';
    const el = document.getElementById('gf-error-text');
    if (el) el.textContent = e.message || 'Erreur inconnue';
  }
}

function gfDownloadZip() {
  const s = window._gfState;
  if (!s.zipBlob) return;
  const url = URL.createObjectURL(s.zipBlob);
  const a   = document.createElement('a');
  a.href = url; a.download = 'Fiches_BIA_generated.zip'; a.click();
  URL.revokeObjectURL(url);
}

async function gfSaveDB() {
  const s      = window._gfState;
  const btn    = document.getElementById('gf-btn-save');
  const result = document.getElementById('gf-save-result');
  if (!s.zipBlob) return;

  const sector = document.getElementById('gf-db-sector')?.value || 'autres';
  const client = (document.getElementById('gf-db-client')?.value || '').trim() || 'Client';

  btn.disabled = true;
  btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Sauvegarde...';
  if (result) result.style.display = 'none';

  const fd = new FormData();
  fd.append('zip_file', s.zipBlob, 'fiches.zip');
  fd.append('sector',   sector);
  fd.append('client',   client);

  try {
    const resp = await fetch('/api/save-fiches-zip', { method: 'POST', body: fd });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || 'Erreur serveur');

    if (result) {
      result.style.cssText = 'display:block;background:#D1FAE5;color:#065F46;border:1px solid #10B981;border-radius:0.4rem;padding:0.5rem 0.75rem;font-size:0.68rem;margin-top:0.6rem;';
      result.innerHTML = `<i class="fa-solid fa-check" style="margin-right:0.4rem;"></i>
        ${data.total_files} fiche(s) sauvegardée(s) dans la base &mdash;
        ${data.total_activities} activité(s) extraite(s)`;
    }
    toast(data.total_files + ' fiche(s) sauvegardée(s) — ' + sector + ' / ' + client, 'ok');
  } catch (e) {
    if (result) {
      result.style.cssText = 'display:block;background:#FEE2E2;color:#B91C1C;border:1px solid #EF4444;border-radius:0.4rem;padding:0.5rem 0.75rem;font-size:0.68rem;margin-top:0.6rem;';
      result.innerHTML = '<i class="fa-solid fa-triangle-exclamation" style="margin-right:0.4rem;"></i>' + e.message;
    }
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i> Sauvegarder';
  }
}

async function gfUseInStep01() {
  const s = window._gfState;
  if (!s.zipBlob) return;

  if (typeof JSZip === 'undefined') {
    toast("JSZip non disponible - telechargez le ZIP et importez a l'etape 01", 'warn');
    navigatePage('page-charger-fiches', 'Synthèse BIA', 'Charger les fiches BIA', 'child-charger');
    return;
  }

  try {
    const zip      = await JSZip.loadAsync(s.zipBlob);
    const files    = [];
    const promises = [];
    zip.forEach((relPath, entry) => {
      if (relPath.toLowerCase().endsWith('.docx') && !entry.dir) {
        promises.push(
          entry.async('blob').then(blob => {
            const fname = relPath.split('/').pop();
            files.push(new File([blob], fname, {
              type: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            }));
          })
        );
      }
    });
    await Promise.all(promises);

    if (files.length) {
      addFilesToShared(files);
      toast(files.length + " fiche(s) chargee(s) a l'etape 01", 'ok');
      navigatePage('page-charger-fiches', 'Synthèse BIA', 'Charger les fiches BIA', 'child-charger');
    } else {
      toast('Aucun .docx trouve dans le ZIP', 'warn');
    }
  } catch (e) {
    toast('Erreur extraction ZIP : ' + e.message, 'error');
  }
}

function gfReset() {
  const s = window._gfState;
  s.recFile = null; s.tmplFile = null; s.zipBlob = null; s.fileNames = [];
  document.getElementById('gf-upload-panel').style.display  = '';
  document.getElementById('gf-processing').style.display    = 'none';
  document.getElementById('gf-success').style.display       = 'none';
  document.getElementById('gf-error').style.display         = 'none';
  ['gf-zone-rec','gf-zone-tmpl'].forEach(id => {
    const z = document.getElementById(id); if (z) z.classList.remove('ok','drag');
  });
  ['gf-rec-list','gf-tmpl-list'].forEach(id => {
    const el = document.getElementById(id); if (el) el.innerHTML = '';
  });
  const btn = document.getElementById('gf-btn-generate');
  if (btn) btn.disabled = true;
}

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
  if (genTxt) genTxt.textContent   = total + " fiche(s) pré-chargée(s) depuis l'étape précédente.";

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
  if (!_sharedFiches.length && !_sharedFicheIds.length) { toast("Chargez au moins une fiche BIA à l'étape 01", 'error'); return; }

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
  const btn  = document.getElementById('org-btn-generate');
  const wrap = document.getElementById('org-tree-wrap');
  const proc = document.getElementById('org-processing');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-sm"></span> Génération…';
  wrap.style.display = 'flex';
  wrap.style.flexDirection = 'column';
  if (proc) proc.style.display = 'flex';

  const fd = new FormData();
  fd.append('synthese', window._orgSyntheseFile);

  try {
    const r = await fetch('/api/orgchart', { method: 'POST', body: fd });
    if (!r.ok) throw new Error((await r.json().catch(() => ({detail:r.statusText}))).detail);
    const data = await r.json();
    if (proc) proc.style.display = 'none';
    renderOrgTree(data);
    const exportBtn = document.getElementById('org-btn-export');
    if (exportBtn) { exportBtn.disabled = false; exportBtn.style.cssText += ';cursor:pointer;color:rgba(26,24,20,0.7);'; }
    const depth = data.depth || 3;
    const appCount = ((data.applications || {}).children || []).length;
    toast(`Organigramme généré — ${depth} niveaux${appCount ? ', ' + appCount + ' entités IT' : ''}`, 'ok');
  } catch (e) {
    if (proc) proc.style.display = 'none';
    toast(e.message || 'Erreur génération', 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '<i class="fa-solid fa-sitemap"></i> Régénérer';
  }
}

/* ════════════════════════════════════════════════════════════
   ORGANIGRAMME — D3 COLLAPSIBLE TREE
════════════════════════════════════════════════════════════ */
window._orgState = {
  full:       null,   // {activities, applications, depth}
  mode:       'activities',
  showAll:    false,
  d3Root:     null,   // d3.hierarchy root
  svg:        null,   // d3 svg selection
  gMain:      null,   // main g (receives zoom transform)
  zoom:       null,   // d3.zoom behavior
  nodeIdSeq:  0,
};

const _ORG_COLORS = {
  rouge:  { fill: '#FEE2E2', stroke: '#EF4444', text: '#B91C1C', badge: '#EF4444' },
  orange: { fill: '#FEF3C7', stroke: '#F59E0B', text: '#92400E', badge: '#F59E0B' },
  vert:   { fill: '#D1FAE5', stroke: '#10B981', text: '#065F46', badge: '#10B981' },
  none:   { fill: '#F8F9FA', stroke: '#CBD5E1', text: '#374151', badge: '#9CA3AF' },
};

// ── Node geometry ──────────────────────────────────────────
const _ORG_NW = 188, _ORG_NH = 68;

// ── Text wrap helper (SVG tspan, no foreignObject) ─────────
function _orgWrapText(sel, text, maxPx, maxLines, lineH) {
  sel.text('');
  if (!text) return;
  const chPerPx = 6.0;   // approx px per char at 10.5px bold
  const cap = Math.floor(maxPx / chPerPx);
  const words = String(text).split(/\s+/);
  const lines = [];
  let cur = '';
  for (const w of words) {
    if (lines.length >= maxLines) break;
    const test = cur ? cur + ' ' + w : w;
    if (test.length <= cap) {
      cur = test;
    } else {
      if (cur) lines.push(cur);
      if (lines.length >= maxLines) break;
      cur = (w.length > cap) ? w.slice(0, cap - 1) + '…' : w;
    }
  }
  if (cur && lines.length < maxLines) lines.push(cur);
  if (lines[lines.length - 1] && lines[lines.length - 1].length > cap) {
    lines[lines.length - 1] = lines[lines.length - 1].slice(0, cap - 1) + '…';
  }
  const totalH = (lines.length - 1) * lineH;
  lines.forEach((ln, i) => {
    sel.append('tspan').attr('x', 0).attr('dy', i === 0 ? -totalH / 2 : lineH).text(ln);
  });
}

// ── Entry point ────────────────────────────────────────────
function renderOrgTree(data) {
  if (data && data.activities) {
    window._orgState.full = data;
  } else {
    window._orgState.full = { activities: data, applications: null, depth: 3 };
  }
  window._orgState.mode    = 'activities';
  window._orgState.showAll = false;
  _orgBuildUI();
  _orgInitD3Tree();
}

function _orgBuildUI() {
  const wrap    = document.getElementById('org-tree-wrap');
  const hasApps = !!(window._orgState.full && window._orgState.full.applications &&
    (window._orgState.full.applications.children || []).length);

  wrap.style.cssText += ';display:flex;flex-direction:column;min-height:620px;';
  wrap.innerHTML = `
    <div id="org-toolbar" style="display:flex;align-items:center;gap:0.55rem;padding:0.65rem 1rem;
         border-bottom:1px solid rgba(26,24,20,0.07);background:#FAF8F4;flex-shrink:0;flex-wrap:wrap;">
      <div style="display:flex;border:1px solid rgba(26,24,20,0.12);border-radius:0.4rem;overflow:hidden;">
        <button id="org-tab-act" onclick="_orgSetMode('activities')"
          style="padding:0.3rem 0.8rem;font-size:0.67rem;font-weight:600;border:none;cursor:pointer;
                 font-family:inherit;letter-spacing:0.04em;transition:all 0.15s;">
          <i class="fa-solid fa-person-digging" style="margin-right:0.3rem;"></i>Activités
        </button>
        <button id="org-tab-app" onclick="_orgSetMode('applications')"
          style="padding:0.3rem 0.8rem;font-size:0.67rem;font-weight:600;border:none;cursor:pointer;
                 font-family:inherit;letter-spacing:0.04em;transition:all 0.15s;
                 ${!hasApps ? 'opacity:0.35;pointer-events:none;' : ''}">
          <i class="fa-solid fa-desktop" style="margin-right:0.3rem;"></i>Applications IT
        </button>
      </div>
      <button id="org-show-all" onclick="_orgToggleShowAll()"
        style="padding:0.3rem 0.8rem;font-size:0.67rem;font-weight:600;border:1px solid rgba(26,24,20,0.12);
               border-radius:0.4rem;background:transparent;cursor:pointer;font-family:inherit;">
        <i class="fa-solid fa-table-list" style="margin-right:0.3rem;"></i>Tous les DMIA
      </button>
      <button onclick="_orgResetZoom()"
        style="padding:0.3rem 0.55rem;font-size:0.67rem;border:1px solid rgba(26,24,20,0.12);
               border-radius:0.4rem;background:transparent;cursor:pointer;" title="Recentrer">
        <i class="fa-solid fa-arrows-to-dot"></i>
      </button>
      <div style="flex:1;"></div>
      <div style="display:flex;gap:0.45rem;align-items:center;font-size:0.61rem;">
        <span style="background:#FEE2E2;border:1px solid #EF4444;border-radius:3px;padding:2px 6px;
                     color:#B91C1C;font-weight:700;">Rouge &#8804; J+2</span>
        <span style="background:#FEF3C7;border:1px solid #F59E0B;border-radius:3px;padding:2px 6px;
                     color:#92400E;font-weight:700;">Orange J+3&#8211;J+5</span>
        <span style="background:#D1FAE5;border:1px solid #10B981;border-radius:3px;padding:2px 6px;
                     color:#065F46;font-weight:700;">Vert &gt; J+5</span>
      </div>
      <button onclick="_orgFullscreen()"
        style="padding:0.3rem 0.55rem;font-size:0.67rem;border:1px solid rgba(26,24,20,0.12);
               border-radius:0.4rem;background:transparent;cursor:pointer;">
        <i class="fa-solid fa-expand"></i>
      </button>
    </div>
    <div style="padding:0.25rem 1rem;font-size:0.6rem;color:rgba(26,24,20,0.38);background:#FAF8F4;
                flex-shrink:0;border-bottom:1px solid rgba(26,24,20,0.05);">
      <i class="fa-solid fa-hand-pointer" style="margin-right:0.3rem;"></i>
      Cliquez sur un noeud pour le d&#233;velopper / r&#233;duire &nbsp;&#183;&nbsp;
      Cliquez sur un noeud feuille pour voir ses activit&#233;s &nbsp;&#183;&nbsp; Molette pour zoomer
    </div>
    <div id="org-svg-wrap" style="flex:1;overflow:hidden;min-height:480px;position:relative;
         background:#FAFAF8;cursor:grab;"></div>
    <div id="org-all-dmia-wrap" style="display:none;flex:1;overflow:auto;padding:1rem 1.5rem;
         background:#FAF8F4;"></div>
    <div id="org-dmia-panel" style="display:none;border-top:2px solid rgba(26,24,20,0.06);
         background:#fff;padding:0.9rem 1.4rem;max-height:270px;overflow-y:auto;flex-shrink:0;"></div>
  `;
  _orgUpdateTabStyles();
}

function _orgUpdateTabStyles() {
  const s   = window._orgState;
  const act = document.getElementById('org-tab-act');
  const app = document.getElementById('org-tab-app');
  const all = document.getElementById('org-show-all');
  const svg = document.getElementById('org-svg-wrap');
  const tbl = document.getElementById('org-all-dmia-wrap');
  if (act) { act.style.background = s.mode === 'activities'    ? '#1A1814' : 'transparent'; act.style.color = s.mode === 'activities'    ? '#fff' : 'rgba(26,24,20,0.6)'; }
  if (app) { app.style.background = s.mode === 'applications'  ? '#1A1814' : 'transparent'; app.style.color = s.mode === 'applications'  ? '#fff' : 'rgba(26,24,20,0.6)'; }
  if (all) { all.style.background = s.showAll                  ? '#1A1814' : 'transparent'; all.style.color = s.showAll                  ? '#fff' : 'rgba(26,24,20,0.6)'; }
  if (svg) svg.style.display = s.showAll ? 'none' : '';
  if (tbl) tbl.style.display = s.showAll ? ''     : 'none';
}

function _orgSetMode(mode) {
  const s = window._orgState;
  if (s.mode === mode) return;
  s.mode    = mode;
  s.showAll = false;
  const panel = document.getElementById('org-dmia-panel');
  if (panel) panel.style.display = 'none';
  _orgUpdateTabStyles();
  _orgInitD3Tree();
}

function _orgToggleShowAll() {
  const s = window._orgState;
  s.showAll = !s.showAll;
  _orgUpdateTabStyles();
  const panel = document.getElementById('org-dmia-panel');
  if (panel) panel.style.display = 'none';
  if (s.showAll) _orgRenderAllDmia();
}

// ── D3 tree init ───────────────────────────────────────────
function _orgInitD3Tree() {
  const s = window._orgState;
  const raw = s.mode === 'activities'
    ? s.full.activities
    : (s.full.applications || s.full.activities);
  if (!raw) return;

  s.nodeIdSeq = 0;
  const root = d3.hierarchy(raw, d => (d.children && d.children.length) ? d.children : null);
  root.descendants().forEach(d => { d.id = ++s.nodeIdSeq; d.x0 = 0; d.y0 = 0; });

  // Collapse everything below depth 0
  // (root = depth 0 always visible; depth 1 = shown but collapsed by default)
  root.descendants().forEach(d => {
    if (d.depth >= 1 && d.children) {
      d._children = d.children;
      d.children  = null;
    }
  });
  // Level-1 nodes are already visible as children of root.
  // They start collapsed — user clicks them to expand level 2.

  s.d3Root = root;
  _orgSetupSVG();
  _orgD3Update(root, true);
}

function _orgSetupSVG() {
  const s   = window._orgState;
  const ctr = document.getElementById('org-svg-wrap');
  if (!ctr) return;
  ctr.innerHTML = '';

  const svg   = d3.select(ctr).append('svg').attr('width', '100%').attr('height', '100%');
  const gMain = svg.append('g').attr('class', 'org-g-main');
  gMain.append('g').attr('class', 'org-links');
  gMain.append('g').attr('class', 'org-nodes');

  const zoom = d3.zoom()
    .scaleExtent([0.12, 3])
    .on('zoom', ev => { gMain.attr('transform', ev.transform); });

  svg.call(zoom).on('dblclick.zoom', null);
  ctr.addEventListener('mousedown', () => { ctr.style.cursor = 'grabbing'; });
  ctr.addEventListener('mouseup',   () => { ctr.style.cursor = 'grab'; });

  s.svg   = svg;
  s.gMain = gMain;
  s.zoom  = zoom;
}

// Orthogonal elbow: bottom of source → top of target
function _orgElbow(src, tgt) {
  const sy  = src.y + _ORG_NH / 2;
  const ty  = tgt.y - _ORG_NH / 2;
  const mid = (sy + ty) / 2;
  return `M${src.x},${sy} V${mid} H${tgt.x} V${ty}`;
}

function _orgD3Update(source, initial) {
  const s   = window._orgState;
  if (!s.d3Root || !s.svg) return;
  const dur = initial ? 0 : 380;
  const NW  = _ORG_NW, NH = _ORG_NH;

  // Layout
  d3.tree().nodeSize([NW + 36, NH + 72]).separation((a, b) => a.parent === b.parent ? 1 : 1.25)(s.d3Root);

  const nodes = s.d3Root.descendants();
  const links = s.d3Root.links();
  const sx0   = source.x0 || 0, sy0 = source.y0 || 0;

  // ─ Links ──────────────────────────────────────────────────
  const lSel = s.gMain.select('.org-links').selectAll('.org-link')
    .data(links, d => d.target.id);

  const lEnter = lSel.enter().append('path').attr('class', 'org-link')
    .attr('fill', 'none').attr('stroke-width', 1.5)
    .attr('stroke', '#CBD5E1')
    .attr('d', () => _orgElbow({ x: sx0, y: sy0 }, { x: sx0, y: sy0 }));

  lSel.merge(lEnter).transition().duration(dur)
    .attr('stroke', d => {
      const c = _ORG_COLORS[d.target.data._dmia_color];
      return c ? c.stroke + 'AA' : '#CBD5E1';
    })
    .attr('d', d => _orgElbow(d.source, d.target));

  lSel.exit().transition().duration(dur / 2)
    .attr('d', () => _orgElbow({ x: sx0, y: sy0 }, { x: sx0, y: sy0 }))
    .remove();

  // ─ Nodes ──────────────────────────────────────────────────
  const nSel = s.gMain.select('.org-nodes').selectAll('.org-node')
    .data(nodes, d => d.id);

  const nEnter = nSel.enter().append('g').attr('class', 'org-node')
    .attr('cursor', 'pointer')
    .attr('transform', `translate(${sx0},${sy0})`)
    .on('click', (ev, d) => { ev.stopPropagation(); _orgNodeClick(d); })
    .on('mouseover', function(ev, d) {
      d3.select(this).select('.node-box')
        .attr('filter', 'drop-shadow(0 4px 8px rgba(0,0,0,0.15))');
    })
    .on('mouseout', function() {
      d3.select(this).select('.node-box').attr('filter', null);
    });

  // Box shadow
  nEnter.append('rect').attr('class', 'node-shadow')
    .attr('x', -NW / 2 + 2).attr('y', -NH / 2 + 3)
    .attr('width', NW).attr('height', NH).attr('rx', 10)
    .attr('fill', 'rgba(0,0,0,0.07)');

  // Main rect
  nEnter.append('rect').attr('class', 'node-box')
    .attr('x', -NW / 2).attr('y', -NH / 2)
    .attr('width', NW).attr('height', NH).attr('rx', 10)
    .attr('fill',         d => _ORG_COLORS[d.data._dmia_color || 'none'].fill)
    .attr('stroke',       d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke)
    .attr('stroke-width', d => (d.data._dmia_color && d.data._dmia_color !== 'none') ? 2 : 1.5);

  // Top colour bar
  nEnter.append('rect').attr('class', 'node-bar')
    .attr('x', -NW / 2).attr('y', -NH / 2)
    .attr('width', NW).attr('height', 5).attr('rx', 10)
    .attr('fill',    d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke)
    .attr('opacity', d => (d.data._dmia_color && d.data._dmia_color !== 'none') ? 1 : 0);

  // Label text (multi-line via tspan)
  nEnter.append('text').attr('class', 'node-label')
    .attr('text-anchor', 'middle').attr('dominant-baseline', 'middle')
    .attr('y', d => d.data._dmia_critical ? -10 : 0)
    .attr('font-size', '10.5px').attr('font-weight', '700')
    .attr('fill', d => _ORG_COLORS[d.data._dmia_color || 'none'].text)
    .attr('pointer-events', 'none')
    .each(function(d) { _orgWrapText(d3.select(this), d.data.name || '', NW - 22, 3, 13); });

  // DMIA badge text
  nEnter.append('text').attr('class', 'node-dmia-lbl')
    .attr('text-anchor', 'middle').attr('y', NH / 2 - 11)
    .attr('font-size', '9.5px').attr('font-weight', '700')
    .attr('fill', d => _ORG_COLORS[d.data._dmia_color || 'none'].badge)
    .attr('pointer-events', 'none')
    .text(d => d.data._dmia_critical ? 'DMIA ' + d.data._dmia_critical : '');

  // Count badge (bottom-right corner)
  nEnter.append('text').attr('class', 'node-count')
    .attr('text-anchor', 'end').attr('x', NW / 2 - 6)
    .attr('y', NH / 2 - 9).attr('font-size', '8px')
    .attr('fill', 'rgba(55,65,81,0.45)').attr('pointer-events', 'none')
    .text(d => {
      const act = (d.data._activities || []).length;
      const app = (d.data._applications || []).length;
      const cnt = act || app;
      return cnt ? cnt + (act ? ' act' : ' app') : '';
    });

  // Expand / collapse toggle circle
  nEnter.append('circle').attr('class', 'node-toggle')
    .attr('cy', NH / 2).attr('r', 9)
    .attr('fill', '#fff')
    .attr('stroke', d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke)
    .attr('stroke-width', 1.5)
    .attr('opacity', d => (d.children || d._children) ? 1 : 0);

  nEnter.append('text').attr('class', 'node-toggle-txt')
    .attr('text-anchor', 'middle').attr('dominant-baseline', 'central')
    .attr('y', NH / 2).attr('font-size', '13px').attr('font-weight', '900')
    .attr('pointer-events', 'none')
    .attr('fill', d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke)
    .attr('opacity', d => (d.children || d._children) ? 1 : 0)
    .text(d => d._children ? '+' : (d.children ? '−' : ''));

  // Merge enter + existing for update
  const nAll = nSel.merge(nEnter);

  nAll.transition().duration(dur)
    .attr('transform', d => `translate(${d.x},${d.y})`);

  nAll.select('.node-box')
    .attr('fill',   d => _ORG_COLORS[d.data._dmia_color || 'none'].fill)
    .attr('stroke', d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke);

  nAll.select('.node-toggle')
    .attr('opacity', d => (d.children || d._children) ? 1 : 0)
    .attr('stroke',  d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke);

  nAll.select('.node-toggle-txt')
    .attr('opacity', d => (d.children || d._children) ? 1 : 0)
    .attr('fill',    d => _ORG_COLORS[d.data._dmia_color || 'none'].stroke)
    .text(d => d._children ? '+' : (d.children ? '−' : ''));

  nSel.exit().transition().duration(dur / 2)
    .attr('transform', `translate(${sx0},${sy0})`).attr('opacity', 0).remove();

  // Save positions for next animation
  s.d3Root.descendants().forEach(d => { d.x0 = d.x; d.y0 = d.y; });

  if (initial) setTimeout(_orgFitTree, 80);
}

function _orgFitTree() {
  const s   = window._orgState;
  if (!s.svg || !s.zoom || !s.d3Root) return;
  const ctr = document.getElementById('org-svg-wrap');
  if (!ctr) return;
  const cW  = ctr.clientWidth  || 900;
  const cH  = ctr.clientHeight || 500;
  const all = s.d3Root.descendants();
  if (!all.length) return;
  const xs   = all.map(d => d.x);
  const ys   = all.map(d => d.y);
  const minX = Math.min(...xs) - _ORG_NW / 2 - 24;
  const maxX = Math.max(...xs) + _ORG_NW / 2 + 24;
  const minY = Math.min(...ys) - _ORG_NH / 2 - 24;
  const maxY = Math.max(...ys) + _ORG_NH / 2 + 48;
  const sc   = Math.min(cW / (maxX - minX), (cH - 20) / (maxY - minY), 1.1);
  const tx   = cW / 2 - sc * (minX + (maxX - minX) / 2);
  const ty   = 30    - sc * minY;
  s.svg.transition().duration(420)
    .call(s.zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(sc));
}

function _orgResetZoom() { _orgFitTree(); }

// ── Node click handler ─────────────────────────────────────
function _orgNodeClick(d) {
  const panel = document.getElementById('org-dmia-panel');

  if (d.children || d._children) {
    // Toggle expand / collapse
    if (d.children) { d._children = d.children; d.children = null; }
    else            { d.children = d._children; d._children = null; }
    if (panel) panel.style.display = 'none';
    _orgD3Update(d);
    setTimeout(_orgFitTree, 420);
    return;
  }

  // Leaf structural node — show activities or applications panel
  const isApps = window._orgState.mode === 'applications';
  const items  = isApps ? (d.data._applications || []) : (d.data._activities || []);
  if (!items.length) {
    if (panel) panel.style.display = 'none';
    return;
  }
  _orgShowLeafPanel(d.data.name || '', items, isApps);
}

function _orgShowLeafPanel(nodeName, items, isApps) {
  const panel = document.getElementById('org-dmia-panel');
  if (!panel) return;

  const sorted = [...items].sort((a, b) => {
    if (a.hours == null && b.hours == null) return 0;
    if (a.hours == null) return 1;
    if (b.hours == null) return -1;
    return a.hours - b.hours;
  });

  const key = isApps ? 'app' : 'activity';
  let html = `
    <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.65rem;padding-bottom:0.5rem;
                border-bottom:1px solid rgba(26,24,20,0.06);">
      <div style="font-size:0.71rem;font-weight:700;color:#1A1814;flex:1;">
        <i class="fa-solid fa-${isApps ? 'desktop' : 'person-digging'}"
           style="margin-right:0.4rem;color:#9CA3AF;"></i>
        ${nodeName} &mdash; ${sorted.length}&nbsp;${isApps ? 'application(s)' : 'activité(s)'}
      </div>
      <button onclick="document.getElementById('org-dmia-panel').style.display='none'"
        style="border:none;background:none;cursor:pointer;font-size:0.75rem;color:rgba(26,24,20,0.4);padding:0.15rem;">
        <i class="fa-solid fa-xmark"></i>
      </button>
    </div>
    <div style="display:flex;flex-direction:column;gap:0.28rem;">`;

  sorted.forEach((item, i) => {
    const col  = _ORG_COLORS[item.dmia_color || 'none'];
    const name = item[key] || '—';
    const dmia = item.dmia || '—';
    const extra = isApps
      ? `<span style="font-size:0.57rem;color:rgba(26,24,20,0.5);margin-left:0.45rem;">
           Criticité : ${item.criticite || '—'}</span>`
      : '';
    html += `
      <div onclick="_orgLeafItemClick(${i})" data-leaf-idx="${i}"
        style="display:flex;align-items:center;gap:0.55rem;padding:0.38rem 0.65rem;
               border:1px solid ${col.stroke};border-left:3px solid ${col.stroke};border-radius:0.4rem;
               background:#fff;cursor:pointer;transition:background 0.1s;"
        onmouseover="this.style.background='${col.fill}'" onmouseout="this.style.background='#fff'">
        <div style="width:8px;height:8px;border-radius:50%;background:${col.badge};flex-shrink:0;"></div>
        <div style="flex:1;font-size:0.68rem;font-weight:600;color:#1A1814;">${name}${extra}</div>
        <span style="font-size:0.62rem;font-weight:700;background:${col.fill};color:${col.text};
                     border:1px solid ${col.stroke};border-radius:3px;padding:1px 5px;flex-shrink:0;">
          ${dmia}</span>
      </div>`;
  });

  html += '</div>';
  panel.innerHTML = html;
  panel.style.display = 'block';
  window._orgCurrentLeafItems = sorted;
}

function _orgLeafItemClick(idx) {
  const items = window._orgCurrentLeafItems || [];
  const item  = items[idx];
  if (!item) return;
  const col = _ORG_COLORS[item.dmia_color || 'none'];
  document.querySelectorAll('[data-leaf-idx]').forEach(el => {
    el.style.outline = parseInt(el.dataset.leafIdx) === idx
      ? `2px solid ${col.badge}` : 'none';
  });
}

function _orgRenderAllDmia() {
  const wrap = document.getElementById('org-all-dmia-wrap');
  if (!wrap) return;
  const s      = window._orgState;
  const isApps = s.mode === 'applications';
  const raw    = isApps ? s.full.applications : s.full.activities;

  function collectRaw(n) {
    const items = isApps ? (n._applications || []) : (n._activities || []);
    let all = [...items];
    for (const c of (n.children || [])) all = all.concat(collectRaw(c));
    return all;
  }

  const all    = collectRaw(raw || {});
  const sorted = [...all].sort((a, b) => {
    if (a.hours == null && b.hours == null) return 0;
    if (a.hours == null) return 1;
    if (b.hours == null) return -1;
    return a.hours - b.hours;
  });

  if (!sorted.length) {
    wrap.innerHTML = '<div style="color:rgba(26,24,20,0.35);font-size:0.72rem;text-align:center;padding:2rem;">Aucune donnée disponible.</div>';
    return;
  }

  const lbl = isApps ? 'Application' : 'Activité';
  let html = `
    <div style="font-size:0.7rem;font-weight:700;color:rgba(26,24,20,0.4);text-transform:uppercase;
                letter-spacing:0.1em;margin-bottom:0.75rem;">
      Tous les DMIA — ${sorted.length}&nbsp;${isApps ? 'applications' : 'activités'}
      &nbsp;(trié par criticité)
    </div>
    <div style="overflow-x:auto;">
    <table style="width:100%;border-collapse:collapse;font-size:0.68rem;">
      <thead>
        <tr style="border-bottom:2px solid rgba(26,24,20,0.1);">
          <th style="text-align:left;padding:0.4rem 0.6rem;font-weight:700;color:rgba(26,24,20,0.5);">#</th>
          <th style="text-align:left;padding:0.4rem 0.6rem;font-weight:700;color:rgba(26,24,20,0.5);">${lbl}</th>
          ${isApps ? '<th style="text-align:left;padding:0.4rem 0.6rem;font-weight:700;color:rgba(26,24,20,0.5);">Criticité</th>' : ''}
          <th style="text-align:left;padding:0.4rem 0.6rem;font-weight:700;color:rgba(26,24,20,0.5);">DMIA</th>
          ${isApps ? '<th style="text-align:left;padding:0.4rem 0.6rem;font-weight:700;color:rgba(26,24,20,0.5);">PMDT</th>' : ''}
        </tr>
      </thead><tbody>`;

  sorted.forEach((item, i) => {
    const col  = _ORG_COLORS[item.dmia_color || 'none'];
    const name = isApps ? (item.app || '—') : (item.activity || '—');
    html += `<tr style="border-bottom:1px solid rgba(26,24,20,0.05);${i % 2 ? 'background:rgba(26,24,20,0.014);' : ''}">
      <td style="padding:0.35rem 0.6rem;color:rgba(26,24,20,0.35);">${i + 1}</td>
      <td style="padding:0.35rem 0.6rem;font-weight:500;color:#1A1814;">${name}</td>
      ${isApps ? `<td style="padding:0.35rem 0.6rem;">${item.criticite || '—'}</td>` : ''}
      <td style="padding:0.35rem 0.6rem;">
        <span style="background:${col.fill};color:${col.text};border:1px solid ${col.stroke};
                     border-radius:3px;padding:1px 6px;font-weight:700;">${item.dmia || '—'}</span>
      </td>
      ${isApps ? `<td style="padding:0.35rem 0.6rem;color:rgba(26,24,20,0.6);">${item.pmdt || '—'}</td>` : ''}
    </tr>`;
  });
  html += '</tbody></table></div>';
  wrap.innerHTML = html;
}

function _orgFullscreen() {
  const wrap = document.getElementById('org-tree-wrap');
  if (!wrap) return;
  if (!document.fullscreenElement) {
    wrap.style.borderRadius = '0';
    wrap.requestFullscreen().catch(() => {});
  } else {
    document.exitFullscreen();
    wrap.style.borderRadius = '';
  }
}
document.addEventListener('fullscreenchange', () => {
  const btn = document.querySelector('[onclick="_orgFullscreen()"]');
  if (btn) btn.innerHTML = document.fullscreenElement
    ? '<i class="fa-solid fa-compress"></i>'
    : '<i class="fa-solid fa-expand"></i>';
  if (document.fullscreenElement) setTimeout(_orgFitTree, 120);
});

function exportOrgchart() {
  const s = window._orgState;
  if (!s.svg) return;
  try {
    const svgEl  = document.querySelector('#org-svg-wrap svg');
    if (!svgEl) return;
    const serial = new XMLSerializer().serializeToString(svgEl);
    const blob   = new Blob([serial], { type: 'image/svg+xml' });
    const url    = URL.createObjectURL(blob);
    const a      = document.createElement('a');
    a.href = url; a.download = 'organigramme.svg'; a.click();
    URL.revokeObjectURL(url);
  } catch (e) { toast('Export impossible : ' + e.message, 'error'); }
}

/* ════════════════════════════════════════════════════════════
   INIT DEFAULT PAGE
════════════════════════════════════════════════════════════ */
(function initApp() {
  navigatePage('page-charger-fiches', 'Synthèse BIA', 'Charger les fiches BIA', 'child-charger');
})();

