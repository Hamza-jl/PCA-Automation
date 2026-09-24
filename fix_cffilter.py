import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', encoding='utf-8') as f:
    content = f.read()

start = content.find('window.cfFilterDB = function(q)')
end   = content.find('\n};', content.find('window.cfAddDbFiche', start)) + 3

NEW_BLOCK = r"""window.cfFilterDB = function(q) {
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

  // Use event listeners on the DOM elements — avoids broken inline JS string escaping
  list.querySelectorAll('.cf-db-row:not(.cf-db-loaded)').forEach(row => {
    row.addEventListener('click', () => {
      const id   = parseInt(row.dataset.pid, 10);
      const name = row.dataset.pname;
      cfAddDbFiche(id, name);
    });
  });
};

window.cfAddDbFiche = function(id, name) {
  if (!_sharedFicheIds.find(x => x.id === id)) {
    _sharedFicheIds.push({ id, name });
    _refreshSharedUI();
    cfFilterDB(document.getElementById('cf-db-search')?.value || '');
  }
};"""

new_content = content[:start] + NEW_BLOCK + content[end:]
with open(r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html', 'w', encoding='utf-8') as f:
    f.write(new_content)
print('Done! Lines:', new_content.count('\n'))
