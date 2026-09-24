# Org Chart Vision → BIA Fiches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow users to upload an org chart image, have Granite Vision detect the organizational hierarchy, select which entities to include, and generate BIA fiches — all without an Excel recensement file.

**Architecture:** New `orgchart_vision.py` handles Ollama vision API calls. Two new FastAPI routes handle model listing and image analysis. A new separate endpoint `POST /api/generate-fiches-from-entities` accepts a JSON entity list + template file and reuses the existing `generate_fiche()` pipeline. The frontend adds a second tab to the existing "Générer les fiches BIA" page.

**Tech Stack:** Python 3.11, FastAPI, httpx (already installed), base64 (stdlib), zipfile (stdlib), Ollama local API, vanilla JS + existing CSS in index.html.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `orgchart_vision.py` | Ollama vision call, JSON parse, fallback parser, model listing |
| Modify | `app.py` | Add `GET /api/orgchart/models-vision`, `POST /api/orgchart/analyze`, `POST /api/generate-fiches-from-entities` |
| Modify | `static/index.html` | New "Depuis organigramme" tab with 3-step UI |

---

## Task 1: `orgchart_vision.py` — vision module

**Files:**
- Create: `C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\orgchart_vision.py`

- [ ] **Step 1: Create the file with imports and constants**

```python
"""
orgchart_vision.py — Analyse un organigramme via un modèle vision local (Ollama).

Public API:
    list_vision_models() -> list[str]
    analyze_orgchart(image_bytes, model) -> list[dict]
        Each dict: {name, niveau1, niveau2, niveau3}
"""
from __future__ import annotations

import base64
import json
import re

import httpx

OLLAMA_BASE = "http://localhost:11434"

_SYSTEM_PROMPT = """Tu es un expert en analyse d'organigrammes. 
Analyse l'image fournie et extrait TOUTES les entités organisationnelles visibles (directions, services, départements, comités, postes).
Détermine la hiérarchie de chaque entité (qui est son parent, son grand-parent).

Réponds UNIQUEMENT avec un objet JSON valide, sans texte avant ou après, dans ce format exact :
{
  "entities": [
    {"name": "Nom de l'entité", "niveau1": "parent direct ou vide", "niveau2": "grand-parent ou vide", "niveau3": "arrière-grand-parent ou vide"},
    ...
  ]
}

Règles :
- niveau1 = parent direct dans la hiérarchie (vide si racine)
- niveau2 = grand-parent (vide si inexistant)
- niveau3 = arrière-grand-parent (vide si inexistant)
- Inclure TOUTES les boîtes visibles dans l'organigramme
- Ne pas inventer d'entités non visibles dans l'image
- Répondre uniquement en JSON, aucune explication"""
```

- [ ] **Step 2: Add `list_vision_models()`**

```python
def list_vision_models() -> list[str]:
    """Return Ollama models that support vision (have clip family or 'vision' in name)."""
    try:
        r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=5)
        r.raise_for_status()
        models = r.json().get("models", [])
        vision = []
        for m in models:
            name = m.get("name", "")
            families = m.get("details", {}).get("families") or []
            if "clip" in families or "vision" in name.lower():
                vision.append(name)
        return vision
    except Exception:
        return []
```

- [ ] **Step 3: Add `_parse_entities_fallback()` — regex fallback**

```python
def _parse_entities_fallback(raw_text: str) -> list[dict]:
    """
    Extract entity names from raw text when JSON parsing fails.
    Returns a flat list with empty hierarchy fields.
    """
    # Try to find quoted strings that look like org unit names (>3 chars)
    candidates = re.findall(r'"([^"]{3,80})"', raw_text)
    seen = set()
    entities = []
    for name in candidates:
        name = name.strip()
        # Skip JSON keys we know about
        if name.lower() in ("name", "niveau1", "niveau2", "niveau3", "entities"):
            continue
        if name not in seen:
            seen.add(name)
            entities.append({"name": name, "niveau1": "", "niveau2": "", "niveau3": ""})
    return entities
```

- [ ] **Step 4: Add `analyze_orgchart()` — main entry point**

```python
def analyze_orgchart(image_bytes: bytes, model: str, timeout: float = 120.0) -> list[dict]:
    """
    Send an org chart image to a local Ollama vision model.
    Returns a list of entity dicts: [{name, niveau1, niveau2, niveau3}, ...]
    Raises RuntimeError with a user-friendly message on failure.
    """
    if not model:
        raise RuntimeError("Aucun modèle vision sélectionné.")

    image_b64 = base64.b64encode(image_bytes).decode("utf-8")

    payload = {
        "model": model,
        "system": _SYSTEM_PROMPT,
        "prompt": "Analyse cet organigramme et retourne le JSON des entités organisationnelles.",
        "images": [image_b64],
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 2048,
        },
    }

    try:
        r = httpx.post(
            f"{OLLAMA_BASE}/api/generate",
            json=payload,
            timeout=timeout,
        )
        r.raise_for_status()
    except httpx.ConnectError:
        raise RuntimeError("Ollama non disponible. Lancez `ollama serve` sur ce PC.")
    except httpx.TimeoutException:
        raise RuntimeError(f"Le modèle {model} a mis trop de temps à répondre (>{timeout}s).")
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Erreur Ollama {e.response.status_code}: {e.response.text[:200]}")

    raw = r.json().get("response", "")

    # --- JSON parse attempt ---
    # Strip markdown code fences if the model wrapped its output
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```$", "", cleaned.strip(), flags=re.MULTILINE)

    try:
        data = json.loads(cleaned)
        entities = data.get("entities", [])
        if not isinstance(entities, list):
            raise ValueError("'entities' n'est pas une liste")
        result = []
        for e in entities:
            if not isinstance(e, dict) or not e.get("name"):
                continue
            result.append({
                "name":    str(e.get("name", "")).strip(),
                "niveau1": str(e.get("niveau1", "") or "").strip(),
                "niveau2": str(e.get("niveau2", "") or "").strip(),
                "niveau3": str(e.get("niveau3", "") or "").strip(),
            })
        if result:
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # --- Fallback ---
    fallback = _parse_entities_fallback(raw)
    if fallback:
        return fallback

    raise RuntimeError(
        f"Le modèle n'a pas retourné de JSON valide. Réponse brute :\n{raw[:500]}"
    )
```

- [ ] **Step 5: Quick smoke test in Python REPL**

```python
# Run from BIA_Implementation directory:
python -c "
import orgchart_vision as ov
print('Vision models:', ov.list_vision_models())
"
```
Expected: prints a list (possibly empty if no vision model installed yet) — no import error.

- [ ] **Step 6: Commit**

```bash
git add orgchart_vision.py
git commit -m "feat: add orgchart_vision module for Granite Vision integration"
```

---

## Task 2: New API routes in `app.py`

**Files:**
- Modify: `C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\app.py`

- [ ] **Step 1: Add import at the top of app.py**

Find the line `import risk_analysis as _risk` and add below it:

```python
import orgchart_vision as _ov
```

- [ ] **Step 2: Add `GET /api/orgchart/models-vision` route**

Find the block `@app.post("/api/orgchart")` in app.py and insert these two new routes **before** it:

```python
@app.get("/api/orgchart/models-vision")
async def orgchart_vision_models():
    """Return locally available Ollama vision models."""
    return {"models": _ov.list_vision_models()}


@app.post("/api/orgchart/analyze")
async def orgchart_analyze(
    image: UploadFile = File(..., description="Org chart image (PNG/JPG)"),
    model: str = Form(..., description="Ollama vision model name"),
):
    """
    Analyze an org chart image with a local vision model.
    Returns detected entities with hierarchy.
    """
    allowed = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}
    suffix = Path(image.filename).suffix.lower()
    if suffix not in allowed:
        raise HTTPException(
            status_code=422,
            detail=f"Format non supporté '{suffix}'. Utilisez PNG ou JPG."
        )

    image_bytes = await image.read()
    if len(image_bytes) > 15 * 1024 * 1024:
        raise HTTPException(status_code=422, detail="Image trop grande (max 15 MB).")

    try:
        entities = _ov.analyze_orgchart(image_bytes, model)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))

    return {"entities": entities, "count": len(entities)}
```

- [ ] **Step 3: Add `POST /api/generate-fiches-from-entities` route**

Add this route right after the one you just added:

```python
@app.post("/api/generate-fiches-from-entities")
async def generate_fiches_from_entities(
    template: UploadFile = File(..., description="BIA fiche template (.docx)"),
    entities_json: str = Form(..., description="JSON array of entity dicts"),
    client_name: str = Form("Client", description="Client name for fiche filenames"),
    version: str = Form("2.0", description="Document version"),
):
    """
    Generate one BIA fiche per entity in entities_json.
    entities_json must be a JSON array:
    [{"name": "...", "niveau1": "...", "niveau2": "...", "niveau3": "..."}, ...]
    Returns a ZIP of generated .docx files.
    """
    if not template.filename.lower().endswith(".docx"):
        raise HTTPException(status_code=422, detail="Le modèle BIA doit être un .docx")
    if version not in ("1.0", "2.0"):
        version = "2.0"

    try:
        entities = _json.loads(entities_json)
        if not isinstance(entities, list):
            raise ValueError
    except (ValueError, _json.JSONDecodeError):
        raise HTTPException(status_code=422, detail="entities_json invalide — doit être un tableau JSON.")

    # Convert entities to the structure format expected by generate_fiche()
    structures = [
        {
            "name":     e.get("name", "").strip(),
            "vis_a_vis": "",
            "date":     "",
            "niveau1":  e.get("niveau1", "") or "",
            "niveau2":  e.get("niveau2", "") or "",
            "niveau3":  e.get("niveau3", "") or "",
        }
        for e in entities
        if e.get("name", "").strip()
    ]

    if not structures:
        raise HTTPException(status_code=422, detail="Aucune entité valide fournie.")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        tmpl_path = tmp / template.filename
        tmpl_path.write_bytes(await template.read())
        out_dir = tmp / "fiches"
        out_dir.mkdir()

        from fiche_generator import generate_fiche, _safe_filename
        generated: list[Path] = []
        errors: list[str] = []

        for s in structures:
            safe_name   = _safe_filename(s["name"])
            safe_client = _safe_filename(client_name.strip() or "Client")
            out_path = out_dir / f"{safe_client} - MCO - BIA - {safe_name} - V{version}.docx"
            try:
                generate_fiche(tmpl_path, s, None, out_path,
                               client_name=client_name.strip() or "Client",
                               version=version)
                generated.append(out_path)
            except Exception as exc:
                errors.append(f"{s['name']}: {exc}")

        if not generated:
            raise HTTPException(
                status_code=422,
                detail=errors or ["Aucune fiche générée."],
            )

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for p in generated:
                zf.write(p, p.name)
        zip_bytes = zip_buf.getvalue()

    safe_zip = f"Fiches_BIA_{_safe_zip_name(client_name.strip() or 'Client')}_V{version}.zip"
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_zip}"',
            "X-Generated-Count": str(len(generated)),
            "X-Error-Count":     str(len(errors)),
            "X-Errors":          _safe_header(" | ".join(errors)) if errors else "",
            "Access-Control-Expose-Headers": "X-Generated-Count, X-Error-Count, X-Errors",
        },
    )
```

- [ ] **Step 4: Verify the server starts without errors**

```bash
cd C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation
uvicorn app:app --port 8000 --reload
```
Expected: "Application startup complete." with no import errors.

- [ ] **Step 5: Quick API test**

```bash
curl http://localhost:8000/api/orgchart/models-vision
```
Expected: `{"models":[...]}` — even if the list is empty.

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "feat: add orgchart vision API routes (analyze + generate-fiches-from-entities)"
```

---

## Task 3: Frontend — new tab on "Générer les fiches BIA" page

**Files:**
- Modify: `C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\static\index.html`

- [ ] **Step 1: Find the "Générer les fiches BIA" section**

Search for `page-generer-fiches` in index.html to locate the page container. The existing content (upload zone, template upload, generate button) will become **Tab 1**. You will wrap it and add **Tab 2**.

- [ ] **Step 2: Wrap existing content in tab structure**

Find the inner content of `id="page-generer-fiches"` and replace it with this structure (keep the existing upload/template/generate HTML inside `tab-excel`):

```html
<!-- Tab bar -->
<div class="fiches-tab-bar" style="display:flex;gap:0;margin-bottom:1.5rem;border-bottom:2px solid var(--border);">
  <button class="fiches-tab-btn active" data-tab="tab-excel"
    onclick="switchFichesTab('tab-excel')"
    style="padding:.6rem 1.4rem;border:none;background:none;cursor:pointer;font-weight:600;color:var(--accent);border-bottom:2px solid var(--accent);margin-bottom:-2px;">
    Depuis recensement Excel
  </button>
  <button class="fiches-tab-btn" data-tab="tab-orgchart"
    onclick="switchFichesTab('tab-orgchart')"
    style="padding:.6rem 1.4rem;border:none;background:none;cursor:pointer;font-weight:500;color:var(--text-muted);border-bottom:2px solid transparent;margin-bottom:-2px;">
    Depuis organigramme ✦ IA
  </button>
</div>

<!-- Tab 1: existing Excel flow -->
<div id="tab-excel" class="fiches-tab-panel">
  <!-- [PASTE EXISTING INNER HTML OF page-generer-fiches HERE] -->
</div>

<!-- Tab 2: org chart vision flow -->
<div id="tab-orgchart" class="fiches-tab-panel" style="display:none;">

  <!-- Step 1: Upload & Model -->
  <div id="oc-step1">
    <h3 style="margin-bottom:1rem;">Étape 1 — Charger l'organigramme</h3>

    <div style="margin-bottom:1rem;">
      <label style="font-weight:600;display:block;margin-bottom:.4rem;">Modèle vision (Ollama)</label>
      <select id="oc-model-select" style="width:100%;padding:.5rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);">
        <option value="">Chargement des modèles…</option>
      </select>
      <p id="oc-no-model-warn" style="display:none;color:#e07b39;margin-top:.4rem;font-size:.85rem;">
        ⚠ Aucun modèle vision détecté. Installez Granite Vision :
        <code>ollama pull granite3.2-vision:2b</code>
      </p>
    </div>

    <div id="oc-drop-zone"
      style="border:2px dashed var(--border);border-radius:10px;padding:2.5rem;text-align:center;cursor:pointer;transition:border-color .2s;"
      onclick="document.getElementById('oc-img-input').click()"
      ondragover="event.preventDefault();this.style.borderColor='var(--accent)'"
      ondragleave="this.style.borderColor='var(--border)'"
      ondrop="ocHandleDrop(event)">
      <div style="font-size:2.5rem;margin-bottom:.5rem;">🖼</div>
      <p style="margin:0;font-weight:600;">Glissez-déposez votre organigramme ici</p>
      <p style="margin:.3rem 0 0;color:var(--text-muted);font-size:.85rem;">PNG, JPG, JPEG — max 15 Mo</p>
      <input type="file" id="oc-img-input" accept=".png,.jpg,.jpeg,.gif,.bmp,.webp" style="display:none"
        onchange="ocHandleFile(this.files[0])">
    </div>

    <div id="oc-preview-wrap" style="display:none;margin-top:1rem;text-align:center;">
      <img id="oc-preview-img" style="max-width:100%;max-height:300px;border-radius:8px;border:1px solid var(--border);">
      <p id="oc-preview-name" style="margin:.4rem 0 0;font-size:.85rem;color:var(--text-muted);"></p>
    </div>

    <button id="oc-analyze-btn" onclick="ocAnalyze()"
      style="display:none;margin-top:1.2rem;width:100%;padding:.75rem;background:var(--accent);color:#fff;border:none;border-radius:8px;font-weight:600;cursor:pointer;font-size:1rem;">
      Analyser l'organigramme ✦
    </button>

    <div id="oc-analyzing" style="display:none;text-align:center;padding:1.5rem;">
      <div class="spinner" style="margin:0 auto .8rem;"></div>
      <p style="color:var(--text-muted);">Analyse en cours avec <span id="oc-analyzing-model"></span>…</p>
    </div>
  </div>

  <!-- Step 2: Review checklist (hidden until analysis done) -->
  <div id="oc-step2" style="display:none;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;">
      <h3 style="margin:0;">Étape 2 — Sélectionner les entités</h3>
      <div style="display:flex;gap:.5rem;">
        <button onclick="ocSelectAll(true)"
          style="padding:.3rem .8rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);cursor:pointer;font-size:.85rem;">
          Tout sélectionner
        </button>
        <button onclick="ocSelectAll(false)"
          style="padding:.3rem .8rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);cursor:pointer;font-size:.85rem;">
          Désélectionner
        </button>
        <button onclick="ocReanalyze()"
          style="padding:.3rem .8rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);cursor:pointer;font-size:.85rem;">
          ↺ Réanalyser
        </button>
      </div>
    </div>

    <div id="oc-entity-list" style="border:1px solid var(--border);border-radius:8px;overflow:hidden;max-height:420px;overflow-y:auto;"></div>

    <button onclick="ocAddEntity()"
      style="margin-top:.8rem;padding:.4rem 1rem;border:1px dashed var(--border);border-radius:6px;background:transparent;cursor:pointer;color:var(--text-muted);font-size:.9rem;">
      ＋ Ajouter une entité manuellement
    </button>

    <!-- Step 3: Generate (shown once checklist is populated) -->
    <div id="oc-step3" style="margin-top:2rem;padding-top:1.5rem;border-top:1px solid var(--border);">
      <h3 style="margin-bottom:1rem;">Étape 3 — Générer les fiches</h3>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1rem;">
        <div>
          <label style="font-weight:600;display:block;margin-bottom:.4rem;">Nom du client</label>
          <input id="oc-client-name" type="text" placeholder="Ex: STAR Assurances"
            style="width:100%;padding:.5rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);box-sizing:border-box;">
        </div>
        <div>
          <label style="font-weight:600;display:block;margin-bottom:.4rem;">Version</label>
          <select id="oc-version"
            style="width:100%;padding:.5rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);">
            <option value="2.0">2.0</option>
            <option value="1.0">1.0</option>
          </select>
        </div>
      </div>

      <div style="margin-bottom:1rem;">
        <label style="font-weight:600;display:block;margin-bottom:.4rem;">Modèle de fiche BIA (.docx)</label>
        <input type="file" id="oc-template-input" accept=".docx"
          style="width:100%;padding:.4rem;border:1px solid var(--border);border-radius:6px;background:var(--surface);">
      </div>

      <button id="oc-generate-btn" onclick="ocGenerate()"
        style="width:100%;padding:.75rem;background:#1f6e3e;color:#fff;border:none;border-radius:8px;font-weight:600;cursor:pointer;font-size:1rem;">
        Générer les fiches BIA
      </button>

      <div id="oc-gen-progress" style="display:none;margin-top:1rem;">
        <div style="background:var(--border);border-radius:4px;height:6px;overflow:hidden;">
          <div id="oc-gen-bar" style="height:100%;background:var(--accent);width:0%;transition:width .3s;"></div>
        </div>
        <p id="oc-gen-status" style="margin:.5rem 0 0;font-size:.85rem;color:var(--text-muted);"></p>
      </div>

      <a id="oc-download-link" style="display:none;margin-top:1rem;" download>
        <button style="width:100%;padding:.75rem;background:#2a5c8a;color:#fff;border:none;border-radius:8px;font-weight:600;cursor:pointer;">
          ⬇ Télécharger les fiches (.zip)
        </button>
      </a>

      <div id="oc-gen-errors" style="display:none;margin-top:.8rem;padding:.8rem;background:#fff3cd;border-radius:6px;font-size:.85rem;color:#856404;"></div>
    </div>
  </div>

</div><!-- end tab-orgchart -->
```

- [ ] **Step 3: Add the JavaScript functions before `</script>` tag**

Find the closing `</script>` tag of the main script block in index.html and insert before it:

```javascript
// ─── Org Chart Vision ─────────────────────────────────────────────────────────
let _ocEntities = [];   // [{name, niveau1, niveau2, niveau3, selected}]
let _ocImageFile = null;

function switchFichesTab(tabId) {
  document.querySelectorAll('.fiches-tab-panel').forEach(p => p.style.display = 'none');
  document.getElementById(tabId).style.display = '';
  document.querySelectorAll('.fiches-tab-btn').forEach(b => {
    const active = b.dataset.tab === tabId;
    b.style.color = active ? 'var(--accent)' : 'var(--text-muted)';
    b.style.fontWeight = active ? '600' : '500';
    b.style.borderBottomColor = active ? 'var(--accent)' : 'transparent';
  });
  if (tabId === 'tab-orgchart') ocLoadModels();
}

async function ocLoadModels() {
  const sel = document.getElementById('oc-model-select');
  const warn = document.getElementById('oc-no-model-warn');
  try {
    const r = await fetch('/api/orgchart/models-vision');
    const data = await r.json();
    const models = data.models || [];
    if (models.length === 0) {
      sel.innerHTML = '<option value="">Aucun modèle vision disponible</option>';
      warn.style.display = '';
    } else {
      sel.innerHTML = models.map(m => `<option value="${m}">${m}</option>`).join('');
      warn.style.display = 'none';
    }
  } catch {
    sel.innerHTML = '<option value="">Ollama non disponible</option>';
    warn.style.display = '';
  }
}

function ocHandleDrop(e) {
  e.preventDefault();
  document.getElementById('oc-drop-zone').style.borderColor = 'var(--border)';
  const file = e.dataTransfer.files[0];
  if (file) ocHandleFile(file);
}

function ocHandleFile(file) {
  if (!file) return;
  _ocImageFile = file;
  const preview = document.getElementById('oc-preview-wrap');
  const img = document.getElementById('oc-preview-img');
  const name = document.getElementById('oc-preview-name');
  const analyzeBtn = document.getElementById('oc-analyze-btn');
  const reader = new FileReader();
  reader.onload = e => { img.src = e.target.result; };
  reader.readAsDataURL(file);
  name.textContent = file.name + ' (' + (file.size / 1024).toFixed(0) + ' KB)';
  preview.style.display = '';
  analyzeBtn.style.display = '';
}

async function ocAnalyze() {
  if (!_ocImageFile) return;
  const model = document.getElementById('oc-model-select').value;
  if (!model) { alert('Sélectionnez un modèle vision.'); return; }

  document.getElementById('oc-analyze-btn').style.display = 'none';
  document.getElementById('oc-analyzing').style.display = '';
  document.getElementById('oc-analyzing-model').textContent = model;
  document.getElementById('oc-step2').style.display = 'none';

  const fd = new FormData();
  fd.append('image', _ocImageFile);
  fd.append('model', model);

  try {
    const r = await fetch('/api/orgchart/analyze', { method: 'POST', body: fd });
    const data = await r.json();
    if (!r.ok) throw new Error(data.detail || 'Erreur analyse');
    _ocEntities = (data.entities || []).map(e => ({ ...e, selected: true }));
    ocRenderEntities();
    document.getElementById('oc-step2').style.display = '';
  } catch (err) {
    alert('Erreur : ' + err.message);
    document.getElementById('oc-analyze-btn').style.display = '';
  } finally {
    document.getElementById('oc-analyzing').style.display = 'none';
  }
}

function ocReanalyze() {
  document.getElementById('oc-step2').style.display = 'none';
  document.getElementById('oc-analyze-btn').style.display = _ocImageFile ? '' : 'none';
}

function ocRenderEntities() {
  const list = document.getElementById('oc-entity-list');
  if (_ocEntities.length === 0) {
    list.innerHTML = '<p style="padding:1rem;color:var(--text-muted);">Aucune entité détectée.</p>';
    return;
  }
  list.innerHTML = _ocEntities.map((e, i) => {
    const indent = [e.niveau3, e.niveau2, e.niveau1].filter(Boolean).length;
    return `
    <div class="oc-entity-row" data-idx="${i}"
      style="display:flex;align-items:center;gap:.6rem;padding:.55rem .8rem;padding-left:${0.8 + indent * 1.4}rem;border-bottom:1px solid var(--border);background:var(--surface);">
      <input type="checkbox" ${e.selected ? 'checked' : ''} onchange="ocToggle(${i},this.checked)"
        style="flex-shrink:0;width:16px;height:16px;cursor:pointer;">
      <input type="text" value="${ocEsc(e.name)}" onchange="ocEditName(${i},this.value)"
        style="flex:1;border:none;background:transparent;font-size:.95rem;padding:.15rem .3rem;border-radius:4px;"
        onfocus="this.style.background='var(--bg)'" onblur="this.style.background='transparent'">
      <button onclick="ocToggleHier(${i})" title="Modifier la hiérarchie"
        style="border:none;background:none;cursor:pointer;color:var(--text-muted);font-size:.85rem;padding:.2rem .4rem;">✏</button>
      <button onclick="ocDeleteEntity(${i})"
        style="border:none;background:none;cursor:pointer;color:#c0392b;font-size:1rem;padding:.2rem .4rem;">×</button>
    </div>
    <div id="oc-hier-${i}" style="display:none;padding:.6rem .8rem .6rem ${1.2 + indent * 1.4}rem;background:var(--bg);border-bottom:1px solid var(--border);gap:.5rem;display:none;">
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:.5rem;">
        <div><label style="font-size:.78rem;color:var(--text-muted);">Niveau 1 (parent)</label>
          <input type="text" value="${ocEsc(e.niveau1)}" onchange="ocEditHier(${i},'niveau1',this.value)"
            style="width:100%;padding:.3rem .5rem;border:1px solid var(--border);border-radius:4px;background:var(--surface);font-size:.85rem;box-sizing:border-box;"></div>
        <div><label style="font-size:.78rem;color:var(--text-muted);">Niveau 2</label>
          <input type="text" value="${ocEsc(e.niveau2)}" onchange="ocEditHier(${i},'niveau2',this.value)"
            style="width:100%;padding:.3rem .5rem;border:1px solid var(--border);border-radius:4px;background:var(--surface);font-size:.85rem;box-sizing:border-box;"></div>
        <div><label style="font-size:.78rem;color:var(--text-muted);">Niveau 3</label>
          <input type="text" value="${ocEsc(e.niveau3)}" onchange="ocEditHier(${i},'niveau3',this.value)"
            style="width:100%;padding:.3rem .5rem;border:1px solid var(--border);border-radius:4px;background:var(--surface);font-size:.85rem;box-sizing:border-box;"></div>
      </div>
    </div>`;
  }).join('');
}

function ocEsc(s) { return (s||'').replace(/"/g,'&quot;').replace(/</g,'&lt;'); }
function ocToggle(i, v) { _ocEntities[i].selected = v; }
function ocEditName(i, v) { _ocEntities[i].name = v; }
function ocEditHier(i, field, v) { _ocEntities[i][field] = v; }
function ocDeleteEntity(i) { _ocEntities.splice(i, 1); ocRenderEntities(); }
function ocSelectAll(v) { _ocEntities.forEach(e => e.selected = v); ocRenderEntities(); }
function ocToggleHier(i) {
  const el = document.getElementById('oc-hier-' + i);
  el.style.display = el.style.display === 'none' ? 'grid' : 'none';
}
function ocAddEntity() {
  _ocEntities.push({ name: 'Nouvelle entité', niveau1: '', niveau2: '', niveau3: '', selected: true });
  ocRenderEntities();
  // Scroll to bottom of list
  const list = document.getElementById('oc-entity-list');
  list.scrollTop = list.scrollHeight;
}

async function ocGenerate() {
  const selected = _ocEntities.filter(e => e.selected && e.name.trim());
  if (selected.length === 0) { alert('Sélectionnez au moins une entité.'); return; }
  const tmplInput = document.getElementById('oc-template-input');
  if (!tmplInput.files[0]) { alert('Veuillez sélectionner un modèle de fiche BIA (.docx).'); return; }

  const clientName = document.getElementById('oc-client-name').value.trim() || 'Client';
  const version    = document.getElementById('oc-version').value;

  const btn = document.getElementById('oc-generate-btn');
  const progress = document.getElementById('oc-gen-progress');
  const bar = document.getElementById('oc-gen-bar');
  const status = document.getElementById('oc-gen-status');
  const dlLink = document.getElementById('oc-download-link');
  const errBox = document.getElementById('oc-gen-errors');

  btn.disabled = true;
  btn.textContent = 'Génération en cours…';
  progress.style.display = '';
  bar.style.width = '30%';
  status.textContent = `Génération de ${selected.length} fiche(s)…`;
  dlLink.style.display = 'none';
  errBox.style.display = 'none';

  const fd = new FormData();
  fd.append('template', tmplInput.files[0]);
  fd.append('entities_json', JSON.stringify(selected.map(({name,niveau1,niveau2,niveau3}) => ({name,niveau1,niveau2,niveau3}))));
  fd.append('client_name', clientName);
  fd.append('version', version);

  try {
    const r = await fetch('/api/generate-fiches-from-entities', { method: 'POST', body: fd });
    bar.style.width = '90%';
    if (!r.ok) {
      const err = await r.json().catch(() => ({ detail: 'Erreur serveur' }));
      throw new Error(Array.isArray(err.detail) ? err.detail.join('\n') : err.detail);
    }
    const blob = await r.blob();
    bar.style.width = '100%';
    const genCount = r.headers.get('X-Generated-Count') || '?';
    const errCount = r.headers.get('X-Error-Count') || '0';
    const errMsgs  = r.headers.get('X-Errors') || '';
    status.textContent = `✓ ${genCount} fiche(s) générée(s).`;
    const url = URL.createObjectURL(blob);
    const cd  = r.headers.get('Content-Disposition') || '';
    const fname = (cd.match(/filename="([^"]+)"/) || [])[1] || 'Fiches_BIA.zip';
    dlLink.href = url;
    dlLink.download = fname;
    dlLink.style.display = '';
    if (parseInt(errCount) > 0 && errMsgs) {
      errBox.style.display = '';
      errBox.textContent = 'Avertissements : ' + errMsgs;
    }
  } catch (err) {
    bar.style.width = '0%';
    status.textContent = 'Erreur : ' + err.message;
    status.style.color = '#c0392b';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Générer les fiches BIA';
  }
}
```

- [ ] **Step 4: Load vision models when the org chart tab is first shown**

In the existing `navigatePage` function (or the equivalent page-switch handler), ensure `ocLoadModels()` is called when navigating to `page-generer-fiches`. Find the navigation handler and add:

```javascript
// Inside the navigatePage() function, find the block for 'page-generer-fiches':
if (pageId === 'page-generer-fiches') {
  ocLoadModels();
}
```

If this block doesn't exist yet, add it alongside the other page-specific init calls.

- [ ] **Step 5: Start the server and test the UI manually**

```bash
uvicorn app:app --port 8000 --reload
```

Open `http://localhost:8000` → navigate to "Générer les fiches BIA" → click the "Depuis organigramme" tab.

Verify:
- [ ] Model selector loads (shows models or warning)
- [ ] Drop zone accepts an image file
- [ ] Preview image appears after file selection
- [ ] "Analyser" button appears after image selection
- [ ] Clicking "Analyser" calls the backend and shows the checklist
- [ ] Checkboxes, edit fields, hierarchy expand all work
- [ ] "＋ Ajouter une entité" appends a row
- [ ] "×" deletes a row
- [ ] "Générer les fiches BIA" with a template produces a downloadable ZIP

- [ ] **Step 6: Commit**

```bash
git add static/index.html
git commit -m "feat: add org chart vision tab to Générer les fiches BIA page"
```

---

## Task 4: Install Granite Vision model (manual step)

This is a one-time user action, not code. Document it for reference.

- [ ] **Step 1: Check what vision models are available in Ollama**

```bash
ollama search vision
```

Look for `granite3.2-vision` or similar. Common tags:
- `granite3.2-vision:2b` — IBM Granite Vision 3.2 2B
- `llava:7b` — LLaVA (good fallback, widely available)
- `moondream` — very small, fast vision model

- [ ] **Step 2: Pull the model**

```bash
ollama pull granite3.2-vision:2b
```

Or if not available:
```bash
ollama pull llava:7b
```

- [ ] **Step 3: Verify it appears in the app**

Refresh the "Depuis organigramme" tab — the model should appear in the dropdown.

- [ ] **Step 4: Test with the org chart image**

Upload the org chart PNG, select the model, click "Analyser". The checklist should show all detected entities with their hierarchy pre-filled.

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `analyze_orgchart(image_bytes, model)` | Task 1 Step 4 |
| `list_vision_models()` | Task 1 Step 2 |
| JSON parse + regex fallback | Task 1 Steps 3–4 |
| `GET /api/orgchart/models-vision` | Task 2 Step 2 |
| `POST /api/orgchart/analyze` | Task 2 Step 2 |
| `POST /api/generate-fiches-from-entities` | Task 2 Step 3 |
| Image drop zone + model selector | Task 3 Step 2 |
| Warning when no vision model | Task 3 Steps 2–3 |
| Indented checklist with hierarchy | Task 3 Steps 2–3 |
| Edit entity name inline | Task 3 Step 3 (`ocEditName`) |
| Expand/edit hierarchy per row | Task 3 Step 3 (`ocToggleHier`) |
| Select all / deselect all | Task 3 Step 3 (`ocSelectAll`) |
| Add entity manually | Task 3 Step 3 (`ocAddEntity`) |
| Delete entity | Task 3 Step 3 (`ocDeleteEntity`) |
| Client name + version fields | Task 3 Step 2 (Step 3 form) |
| Template upload + ZIP download | Task 3 Step 3 (`ocGenerate`) |
| Error >15MB rejected | Task 2 Step 2 (`orgchart_analyze`) |
| No new Python dependencies | Confirmed — uses httpx + stdlib only |
| No changes to fiche_generator.py | Confirmed — `generate_fiche()` called directly |

**Type consistency check:** `analyze_orgchart` returns `list[dict]` with keys `name/niveau1/niveau2/niveau3` — matches what `generate-fiches-from-entities` expects and what `ocRenderEntities` renders. ✓

**Placeholder scan:** No TBDs, all code blocks complete. ✓
