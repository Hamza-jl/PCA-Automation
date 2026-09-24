# Org Chart Vision → BIA Fiches Generation

**Date:** 2026-06-18  
**Status:** Approved

## Overview

Add a second input path on the "Générer les fiches BIA" page: instead of uploading a recensement Excel file, the user uploads an org chart image. A local Granite Vision 3.1 2B model (via Ollama) extracts all organizational entities with their hierarchy. The user reviews and selects which entities to include, then generates BIA fiches using the existing fiche generation pipeline.

## Backend

### New file: `orgchart_vision.py`

**`analyze_orgchart(image_bytes: bytes, model: str) → list[dict]`**

- Base64-encodes the image
- Calls `POST http://localhost:11434/api/generate` with `stream: false`
- System prompt instructs the model to output strict JSON:
  ```json
  {
    "entities": [
      {"name": "Gouverneur", "niveau1": "", "niveau2": "", "niveau3": ""},
      {"name": "Directeur Exécutif", "niveau1": "Gouverneur", "niveau2": "", "niveau3": ""},
      {"name": "Direction des SI", "niveau1": "Gouverneur", "niveau2": "Directeur Exécutif", "niveau3": ""}
    ]
  }
  ```
- JSON parse attempt first; regex fallback extracts quoted/bracketed names as flat entities if JSON is malformed
- Returns `list[dict]` with keys: `name`, `niveau1`, `niveau2`, `niveau3`

**`list_vision_models() → list[str]`**

- Calls `/api/tags`
- Filters models whose `details.families` contains `"clip"` OR whose name contains `"vision"`
- Returns list of model name strings

### New API routes in `app.py`

| Method | Path | Input | Output |
|--------|------|-------|--------|
| POST | `/api/orgchart/analyze` | `multipart/form-data`: `image` (file), `model` (str) | `{"entities": [...]}` |
| GET | `/api/orgchart/models` | — | `{"models": [...]}` |

The existing `/api/generate-fiches` endpoint is extended to accept an optional `entities` JSON body field (list of dicts with `name/niveau1/niveau2/niveau3`) as an alternative to the xlsx path. When `entities` is present, `parse_structures()` is bypassed and the entity list is passed directly to `generate_fiche()`.

## Frontend

### New tab on "Générer les fiches BIA" page

Two tabs: **"Depuis recensement Excel"** (existing, unchanged) | **"Depuis organigramme"** (new)

#### Step 1 — Upload & Analyze
- Image drop zone accepting PNG, JPG, JPEG, PDF (first page only for PDF)
- Vision model selector — populated from `/api/orgchart/models`, pre-selects first result
- Warning banner when no vision models found: "Aucun modèle vision détecté. Installez Granite Vision: `ollama pull granite3.2-vision:2b`"
- "Analyser l'organigramme" button → POST to `/api/orgchart/analyze` → spinner overlay

#### Step 2 — Review & Select (shown after successful analysis)
- Indented checklist: niveau1 → niveau2 → niveau3 indentation via padding-left
- "Tout sélectionner" / "Tout désélectionner" toggle button
- Each row:
  - Checkbox (checked by default)
  - Entity name (inline-editable)
  - Expand arrow → reveals editable niveau1 / niveau2 / niveau3 fields for hierarchy correction
- "＋ Ajouter une entité" button appends a blank editable row
- Delete (×) button per row

#### Step 3 — Generate (always visible below checklist once Step 2 is shown)
- Same "Nom du client" and "Version" fields as existing tab
- "Générer les fiches BIA" button → submits selected entities to extended `/api/generate-fiches`
- Progress bar + "Télécharger (.zip)" button on completion

## Data Flow

```
[Image upload] 
    → POST /api/orgchart/analyze 
    → orgchart_vision.analyze_orgchart() 
    → Ollama Granite Vision 3.1 2B 
    → JSON entity list
    → Frontend checklist
    → User selects/edits
    → POST /api/generate-fiches {entities: [...], client_name, version}
    → fiche_generator.generate_fiche() × N
    → ZIP download
```

## Error Handling

- Ollama not running → clear error: "Ollama non disponible. Lancez `ollama serve`."
- No vision model installed → model selector shows warning + install hint
- JSON parse failure → regex fallback; if fallback also empty → error with raw model response shown so user can debug prompt
- Image too large (>10 MB) → frontend rejects before upload
- Fiche generation failure for individual entity → skip and report in error list (same as existing behavior)

## Constraints

- No changes to `fiche_generator.py` or `bia_etl.py`
- No new Python dependencies (uses `httpx` already in requirements, `base64` stdlib, `zipfile` stdlib)
- Model name is user-selectable; default is whatever vision model is installed — no hardcoded model name
