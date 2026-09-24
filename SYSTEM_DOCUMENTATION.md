# BIA Automatique — Full System Documentation

> **Scope**: This document describes every component of the BIA Automatique platform: its
> architecture, every API endpoint, the full ETL pipeline (extract → transform → load),
> the fuzzy-matching engine, the org-chart builder, the project database, the DMIA
> suggestion system, and the frontend SPA — including edge cases, design rationale, and
> what each feature works with.

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Backend API — app.py](#2-backend-api--apppy)
3. [ETL Pipeline — bia_etl.py](#3-etl-pipeline--bia_etlpy)
   - 3.1 [EXTRACT — Docx Parser](#31-extract--docx-parser)
   - 3.2 [TRANSFORM — Sheet Data Builders](#32-transform--sheet-data-builders)
   - 3.3 [LOAD — Writing to the Synthèse](#33-load--writing-to-the-synthèse)
   - 3.4 [Fuzzy Matching Engine](#34-fuzzy-matching-engine)
   - 3.5 [Bootstrap from Recensement](#35-bootstrap-from-recensement)
   - 3.6 [Org-Tree Extraction](#36-org-tree-extraction)
4. [Project Database — projects_db.py](#4-project-database--projects_dbpy)
5. [Frontend SPA — index.html](#5-frontend-spa--indexhtml)
6. [Supporting Modules](#6-supporting-modules)
7. [Data Flow — End to End](#7-data-flow--end-to-end)
8. [File Formats Supported](#8-file-formats-supported)
9. [Configuration & Thresholds Reference](#9-configuration--thresholds-reference)

---

## 1. System Architecture

```
Browser (index.html — SPA)
     │
     │  HTTP / multipart-form / JSON
     ▼
FastAPI server (app.py — port 8000)
     │
     ├── bia_etl.py         ETL: extract .docx → fill .xlsx
     ├── fiche_generator.py Generate .docx fiches from recensement + template
     ├── fiche_writer.py    Fill a single .docx fiche from editor payload
     ├── activity_filler.py Inject activity list into existing .docx
     ├── bia_compare.py     Compare two Synthèse BIA Excel files
     ├── llm_fallback.py    Ollama LLM hook (optional enhancement)
     └── projects_db.py     SQLite: project history + DMIA suggestion store
             │
             ├── projects.db              (SQLite, auto-created)
             └── projects_data/           (file library on disk)
                  └── <sector>/<client>/
                       ├── fiche/         uploaded .docx files
                       ├── synthese/      uploaded .xlsx files
                       └── fiche_editor/  JSON sessions from the web editor
```

**Technology stack**
| Layer | Library |
|---|---|
| Web server | FastAPI + uvicorn |
| Word documents | python-docx |
| Excel files | openpyxl |
| Fuzzy matching | rapidfuzz (RapidFuzz Levenshtein) |
| Org-chart render | D3.js v7 (browser) |
| ZIP handling | zipfile (server), JSZip (browser) |
| LLM (optional) | Ollama (local) |
| Fiche generation AI | OpenAI GPT-4o Vision |
| Database | SQLite (built-in) |

---

## 2. Backend API — app.py

All routes are served from `http://localhost:8000`.
Static files (JS, CSS) → `/static/`.
Main SPA → `GET /` → returns `static/index.html`.

---

### `POST /api/detect-recensement`

**Purpose**: Pre-flight inspection of an uploaded recensement Excel file.
Returns the auto-detected column mapping and confidence so the UI can show
or hide the manual column-override panel.

**Input**: `multipart/form-data`
| Field | Type | Description |
|---|---|---|
| `recensement` | `.xlsx` file | Fiche de recensement |

**Response** (`application/json`):
```json
{
  "header_row": 3,
  "all_columns": [{"index": 0, "name": "Division"}, ...],
  "mapping": {"division": 0, "unite": 2, "departement": 4},
  "confidence": "high",
  "matched_names": {"division": "Direction", "unite": null, "departement": "Département"}
}
```

**Confidence levels**:
- `high` → ≥ 2 hierarchy columns matched
- `medium` → 1 column matched
- `low` → no columns matched (columns must be set manually)

---

### `POST /api/process`

**Purpose**: Main ETL endpoint. Accepts one or more BIA fiche `.docx` files
plus a Synthèse BIA `.xlsx` template and produces a filled Excel file.

**Two-step workflow inside this endpoint:**
1. **Optional bootstrap** — if `recensement` is provided, seeds every sheet of
   the synthèse with the client's org structure (one blank row per entity).
2. **Fill loop** — for each `.docx` fiche, runs EXTRACT → TRANSFORM → LOAD,
   accumulating data into the same output file sequentially.

**Input**: `multipart/form-data`
| Field | Type | Default | Description |
|---|---|---|---|
| `fiches` | Multiple `.docx` | required | BIA fiche files |
| `synthese` | `.xlsx` | required | Master synthèse template |
| `recensement` | `.xlsx` | optional | Org structure source |
| `llm_model` | string | `""` | Ollama model name, e.g. `qwen2.5:3b` |
| `column_mapping` | JSON string | `""` | Manual override: `{"division":2,"unite":5}` |

**Response**: Binary `.xlsx` file with custom headers:
- `X-Processed-Count` — number of fiches successfully processed
- `X-Error-Count` — number of errors/warnings
- `X-Errors` — pipe-separated error messages (latin-1 encoded)

**Error handling**:
- Non-`.docx` files are skipped with a warning (not a hard error).
- Empty/unreadable fiches log a warning and are skipped.
- Entities from fiches that are not found in the synthèse are reported as
  `"Entite absente du recensement"` warnings in the response headers.

---

### `POST /api/generate-fiches`

**Purpose**: Step 00 of the workflow. Generates one BIA fiche `.docx` per
department/structure from a recensement Excel file + a BIA template `.docx`.
Returns a ZIP archive containing all generated files.

**Input**: `multipart/form-data`
| Field | Type | Default | Description |
|---|---|---|---|
| `recensement` | `.xlsx` | required | Source of org structure |
| `template` | `.docx` | required | BIA fiche template |
| `version` | string | `"2.0"` | Document version: `"1.0"` or `"2.0"` |
| `openai_key` | string | `""` | OpenAI API key (overrides env var) |
| `client_name` | string | `""` | Client name — skips AI logo detection when set |

**How it works** (inside `fiche_generator.py`):
1. Reads all unique org structures from the recensement.
2. Optionally detects the client name/logo from the Excel header using GPT-4o Vision.
3. For each structure, clones the template `.docx` and fills in the department name,
   date, version, and any other fields the template exposes.
4. Packs all generated files into a single ZIP.

**Response headers**:
- `X-Generated-Count` — number of fiches generated
- `X-Error-Count` / `X-Errors`
- `X-Client-Name` — detected or provided client name

---

### `POST /api/save-fiches-zip`

**Purpose**: Saves a previously generated ZIP of fiches to the local project
library and registers each file in the SQLite database.

**Input**: `multipart/form-data`
| Field | Type | Default | Description |
|---|---|---|---|
| `zip_file` | `.zip` | required | ZIP containing `.docx` fiches |
| `sector` | string | `"autres"` | `banques` / `assurances` / `autres` |
| `client` | string | `"Client"` | Client name |

**Process**:
1. Opens the ZIP in memory.
2. For each `.docx` inside → writes to `projects_data/<sector>/<client>/fiche/`.
3. Calls `extract_dmias_from_fiche()` on a temp copy to pull activity/DMIA pairs.
4. Calls `add_project()` → inserts a row in `projects` + rows in `activity_dmia`.

**Response**:
```json
{
  "saved": [{"file": "...", "project_id": 5, "activities": 3}, ...],
  "errors": [],
  "total_files": 4,
  "total_activities": 12
}
```

---

### `POST /api/orgchart`

**Purpose**: Parses a Synthèse BIA `.xlsx` and returns the full organisation
hierarchy as a nested JSON tree ready for D3.js rendering.

**Input**: `multipart/form-data`
| Field | Type | Description |
|---|---|---|
| `synthese` | `.xlsx` | Synthèse BIA file |

**Response structure**:
```json
{
  "depth": 2,
  "activities": {
    "name": "Organisation",
    "_level": "root",
    "_path": [],
    "_dmia_color": "rouge",
    "_dmia_hours": 4,
    "children": [
      {
        "name": "DOSI",
        "_level": "n1",
        "_dmia_color": "rouge",
        "_activities": [{"activity": "...", "dmia": "4H", "hours": 4, "dmia_color": "rouge"}],
        "children": [...]
      }
    ]
  },
  "applications": { /* same structure, leaves have _applications instead of _activities */ }
}
```

`_dmia_color` values: `"rouge"` (≤ 48 h), `"orange"` (72–120 h), `"vert"` (> 120 h), `"none"` (no data).

---

### `POST /api/compare`

**Purpose**: Deep comparison of two Synthèse BIA Excel files (e.g. original
vs. system-generated). Returns a structured similarity report.

**Input**: Two `.xlsx` files: `original` and `generated`.

---

### `POST /api/fill-activities`

**Purpose**: Injects a list of activities into an existing BIA fiche `.docx`.

**Sections auto-filled**:
- §3 – Présentation générale de l'activité
- §4 – Description et criticité (one row per activity)
- §5.2 – Évaluation des impacts (one table per activity)
- §5.3 – DMIA (one row per activity)

**Input**:
| Field | Type | Description |
|---|---|---|
| `fiche` | `.docx` | Generated BIA fiche |
| `activities` | JSON string | `[{"name": "...", "description": "..."}, ...]` |

---

### `POST /api/fill-fiche`

**Purpose**: Fills a complete BIA fiche `.docx` from the web-editor payload.
More complete than `/api/fill-activities` — covers all sections including
§7.1 Montée en charge.

**Sections auto-filled**:
- §3, §4, §5.2, §5.3 (same as fill-activities)
- §7.1 – Montée en charge (7 metrics × N activities, DMIA blocking respected)

---

### `POST /api/sync-check`

**Purpose**: Compares DMIA values between a Synthèse BIA and a set of fiches.
Identifies activities where the DMIA in the fiche differs from the arbitrated
(or recommended) DMIA in the synthèse.

**Input**: Synthèse `.xlsx` + optionally uploaded `.docx` fiches + DB project IDs.

**Matching logic**:
- Entity matching: substring containment (case-insensitive).
- Activity matching: substring containment (case-insensitive).
- DMIA comparison: parsed to minutes using `parse_dmia_minutes()`.

**Response**: `{"mismatches": [...], "total": N}`

---

### Project Database Endpoints

| Method | Route | Description |
|---|---|---|
| `GET` | `/api/projects/list` | All projects grouped `sector → client → [files]` |
| `POST` | `/api/projects/upload` | Save files + extract activities into DB |
| `DELETE` | `/api/projects/{id}` | Remove project + cascade delete activity rows |
| `GET` | `/api/dmia-suggestions?activity=...&sector=...` | Suggestions for an activity name |
| `POST` | `/api/dmia-alerts` | Alerts where DB has a lower DMIA for similar activities |
| `GET` | `/api/fiche-projects?sector=...` | List editor/uploaded fiche projects for selector |
| `GET` | `/api/project-form/{id}` | Load a saved form session for the fiche editor |
| `POST` | `/api/save-fiche-project` | Save/update a fiche editor session |
| `GET` | `/api/ollama-status?model=...` | Check if Ollama is running with the specified model |

---

## 3. ETL Pipeline — bia_etl.py

The ETL pipeline transforms BIA fiches (`.docx`) into a filled Synthèse BIA (`.xlsx`).
It is intentionally **rule-based, not LLM-based**: the source data lives in structured
Word tables, so deterministic header-fingerprint detection is more reliable and
cheaper than any AI approach.

The only "AI" used in this module is **rapidfuzz** for fuzzy Levenshtein matching when
looking up department rows — because real-world templates contain typos and accent
variations that exact string matching cannot handle.

---

### 3.1 EXTRACT — Docx Parser

**Entry point**: `extract(docx_path, llm_model=None)`

**Returns**: `BIAFiche` dataclass

**Data classes extracted**:

| Class | What it represents |
|---|---|
| `Activity` | Name, resources, critical period, criticality, volume |
| `ImpactRow` | Per-activity impact scores (IM/DI/JR/FIN) across time scenarios |
| `Exchange` | Internal/external functional exchange with correspondents |
| `RampUpRow` | Montée en charge row (Effectif, Positions, Télétravail…) |
| `KeyPerson` | Collaborateur clé (function, name, seniority, replacements) |
| `ITApplication` | Application IT with criticality, DMIA, PMDT, workaround |
| `OtherEquipment` | Other equipment with time-horizon quantities |
| `CriticalDoc` | Critical document with storage type and duplication info |

#### Table detection strategy

Tables are identified by **column header fingerprints**, not positional indices.
This is critical because different fiches have different numbers of activities,
which shifts the index of every subsequent table.

| Table type | Detection rule |
|---|---|
| Identification | Any cell in any row contains "Entit" or "Présents" |
| Activity list | `len(cols) >= 4` AND headers contain "Activit" AND "Ressource" |
| Impact matrix | `len(rows) >= 4` AND "Interruption" in header row |
| DMIA table | Headers contain "DMIA" AND ("Processus" OR "Activit" OR "Désignation") |
| Exchange table | Headers contain "Groupes fonctionnels" OR "Correspondants" |
| Ramp-up table | Headers contain "Montée en charge" AND "Nominal" |
| Key people | Headers contain "Fonction" AND "Nom" AND ("Prénom" OR "Prenom") |
| IT applications | Headers contain "Application" AND "DMIA" AND "PMDT" |
| Other equipment | Headers contain ("Désignation" OR "Designation") AND ("H+" OR "J+") |
| Critical docs | Headers contain ("Documents" OR "Fichiers") AND "stockage" |

#### Subheader row filtering (`_is_subheader_row`)

Word renders horizontally merged category rows by repeating the same text
in every cell (e.g. `['Marketing', 'Marketing', 'Marketing', 'Marketing']`).
These are detected and skipped: a row is a subheader if all its non-empty
cells contain the same string and the row has ≥ 2 cells.

#### Two fiche formats

| Format | Scenarios | Impact columns |
|---|---|---|
| **STAR (2-col)** | Scénario A (< 1 jour), Scénario B (≥ 5 jours) | 2 data columns |
| **GAT (4-col)** | 1H, 4H, 1J, 2-3J | 4 data columns — direct mapping to synthèse columns |

The format is auto-detected by counting non-label columns in the impact matrix header:
`data_col_count = len(header_row) - 1`. If ≥ 4 → GAT format (`is_4col = True`).

#### Internal vs. External exchanges

The exchange table parser detects I/E type three ways (in order of priority):
1. Explicit I/E column in the table headers.
2. Table-level header text: "externe" → `E`, "interne" → `I`.
3. Default fallback: `I`.

Column positions are always resolved by header content (`_find_col`), never by
index — so tables with or without optional columns (Criticité, Typologie, T/R)
all parse correctly.

#### LLM fallback (optional)

When `llm_model` is provided and Ollama is running with that model, tables the
rule-based parser could not classify are sent to the LLM via `llm_fallback.py`.
When Ollama is not available, the behaviour is identical to the default (silent no-op).

---

### 3.2 TRANSFORM — Sheet Data Builders

**TRANSFORM functions** convert `BIAFiche` into lists of `{column_name: value}` dicts,
one per sheet:

| Transform function | Target sheet | Key notes |
|---|---|---|
| `transform_activites` | Activités | Direct row per activity |
| `transform_impact_dmia` | Impact DMIA | STAR: scenario A propagated to 1H/4H/1J; B → 2-3J. GAT: direct |
| `transform_applications_it` | Applications IT | Direct row per application |
| `transform_redemarrages` | Redémarrages des applications | Grouped into Lot 1/2/3 by DMIA |
| `transform_echanges_internes` | Echanges I | Only exchanges where `ie_type == "I"` |
| `transform_echanges_externes` | Echanges E | Only exchanges where `ie_type == "E"` |
| `transform_montee_en_charge` | Montée en charge | Only primary labels: Effectif, Positions, Télétravail |
| `transform_collaborateurs` | Collaborateurs Clés | Direct row per key person |
| `transform_autres_eqt` | Autres Eqt IT | Direct row per equipment item |
| `transform_doc_critiques` | Doc critiques | Duplication field split into flag + method |

#### Impact score calculation

```
Score = IM_value × 4 + DI_value × 1 + JR_value × 2 + FIN_value × 3
```

Where weights are hardcoded from the fiche impact scale table. The synthesis
`Echelle d'impact` sheet maps the total score to Faible / Significatif / Majeur / Catastrophique.

#### Application restart lots (`_dmia_lot`)

Applications are assigned a restart priority based on their DMIA:
- **Lot 1**: H0 → J+1 (within 24 hours)
- **Lot 2**: J+2 → J+5 (days 2–5)
- **Lot 3**: Beyond J+5 (or unrecognised DMIA)

#### Ramp-up row filter

Only three **primary** labels are written to the synthesis:
`Effectif`, `Positions`, `Télétravail`.
Derived rows (`Effectif cumulé`, `% Effectif`, etc.) are computed inside the
Excel template by formulae and must **not** be overwritten.

---

### 3.3 LOAD — Writing to the Synthèse

**Entry point**: `load(synthesis_path, fiche, dry_run, output_path, verbose, unmatched)`

The LOAD phase:
1. Opens the output `.xlsx` (already bootstrapped if recensement was provided).
2. For each sheet in `TRANSFORM_MAP`:
   a. Reads the sheet's column headers → `col_map`.
   b. Calls the corresponding transform function → list of row dicts.
   c. Finds the department row using **fuzzy matching** (see §3.4).
   d. Clears old data rows for that entity (to avoid duplicates on re-import).
   e. Writes new rows starting at the department row, copying cell styles.
3. Saves the file in place (sequential accumulation: next fiche reads the already-updated file).

#### Constants
```python
HEADER_ROW    = 4   # 1-based row index of column headers in the synthèse
DATA_START_ROW = 5  # First row of data (immediately after the header row)
DEPT_COLUMNS  = [2, 3, 4]  # Columns B, C, D = Division, Unité, Département
```

#### Row insertion for extra activities

When a department already has data rows in the sheet, `_insert_rows_for_fiche`
inserts the necessary number of additional rows (copying cell styles from the
first data row) before writing. This preserves Excel formatting for all clients.

---

### 3.4 Fuzzy Matching Engine

The fuzzy matching engine is the most sophisticated algorithmic component.
It solves a real-world problem: **the department name in the fiche rarely
matches the department name in the synthesis template exactly** — due to
accents, typos, abbreviations, and word-order differences.

#### `_find_department_row(ws, dept_name)`

**Algorithm**:

1. **Build candidate pool**: Scan all rows ≥ `DATA_START_ROW` in columns B, C, D.
   The most specific non-empty column value (rightmost = Département) is used.
   Rows whose value matches a header placeholder word (e.g. `"Division"`,
   `"Département"`) are excluded from the candidate pool.

2. **Normalise**: Both the query and all candidates are lowercased + accent-stripped
   via `unicodedata.normalize("NFD")` so `"Réglementaire"` and `"Reglementaire"`
   compare identically.

3. **Build query variants**:
   - `dept_name` stripped of common org-unit prefixes (`_strip_org_prefix`):
     `"Département Achat"` → `"Achat"` (more precise match against short cell values).
   - Original `dept_name` as fallback.
   Both variants are tried; the best score across all variants wins.

4. **Two scorers run in parallel**:

   | Scorer | rapidfuzz function | What it measures |
   |---|---|---|
   | `token_sort_ratio` | `fuzz.token_sort_ratio` | Levenshtein ratio on alphabetically-sorted tokens — handles word-order differences |
   | `token_set_ratio` | `fuzz.token_set_ratio` | Ratio after removing common tokens — handles subset relationships |

5. **Acceptance gate** (both scorers must agree):
   ```
   sort_score >= 88  AND  set_score >= 60  AND  both point to the same row
   ```
   This prevents false positives like `"GAT Invest"` → `"GAT VIE"`
   (sort=86.5 < 88 → rejected).
   High-confidence fallback: `sort_score >= 90` is accepted even without set agreement.

#### `_fuzzy_col(col_name, col_map)`

Matches a transform function's column key against the sheet's actual column headers.
Uses `fuzz.token_sort_ratio` with a threshold of **70**. This handles minor label
variations across client templates (e.g. `"DMIA Exprimée"` vs `"DMIA Exprimée (H, J)"`).

#### Prefix stripping (`_strip_org_prefix`)

Common French org-unit prefix words are removed before fuzzy matching:
> `Direction`, `Département`, `Service`, `Division`, `Unité`, `Direction Générale`, etc.

This avoids the ambiguity where "Direction X" and "Direction Y" both have a very
high set-ratio match against the prefix word alone.

---

### 3.5 Bootstrap from Recensement

**Entry point**: `bootstrap_synthese(recensement_path, master_template_path, output_path)`

#### Column auto-detection (`detect_recensement_columns` / `read_recensement`)

The recensement can use any column names — clients never use the same headers:

**Synonym map** (`_NIVEAU_SYNONYMS`):

| Level | Accepted header keywords (substring match) |
|---|---|
| `division` | division, direction, direction generale, direction centrale, pole, branche, groupe, bu, business unit, filiere, secteur, perimetre, entite mere |
| `unite` | unite, service, sous-direction, departement central, direction departementale |
| `departement` | departement, entite, equipe, cellule, bureau, team, agence, etablissement |

**Detection algorithm**:
1. Scan rows 1–15 of the active sheet.
2. For each row, count how many cells match a synonym for division/unite/departement.
3. Pick the row with the highest score (stop early if score ≥ 2).
4. Confidence: `high` (≥ 2 matches), `medium` (1 match), `low` (0 matches).
5. Fallback: if nothing matched, treat column 0 as `division`.

**`custom_mapping` override**: The UI allows the user to manually specify
column indices (`{division: 0, unite: 2, departement: 4}`) which bypasses
auto-detection entirely.

#### Seeding the synthèse

For each sheet in `TRANSFORM_MAP` (except `Redémarrages`):
1. Delete all existing data rows (rows ≥ `DATA_START_ROW`) to remove sample data.
2. Insert exactly `N` rows (one per entity from the recensement), copying
   cell styles from the first data row to preserve formatting.
3. Write Division / Unité / Département into columns B, C, D of each row.

This guarantees the LOAD phase always finds a row to write to, because the
synthesis now mirrors the actual org structure.

---

### 3.6 Org-Tree Extraction

**Entry point**: `extract_org_tree(synthese_path)` (called by `/api/orgchart`)

Returns a two-tree structure: one for activities, one for applications IT.

#### Step 1: Detect main sheet + header row

Scans all sheets in order. For each non-skipped sheet:
- Calls `_find_hdr_row(ws)` → scores rows 1–10 by counting cells whose text
  contains a keyword from `_HDR_KEYWORDS` (structure, direction, departement,
  activite, dmia, application, processus, service, etc.).
- Checks all columns in the detected header row for structure-level labels:
  `"Structure Niveau N"` (classic format) or standalone labels from
  `_STRUCT_LABELS` (Direction, Département, Service, Division, Unité…).
- Picks the first sheet that has at least one matching structural column.

**Fallback**: If no sheet is found, defaults to columns [2, 3, 4] and header row 5.

#### Step 2: Entity collection with carry-forward

```
for each data row (from header_row + 1):
    raw = [cell values for struct_cols]
    if raw is entirely empty → skip
    carry-forward: prev[i] = raw[i] if raw[i] else prev[i]
    key = normalised tuple of prev values
    if key is new → add to entities list
```

**Carry-forward** is essential because merged Excel cells (e.g. "Direction" spanning
rows 2–10) appear non-empty only in the first row; subsequent rows return empty.
Without carry-forward, only the first entity per direction would be detected.

#### Step 3: DMIA data reading (`_read_dmia_sheet`)

The function tries to locate the best DMIA sheet:
1. **Priority 1**: Sheet whose name contains both "impact" and "dmia" (accent-normalised).
2. **Priority 2**: Sheet whose name contains "arbit" (arbitrée).
3. **Priority 3**: Sheet whose name contains "dmia" alone (standalone DMIA sheet).

For the chosen sheet:
- Runs `_find_hdr_row` to locate headers.
- Builds a header map with **accent normalization** (so `"Activité"` = `"Activite"`).
- Searches for the activity column using priority labels:
  `"Activite"`, `"Activite/Processus"`, `"Processus"`.
- Searches for the DMIA column using priority order:
  `DMIA Arbitrée > DMIA Préconisée > DMIA Recommandée > DMIA Exprimée > DMIA`.
- Applies **carry-forward** on structure columns (same logic as entity collection).
- Stores `activity_name → DMIA` keyed by the normalised entity tuple.

If a `DMIA arbitrée` sheet also exists, its data **overrides** the primary sheet's data
for matching entities.

#### Step 4: Applications IT data reading

Same header-detection and carry-forward pattern, but looks for an "Applications" or
"Applications IT" sheet. Reads: Application name, Niveau de criticité, DMIA, PMDT,
Contournement.

Criticality string → color:
| Criticality label | Color |
|---|---|
| V / Vital / Mission Critique / MC | rouge |
| C* / Critique | orange |
| C / Peu Critique / PC | vert |

#### Step 5: Build annotated trees (`_build_dynamic_tree`)

Builds a nested tree from the flat entity list, then annotates leaves with
DMIA/activity data and propagates colors upward:

- **Leaf nodes**: look up their `_path` tuple in `dmia_by_entity` dict.
  Worst (minimum hours) DMIA determines the node's color.
- **Internal nodes**: worst color among children propagates upward.

---

## 4. Project Database — projects_db.py

SQLite database at `projects.db`. Auto-initialised on first use.

### Schema

```sql
CREATE TABLE projects (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sector       TEXT NOT NULL,            -- 'banques', 'assurances', 'autres'
    client       TEXT NOT NULL,            -- client name
    project_name TEXT NOT NULL DEFAULT '', -- filename or editor title
    file_type    TEXT NOT NULL DEFAULT 'fiche',  -- 'fiche' | 'synthese' | 'fiche_editor'
    created_at   TEXT DEFAULT (datetime('now')),
    form_data_path TEXT DEFAULT ''         -- path to JSON session (editor only)
);

CREATE TABLE activity_dmia (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id     INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    activity_name  TEXT NOT NULL,
    departement    TEXT DEFAULT '',
    dmia_exprimee  TEXT DEFAULT '',  -- raw string: '4H', 'J+2', etc.
    dmia_minutes   INTEGER DEFAULT -1  -- parsed: -1 = unrecognised
);

CREATE INDEX idx_act_name ON activity_dmia(activity_name COLLATE NOCASE);
CREATE INDEX idx_act_dmia ON activity_dmia(dmia_minutes);
```

### File storage

Files are stored under:
```
projects_data/<sector>/<client>/<file_type>/<filename>
```
Sector and client names are slugified (non-word chars → `_`, max 60 chars).

### DMIA Suggestions (`get_dmia_suggestions`)

Returns historical DMIA values for similar activities from the database.

**Two-stage lookup**:

1. **Exact match** (case-insensitive via `COLLATE NOCASE`): finds rows where
   `activity_name` is exactly equal to the query. Groups by `dmia_exprimee`
   and sorts by frequency DESC, dmia_minutes ASC.

2. **Keyword match** (if exact match returns < `limit` results): extracts
   "meaningful" words from the activity name (> 3 chars, not in French stop words),
   then queries `LOWER(activity_name) LIKE '%word%'` for each word.

**Stop words** (excluded from keyword search):
`les, des, une, dans, pour, par, avec, sur, son, ses, leur, lors, sous, entre, vers, sans, oui, via, etc.` (and more)

**Filter**: Only entries with `dmia_minutes >= 0` (successfully parsed DMIA) are returned.

**Sorted** by `dmia_minutes ASC` (shortest DMIA first) in the final result.

### DMIA Alerts (`get_lower_dmia_alerts`)

For each activity that already has a DMIA set, checks whether any similar
activity in the DB has a **lower** DMIA. If so, generates an alert suggesting
the lower value should be investigated.

Uses word-level LIKE matching (same keyword extraction, words > 2 chars),
filtered to `ad.dmia_minutes < current_dmia_minutes`.

### Fiche Editor Persistence

| Operation | Function | Storage |
|---|---|---|
| Save session | `save_fiche_form()` | `projects` row + `activity_dmia` rows + JSON file |
| Load session | `load_fiche_form()` | Reads JSON file from path stored in `form_data_path` |
| List projects | `list_fiche_editor_projects()` | Both `fiche_editor` and `fiche` types |
| Import from .docx | `get_fiche_docx_form_data()` | Calls `extract_full_form_from_fiche()` |

The JSON session file is stored at:
```
projects_data/<sector>/<client>/fiche_editor/form_<project_id>.json
```

### `extract_full_form_from_fiche(fiche_path)`

Parses a `.docx` fiche and produces the same form-data dict structure that the
web editor sends on save. This allows uploaded `.docx` files to be opened in the
fiche editor for review or modification, even if they were never created through the editor.

---

## 5. Frontend SPA — index.html

Single-page application. Navigation is handled entirely in JavaScript:
`navigatePage(pageId, sectionTitle, pageTitle, sidebarChildId)`.
No page reloads — only CSS `.active` class toggling.

### Workflow Steps (wf-stepper)

| Step ID | Page | Description |
|---|---|---|
| `wfs-0` | `page-generer-fiches` | Générer les fiches BIA (from recensement) |
| `wfs-1` | `page-charger-fiches` | Charger les fiches BIA (upload .docx files) |
| `wfs-2` | `page-generer-synthese` | Générer la Synthèse BIA (run ETL) |
| `wfs-3` | `page-sync` | Synchronisation (compare fiches vs synthèse) |
| `wfs-4` | `page-organigramme` | Organigramme interactif (D3 tree) |

### Page: Générer les fiches BIA (Step 00)

**State object**: `window._gfState = { recFile, tmplFile, zipBlob, fileNames }`

**Flow**:
1. User drops recensement `.xlsx` and template `.docx` into drop zones.
2. Optional: client name, version (1.0/2.0), OpenAI API key.
3. `gfGenerate()` → `POST /api/generate-fiches` → receives ZIP response.
4. JSZip extracts filenames client-side → success panel shows file list.
5. Options: download ZIP, save to DB (`gfSaveDB()`), or use in Step 01 (`gfUseInStep01()`).

`gfUseInStep01()` uses JSZip to unzip in the browser, creates `File` objects,
and calls `addFilesToShared()` to inject them into Step 01's shared state,
then navigates to `page-charger-fiches`.

### Page: Charger les fiches BIA (Step 01)

**Drop zone**: accepts multiple `.docx` files.
**Shared state**: `window._sharedFiches` / `window._sharedFicheIds`.
Files added here from Step 00 (via `addFilesToShared`) or uploaded directly.

### Page: Générer la Synthèse (Step 02)

**Additional inputs**:
- Synthèse template `.xlsx` (required)
- Fiche de recensement `.xlsx` (optional — enables bootstrap)
- Manual column mapping (shown if confidence < high, hidden otherwise)
- Ollama model name (optional LLM enhancement)

Calls `POST /api/process`. On success, triggers browser download of the filled `.xlsx`.

### Page: Synchronisation (Step 03)

Allows selecting previously saved DB projects or uploading new `.docx` fiches,
then comparing against an uploaded Synthèse BIA. Calls `POST /api/sync-check`.
Displays mismatches in a table with the option to mark each as "resolved".

### Page: Organigramme (Step 04)

**D3.js collapsible tree** built with `d3.hierarchy()` + `d3.tree()`.

**Features**:
- **Two modes**: Activities (default) and Applications IT.
- **Color coding by DMIA**:
  - Rouge: ≤ 48 h (H0 → J+2)
  - Orange: 72–120 h (J+3 → J+5)
  - Vert: > 120 h
  - Grey: no data
- **Click to expand/collapse** intermediate nodes; click leaf → detail panel.
- **Leaf panel**: bottom drawer showing all activities/applications for that department with their DMIA values.
- **Tous les DMIA table**: scrollable summary of all entities and their worst DMIA.
- **Zoom & pan**: mouse wheel + drag via `d3.zoom()`.
- **Fullscreen**: browser Fullscreen API.
- **Export SVG**: serialises the D3 SVG to a `.svg` download.

**Collapsing logic**: initial render shows root + level 1 expanded, all deeper
nodes collapsed. Clicking an intermediate node toggles its `_children` ↔ `children`.
After each toggle, `_orgFitTree()` re-fits the viewport to the visible nodes.

### Page: Projets

Accessible via the sidebar. Lists all projects in the SQLite database grouped by
sector → client. Supports upload of new files and deletion of projects.

### Page: Fiche Editor

Web-based form editor for creating or editing a BIA fiche directly in the browser.
Loads saved sessions via `GET /api/project-form/{id}`.
Saves via `POST /api/save-fiche-project`.
Generates a filled `.docx` via `POST /api/fill-fiche`.

Supports DMIA suggestions: as the user types an activity name, `GET /api/dmia-suggestions`
is called and results are shown as clickable chips.
Supports DMIA alerts: if the current DMIA is higher than the DB benchmark,
a warning badge appears on the activity row.

---

## 6. Supporting Modules

### `fiche_generator.py`

Generates blank BIA fiche `.docx` files pre-filled with the department/structure name.

- Reads org structures from the recensement (via `read_recensement`).
- Optionally calls GPT-4o Vision to detect the client name and logo from the Excel file.
- For each structure: clones the template `.docx`, performs find-and-replace on
  placeholders (`{{CLIENT_NAME}}`, `{{DEPARTEMENT}}`, `{{VERSION}}`, etc.).
- Returns a list of generated file paths + any errors.

### `fiche_writer.py`

Fills a BIA fiche template `.docx` from structured JSON data (the editor payload).

**Sections handled**:
- §3 – Présentation générale: inserts activity names/descriptions into the table.
- §4 – Description et criticité: one row per activity.
- §5.2 – Évaluation des impacts: clones the impact matrix template row for each activity.
- §5.3 – DMIA: one row per activity with expressed DMIA + first actions.
- §7.1 – Montée en charge: writes 7 metrics across N activities, respects DMIA blocking
  (activities blocked until their DMIA show `—` for earlier time horizons).

**`parse_dmia_minutes(s)`**: Converts DMIA strings to minutes for numerical comparison.

### `activity_filler.py`

Simplified fiche filler — only injects activity names and descriptions into §3 and §4.
Used by `/api/fill-activities`.

### `bia_compare.py`

Compares two Synthèse BIA Excel files cell by cell across all data sheets.
Returns a similarity report with per-sheet match rates.

### `llm_fallback.py`

Optional Ollama integration. If an Ollama server is running locally with the
specified model, tables that the rule-based parser could not classify are sent
as text to the LLM for interpretation. The LLM response is parsed back into
the appropriate dataclass field.

**`is_ollama_available(model)`**: Pings `http://localhost:11434/api/tags` and checks
that the specified model appears in the response.

---

## 7. Data Flow — End to End

### Standard workflow

```
Step 00 — Generate fiches
  Recensement .xlsx + Template .docx
    → POST /api/generate-fiches
    → fiche_generator.py reads org structures
    → GPT-4o Vision detects client (optional)
    → One .docx per structure generated
    → ZIP returned
    → (optional) POST /api/save-fiches-zip → SQLite + disk

Step 01 — Load fiches
  .docx files placed in shared state or uploaded
  (may come from Step 00 ZIP via JSZip browser extraction)

Step 02 — Generate Synthèse
  .docx fiches + Synthèse template .xlsx + (optional) Recensement .xlsx
    → POST /api/process
    → bootstrap_synthese() seeds org rows from recensement (if provided)
    → For each .docx:
         extract() → BIAFiche dataclass
         transform_*() → {column_name: value} rows
         load() → fuzzy-match dept row → write to .xlsx
    → Filled .xlsx returned as download

Step 03 — Sync Check
  Synthèse .xlsx + Fiches .docx (or DB project IDs)
    → POST /api/sync-check
    → Parse synthèse DMIA values
    → Parse fiche DMIA values
    → Compare → list of mismatches

Step 04 — Organigramme
  Synthèse .xlsx
    → POST /api/orgchart
    → extract_org_tree() → nested JSON
    → D3.js renders collapsible tree with DMIA color coding
```

### DMIA suggestion loop

```
Fiche Editor: user types activity name
  → GET /api/dmia-suggestions?activity=<name>&sector=<sector>
  → get_dmia_suggestions():
      1. Exact NOCASE match in activity_dmia table
      2. Keyword LIKE match for remaining slots
  → Returns up to 8 suggestions sorted by dmia_minutes ASC
  → UI renders clickable chips; user can click to auto-fill DMIA field
  
  → GET /api/dmia-alerts (on DMIA change)
  → get_lower_dmia_alerts():
      Checks if DB has a lower DMIA for similar activities
  → Warning badge shown if lower value exists in DB
```

---

## 8. File Formats Supported

### Input

| Format | Accepted by | Notes |
|---|---|---|
| `.docx` (BIA fiche) | `/api/process`, `/api/fill-activities`, `/api/fill-fiche`, `/api/sync-check`, `/api/projects/upload` | Both STAR (2-col) and GAT (4-col) impact formats |
| `.xlsx` (Synthèse BIA) | `/api/process`, `/api/orgchart`, `/api/compare`, `/api/sync-check`, `/api/projects/upload` | Classic (headers row 5) and compact (headers row 1) layouts |
| `.xlsx` (Recensement) | `/api/process`, `/api/generate-fiches`, `/api/detect-recensement` | Any column naming scheme — see synonym map |
| `.zip` | `/api/save-fiches-zip` | ZIP of `.docx` fiches from Step 00 |

### Output

| Endpoint | Output |
|---|---|
| `/api/process` | Filled Synthèse BIA `.xlsx` |
| `/api/generate-fiches` | ZIP of `.docx` fiches |
| `/api/fill-activities` | Updated BIA fiche `.docx` |
| `/api/fill-fiche` | Complete BIA fiche `.docx` |
| `/api/orgchart` | JSON tree (for D3.js) |

---

## 9. Configuration & Thresholds Reference

| Parameter | Location | Value | Meaning |
|---|---|---|---|
| `HEADER_ROW` | `bia_etl.py` | `4` | 1-based row index of column headers in the Synthèse |
| `DATA_START_ROW` | `bia_etl.py` | `5` | First data row in the Synthèse |
| `DEPT_COLUMNS` | `bia_etl.py` | `[2, 3, 4]` | B=Division, C=Unité, D=Département |
| `SORT_THRESHOLD` | `_find_department_row` | `88` | Minimum `token_sort_ratio` for row match |
| `SET_THRESHOLD` | `_find_department_row` | `60` | Minimum `token_set_ratio` for row match |
| High-confidence fallback | `_find_department_row` | `90` | `token_sort_ratio` alone is sufficient |
| Column fuzzy threshold | `_fuzzy_col` | `70` | Minimum `token_sort_ratio` for column match |
| DMIA color — rouge | `_dmia_color` | `≤ 48 h` | H0 → J+2 |
| DMIA color — orange | `_dmia_color` | `72–120 h` | J+3 → J+5 |
| DMIA color — vert | `_dmia_color` | `> 120 h` | J+6 and beyond |
| Lot 1 | `_dmia_lot` | H0 → J+1 | Restart within 24 h |
| Lot 2 | `_dmia_lot` | J+2 → J+5 | Restart days 2–5 |
| Lot 3 | `_dmia_lot` | > J+5 | Lower-priority restart |
| Suggestion limit | `get_dmia_suggestions` | `8` | Max returned per query |
| Recensement scan depth | `detect_recensement_columns` | rows 1–15 | Max rows scanned for headers |
| Org-tree header scan | `_find_hdr_row` | rows 1–10 | Max rows scored for best header row |
| Detection early stop | `detect_recensement_columns` | score ≥ 2 | Stop scanning after a confident match |
| Server port | `app.py` | `8000` | uvicorn bind port |
| DB path | `projects_db.py` | `projects.db` | SQLite file next to `app.py` |
| File library root | `projects_db.py` | `projects_data/` | Persistent file storage |
