# BIA Automatique — Architecture Documentation

## Table of Contents
1. [Logical Model (Modèle Logique)](#logical-model)
2. [Low-Level Design (Conception Détaillée)](#low-level-design)
3. [Logical-to-Physical Transition](#transition-l2p)
4. [System Flows](#system-flows)

---

## Logical Model (Modèle Logique)

### 1.1 Domain Entities

The BIA system operates on three fundamental domain concepts:

#### **Project**
- Represents a single uploaded document (fiche or synthèse)
- Attributes: sector, client, project_name, file_type, created_at
- Acts as the root aggregate for all analysis data extracted from that document
- Examples: "STAR Assurances — BIA Animation Commerciale", "GAT Assurances — BIA Siège"

#### **Activity** (Activité / Processus)
- A business process or service line within an organization
- Attributes: name, department, DMIA (Délai Maximum d'Indisponibilité Acceptable)
- Relationships:
  - Belongs to exactly one Project
  - Can have multiple Impact dimensions (staff, apps, equipment)
  - Can be associated with a Risk (via risk_brut + risk_net)
- Semantics: An activity is the atomic unit of business continuity analysis

#### **Risk** (Risque)
- A quantified measure of operational exposure via a 2-factor scoring model
- Attributes: impact (1–4), probabilité (1–4), risque_brut (1–16), efficacité, risque_net
- Relationships:
  - One Risk per Activity per Risk Type (e.g., supplier risk, geographic risk)
  - Risque_net depends on: risque_brut + control efficacy
- Semantics: Risk = Impact × Probability, then mitigated by control effectiveness

#### **Impact** (Dimension d'Impact)
- A consequence dimension of activity loss (staff, IT applications, equipment)
- Subtypes:
  - **Human Impact**: Collaborators (effectif, nominal, positions, télétravail)
  - **Applications Impact**: IT apps and their recovery lots (Lot 1/2/3)
  - **Equipment Impact**: Critical assets (servers, network, etc.)
- Relationships: Each activity has 0..* impact dimensions

#### **Fiche** (Individual BIA form)
- A Microsoft Word document containing:
  - Header: entity identity (name, date, vis-à-vis contact)
  - Activities table: activity names + DMIAs
  - Impact summary: staff, applications, equipment
  - Risk cartography: risk_brut per activity
- File-based representation of a single organizational unit's BIA data

#### **Synthèse** (Aggregated Analysis Sheet)
- An Excel workbook consolidating fiches from multiple departments
- Structure:
  - **Impact DMIA** sheet: all activities + their computed DMIA
  - **Applications IT** sheet: all apps grouped by recovery lot
  - **Collaborateurs Clés** sheet: ramp-up curves (reprise RH %)
  - **Montée en Charge** sheet: per-horizon resource availability
  - **Risk sheets**: cartography, mitigation matrix (risque_brut vs risque_net)
  - **Documents Critiques**: backup/failover docs not to be lost
  - **Équipements** sheet: equipment recovery horizons
- Semantics: Single source of truth for enterprise-wide BIA

---

### 1.2 Logical Relationships

```
┌─────────────┐
│   Project   │ (uploaded document: fiche or synthèse)
├─────────────┤
│ id, sector  │
│ client, ... │
└──────┬──────┘
       │ 1
       │
       ├──────────────────────────┐
       │ 1..* activities          │ 1..* equipment
       │                          │
       ▼                          ▼
┌─────────────┐            ┌──────────────┐
│  Activity   │            │  Equipment   │
├─────────────┤            ├──────────────┤
│ name, DMIA  │            │ designation  │
│ department  │            │ horizons     │
└──────┬──────┘            └──────────────┘
       │
       │ 1
       │
       ├─────────────────┐
       │                 │
       ▼                 ▼
   ┌────────┐       ┌──────────┐
   │ Impact │       │   Risk   │
   ├────────┤       ├──────────┤
   │ type   │       │ impact   │
   │ value  │       │ prob.    │
   └────────┘       │ brut/net │
                    └──────────┘
```

**Key Invariants:**
- Each Activity's DMIA must be expressed (J+3, J+7, J+10, J+15, J+30, or "Pas applicable")
- Risk.risque_net ≤ Risk.risque_brut (mitigation cannot increase risk)
- Activities in a Synthèse must be matched 1:1 with Fiche activities (via fuzzy matching)

---

## Low-Level Design (Conception Détaillée)

### 2.1 Data Flow — Extract Phase (ETL)

**Input:** User uploads N fiches (.docx) + 1 synthèse template (.xlsx)

**Process:**

```python
# bia_etl.py :: extract()
# ─────────────────────────

# Step 1: DOCUMENT PARSING (Python-docx + regex)
for fiche in fiches:
    tables = parse_docx_tables(fiche)
    
    # Identify tables by header row signature
    activities_table = _find_table_by_headers(['Processus', 'DMIA', ...])
    dmia_summary     = _find_table_by_headers(['Activité', 'Délai Max', ...])
    impact_table     = _find_table_by_headers(['Collaborateurs', ...])
    
    # Extract structured data
    activities = extract_activities_from_table(activities_table)
    # activities = [
    #   {'name': 'Veille LBAFT', 'dmia': 'J+10', 'department': 'Compliance'},
    #   ...
    # ]
    
    dmias = extract_dmia_summary(dmia_summary)
    # dmias = {'Veille LBAFT': 'J+10', ...}  # 2nd pass for non-matches


# Step 2: FUZZY MATCHING (rapidfuzz)
# ────────────────────────────────────
# Problem: activity names in summary table differ from main table
#   Main table: "Veille en matière de lutte contre le blanchiment d'argent (LBAFT)"
#   Summary:    "Veille LBAFT"
#   DMIA lookup would fail on exact string match

matched_dmias = two_pass_matching(activities, dmias)
# Pass 1: Exact match (case-insensitive, whitespace-normalized)
# Pass 2: Fuzzy (similarity threshold 0.85+, greedy assignment)
# Result: every activity gets a DMIA, even with name variations


# Step 3: SPREADSHEET LOAD (openpyxl)
# ────────────────────────────────────
synthese_wb = openpyxl.load_workbook(template_xlsx)
impacts_sheet = synthese_wb['Impact DMIA']

for activity, dmia in matched_dmias.items():
    row_idx = find_empty_row(impacts_sheet)
    impacts_sheet[f'A{row_idx}'] = activity
    impacts_sheet[f'B{row_idx}'] = dmia
    impacts_sheet[f'C{row_idx}'] = compute_dmia_numeric(dmia)  # J+7 → 7 * 24 * 60 = 10080 min

synthese_wb.save(output_path)
```

**Output:** Populated Excel with activities + DMIAs in Impact sheet

---

### 2.2 Data Flow — Transform Phase (Risk Scoring)

**Input:** Synthèse Excel with activities, impact, efficacité columns

**Process:**

```python
# risk_scoring.py :: score_risks()
# ─────────────────────────────────

# Step 1: SCALE DETECTION
# ────────────────────────
# Read Échelle sheet if it exists, else use DEFAULT_SCALE
scale = read_scale_from_echelle(synthese) or DEFAULT_SCALE
# scale = [(3, "Faible"), (8, "Moyen"), (11, "Fort"), (16, "Extrême")]

efficacite_levels = read_efficacite_dropdown(synthese) or DEFAULT_EFFICACITE
# = ["Inexistant", "Incomplet/inefficace", "Assez Satisfaisant", 
#    "Satisfaisant", "Très Satisfaisant"]


# Step 2: COMPUTE BRUT RISK
# ──────────────────────────
# For each row in Impact sheet:
for row in risks_sheet.iter_rows():
    impact = int(row['Impact'].value)          # 1..4
    probabilite = int(row['Probabilité'].value)  # 1..4
    
    niveau_brut = impact * probabilite         # 1..16
    
    # Look up label in scale
    risque_brut = lookup_risk_label(niveau_brut, scale)
    # niveau_brut=6 → "Moyen", niveau_brut=12 → "Fort"
    
    row['Niveau du Risque Brut'].value = niveau_brut
    row['Risque Brut'].value = risque_brut


# Step 3: APPLY MITIGATION (Efficacité → Risque Net)
# ────────────────────────────────────────────────────
# Build mitigation matrix from "Matrice Risque-Maîtrise" sheet
mitigation = read_mitigation_matrix(synthese)
# mitigation = {
#   "Très Satisfaisant":    {Faible: "Accepté", Moyen: "Accepté", Fort: "Faible", ...},
#   "Satisfaisant":         {Faible: "Accepté", Moyen: "Accepté", Fort: "Faible", ...},
#   ...
# }

for row in risks_sheet.iter_rows():
    risque_brut = row['Risque Brut'].value     # e.g., "Fort"
    efficacite = row['Efficacité'].value       # e.g., "Satisfaisant"
    
    # Apply matrix lookup
    risque_net = mitigation.get(efficacite, {}).get(risque_brut, risque_brut)
    # ("Satisfaisant", "Fort") → "Faible"
    
    row['Risque Net'].value = risque_net
```

**Key Algorithm:**
- **Brut Risk**: Pure math (Impact × Probability) + scale lookup
- **Net Risk**: Matrix-driven (risque_brut + control_efficacy) → mitigation outcome

**Output:** Updated spreadsheet with Risque Brut + Risque Net for every activity

---

### 2.3 Data Flow — Load Phase (Database Persistence)

**Input:** Scored synthèse + user metadata (sector, client)

**Process:**

```python
# projects_db.py :: save_project()
# ──────────────────────────────────

# Step 1: REGISTER PROJECT
# ────────────────────────
project_id = conn.execute("""
    INSERT INTO projects (sector, client, project_name, file_type)
    VALUES (?, ?, ?, ?)
""", (sector, client, project_name, 'synthese')).lastrowid
# → project_id = 42


# Step 2: EXTRACT & SAVE ACTIVITIES
# ───────────────────────────────────
for row in synthese['Impact DMIA']:
    activity_name = row['Processus'].value
    dmia_expressed = row['DMIA'].value           # "J+10"
    dmia_minutes = dmia_to_minutes(dmia_expressed)  # 14400
    
    conn.execute("""
        INSERT INTO activity_dmia 
        (project_id, activity_name, departement, dmia_exprimee, dmia_minutes)
        VALUES (?, ?, ?, ?, ?)
    """, (project_id, activity_name, dept, dmia_expressed, dmia_minutes))


# Step 3: CREATE FILE STORAGE PATH & ARCHIVE
# ─────────────────────────────────────────────
storage_path = project_dir(sector, client, 'synthese') / f'{project_id}.xlsx'
synthese.save(storage_path)


# Step 4: LINK FICHES TO SYNTHÈSE (if provided)
# ───────────────────────────────────────────────
if fiche_project_ids:
    for fiche_id in fiche_project_ids:
        conn.execute("""
            INSERT INTO synthese_fiche_links 
            (synthese_project_id, fiche_project_id)
            VALUES (?, ?)
        """, (project_id, fiche_id))
```

**Database Schema (SQLite):**

```sql
projects
├── id (PK)
├── sector, client, project_name, file_type
├── created_at
└── [ON DELETE CASCADE]
    ├─→ activity_dmia
    │   ├── id (PK)
    │   ├── project_id (FK)
    │   ├── activity_name, departement, dmia_exprimee, dmia_minutes
    │   └── [indexed: activity_name, dmia_minutes]
    │
    ├─→ fiche_equipment
    │   ├── id (PK)
    │   ├── project_id (FK)
    │   ├── entity, designation, horizons (JSON)
    │   └── [indexed: project_id]
    │
    └─→ synthese_fiche_links (bidirectional)
        ├── synthese_project_id (FK → projects)
        ├── fiche_project_id (FK → projects)
        └── UNIQUE(synthese, fiche)
```

**Key Design Decisions:**
- **ON DELETE CASCADE**: Removing a project cascades to all its activities, links
- **Soft-link table** (synthese_fiche_links): No direct foreign key constraint; allows fiches to exist independently
- **JSON horizons field**: Stores per-horizon recovery data for equipment flexibly
- **Indexed columns**: activity_name (text search), dmia_minutes (range queries for reprise curves)

---

### 2.4 API Layer (FastAPI)

**Endpoints:**

| Method | Path | Input | Process | Output |
|--------|------|-------|---------|--------|
| POST | `/api/process` | Synthèse + fiches | ETL pipeline (extract → transform → load) | Excel + metadata JSON |
| POST | `/api/generate-fiches` | Recensement + template | Fiche cloning + logo replacement + activity filling | ZIP of .docx files |
| POST | `/api/rapport-bia/slides-data` | Synthèse ID | Extract all slide content (meta, charts, tables) | JSON (serializable) |
| POST | `/api/rapport-bia/generate-pptx` | Synthèse ID | Render JSON data into PPTX (60+ slides) | Binary .pptx |
| GET | `/api/rapport-bia/download` | Synthèse ID | Retrieve cached/rendered output | Binary download |

**Request/Response Pattern:**

```python
# Long-running operation → SSE streaming
POST /api/process → {job_id: "uuid"}

# Client polls with job_id
GET /api/process/{job_id} → SSE stream:
{
  "type": "progress",
  "current": 5,
  "total": 20,
  "step": "risk_scoring",
  "message": "Calcul des risques…"
}
{
  "type": "progress",
  "current": 20,
  "total": 20,
  "step": "complete",
  "downloadUrl": "/api/process/{job_id}/download"
}
```

---

## Logical-to-Physical Transition

### 3.1 Domain Concepts → Data Structures

| Logical Concept | Physical Representation | Storage | Access Pattern |
|-----------------|-------------------------|---------|-----------------|
| **Project** | `projects` table row | SQLite (in-memory cache on startup) | PK lookup by id, scan by sector/client |
| **Activity** | `activity_dmia` table row | SQLite (indexed by name) | Fuzzy match, range on dmia_minutes |
| **Risk** | Excel cell (Risque Brut/Net columns) | Spreadsheet formula + computed value | Read after ETL transform |
| **Fiche** | .docx Word document | Filesystem `projects_data/{sector}/{client}/fiche/` | Path-based by project_id |
| **Synthèse** | .xlsx Excel workbook | Filesystem `projects_data/{sector}/{client}/synthese/` | Path-based by project_id |
| **Impact dimension** | Excel columns (Collaborateurs, Applications, Équipements) | Multi-sheet in synthèse workbook | Column scan + aggregation |

---

### 3.2 Processing Pipeline → Code Modules

**Logical Flow:**
```
Input → Parse → Normalize → Match → Score → Persist → Render
```

**Physical Implementation:**

```
Input (User Upload)
    ↓
[bia_etl.py :: extract]        — Document parsing (python-docx)
    ↓ 
Extracted Data (activity[], dmia[], impact_tables[])
    ↓
[bia_etl.py :: transform]      — Fuzzy matching (rapidfuzz) + DMIA resolution
    ↓
Matched Data (activity→dmia mapping, formatted for Excel)
    ↓
[bia_etl.py :: load]           — Write to spreadsheet (openpyxl)
    ↓
Synthèse XLSX (populated, but no risks yet)
    ↓
[risk_scoring.py]              — Compute risque_brut + risque_net
    ↓
Synthèse XLSX (with risk columns filled)
    ↓
[projects_db.py]               — Register project, save activities, archive file
    ↓
Database + Filesystem
    ↓
[bia_slides_data.py]           — Extract chart data from synthèse
    ↓
JSON (slide serialization)
    ↓
[bia_report_pptx.py]           — Render JSON → PPTX slides
    ↓
Output (PPTX or HTML gallery)
```

---

### 3.3 Constraint Mapping

| Logical Invariant | Physical Enforcement | Mechanism |
|-------------------|----------------------|-----------|
| Activity DMIA is mandatory | NOT NULL in database | Insert validation in bia_etl |
| Project ownership integrity | Foreign key ON DELETE CASCADE | SQLite PRAGMA foreign_keys |
| Risk.net ≤ Risk.brut | Mitigation matrix logic | risk_scoring._mitigation_matrix |
| Fiche ↔ Synthèse matching | Fuzzy similarity threshold 0.85+ | two_pass_matching() |
| No duplicate synthèse-fiche links | UNIQUE constraint | SQLite UNIQUE(synthese_id, fiche_id) |

---

### 3.4 Query Patterns

**Lookup Activity by Name (with typo tolerance):**
```python
# Logical: "Find the activity matching this name"
# Physical:
query = """
    SELECT * FROM activity_dmia 
    WHERE project_id = ? 
    AND LOWER(activity_name) LIKE ?
"""
# + fuzzy_match(query_name, all_names) if not exact match
```

**Aggregate Reprise Curves (Collaborators Recovery Over Time):**
```python
# Logical: "What % of staff can resume work by J+X?"
# Physical:
query = """
    SELECT 
      dmia_minutes,
      COUNT(*) as count,
      SUM(CASE WHEN dmia_minutes <= 1440 THEN 1 ELSE 0 END) as day1_recoverable
    FROM activity_dmia
    WHERE project_id = ?
    GROUP BY dmia_minutes
    ORDER BY dmia_minutes ASC
"""
# Transform result into [J+1 → 20%, J+3 → 60%, J+7 → 85%, J+30 → 100%]
```

**Risk Dashboard (by Severity):**
```python
# Logical: "What risks are unacceptable after mitigation?"
# Physical:
risks_sheet = synthese['Cartographie']
for row in risks_sheet.iter_rows():
    if row['Risque Net'].value not in ['Accepté', 'Faible']:
        # Red alert: needs action plan
```

---

## System Flows

### 4.1 End-to-End: Fiche Upload → Synthèse Update → Risk Report

**Scenario:** User uploads 5 BIA fiches for STAR Assurances + existing synthèse template

**Logical Flow:**
1. User provides: 5 × .docx fiches, 1 × .xlsx synthèse template
2. System extracts activities + DMIAs from each fiche
3. System normalizes names & matches against synthèse activities
4. System computes risks (Impact × Prob) + applies mitigation (efficacité)
5. System persists activities to database
6. System renders risk cartography slide + reprise curves
7. User downloads PPTX report

**Physical Implementation:**

```python
# app.py :: POST /api/process
# ─────────────────────────────

@app.post("/api/process")
async def process_bia(
    synthese: UploadFile,  # template.xlsx
    fiches: List[UploadFile],  # [fiche1.docx, fiche2.docx, ...]
    sector: str = Form(...),  # "Assurance"
    client: str = Form(...),  # "STAR Assurances"
):
    job_id = str(uuid.uuid4())
    event_q = Queue()
    
    # Spawn background thread to avoid blocking
    thread = threading.Thread(
        target=_run_process_sync,
        args=(
            synthese.read(), 
            [(f.filename, f.read()) for f in fiches],
            None, None, None, [],
            event_q, job_id
        ),
        daemon=True
    )
    thread.start()
    
    # Return job_id immediately
    return {"job_id": job_id}


# app.py :: GET /api/process/{job_id}
# ─────────────────────────────────────

@app.get("/api/process/{job_id}")
async def stream_process_result(job_id: str):
    """
    SSE endpoint: client polls this, receives progress events
    """
    async def event_generator():
        while True:
            try:
                event = event_q.get(timeout=1)
                yield f"data: {json.dumps(event)}\n\n"
                
                if event.get("type") == "complete":
                    # Store result for download
                    _job_store[job_id] = event["bytes"]
                    break
            except queue.Empty:
                yield f": keepalive\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


# Inside _run_process_sync (background thread):
# ──────────────────────────────────────────────

def _run_process_sync(..., event_q, job_id):
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        synthese_path = tmp / "master.xlsx"
        output_path = tmp / "output.xlsx"
        
        # Phase 1: ETL EXTRACT
        event_q.put({
            "type": "progress",
            "current": 0,
            "total": len(fiche_list),
            "step": "extract",
            "message": "Extraction des activités…"
        })
        
        for i, (filename, fiche_bytes) in enumerate(fiche_list):
            fiche_path = tmp / filename
            fiche_path.write_bytes(fiche_bytes)
            
            activities = extract(fiche_path)  # bia_etl.py
            event_q.put({"type": "progress", "current": i+1, ...})
        
        
        # Phase 2: TRANSFORM (Fuzzy matching + Risk scoring)
        event_q.put({
            "type": "progress",
            "step": "risk_scoring",
            "message": "Calcul des risques…"
        })
        
        synthese_wb = openpyxl.load_workbook(synthese_path)
        
        # Populate + score
        load(synthese_wb, activities, output_path)
        risk_scoring.score_risks(output_path)
        
        
        # Phase 3: PERSIST TO DATABASE
        project_id = projects_db.save_project(
            sector, client, activities, output_path
        )
        
        
        # Phase 4: RENDER OUTPUTS
        slides_json = bia_slides_data.extract_slides_data(output_path)
        pptx_bytes = bia_report_pptx.generate_pptx(slides_json)
        
        
        # Phase 5: COMPLETE
        event_q.put({
            "type": "complete",
            "bytes": pptx_bytes,
            "downloadUrl": f"/api/process/{job_id}/download"
        })
```

---

### 4.2 Risk Scoring Deep Dive

**Logical Question:** "Given an activity's Impact (4/4) and Probability (3/4), and assuming our controls are 'Satisfaisant', what is the residual risk?"

**Physical Computation:**

```python
# Input row from Excel:
Impact = 4
Probabilité = 3
Efficacité = "Satisfaisant"

# Step 1: Compute Brut
niveau_brut = 4 * 3 = 12

# Step 2: Scale lookup
scale = [(3, "Faible"), (8, "Moyen"), (11, "Fort"), (16, "Extrême")]
# Find band where 12 falls
risque_brut = "Extrême"  # 12 ≤ 16 → Extrême

# Step 3: Mitigation matrix lookup
mitigation["Satisfaisant"]["Extrême"] = "Moyen"
risque_net = "Moyen"

# Output:
Niveau du Risque Brut: 12
Risque Brut: Extrême
Risque Net: Moyen
```

**Why This Matters:**
- **Brut = 12 (Extrême)**: Raw exposure is high — impact × frequency is severe
- **Net = Moyen**: Our "Satisfaisant" controls (regular testing, redundancy, etc.) reduce it to manageable level
- **Gap = "Extrême" → "Moyen"**: Shows control effectiveness; if we lose these controls, risk jumps back to Extrême

---

## Summary: Layers

| Layer | Logical Concern | Physical Implementation | Example |
|-------|-----------------|------------------------|---------|
| **Business Logic** | "How do risks compound?" | Risk = Impact × Prob matrix | Brut score (1–16) |
| **Domain Model** | "What is an Activity?" | `activity_dmia` SQL table | One row per activity + DMIA |
| **API Contract** | "How do users interact?" | FastAPI endpoints + SSE | POST /api/process → job streaming |
| **Storage** | "Where does data live?" | SQLite + filesystem | Database: metadata; Filesystem: .xlsx/.docx |
| **Rendering** | "How is analysis presented?" | HTML/PPTX generation | Slides with risk cartography + reprise curves |

---

**End of Architecture Documentation**
