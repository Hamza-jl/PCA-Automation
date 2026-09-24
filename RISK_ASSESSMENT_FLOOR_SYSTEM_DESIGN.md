# Risk Assessment Floor System Design
## Floor-Based Photo Tagging & Note Linking for BIA Automatique

**Status**: Design Phase  
**Date**: September 2026  
**Author**: Risk Assessment Feature Team  

---

## 1. Requirements

### Functional Requirements

#### User Flow
1. **Building Type Selection** (Step 1)
   - 7 predefined options: Bâtiment-Siège, Bâtiment Agence, Archive, Data Center, etc.
   - Ability to add custom building types
   - Selection persists for current session

2. **Floor Configuration** (Step 2)
   - User enters number of floors (1–50)
   - System generates floor icons (Floor 1, Floor 2, ..., Floor N)
   - Visual representation (numbered pills/buttons)

3. **Component Selection** (Step 3, per floor)
   - Click floor → reveals component picker
   - Components extracted from Excel questionnaires:
     - From GAT: Data Center, Electrical Systems (Armoire Electrique), etc.
     - From ASTREE: Site-specific components
   - Component list should be **dynamic** (read from configured Excel templates)
   - Each component is a selectable option (icon + label)

4. **Note + Photo Attachment** (Step 4)
   - User writes text note about selected component
   - User can:
     - Take a photo via device camera (requires camera permission)
     - Upload existing photo from device
   - Multiple photos per note (1:many relationship)
   - Photos attached before/after note creation

5. **Organization in UI**
   - View hierarchy: Building → Floor → Component → [Note + Photos]
   - User can navigate between floors, edit/delete notes

### Non-Functional Requirements

| Requirement | Target |
|-------------|--------|
| **Responsiveness** | UI state change (floor click) < 200ms |
| **Photo Upload** | < 5MB per image, handle slow networks (3G+) |
| **Offline Support** | Notes/photos cached locally; sync on reconnect |
| **Camera Access** | iOS + Android (web: prompt for permission) |
| **Storage** | ~100MB per assessment (100 photos @ 1MB avg) |
| **Search/Recall** | User can query notes by floor/component/date within 2s |

---

## 2. High-Level Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                       Frontend (React/Vue)                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐    │
│  │ Building    │  │ Floor        │  │ Component       │    │
│  │ Selector    │→ │ Grid View    │→ │ Picker Modal    │    │
│  │ (Dropdown)  │  │ (1-50 icons) │  │ (from Excel)    │    │
│  └─────────────┘  └──────────────┘  └─────────────────┘    │
│                                             ↓               │
│                    ┌──────────────────────────────────┐    │
│                    │  Note + Photo Capture Panel      │    │
│                    │  - Camera access (device)        │    │
│                    │  - File upload (fallback)        │    │
│                    │  - Note text editor              │    │
│                    │  - Photo preview (carousel)      │    │
│                    └──────────────────────────────────┘    │
│                              ↓                              │
│                    ┌──────────────────────────────────┐    │
│                    │  Local Cache (IndexedDB)         │    │
│                    │  - In-flight photos              │    │
│                    │  - Draft notes                   │    │
│                    │  - Floor metadata                │    │
│                    └──────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
              ↓ HTTP/WebSocket ↓ (multipart/form-data)
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Backend (app.py)                 │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  POST /api/risk-assessment/buildings                        │
│  POST /api/risk-assessment/{assessment_id}/floors           │
│  POST /api/risk-assessment/{assessment_id}/notes            │
│  POST /api/risk-assessment/{assessment_id}/photos/upload    │
│  GET  /api/risk-assessment/{assessment_id}/summary          │
│                                                              │
│  Processing:                                                │
│  ├─ Photo ingestion (validate, resize, compress)            │
│  ├─ EXIF extraction (orientation, timestamp, GPS)           │
│  ├─ Photo tagging (floor, component, note_id)               │
│  ├─ Metadata storage (SQLite + filesystem)                  │
│  └─ AI-ready export format (JSON for synthesis)             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
              ↓ SQLite + Filesystem
┌─────────────────────────────────────────────────────────────┐
│                    Data Layer                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  SQLite:                                                    │
│  ├─ risk_assessments (id, sector, client, building_type)   │
│  ├─ assessment_floors (assessment_id, floor_num, metadata) │
│  ├─ assessment_notes (id, floor_id, component, text)       │
│  ├─ assessment_photos (id, note_id, filename, tags)        │
│  └─ photo_tags (photo_id, tag_key, tag_value)              │
│                                                              │
│  Filesystem:                                                │
│  ├─ projects_data/{sector}/{client}/risk_assessment/{id}/  │
│  │  ├─ photos/{photo_id}_{floor}_{component}.jpg          │
│  │  ├─ thumbnails/{photo_id}_thumb.jpg (200x200)          │
│  │  └─ metadata.json (aggregated floor/component data)     │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow (User Workflow)

```
1. User opens Risk Assessment section
           ↓
2. Selects Building Type (Siège, Agence, Archive, etc.)
           ↓
3. Enters Number of Floors (e.g., 4)
           ↓
4. System creates 4 floor icons (visual grid)
           ↓
5. User clicks Floor 2 icon
           ↓
6. Component options appear (Data Center, Electrical Cabinet, etc.)
           ↓
7. User selects "Data Center" component
           ↓
8. Note + Photo Capture panel opens
           ├─ User writes note: "Generator out of fuel, needs replacement ASAP"
           ├─ User takes 2 photos (camera)
           ├─ Photos are cached locally + shown in preview carousel
           └─ User clicks "Save Note"
           ↓
9. Backend processes:
   ├─ Stores note in SQLite (assessment_notes)
   ├─ Stores photos on filesystem with metadata tags
   ├─ Creates photo_tags entries: {floor: 2, component: "data-center"}
   ├─ Generates thumbnail for UI preview
   └─ Marks note/photos as "synced"
           ↓
10. UI updates: Floor 2 now shows "1 component noted" badge
           ↓
11. User can review all Floor 2 notes, or export for synthesis

```

---

## 3. Data Model

### SQLite Schema

```sql
-- Main assessment record
CREATE TABLE risk_assessments (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id         INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    sector             TEXT NOT NULL,
    client             TEXT NOT NULL,
    building_type      TEXT NOT NULL,  -- 'Siège', 'Agence', 'Archive', 'Data Center', custom
    num_floors         INTEGER NOT NULL DEFAULT 1,
    assessment_date    TEXT DEFAULT (datetime('now')),
    status             TEXT DEFAULT 'in_progress'  -- 'in_progress', 'complete', 'exported'
);

-- Per-floor metadata
CREATE TABLE assessment_floors (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    assessment_id      INTEGER NOT NULL REFERENCES risk_assessments(id) ON DELETE CASCADE,
    floor_number       INTEGER NOT NULL,
    floor_label        TEXT DEFAULT '',  -- User can rename: "Floor 2" → "Server Room Floor"
    notes_count        INTEGER DEFAULT 0,
    status             TEXT DEFAULT 'active'
);

-- Notes tied to floor + component
CREATE TABLE assessment_notes (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    floor_id           INTEGER NOT NULL REFERENCES assessment_floors(id) ON DELETE CASCADE,
    component          TEXT NOT NULL,  -- 'data-center', 'electrical-cabinet', 'hvac', etc.
    note_text          TEXT NOT NULL,
    created_at         TEXT DEFAULT (datetime('now')),
    updated_at         TEXT DEFAULT (datetime('now')),
    status             TEXT DEFAULT 'draft'  -- 'draft', 'saved', 'exported'
);

-- Photos linked to notes
CREATE TABLE assessment_photos (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id            INTEGER NOT NULL REFERENCES assessment_notes(id) ON DELETE CASCADE,
    filename           TEXT NOT NULL,  -- e.g., "photo_2026-09-01_14-32-45_thumb.jpg"
    filesize           INTEGER,        -- bytes
    mime_type          TEXT DEFAULT 'image/jpeg',
    
    -- Photo metadata
    width              INTEGER,
    height             INTEGER,
    orientation        INTEGER DEFAULT 1,  -- EXIF orientation (1-8)
    
    -- Extraction metadata
    exif_timestamp     TEXT,           -- When photo was taken (device time)
    exif_gps_lat       REAL,
    exif_gps_lon       REAL,
    exif_camera_model  TEXT,
    
    -- Processing
    upload_timestamp   TEXT DEFAULT (datetime('now')),
    is_synced          BOOLEAN DEFAULT 0,
    is_compressed      BOOLEAN DEFAULT 0,
    thumbnail_path     TEXT,           -- relative: photos/thumb/{id}.jpg
    original_path      TEXT            -- relative: photos/original/{id}.jpg
);

-- Photo tags for AI synthesis
CREATE TABLE photo_tags (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    photo_id           INTEGER NOT NULL REFERENCES assessment_photos(id) ON DELETE CASCADE,
    
    -- Dimensional tags (for filtering/grouping)
    assessment_id      INTEGER,        -- denormalized for quick queries
    floor_number       INTEGER,
    component_type     TEXT,
    
    -- AI-relevant tags
    condition          TEXT,           -- 'good', 'warning', 'critical', 'unknown'
    risk_level         TEXT,           -- 'low', 'medium', 'high', 'extreme'
    equipment_status   TEXT,           -- 'operational', 'degraded', 'failed', 'offline'
    
    -- User-provided tags
    custom_tags        TEXT,           -- JSON: ["fire-hazard", "water-damage"]
    
    created_at         TEXT DEFAULT (datetime('now'))
);

-- Index for performance
CREATE INDEX idx_assessment_id ON assessment_notes(assessment_id);
CREATE INDEX idx_floor_id ON assessment_notes(floor_id);
CREATE INDEX idx_photo_tags_floor ON photo_tags(assessment_id, floor_number);
CREATE INDEX idx_photo_tags_component ON photo_tags(component_type);
```

---

## 4. API Endpoints

### Building Type & Floor Management

```
POST /api/risk-assessment/start
├─ Payload: { sector, client, building_type, num_floors }
├─ Response: { assessment_id, floors: [{id, number, label}] }
└─ Side effect: Creates risk_assessment + assessment_floor rows

GET /api/risk-assessment/{assessment_id}
└─ Response: { assessment_id, building_type, num_floors, floors: [...], notes_by_floor: {...} }

PUT /api/risk-assessment/{assessment_id}/floors/{floor_id}
├─ Payload: { floor_label }  -- User renames floor
└─ Response: { updated_at }
```

### Note & Photo Management

```
POST /api/risk-assessment/{assessment_id}/notes
├─ Payload: multipart/form-data {
│     floor_id,
│     component,
│     note_text,
│     photos: [File, File, ...],
│     tags: { condition: 'critical', risk_level: 'high' }
│   }
├─ Processing:
│   1. Create note in SQLite
│   2. For each photo:
│      - Save to filesystem: projects_data/{sector}/{client}/risk_assessment/{assessment_id}/photos/
│      - Extract EXIF data
│      - Generate thumbnail (200x200, 30KB max)
│      - Create assessment_photos + photo_tags rows
│   3. Return location + metadata
├─ Response: {
│     note_id,
│     floor_id,
│     component,
│     created_at,
│     photos: [
│       { photo_id, url, thumbnail_url, exif_timestamp, tags }
│     ]
│   }
└─ Idempotent: If photo already exists (by hash), skip upload
```

```
PUT /api/risk-assessment/{assessment_id}/notes/{note_id}
├─ Payload: { note_text, tags: {...} }
└─ Response: { updated_at, tags }

DELETE /api/risk-assessment/{assessment_id}/notes/{note_id}
├─ Side effect: Cascades to photos + photo_tags
└─ Response: { deleted_at }

POST /api/risk-assessment/{assessment_id}/photos/upload
├─ Payload: multipart/form-data { photo_file, note_id }
├─ Response: { photo_id, thumbnail_url, exif_data }
└─ Can be called independently (photo saved to temporary state until linked to note)
```

### Export for AI Synthesis

```
GET /api/risk-assessment/{assessment_id}/export/json
├─ Response: {
│     assessment: { id, building_type, num_floors, ... },
│     floors: [
│       {
│         floor_number,
│         floor_label,
│         components: [
│           {
│             component_type: 'data-center',
│             notes: [
│               {
│                 text: "...",
│                 created_at: "...",
│                 tags: { condition: 'critical', risk_level: 'high' },
│                 photos: [
│                   {
│                     photo_id: "...",
│                     url: "/api/risk-assessment/.../photos/photo_123.jpg",
│                     thumbnail_url: "...",
│                     exif: { timestamp, camera_model, ... }
│                   }
│                 ]
│               }
│             ]
│           }
│         ]
│       }
│     ]
│   }
└─ Purpose: Feed into AI agent for synthesis document generation

GET /api/risk-assessment/{assessment_id}/export/markdown
└─ Generates Markdown summary with photo embeds (for report generation)
```

---

## 5. Frontend Component Architecture

### React/Vue Component Structure

```
<RiskAssessmentView>
  ├─ <BuildingTypeSelector>
  │  └─ Dropdown: 7 options + "Add Custom"
  │
  ├─ <FloorCountInput>
  │  └─ Number input → "5 floors" → generates floor icons
  │
  ├─ <FloorGrid>
  │  ├─ Floor 1 [icon] ← click
  │  ├─ Floor 2 [icon]
  │  ├─ Floor 3 [icon]
  │  └─ ...
  │
  ├─ <ComponentPicker> (modal, shown on floor click)
  │  ├─ "Data Center"    [icon]
  │  ├─ "Electrical"     [icon]
  │  ├─ "HVAC System"    [icon]
  │  ├─ "Security"       [icon]
  │  └─ "Other"          [icon]
  │
  └─ <NotePhotoPanel> (shown after component selected)
     ├─ <PhotoCapture>
     │  ├─ Camera button (device camera via getUserMedia)
     │  ├─ File picker (fallback upload)
     │  └─ Photo carousel (preview uploaded images)
     │
     ├─ <NoteEditor>
     │  ├─ Text area (rich text optional)
     │  └─ Tags panel (condition, risk_level dropdowns)
     │
     └─ <SaveButton>
        └─ POST /api/risk-assessment/.../notes (multipart)
```

### Frontend State Management

```javascript
// Local state (IndexedDB cache for offline support)
{
  assessment_id: "42",
  building_type: "Siège",
  num_floors: 4,
  current_floor: 2,
  current_component: "data-center",
  
  draft_note: {
    text: "...",
    photos: [
      { file: File, local_id: "uuid", preview_url: "blob:..." }
    ],
    tags: { condition: "critical", risk_level: "high" }
  },
  
  saved_notes: {
    "floor_2": [
      { 
        note_id: "...", 
        component: "data-center",
        text: "...",
        photos: [{photo_id, url}],
        created_at: "..."
      }
    ]
  },
  
  sync_queue: [
    { type: "note", payload: {...}, status: "pending" }
  ]
}
```

---

## 6. Photo Storage & Tagging Strategy

### File Organization

```
projects_data/
└─ {sector}/
   └─ {client}/
      └─ risk_assessment/
         └─ {assessment_id}/
            ├─ metadata.json
            │  └─ { assessment_id, building_type, num_floors, ... }
            │
            ├─ photos/
            │  ├─ original/
            │  │  ├─ photo_001_floor2_datacenter.jpg (original, 3-5MB)
            │  │  ├─ photo_002_floor2_datacenter.jpg
            │  │  └─ ...
            │  │
            │  └─ thumbnails/
            │     ├─ photo_001_thumb.jpg (200x200, ~30KB)
            │     ├─ photo_002_thumb.jpg
            │     └─ ...
            │
            └─ notes.json
               └─ [
                    {
                      note_id: "001",
                      floor: 2,
                      component: "data-center",
                      text: "Generator out of fuel...",
                      created_at: "2026-09-01T14:32:45Z",
                      photos: ["photo_001", "photo_002"],
                      tags: {
                        floor_number: 2,
                        component_type: "data-center",
                        condition: "critical",
                        risk_level: "high"
                      }
                    }
                  ]
```

### Photo Processing Pipeline

```python
@app.post("/api/risk-assessment/{assessment_id}/notes")
async def create_note(request: Request, assessment_id: str):
    """
    1. Validate note + photos
    2. Process each photo (compress, thumbnail, EXIF)
    3. Store in database + filesystem
    4. Tag for AI synthesis
    5. Return metadata
    """
    
    form = await request.form()
    note_text = form.get("note_text")
    component = form.get("component")
    photos = form.getlist("photos")  # List[UploadFile]
    tags = json.loads(form.get("tags", "{}"))
    
    # Create note in DB
    note_id = db.insert("assessment_notes", {
        "floor_id": form.get("floor_id"),
        "component": component,
        "note_text": note_text
    })
    
    # Process each photo
    photo_results = []
    for photo_file in photos:
        # Read and validate
        photo_bytes = await photo_file.read()
        if not is_valid_image(photo_bytes):
            raise HTTPException(400, "Invalid image format")
        
        # Extract EXIF
        exif_data = extract_exif(photo_bytes)
        
        # Compress + resize
        original_path = save_photo(assessment_id, note_id, photo_bytes, "original")
        thumbnail_bytes = create_thumbnail(photo_bytes, 200, 200)
        thumbnail_path = save_photo(assessment_id, note_id, thumbnail_bytes, "thumbnails")
        
        # Store in DB
        photo_id = db.insert("assessment_photos", {
            "note_id": note_id,
            "filename": photo_file.filename,
            "original_path": original_path,
            "thumbnail_path": thumbnail_path,
            "width": exif_data.get("width"),
            "height": exif_data.get("height"),
            "exif_timestamp": exif_data.get("datetime")
        })
        
        # Tag for AI synthesis
        db.insert("photo_tags", {
            "photo_id": photo_id,
            "assessment_id": assessment_id,
            "floor_number": form.get("floor_id"),
            "component_type": component,
            "condition": tags.get("condition", "unknown"),
            "risk_level": tags.get("risk_level", "unknown")
        })
        
        photo_results.append({
            "photo_id": photo_id,
            "url": f"/api/risk-assessment/{assessment_id}/photos/{photo_id}",
            "thumbnail_url": f"/api/risk-assessment/{assessment_id}/photos/{photo_id}/thumb"
        })
    
    return {
        "note_id": note_id,
        "component": component,
        "created_at": datetime.now().isoformat(),
        "photos": photo_results
    }
```

---

## 7. AI Synthesis Integration

### Export Format for AI Agent

When user clicks "Generate Synthesis", backend exports:

```json
{
  "assessment_metadata": {
    "assessment_id": "42",
    "sector": "assurances",
    "client": "GAT Assurances",
    "building_type": "Siège",
    "num_floors": 4,
    "assessment_date": "2026-09-01T14:00:00Z",
    "total_notes": 18,
    "total_photos": 45
  },
  
  "floors": [
    {
      "floor_number": 1,
      "floor_label": "Ground Floor",
      "components": [
        {
          "component_type": "data-center",
          "component_label": "Data Center",
          "notes": [
            {
              "note_id": "001",
              "text": "Generator out of fuel, needs replacement",
              "created_at": "2026-09-01T14:32:45Z",
              "tags": {
                "condition": "critical",
                "risk_level": "high",
                "equipment_status": "offline"
              },
              "photos": [
                {
                  "photo_id": "photo_001",
                  "url": "/api/photos/photo_001.jpg",
                  "exif": {
                    "timestamp": "2026-09-01T14:32:00Z",
                    "camera_model": "iPhone 14 Pro"
                  },
                  "description": "Photo of generator with low fuel indicator"
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

**AI Synthesis Workflow:**
```
1. LLM receives JSON above
2. For each floor → for each component → for each note:
   - Analyze note text + photos
   - Generate risk assessment paragraph
   - Suggest mitigation actions
   - Link to project's risk_scoring table (if match exists)
3. Output: Synthesis report with:
   - Visual floor diagrams (annotated with findings)
   - Risk summary by floor
   - Photo gallery (organized by floor/component)
   - Action items prioritized by risk_level
```

---

## 8. Offline Support & Sync

### Local Caching (IndexedDB)

```javascript
// IndexedDB schema
{
  stores: {
    assessments: { keyPath: 'assessment_id' },
    floors: { keyPath: 'id', indexes: ['assessment_id'] },
    notes_draft: { keyPath: 'temp_id', indexes: ['assessment_id', 'floor_id'] },
    photos_pending: { keyPath: 'blob_id', indexes: ['note_temp_id'] },
    sync_queue: { keyPath: 'id', indexes: ['status', 'created_at'] }
  }
}

// When user saves note offline:
1. Store note + photos in IndexedDB
2. Add to sync_queue with status: 'pending'
3. Show UI badge: "Offline - will sync when connected"

// When connection returns:
1. Detect connection (online event)
2. Process sync_queue in order:
   - POST /api/risk-assessment/.../notes (multipart with cached photos)
   - On success: mark as 'synced', remove from IndexedDB
   - On failure: retry with exponential backoff (max 3 times)
3. Update UI: "Synced 5 notes"
```

---

## 9. Component Extraction from Excel

### Dynamic Component List

Instead of hardcoding, components should be extracted from Excel templates:

```python
def get_components_from_excel(questionnaire_path: str) -> List[str]:
    """
    Extract component options from questionnaire Excel template.
    Expected structure:
    - Sheet 1: Contains list of components (rows: Data Center, Electrical, HVAC, etc.)
    - Column A: Component name
    - Column B: Icon/description
    """
    wb = openpyxl.load_workbook(questionnaire_path)
    
    # Try multiple sheet names (questionnaires vary)
    for sheet_name in ["Composants", "Components", "Équipements", wb.sheetnames[0]]:
        try:
            ws = wb[sheet_name]
            components = []
            for row in ws.iter_rows(min_row=2, max_row=50, values_only=True):
                if row[0]:  # Non-empty component name
                    components.append(row[0])
            if components:
                return components
        except:
            continue
    
    # Fallback: common components
    return [
        "Data Center", "Electrical Cabinet", "HVAC System",
        "Security System", "Network Equipment", "Backup Power",
        "Fire Suppression", "Access Control", "Other"
    ]

# Usage in app startup
QUESTIONNAIRES = {
    "gat": "/path/to/GAT_questionnaire.xlsx",
    "astree": "/path/to/ASTREE_questionnaire.xlsx"
}

COMPONENTS = {
    "Siège": get_components_from_excel(QUESTIONNAIRES["gat"]),
    "Agence": get_components_from_excel(QUESTIONNAIRES["astree"]),
    "Data Center": ["Power Supply", "Cooling", "Server Racks", ...],
    "Archive": ["Storage Units", "Climate Control", "Access", ...]
}
```

---

## 10. Scale & Reliability

### Load Assumptions

| Metric | Value |
|--------|-------|
| Concurrent users | 5–10 per office |
| Assessments per month | ~50 |
| Photos per assessment | 20–100 |
| Avg photo size | 3–5 MB |
| Avg storage per assessment | 100–500 MB |
| Daily peak traffic | 200 API calls |

### Storage Capacity

```
Yearly growth:
- 50 assessments/month × 12 = 600 assessments/year
- 600 × 250MB avg = 150 GB/year
- Manageable on local disk; archive old assessments to cold storage

Photo serving:
- Serve thumbnails (30KB) for list views
- Serve originals on demand (lazy loading)
- Use browser caching (Cache-Control: max-age=86400)
```

### Resilience

| Failure Mode | Mitigation |
|--------------|-----------|
| Network drops mid-upload | IndexedDB cache + resume on reconnect |
| Photo corruption | Validate on upload; show error to user |
| Missing EXIF data | Use file timestamp as fallback |
| Database write fails | Transaction rollback; UI shows "retry" |
| Disk full | Warn user; compress old photos to cold storage |

---

## 11. Trade-off Analysis

### Decision: Photo Storage (Filesystem vs. Blob Storage)

| Approach | Pros | Cons |
|----------|------|------|
| **Local Filesystem** (chosen) | Simple, instant, no API latency, works offline | Single-server SPOF, manual backup needed |
| **Cloud Storage (S3/Azure)** | Scalable, redundant, easy backup | Costs $$, latency, needs credentials |

**Decision**: Start with local filesystem (aligns with existing projects_data structure). Migrate to S3 if scaling to multiple offices later.

---

### Decision: Photo Compression

| Level | Quality | File Size | Rationale |
|-------|---------|-----------|-----------|
| **High (chosen)** | 85% JPEG | 800KB–1.5MB | Good quality for risk assessment analysis |
| Medium | 75% JPEG | 400–700KB | Too much quality loss for detail work |
| Low | 60% JPEG | 200–400KB | Unacceptable for large equipment assessment |

Original always kept; thumbnails for UI scrolling.

---

### Decision: Camera API

| Option | Pros | Cons |
|--------|------|------|
| **WebRTC getUserMedia** (chosen) | Works iOS + Android, instant | Requires HTTPS + permission prompt |
| Native app wrapper | Better UX | Higher development cost |
| File picker only | Simplest | Forces users to pre-capture |

**Decision**: getUserMedia + fallback to file picker. Progressive enhancement: app gets better if camera available.

---

### Decision: AI Tagging

| Approach | Pros | Cons |
|----------|------|------|
| **Manual user tags** (chosen for v1) | Accurate, owned by user | Labor-intensive, error-prone |
| Auto-tag (computer vision) | Fast, scalable | Requires ML model, false positives |
| Hybrid (auto-suggest, user confirms) | Best of both | Complex, higher latency |

**Decision**: v1 manual tags. Auto-tagging (v2) when photo ML budget increases.

---

## 12. Implementation Roadmap

### Phase 1: MVP (Sprint 1–2, 2 weeks)
- [ ] Building type selector (7 options hardcoded)
- [ ] Floor grid UI (visual floor icons)
- [ ] Component picker (7 options hardcoded)
- [ ] Note + photo capture (device camera + file picker)
- [ ] Basic SQLite schema (notes + photos only)
- [ ] Photo storage (filesystem)
- [ ] Manual tagging (dropdowns for condition/risk_level)

### Phase 2: Enhancement (Sprint 3–4, 2 weeks)
- [ ] Extract components from Excel (dynamic list)
- [ ] Export to JSON for AI synthesis
- [ ] Offline support (IndexedDB caching)
- [ ] Photo carousel preview (multiple photos per note)
- [ ] EXIF extraction (timestamp, camera model)
- [ ] Thumbnail generation

### Phase 3: AI Integration (Sprint 5–6, 2 weeks)
- [ ] AI synthesis agent (takes JSON export, generates report)
- [ ] Markdown export with photo embeds
- [ ] Risk scoring integration (link notes to existing risk_scoring tables)
- [ ] Visual floor diagrams (AI-annotated)

### Phase 4: Polish & Scale (Sprint 7+)
- [ ] Cloud storage migration (S3)
- [ ] Auto-tagging (computer vision)
- [ ] Performance optimization (lazy loading, pagination)
- [ ] Mobile app wrapper (if needed)

---

## 13. Example User Journey (End-to-End)

```
Sep 1, 2026, 10:00 AM — Consultant arrives at GAT Assurances Siège

1. Opens BIA Automatique app
2. Selects "Risk Assessment" → "Analyse des Risques"
3. Chooses "Bâtiment-Siège" from dropdown
4. Enters "4" floors
5. UI shows 4 floor buttons: [Floor 1] [Floor 2] [Floor 3] [Floor 4]

6. Clicks [Floor 2]
   → Sees component options: [Data Center] [Electrical Cabinet] [HVAC] [Security] [Other]

7. Clicks [Data Center]
   → Note panel opens

8. Types note: "Generator out of fuel, needs replacement ASAP. Risk of 12+ hour outage if power lost."

9. Taps "Take Photo" → Camera opens
   → Takes 2 photos of generator, fuel gauge

10. Both photos upload to local cache (IndexedDB)
    → Carousel shows previews

11. Selects tags: Condition = "Critical", Risk Level = "High"

12. Clicks "Save Note"
    → API POST /api/risk-assessment/{id}/notes
    → Backend saves note, processes photos, creates tags
    → UI shows success badge

13. Repeats for other floors/components (e.g., Floor 3 Electrical Cabinet, Floor 1 HVAC)

... [continues through day] ...

3:00 PM — Back at office

14. Opens "Generate Synthesis" button
    → Backend exports all 18 notes + 45 photos as JSON
    → Passes to AI agent

15. AI agent generates:
    - Floor diagrams (annotated with findings)
    - Risk summary by floor
    - Action items prioritized by risk_level
    - Photo gallery linked to notes

16. Consultant downloads PPTX report with visuals
```

---

## 14. Appendix: Component Extraction from Excel

From the files you provided:

### GAT Assurances Questionnaire
```
Sheet: "Siège"
Likely components:
- Data Center (dedicated sheet)
- Electrical Systems
- HVAC
- Network Infrastructure
- Security Systems
```

### ASTREE Assurances Questionnaire
```
Sheet: "Préalables Agences"
Likely components:
- Backup Systems
- Communication Systems
- Emergency Equipment
- Fire/Safety Systems
```

**Action**: Parse these sheets at app startup to populate component dropdown.

---

## Summary

This system design enables:
✅ **User-friendly floor-based assessment** with visual floor grid + component picker  
✅ **Photo tagging & linking** for complete context retention  
✅ **Offline support** via IndexedDB caching + sync on reconnect  
✅ **AI-ready export** (structured JSON) for synthesis document generation  
✅ **Scalable storage** (local filesystem + cloud migration path)  
✅ **Phased implementation** (MVP in 2 weeks, full feature set in 6 weeks)

