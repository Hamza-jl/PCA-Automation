# PCA-Automation

A structured ETL pipeline and web interface for automating the Business Impact Analysis (BIA) synthesis process within a Business Continuity Plan (PCA) project.

Built during a consulting engagement at STAR Assurances, where BIA workshops with client departments produced individual fiche BIA documents that needed to be consolidated into a multi-sheet synthesis workbook. The manual effort was significant and low in added value. This tool eliminates it.

---

## Context

A BIA (Business Impact Analysis) engagement typically produces:

- One **fiche BIA** per organizational unit (.docx) containing: list of activities and criticality levels, impact matrices per activity and time horizon, DMIA (maximum admissible interruption duration) per process, internal and external exchanges, IT applications with recovery objectives, key personnel and backups, ramp-up staffing requirements, other equipment needs, and critical documents.

- One **synthese BIA** (.xlsx) with a pre-structured sheet per data category, where each row corresponds to a department and all data columns are empty pending manual population.

The tool extracts every structured table from the Word fiches and writes the data into the correct rows and columns of the Excel synthesis, handling multi-row expansion, fuzzy department name matching, and weighted impact score computation automatically.

---

## Architecture

```
BIA_Implementation/
    app.py           FastAPI backend — file upload, ETL orchestration, XLSX response
    bia_etl.py       ETL core — extract, transform, load pipeline
    static/
        index.html   Single-page frontend — drag-and-drop UI, state machine, download
```

### ETL Pipeline (bia_etl.py)

Three phases:

**Extract** reads a `.docx` fiche using `python-docx`. Tables are identified by header fingerprints rather than positional indices, which makes the parser robust to fiches with varying numbers of activities (and therefore varying numbers of impact tables). Each table type has a dedicated detector function (`_is_activity_list_table`, `_is_exchange_table`, etc.).

**Transform** maps the extracted data to the synthesis sheet schema. Each sheet has a dedicated transform function that returns a list of row dictionaries. The impact score is computed as a weighted sum: `Score = IM x 4 + DI x 1 + JR x 2 + FIN x 3`, using the weights from the impact scale table embedded in the fiche. The fiche captures two impact scenarios (A: interruption < 1 day, B: interruption >= 5 days); scenario A is propagated to the 1H, 4H, and 1J columns of the synthesis, scenario B to the 2-3J column, with a flag comment for human review.

**Load** writes to the Excel file using `openpyxl`. It locates the correct row for each department using `rapidfuzz` token-sort ratio matching (necessary because the synthesis template contains a typo in one department name). When a department has multiple activities or applications, it inserts the required number of rows while copying the row style and repeating the Division/Unite/Departement identifiers.

### Why no LLM

All source data is already structured in Word tables. The mapping from fiche to synthesis is deterministic. Validation values (criticality levels, exchange types, storage modes) are finite sets handled by lookup dictionaries. An LLM would add cost, latency, and hallucination risk for a task that requires no language understanding.

### Backend (app.py)

FastAPI endpoint `POST /api/process` accepts multipart form data with one or more `.docx` fiches and one `.xlsx` template. Each fiche is processed sequentially against the same output file, so all departments accumulate into a single synthesis. The filled workbook is returned as binary response with custom headers reporting the number of processed fiches and any per-fiche errors.

### Frontend (static/index.html)

Self-contained HTML file with no build step. Uses Tailwind CSS (CDN), Cormorant Garamond and Montserrat via Google Fonts, GSAP with ScrollTrigger for page load and scroll animations, and Font Awesome for icons. Implements a three-state UI machine (upload, processing, success/error) with drag-and-drop file zones, animated step indicators, and a blob download trigger for the returned XLSX.

---

## Sheets Populated

| Sheet | Source in fiche |
|---|---|
| Activites | Activities table (name, resources, critical period, criticality) |
| Impact DMIA | Per-activity impact matrices + DMIA table |
| Applications IT | IT applications table (name, criticality, DMIA, PMDT) |
| Echanges I | Exchanges table, internal rows (I) |
| Echanges E | Exchanges table, external rows (E) |
| Montee en charge | Ramp-up staffing table (all rows: Effectif, Positions, Teletravail, etc.) |
| Collaborateurs Cles | Key personnel table |
| Autres Eqt IT | Other equipment table with time horizons |
| Doc critiques | Critical documents table |

---

## Requirements

Python 3.10 or higher.

```
fastapi
uvicorn
python-multipart
python-docx
openpyxl
rapidfuzz
```

Install:

```bash
pip install fastapi uvicorn python-multipart python-docx openpyxl rapidfuzz
```

---

## Usage

### Web interface

Start the server:

```bash
cd BIA_Implementation
python app.py
```

Open `http://localhost:8000` in a browser. Upload one or more fiche BIA `.docx` files in the left drop zone and the synthesis template `.xlsx` in the right drop zone, then click Generate. The filled file downloads automatically.

### Command line

Single fiche:

```bash
python bia_etl.py \
  --fiche "path/to/fiche.docx" \
  --synthese "path/to/synthese.xlsx"
```

Entire folder of fiches:

```bash
python bia_etl.py \
  --fiches-dir "path/to/fiches/" \
  --synthese "path/to/synthese.xlsx"
```

Dry run (extract and print without writing):

```bash
python bia_etl.py --fiche "path/to/fiche.docx" --synthese "path/to/synthese.xlsx" --dry-run
```

Output is always written to a new file suffixed `_filled.xlsx`. The original template is never modified.

---

## Extending to new clients

The fiche BIA structure is standardized within this methodology. For a new client engagement:

1. Drop the new `.docx` fiches into the folder and run the pipeline — no code changes needed if the table structure follows the same convention.
2. If the synthesis template has different sheet names or column headers, update the `TRANSFORM_MAP` dictionary in `bia_etl.py` and the corresponding transform functions.
3. If the client fiche uses a different impact scale or weights, update `IMPACT_WEIGHTS` and the scoring logic in `compute_score`.

---

## Limitations

- Impact time horizons: the fiche captures two scenarios (< 1 day and >= 5 days). The synthesis expects four columns (1H, 4H, 1J, 2-3J). The pipeline propagates the short-horizon scenario across the three short columns and uses the long-horizon scenario for the 2-3J column. Cells are flagged with a comment for review.
- The `Doc critiques` sheet is populated only if the fiche contains a recognizable critical documents table. Fiches that leave this section as "A completer" will produce no rows for that sheet.
- Department matching uses fuzzy string similarity with a threshold of 60. If a fiche entity name is very different from what appears in the synthesis (for example, a newly added department not present in the template), the row will not be found and a warning is printed.

---

## License

Internal use — Devoteam consulting engagement for STAR Assurances.
