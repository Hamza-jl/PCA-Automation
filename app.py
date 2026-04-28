"""
BIA Web App — FastAPI backend
Accepts DOCX fiches + XLSX synthèse template, runs ETL, returns filled XLSX.
"""
import shutil
import sys
import tempfile
from pathlib import Path
from typing import List

# Ensure this file's directory is on sys.path so `bia_etl` is always importable
# regardless of which directory Python was launched from.
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from bia_etl import extract, load

app = FastAPI(title="BIA Automatique")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent

# Serve static files using absolute path — works regardless of working directory
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    return (BASE_DIR / "static" / "index.html").read_text(encoding="utf-8")


@app.post("/api/process")
async def process_bia(
    fiches: List[UploadFile] = File(..., description="Fiches BIA (.docx)"),
    synthese: UploadFile = File(..., description="Synthèse BIA template (.xlsx)"),
):
    """
    Accepts N docx fiches + 1 xlsx template.
    Runs the ETL pipeline sequentially (each fiche updates the same output file).
    Returns the filled xlsx as binary response.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # The output file starts as a copy of the template
        output = tmp / "output.xlsx"
        output.write_bytes(await synthese.read())

        errors: list[str] = []
        processed = 0

        for upload in fiches:
            if not upload.filename.lower().endswith(".docx"):
                errors.append(f"{upload.filename}: fichier ignoré (pas un .docx)")
                continue

            fiche_path = tmp / upload.filename
            fiche_path.write_bytes(await upload.read())

            try:
                fiche = extract(fiche_path)
                # output_path=output → overwrites in place so the next fiche
                # reads the already-updated file (sequential accumulation)
                load(output, fiche, dry_run=False, output_path=output, verbose=False)
                processed += 1
            except Exception as exc:
                errors.append(f"{upload.filename}: {exc}")

        if processed == 0:
            raise HTTPException(
                status_code=422,
                detail=errors or ["Aucune fiche .docx valide fournie."],
            )

        content = output.read_bytes()

    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": 'attachment; filename="Synthese_BIA_filled.xlsx"',
            "X-Processed-Count": str(processed),
            "X-Error-Count": str(len(errors)),
            "X-Errors": " | ".join(errors) if errors else "",
            "Access-Control-Expose-Headers": "X-Processed-Count, X-Error-Count, X-Errors",
        },
    )


if __name__ == "__main__":
    import uvicorn
    # reload=False avoids the Windows multiprocessing crash where the
    # reloader worker can't find app.py when cwd differs from the file's location
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
