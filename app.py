"""
BIA Web App — FastAPI backend

Endpoints:
  POST /api/process          – Fill the Synthèse BIA from individual fiches (.docx)
  POST /api/generate-fiches  – Generate one BIA fiche per department from a
                                recensement file (.xlsx) + BIA template (.docx)
"""
import io
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import List

# Ensure this file's directory is on sys.path so local modules are importable
# regardless of which directory Python was launched from.
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from bia_etl import extract, load
from fiche_generator import generate_all_fiches

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


@app.post("/api/generate-fiches")
async def generate_fiches(
    recensement: UploadFile = File(..., description="Fiche de recensement (.xlsx)"),
    template: UploadFile = File(..., description="BIA fiche template (.docx)"),
    version: str = Form("2.0", description="Document version: '1.0' or '2.0'"),
    openai_key: str = Form("", description="OpenAI API key (optional, overrides env var)"),
):
    """
    Accepts a recensement Excel file + a BIA fiche template (.docx).
    Uses GPT-4o Vision to detect the client name and logo from the xlsx.
    Generates one filled BIA fiche per department / structure.
    Returns a ZIP archive containing all generated .docx files.
    """
    if not recensement.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=422, detail="Le fichier de recensement doit être un .xlsx")
    if not template.filename.lower().endswith(".docx"):
        raise HTTPException(status_code=422, detail="Le modèle BIA doit être un .docx")
    if version not in ("1.0", "2.0"):
        version = "2.0"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        xlsx_path = tmp / recensement.filename
        xlsx_path.write_bytes(await recensement.read())

        tmpl_path = tmp / template.filename
        tmpl_path.write_bytes(await template.read())

        out_dir = tmp / "fiches"

        generated, errors = generate_all_fiches(
            xlsx_path, tmpl_path, out_dir,
            version=version,
            openai_api_key=openai_key or None,
        )

        if not generated:
            raise HTTPException(
                status_code=422,
                detail=errors or ["Aucune structure trouvée dans le fichier de recensement."],
            )

        # Pack all generated fiches into a single ZIP
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fiche_path in generated:
                zf.write(fiche_path, fiche_path.name)
        zip_bytes = zip_buf.getvalue()

    # Extract client name from the first generated filename for the response
    client_name = "Client"
    if generated:
        client_name = generated[0].name.split(" - MCO")[0]

    safe_zip_name = f"Fiches_BIA_{_safe_zip_name(client_name)}_V{version}.zip"

    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_zip_name}"',
            "X-Generated-Count": str(len(generated)),
            "X-Error-Count": str(len(errors)),
            "X-Errors": " | ".join(errors) if errors else "",
            "X-Client-Name": client_name,
            "Access-Control-Expose-Headers": (
                "X-Generated-Count, X-Error-Count, X-Errors, X-Client-Name"
            ),
        },
    )


def _safe_zip_name(name: str) -> str:
    return re.sub(r'[^\w\-]', '_', name)[:40]


if __name__ == "__main__":
    import uvicorn
    # reload=False avoids the Windows multiprocessing crash where the
    # reloader worker can't find app.py when cwd differs from the file's location
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
