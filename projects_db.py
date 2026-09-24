"""
projects_db.py
==============
SQLite-backed store for BIA project history.

Schema
------
projects      — one row per uploaded file (synthèse or fiche)
activity_dmia — one row per (activity, DMIA) pair extracted from that file
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Optional

# ── paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
DB_PATH      = BASE_DIR / "projects.db"
PROJECTS_DIR = BASE_DIR / "projects_data"   # projects_data/sector/client/type/


# ── bootstrap ─────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    return c


def init_db() -> None:
    PROJECTS_DIR.mkdir(exist_ok=True)
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS projects (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                sector       TEXT    NOT NULL,
                client       TEXT    NOT NULL,
                project_name TEXT    NOT NULL DEFAULT '',
                file_type    TEXT    NOT NULL DEFAULT 'fiche',
                created_at   TEXT    DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS activity_dmia (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id     INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                activity_name  TEXT    NOT NULL,
                departement    TEXT    DEFAULT '',
                dmia_exprimee  TEXT    DEFAULT '',
                dmia_minutes   INTEGER DEFAULT -1
            );
            CREATE INDEX IF NOT EXISTS idx_act_name ON activity_dmia(activity_name COLLATE NOCASE);
            CREATE INDEX IF NOT EXISTS idx_act_dmia ON activity_dmia(dmia_minutes);
            CREATE TABLE IF NOT EXISTS fiche_equipment (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                entity       TEXT    DEFAULT '',
                designation  TEXT    NOT NULL,
                horizons     TEXT    DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_fiche_eq_proj ON fiche_equipment(project_id);
            CREATE TABLE IF NOT EXISTS synthese_fiche_links (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                synthese_project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                fiche_project_id    INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                UNIQUE(synthese_project_id, fiche_project_id)
            );
            CREATE INDEX IF NOT EXISTS idx_sfl_syn ON synthese_fiche_links(synthese_project_id);
            CREATE INDEX IF NOT EXISTS idx_sfl_fic ON synthese_fiche_links(fiche_project_id);

            -- ── Risk Assessment Tables ──────────────────────────────────────────
            CREATE TABLE IF NOT EXISTS risk_assessments (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id         INTEGER REFERENCES projects(id) ON DELETE CASCADE,
                sector             TEXT    NOT NULL,
                client             TEXT    NOT NULL,
                building_type      TEXT    NOT NULL,
                num_floors         INTEGER NOT NULL DEFAULT 1,
                assessment_date    TEXT    DEFAULT (datetime('now')),
                status             TEXT    DEFAULT 'in_progress'
            );

            CREATE TABLE IF NOT EXISTS assessment_floors (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                assessment_id      INTEGER NOT NULL REFERENCES risk_assessments(id) ON DELETE CASCADE,
                floor_number       INTEGER NOT NULL,
                floor_label        TEXT    DEFAULT '',
                notes_count        INTEGER DEFAULT 0,
                status             TEXT    DEFAULT 'active'
            );

            CREATE TABLE IF NOT EXISTS assessment_notes (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                floor_id           INTEGER NOT NULL REFERENCES assessment_floors(id) ON DELETE CASCADE,
                component          TEXT    NOT NULL,
                note_text          TEXT    NOT NULL,
                created_at         TEXT    DEFAULT (datetime('now')),
                updated_at         TEXT    DEFAULT (datetime('now')),
                status             TEXT    DEFAULT 'draft'
            );

            CREATE TABLE IF NOT EXISTS assessment_photos (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                note_id            INTEGER NOT NULL REFERENCES assessment_notes(id) ON DELETE CASCADE,
                filename           TEXT    NOT NULL,
                filesize           INTEGER,
                mime_type          TEXT    DEFAULT 'image/jpeg',
                width              INTEGER,
                height             INTEGER,
                orientation        INTEGER DEFAULT 1,
                exif_timestamp     TEXT,
                upload_timestamp   TEXT    DEFAULT (datetime('now')),
                is_compressed      BOOLEAN DEFAULT 0,
                thumbnail_path     TEXT,
                original_path      TEXT
            );

            CREATE TABLE IF NOT EXISTS photo_tags (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                photo_id           INTEGER NOT NULL REFERENCES assessment_photos(id) ON DELETE CASCADE,
                assessment_id      INTEGER,
                floor_number       INTEGER,
                component_type     TEXT,
                condition          TEXT,
                risk_level         TEXT,
                equipment_status   TEXT,
                created_at         TEXT    DEFAULT (datetime('now'))
            );

            -- Types de bâtiment ajoutés par l'utilisateur (en plus du catalogue standard)
            CREATE TABLE IF NOT EXISTS assessment_building_types (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                label              TEXT    NOT NULL UNIQUE,
                icon               TEXT    DEFAULT 'fa-building',
                created_at         TEXT    DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_risk_assessment_project ON risk_assessments(project_id);
            CREATE INDEX IF NOT EXISTS idx_assessment_floor ON assessment_floors(assessment_id);
            CREATE INDEX IF NOT EXISTS idx_assessment_notes ON assessment_notes(floor_id);
            CREATE INDEX IF NOT EXISTS idx_photo_tags_floor ON photo_tags(assessment_id, floor_number);
            CREATE INDEX IF NOT EXISTS idx_photo_tags_component ON photo_tags(component_type);
        """)


# ── storage helpers ────────────────────────────────────────────────────────────

def project_dir(sector: str, client: str, file_type: str) -> Path:
    d = PROJECTS_DIR / _slug(sector) / _slug(client) / file_type
    d.mkdir(parents=True, exist_ok=True)
    return d


def _slug(s: str) -> str:
    import re
    return re.sub(r'[^\w\-]', '_', s.strip())[:60]


# ── write ──────────────────────────────────────────────────────────────────────

def add_project(
    sector: str,
    client: str,
    project_name: str,
    file_type: str,
    activities: list[dict],
) -> int:
    """
    Insert a project and its activity/DMIA rows.
    activities items: {activity_name, departement, dmia_exprimee, dmia_minutes}
    Returns the new project id.
    """
    init_db()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO projects (sector, client, project_name, file_type) VALUES (?,?,?,?)",
            (sector.strip(), client.strip(), project_name.strip(), file_type.strip()),
        )
        pid = cur.lastrowid
        con.executemany(
            "INSERT INTO activity_dmia (project_id, activity_name, departement, dmia_exprimee, dmia_minutes) VALUES (?,?,?,?,?)",
            [
                (
                    pid,
                    a.get("activity_name", ""),
                    a.get("departement", ""),
                    a.get("dmia_exprimee", ""),
                    int(a.get("dmia_minutes", -1)),
                )
                for a in activities
                if a.get("activity_name", "").strip()
            ],
        )
    return pid


def delete_project(project_id: int) -> None:
    init_db()
    with _conn() as con:
        con.execute("DELETE FROM projects WHERE id = ?", (project_id,))


def store_fiche_equipment(project_id: int, entity: str, items: list[dict]) -> None:
    """
    Persist equipment items for a fiche.
    items: [{"name": str, "horizons": {"H+4": 2, ...}}, ...]
    """
    import json as _json
    init_db()
    with _conn() as con:
        con.execute("DELETE FROM fiche_equipment WHERE project_id=?", (project_id,))
        con.executemany(
            "INSERT INTO fiche_equipment (project_id, entity, designation, horizons) VALUES (?,?,?,?)",
            [(project_id, entity, item["name"], _json.dumps(item.get("horizons", {})))
             for item in items if item.get("name")],
        )


def get_equipment_by_filter(sector: str = "", client: str = "") -> list[dict]:
    """
    Return all stored fiche equipment rows matching sector/client filters.
    Each row: {project_id, sector, client, project_name, entity, designation, horizons}
    """
    import json as _json
    init_db()
    q = """
        SELECT p.id as project_id, p.sector, p.client, p.project_name,
               fe.entity, fe.designation, fe.horizons
        FROM fiche_equipment fe
        JOIN projects p ON fe.project_id = p.id
        WHERE 1=1
    """
    params: list = []
    if sector:
        q += " AND p.sector = ? COLLATE NOCASE"
        params.append(sector)
    if client:
        q += " AND p.client = ? COLLATE NOCASE"
        params.append(client)
    q += " ORDER BY p.client, fe.designation"

    with _conn() as con:
        rows = con.execute(q, params).fetchall()

    return [
        {
            "project_id":   r["project_id"],
            "sector":       r["sector"],
            "client":       r["client"],
            "project_name": r["project_name"],
            "entity":       r["entity"],
            "designation":  r["designation"],
            "horizons":     _json.loads(r["horizons"] or "{}"),
        }
        for r in rows
    ]


def link_fiche_to_synthese(synthese_id: int, fiche_id: int) -> None:
    init_db()
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO synthese_fiche_links (synthese_project_id, fiche_project_id) VALUES (?,?)",
            (synthese_id, fiche_id),
        )


def unlink_fiche_from_synthese(synthese_id: int, fiche_id: int) -> None:
    init_db()
    with _conn() as con:
        con.execute(
            "DELETE FROM synthese_fiche_links WHERE synthese_project_id=? AND fiche_project_id=?",
            (synthese_id, fiche_id),
        )


def get_linked_fiches(synthese_id: int) -> list[dict]:
    """Return all fiche projects linked to a synthèse."""
    init_db()
    with _conn() as con:
        rows = con.execute("""
            SELECT p.id, p.sector, p.client, p.project_name, p.file_type
            FROM synthese_fiche_links sfl
            JOIN projects p ON sfl.fiche_project_id = p.id
            WHERE sfl.synthese_project_id = ?
            ORDER BY p.client, p.project_name
        """, (synthese_id,)).fetchall()
    return [dict(r) for r in rows]


def get_synthese_projects(sector: str = "", client: str = "") -> list[dict]:
    """Return projects with file_type='synthese', optionally filtered."""
    init_db()
    q = "SELECT id, sector, client, project_name, file_type FROM projects WHERE file_type='synthese'"
    params: list = []
    if sector:
        q += " AND sector=? COLLATE NOCASE"; params.append(sector)
    if client:
        q += " AND client=? COLLATE NOCASE"; params.append(client)
    q += " ORDER BY sector, client, project_name"
    with _conn() as con:
        rows = con.execute(q, params).fetchall()
    return [dict(r) for r in rows]


def get_project_file_path(project_id: int) -> Optional[Path]:
    """Locate the actual file on disk for a project."""
    init_db()
    with _conn() as con:
        row = con.execute(
            "SELECT sector, client, project_name, file_type FROM projects WHERE id=?",
            (project_id,)
        ).fetchone()
    if not row:
        return None

    ext = ".xlsx" if row["file_type"] == "synthese" else ".docx"
    pname_low = row["project_name"].lower().strip()

    # Folders to search in priority order
    # fiche_editor projects may have their source docx in 'fiche/' folder
    search_types = [row["file_type"]]
    if row["file_type"] == "fiche_editor":
        search_types.append("fiche")

    for ftype in search_types:
        folder = project_dir(row["sector"], row["client"], ftype)
        # Exact match
        exact = folder / row["project_name"]
        if exact.exists():
            return exact
        # Case-insensitive / accent-insensitive match
        for f in folder.glob(f"*{ext}"):
            if f.name.lower().strip() == pname_low:
                return f
        # Accent-stripped match
        from bia_etl import _strip_accents
        pname_stripped = _strip_accents(pname_low)
        for f in folder.glob(f"*{ext}"):
            if _strip_accents(f.name.lower().strip()) == pname_stripped:
                return f

    # Last resort: single file in primary folder
    folder = project_dir(row["sector"], row["client"], row["file_type"])
    all_files = list(folder.glob(f"*{ext}"))
    return all_files[0] if len(all_files) == 1 else None


def list_sectors_clients() -> dict:
    """Return {sector: [client, ...]} for populating filter dropdowns."""
    init_db()
    with _conn() as con:
        rows = con.execute(
            "SELECT DISTINCT sector, client FROM projects ORDER BY sector, client"
        ).fetchall()
    tree: dict = {}
    for r in rows:
        tree.setdefault(r["sector"], [])
        if r["client"] not in tree[r["sector"]]:
            tree[r["sector"]].append(r["client"])
    return tree


# ── read ───────────────────────────────────────────────────────────────────────

def list_projects() -> dict:
    """
    Returns {sector: {client: [project_row, ...]}} nested dict.
    """
    init_db()
    with _conn() as con:
        rows = con.execute("""
            SELECT p.*, COUNT(ad.id) AS activity_count
            FROM projects p
            LEFT JOIN activity_dmia ad ON ad.project_id = p.id
            GROUP BY p.id
            ORDER BY p.sector, p.client, p.created_at DESC
        """).fetchall()

    tree: dict = {}
    for r in rows:
        s, cl = r["sector"], r["client"]
        tree.setdefault(s, {}).setdefault(cl, []).append({
            "id":             r["id"],
            "project_name":   r["project_name"],
            "file_type":      r["file_type"],
            "activity_count": r["activity_count"],
            "created_at":     r["created_at"],
        })
    return tree


def get_dmia_suggestions(
    activity_name: str,
    sector: str = "",
    limit: int = 8,
) -> list[dict]:
    """
    Return DMIA suggestions for a given activity name.
    Tries exact match first, then meaningful-keyword match.
    Only returns entries with a valid parsed DMIA (dmia_minutes > 0).
    Results sorted by dmia_minutes ascending.
    """
    init_db()
    if not activity_name.strip():
        return []

    # French + BIA-noise stop words — never useful as search keywords
    _STOP = {
        "les", "des", "une", "dans", "pour", "par", "avec", "sur", "son",
        "ses", "leur", "leurs", "aux", "que", "qui", "est", "sont", "ont",
        "cette", "ces", "tout", "plus", "mais", "aussi", "donc", "ainsi",
        "lors", "sous", "entre", "vers", "sans", "bien", "peu", "non",
        "oui", "via", "chez", "etc",
    }

    def _keywords(name: str) -> list[str]:
        """Words longer than 3 chars that carry meaning."""
        return [w for w in name.lower().split() if len(w) > 3 and w not in _STOP]

    # Only suggest entries with a parseable DMIA time value (>= 0; -1 = unrecognised)
    _VALID = "ad.dmia_minutes >= 0"

    with _conn() as con:
        seen_dmia: set = set()
        results: list[dict] = []

        # ── exact match ────────────────────────────────────────
        q_exact = """
            SELECT ad.activity_name, ad.dmia_exprimee, ad.dmia_minutes,
                   p.client, p.sector, p.project_name,
                   COUNT(*) AS freq
            FROM activity_dmia ad
            JOIN projects p ON ad.project_id = p.id
            WHERE ad.activity_name = ? COLLATE NOCASE
              AND {valid}
              {sector_filter}
            GROUP BY ad.dmia_exprimee
            ORDER BY freq DESC, ad.dmia_minutes ASC
            LIMIT ?
        """.format(
            valid=_VALID,
            sector_filter="AND p.sector = ? COLLATE NOCASE" if sector else "",
        )

        params_exact = [activity_name] + ([sector] if sector else []) + [limit]
        for row in con.execute(q_exact, params_exact):
            d = row["dmia_exprimee"]
            if d not in seen_dmia:
                seen_dmia.add(d)
                results.append(dict(row))

        # ── keyword match if not enough ─────────────────────────
        if len(results) < limit:
            words = _keywords(activity_name)
            if words:
                like_parts = " OR ".join(
                    "LOWER(ad.activity_name) LIKE ?" for _ in words
                )
                params_kw = [f"%{w}%" for w in words]
                if sector:
                    params_kw.append(sector)
                params_kw.append(limit)

                q_kw = f"""
                    SELECT ad.activity_name, ad.dmia_exprimee, ad.dmia_minutes,
                           p.client, p.sector, p.project_name,
                           COUNT(*) AS freq
                    FROM activity_dmia ad
                    JOIN projects p ON ad.project_id = p.id
                    WHERE ({like_parts})
                      AND {_VALID}
                      {"AND p.sector = ? COLLATE NOCASE" if sector else ""}
                    GROUP BY ad.activity_name, ad.dmia_exprimee
                    ORDER BY freq DESC, ad.dmia_minutes ASC
                    LIMIT ?
                """
                for row in con.execute(q_kw, params_kw):
                    d = row["dmia_exprimee"]
                    if d not in seen_dmia:
                        seen_dmia.add(d)
                        results.append(dict(row))

    results.sort(key=lambda x: x.get("dmia_minutes", 999_999))
    return results[:limit]


def get_lower_dmia_alerts(
    activities: list[dict],
    sector: str = "",
) -> list[dict]:
    """
    For each activity that already has a DMIA set, check whether any similar
    activity in the DB has a *lower* DMIA.
    activities items: {activity_name, dmia_exprimee, dmia_minutes}
    Returns list of alert dicts (empty = no alerts).
    """
    init_db()
    alerts: list[dict] = []

    with _conn() as con:
        for act in activities:
            name      = act.get("activity_name", "").strip()
            cur_mins  = int(act.get("dmia_minutes", -1))
            cur_dmia  = act.get("dmia_exprimee", "")

            if not name or cur_mins < 0:
                continue

            words = [w for w in name.lower().split() if len(w) > 2]
            if not words:
                continue

            like_parts = " OR ".join("LOWER(ad.activity_name) LIKE ?" for _ in words)
            params = [f"%{w}%" for w in words]
            if sector:
                params.append(sector)
            params.append(cur_mins)

            row = con.execute(f"""
                SELECT ad.activity_name, ad.dmia_exprimee, ad.dmia_minutes,
                       p.client, p.sector
                FROM activity_dmia ad
                JOIN projects p ON ad.project_id = p.id
                WHERE ({like_parts})
                  {"AND p.sector = ? COLLATE NOCASE" if sector else ""}
                  AND ad.dmia_minutes >= 0
                  AND ad.dmia_minutes < ?
                ORDER BY ad.dmia_minutes ASC
                LIMIT 1
            """, params).fetchone()

            if row:
                alerts.append({
                    "activity_name":        name,
                    "current_dmia":         cur_dmia,
                    "current_dmia_minutes": cur_mins,
                    "lower_dmia":           row["dmia_exprimee"],
                    "lower_dmia_minutes":   row["dmia_minutes"],
                    "source_client":        row["client"],
                    "source_sector":        row["sector"],
                    "source_activity":      row["activity_name"],
                })

    return alerts


# ── fiche-editor save / load ───────────────────────────────────────────────────

def _ensure_form_col() -> None:
    """One-time migration: add form_data_path column when it doesn't yet exist."""
    try:
        with _conn() as con:
            con.execute(
                "ALTER TABLE projects ADD COLUMN form_data_path TEXT DEFAULT ''"
            )
    except Exception:
        pass  # column already present


def save_fiche_form(
    sector: str,
    client: str,
    project_name: str,
    form_data: dict,
    project_id: Optional[int] = None,
) -> int:
    """
    Persist a fiche-editor session.
    * Creates a new project row when project_id is None.
    * Updates the existing row (and refreshes activity_dmia) when project_id
      points to an existing record.
    Returns the project_id.
    """
    import json as _json_mod
    init_db()
    _ensure_form_col()

    folder = project_dir(sector, client, "fiche_editor")

    with _conn() as con:
        # ── upsert project row ─────────────────────────────────────
        if project_id is not None:
            exists = con.execute(
                "SELECT id FROM projects WHERE id=?", (project_id,)
            ).fetchone()
            if not exists:
                project_id = None  # record was deleted — recreate

        if project_id is None:
            cur = con.execute(
                "INSERT INTO projects (sector, client, project_name, file_type) "
                "VALUES (?,?,?,?)",
                (sector.strip(), client.strip(), project_name.strip(), "fiche_editor"),
            )
            project_id = cur.lastrowid
        else:
            con.execute(
                "UPDATE projects "
                "SET sector=?, client=?, project_name=?, file_type='fiche_editor' "
                "WHERE id=?",
                (sector.strip(), client.strip(), project_name.strip(), project_id),
            )
            con.execute(
                "DELETE FROM activity_dmia WHERE project_id=?", (project_id,)
            )

        # ── store activity/DMIA pairs for suggestions ──────────────
        from fiche_writer import parse_dmia_minutes
        act_rows = [
            (
                project_id,
                a.get("name", "").strip(),
                "",   # département
                a.get("dmia_exprimee", "").strip(),
                parse_dmia_minutes(a.get("dmia_exprimee", "") or ""),
            )
            for a in form_data.get("activities", [])
            if a.get("name", "").strip()
        ]
        if act_rows:
            con.executemany(
                "INSERT INTO activity_dmia "
                "(project_id, activity_name, departement, dmia_exprimee, dmia_minutes) "
                "VALUES (?,?,?,?,?)",
                act_rows,
            )

        # ── write JSON file ────────────────────────────────────────
        json_path = folder / f"form_{project_id}.json"
        con.execute(
            "UPDATE projects SET form_data_path=? WHERE id=?",
            (str(json_path), project_id),
        )

    json_path = folder / f"form_{project_id}.json"
    json_path.write_text(
        _json_mod.dumps(form_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return project_id


def load_fiche_form(project_id: int) -> Optional[dict]:
    """Return the saved form_data dict for a project, or None."""
    import json as _json_mod
    init_db()
    _ensure_form_col()

    with _conn() as con:
        row = con.execute(
            "SELECT form_data_path, sector, client, project_name "
            "FROM projects WHERE id=?",
            (project_id,),
        ).fetchone()

    if not row or not row["form_data_path"]:
        return None
    path = Path(row["form_data_path"])
    if not path.exists():
        return None

    data = _json_mod.loads(path.read_text(encoding="utf-8"))
    # Inject meta so the editor can restore its save-modal defaults
    data["_meta"] = {
        "project_id":   project_id,
        "sector":       row["sector"],
        "client":       row["client"],
        "project_name": row["project_name"],
    }
    return data


def list_fiche_editor_projects(sector: str = "") -> list[dict]:
    """
    Return ALL fiche-type projects grouped by client, for the selector browser.
    Includes:
      - 'fiche_editor'  → created/saved through the web editor  (has form JSON)
      - 'fiche'         → uploaded .docx files via the projects page
    Excludes 'synthese' (Excel synthesis files — not editable as fiches).
    """
    init_db()
    _ensure_form_col()

    q = """
        SELECT id, sector, client, project_name, file_type,
               created_at, form_data_path
        FROM projects
        WHERE file_type IN ('fiche_editor', 'fiche')
    """
    params: list = []
    if sector:
        q += " AND sector = ? COLLATE NOCASE"
        params.append(sector)
    q += " ORDER BY client, created_at DESC"

    with _conn() as con:
        rows = con.execute(q, params).fetchall()

    by_client: dict = {}
    for r in rows:
        fp        = r["form_data_path"] or ""
        has_json  = bool(fp) and Path(fp).exists()
        # fiche_editor with no JSON on disk = deleted externally, skip
        if r["file_type"] == "fiche_editor" and not has_json:
            continue
        cl = r["client"]
        by_client.setdefault(cl, []).append({
            "id":           r["id"],
            "project_name": r["project_name"],
            "created_at":   r["created_at"],
            "sector":       r["sector"],
            "file_type":    r["file_type"],  # 'fiche_editor' | 'fiche'
            "has_json":     has_json,        # True → full editor restore possible
        })

    return [{"client": c, "projects": ps} for c, ps in by_client.items()]


def get_project_meta(project_id: int) -> Optional[dict]:
    """Return basic metadata for any project (used for the editor context bar)."""
    init_db()
    _ensure_form_col()
    with _conn() as con:
        row = con.execute(
            "SELECT id, sector, client, project_name, file_type, form_data_path "
            "FROM projects WHERE id=?",
            (project_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "id":           row["id"],
        "sector":       row["sector"],
        "client":       row["client"],
        "project_name": row["project_name"],
        "file_type":    row["file_type"],
        "has_json":     bool(row["form_data_path"]) and Path(row["form_data_path"]).exists(),
    }


def get_fiche_docx_form_data(project_id: int) -> Optional[dict]:
    """
    For an uploaded-fiche project (file_type='fiche'), locate its .docx on disk
    and return a complete form_data dict (same structure as the editor payload)
    by calling extract_full_form_from_fiche().
    Returns None if the file is not found.
    """
    init_db()
    with _conn() as con:
        row = con.execute(
            "SELECT sector, client, project_name, file_type FROM projects WHERE id=?",
            (project_id,),
        ).fetchone()
    if not row or row["file_type"] != "fiche":
        return None

    folder = project_dir(row["sector"], row["client"], "fiche")
    if not folder.exists():
        return None

    # Try to match the exact file by project_name (which stores the original filename)
    pname = row["project_name"]
    docx_path: Optional[Path] = None
    exact = folder / pname
    if exact.exists():
        docx_path = exact
    else:
        # Fallback: case-insensitive name match (handles encoding differences)
        pname_low = pname.lower().strip()
        for f in folder.glob("*.docx"):
            if f.name.lower().strip() == pname_low:
                docx_path = f
                break
    if docx_path is None:
        # Last resort: most recently modified file (only if there is exactly one)
        all_docx = list(folder.glob("*.docx"))
        if len(all_docx) == 1:
            docx_path = all_docx[0]
        else:
            return None
    try:
        form = extract_full_form_from_fiche(docx_path)
        form["_meta"] = {
            "project_id":   project_id,
            "sector":       row["sector"],
            "client":       row["client"],
            "project_name": row["project_name"],
            "source":       "docx_import",
        }
        return form
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return None


def extract_full_form_from_fiche(fiche_path: Path) -> dict:
    """
    Parse a BIA fiche .docx and return a complete form_data dict that maps
    1-to-1 with the web editor's payload (all sections: suivi, entity,
    participants, activities, impacts, DMIA, échanges, montée en charge,
    collaborateurs, applications, équipements, documents, observations).

    Identification strategy: tables are detected by their header row content,
    not by index — so the extractor works regardless of table ordering or
    whether optional sections are present.
    """
    import re
    from docx import Document
    from fiche_writer import parse_dmia_minutes

    doc = Document(str(fiche_path))

    # ── helpers ────────────────────────────────────────────────────────────────
    def row_cells(row) -> list[str]:
        """Return unique cell texts (dedup merged cells by object identity)."""
        seen: set = set()
        out: list[str] = []
        for cell in row.cells:
            if id(cell) not in seen:
                seen.add(id(cell))
                out.append(cell.text.strip())
        return out

    def is_junk(v: str) -> bool:
        return v.lower().strip() in ("", "xx", "x", "-", "--", "na", "n/a",
                                     "nd", "nc", "non applicable", "none")

    IMPACT_LABELS = [
        "Image de marque",
        "Désorganisation interne",
        "Juridique et réglementaire",
        "Financier",
    ]

    # ── result skeleton ────────────────────────────────────────────────────────
    form: dict = {
        "suivi":   {"entite": "", "redacteur": "", "version": "",
                    "date_maj": "", "reference": ""},
        "entity":  {"nom_responsable": "", "organisation": "",
                    "contraintes": "", "periodes_critiques": "",
                    "historique_interruptions": ""},
        "participants":      [],
        "activities":        [],
        "echanges":          [],
        "montee_en_charge":  {},
        "collaborateurs_cles": [],
        "applications":      [],
        "app_availability":  [],
        "documents":         [],
        "observations":      "",
    }

    # ── scan body for observations text (after the "Observations" heading) ─────
    obs_lines: list[str] = []
    in_obs = False
    from docx.oxml.ns import qn
    for child in doc.element.body:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag == "p":
            from docx.text.paragraph import Paragraph
            para = Paragraph(child, doc)
            txt = para.text.strip()
            style_name = para.style.name.lower() if para.style else ""
            if "heading" in style_name and "observation" in txt.lower():
                in_obs = True
                continue
            if in_obs and txt:
                obs_lines.append(txt)
        elif tag == "tbl" and in_obs:
            break  # stop at next table after observations heading
    form["observations"] = "\n".join(obs_lines)

    # ── deferred: impact tables to match to activities later ──────────────────
    impact_tables: list[dict] = []   # [{act_name, impacts{}}]

    # ── iterate all tables ─────────────────────────────────────────────────────
    for table in doc.tables:
        if not table.rows:
            continue
        r0 = row_cells(table.rows[0])
        r0_txt = " ".join(r0).lower()

        # ══ 1.  FICHE DE SUIVI / PARTICIPANTS ════════════════════════════════
        # Identified by: first row has exactly 2 unique cells, col0="Entité"
        if r0 and "entité" in r0[0].lower() and len(r0) <= 3:
            if len(r0) >= 2 and not is_junk(r0[1]):
                form["suivi"]["entite"] = r0[1]

            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                c0 = cells[0].strip()
                c0l = c0.lower()

                # Participant row: col0 = "Présents" or blank, col1 = name
                if ("présent" in c0l or c0l == "") and len(cells) >= 3:
                    nom = cells[1].strip()
                    fct = cells[2].strip() if len(cells) > 2 else ""
                    # skip header row ("Nom", "Fonction") and placeholders
                    if nom and nom.lower() not in ("nom", "xx", ""):
                        form["participants"].append({
                            "nom": nom,
                            "fonction": fct,
                            "present": c0 if c0 else "Présents",
                        })
                elif "rédacteur" in c0l or "redacteur" in c0l:
                    if len(cells) >= 2 and not is_junk(cells[1]):
                        form["suivi"]["redacteur"] = cells[1]
                elif c0l == "version":
                    if len(cells) >= 2 and not is_junk(cells[1]):
                        form["suivi"]["version"] = cells[1]
                elif "mise à jour" in c0l or "mise a jour" in c0l:
                    if len(cells) >= 2 and not is_junk(cells[1]):
                        form["suivi"]["date_maj"] = cells[1]
                elif "référence" in c0l or "reference" in c0l:
                    if len(cells) >= 2 and not is_junk(cells[1]):
                        form["suivi"]["reference"] = cells[1]
            continue

        # ══ 2.  FICHE D'IDENTITÉ ══════════════════════════════════════════════
        # Identified by: col0 contains "responsable" and at least 5 rows
        if (r0 and "responsable" in r0[0].lower()
                and len(table.rows) >= 4 and len(table.columns) >= 2):
            field_map = [
                ("responsable",   "nom_responsable"),
                ("organisation",  "organisation"),
                ("contrainte",    "contraintes"),
                ("période",       "periodes_critiques"),
                ("périodes",      "periodes_critiques"),
                ("historique",    "historique_interruptions"),
            ]
            for row in table.rows:
                cells = row_cells(row)
                if len(cells) < 2:
                    continue
                c0l = cells[0].lower()
                val = cells[1].strip()
                if is_junk(val):
                    continue
                for kw, field in field_map:
                    if kw in c0l:
                        form["entity"][field] = val
                        break
            continue

        # ══ 2b.  PRÉSENTATION GÉNÉRALE DES ACTIVITÉS (§3) ═══════════════════
        # Identified by: R0[0]="Activité", R0[1]="Présentation/Description/
        #   Sous-processus", 2 cols, no "ressource"/"criticit" in headers
        if (r0 and "activit" in r0[0].lower()
                and len(r0) in (2, 3)
                and not any("ressource" in c.lower() or "criticit" in c.lower()
                            for c in r0)
                and any("présent" in c.lower() or "description" in c.lower()
                        or "sous" in c.lower() or "général" in c.lower()
                        or "general" in c.lower()
                        for c in r0[1:])):
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                name = cells[0].strip()
                if not name or is_junk(name) or name.lower() in ("activité", "activite"):
                    continue
                desc = cells[1].strip() if len(cells) > 1 else ""
                if is_junk(desc):
                    desc = ""
                # Match to existing activity, or create new one
                matched = False
                for act in form["activities"]:
                    nl, pl = act["name"].lower().strip(), name.lower().strip()
                    if nl == pl or pl in nl or nl in pl:
                        if desc:
                            act["description"] = desc
                        matched = True
                        break
                if not matched and name:
                    form["activities"].append({
                        "name": name, "description": desc,
                        "ressources_utilisees": "", "periode_critique": "",
                        "criticite": "", "dmia_exprimee": "",
                        "premieres_actions": "",
                        "impacts": {lbl: {"A": "", "B": ""} for lbl in IMPACT_LABELS},
                    })
            continue

        # ══ 3.  DESCRIPTION ET CRITICITÉ DES ACTIVITÉS ═══════════════════════
        # Identified by: R0 has "Activité" col + "Ressources" or "Criticité"
        if (r0 and "activit" in r0[0].lower()
                and len(r0) >= 3
                and any("ressource" in c.lower() or "criticit" in c.lower()
                        for c in r0)):
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                name = cells[0].strip()
                if not name or name.lower() in ("activité", "activite", "xx", ""):
                    continue
                form["activities"].append({
                    "name":                 name,
                    "description":          "",
                    "ressources_utilisees": cells[1].strip() if len(cells) > 1 else "",
                    "periode_critique":     cells[2].strip() if len(cells) > 2 else "",
                    "criticite":            cells[3].strip() if len(cells) > 3 else "",
                    "dmia_exprimee":        "",
                    "premieres_actions":    "",
                    "impacts":              {lbl: {"A": "", "B": ""} for lbl in IMPACT_LABELS},
                })
            continue

        # ══ 4.  IMPACT PAR ACTIVITÉ ═══════════════════════════════════════════
        # Identified by: "Interruption" appears in R0 (col1 or col2)
        # AND the table has ~5 rows and ~3 cols
        if ("interruption" in r0_txt
                and len(table.rows) >= 4 and len(table.columns) >= 3):
            act_name = r0[0].strip() if r0 else ""
            # Nouveau format : l'en-tête ne porte plus le nom de l'activité
            # mais un libellé générique ("Impacts / Sévérité"), qui se
            # retrouvait ajouté comme activité fantôme.
            _an = act_name.lower()
            if ("impact" in _an and ("sévérité" in _an or "severite" in _an
                                     or "/" in act_name)) or not act_name:
                act_name = ""
            impacts: dict = {lbl: {"A": "", "B": ""} for lbl in IMPACT_LABELS}
            kw_map = {
                "image":        "Image de marque",
                "désorganisat": "Désorganisation interne",
                "desorganisat": "Désorganisation interne",
                "juridique":    "Juridique et réglementaire",
                "financier":    "Financier",
            }
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                c0l = cells[0].lower()
                for kw, lbl in kw_map.items():
                    if kw in c0l:
                        a_val = cells[1].strip() if len(cells) > 1 else ""
                        b_val = cells[2].strip() if len(cells) > 2 else ""
                        impacts[lbl] = {
                            "A": "" if is_junk(a_val) else a_val,
                            "B": "" if is_junk(b_val) else b_val,
                        }
                        break
            if act_name:
                impact_tables.append({"act_name": act_name, "impacts": impacts})
            continue

        # ══ 5.  DMIA PAR PROCESSUS ════════════════════════════════════════════
        # Le nouveau format intitule la première colonne "Désignation de
        # l'activité" et non "Processus" : sans cette tolérance, aucune DMIA
        # n'était rattachée aux activités et l'éditeur les effaçait à
        # l'enregistrement.
        if (r0 and any("dmia" in c.lower() for c in r0)
                and any(k in r0[0].lower() for k in
                        ("processus", "désignation", "designation", "activit"))):
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                proc = cells[0].strip()
                if not proc or is_junk(proc):
                    continue
                dmia    = (cells[1].strip() if len(cells) > 1 else "")
                actions = (cells[2].strip() if len(cells) > 2 else "")
                if is_junk(dmia):
                    dmia = ""
                if is_junk(actions):
                    actions = ""

                # Try exact match, then substring match
                matched = False
                for act in form["activities"]:
                    if act["name"].strip().lower() == proc.lower():
                        act["dmia_exprimee"] = dmia
                        act["premieres_actions"] = actions
                        matched = True
                        break
                if not matched:
                    for act in form["activities"]:
                        nl, pl = act["name"].lower(), proc.lower()
                        if pl in nl or nl in pl:
                            act["dmia_exprimee"] = dmia
                            act["premieres_actions"] = actions
                            matched = True
                            break
                if not matched and proc:
                    # Activity only in DMIA table — add it
                    form["activities"].append({
                        "name":                 proc,
                        "description":          "",
                        "ressources_utilisees": "",
                        "periode_critique":     "",
                        "criticite":            "",
                        "dmia_exprimee":        dmia,
                        "premieres_actions":    actions,
                        "impacts":              {lbl: {"A": "", "B": ""} for lbl in IMPACT_LABELS},
                    })
            continue

        # ══ 6.  ÉCHANGES D'INFORMATION ════════════════════════════════════════
        # Identified by: R0[0] contains "groupes" or "correspondants", 5 cols
        if (r0 and ("groupes" in r0[0].lower() or "correspondants" in r0[0].lower())
                and len(table.columns) >= 4):
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                groupes = cells[0].strip()
                if is_junk(groupes):
                    continue
                form["echanges"].append({
                    "groupes":     groupes,
                    "ie":          cells[1].strip() if len(cells) > 1 else "",
                    "type_info":   cells[2].strip() if len(cells) > 2 else "",
                    "tr":          cells[3].strip() if len(cells) > 3 else "",
                    "ressources_si": cells[4].strip() if len(cells) > 4 else "",
                })
            continue

        # ══ 7.  MONTÉE EN CHARGE ══════════════════════════════════════════════
        # Identified by: R0[0] contains "montée en charge" or "effectif"
        # with multiple time-period columns
        if (r0 and len(table.columns) >= 5
                and ("montée en charge" in r0_txt or "montee en charge" in r0_txt
                     or ("effectif" in r0[0].lower() and len(r0) > 4))):
            # Collect column headers (skip col0 = metric label)
            time_cols = []
            for c in r0[1:]:
                label = c.strip()
                if label and label.lower() != "commentaires":
                    time_cols.append(label)

            metric_map = {
                "% effectif":         "% Effectif cumulé",
                "effectif cumulé":    "Effectif cumulé",
                "effectif cumule":    "Effectif cumulé",
                "positions cumulées": "Positions cumulées",
                "positions cumulees": "Positions cumulées",
                "télétravail cumulé": "Télétravail cumulé",
                "teletravail cumule": "Télétravail cumulé",
                "effectif":           "Effectif",
                "positions":          "Positions",
                "télétravail":        "Télétravail",
                "teletravail":        "Télétravail",
            }
            montee: dict = {}
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                metric = cells[0].strip()
                if not metric:
                    continue
                ml = metric.lower()
                canonical = None
                # Longest key match first (so "effectif cumulé" beats "effectif")
                for kw in sorted(metric_map.keys(), key=len, reverse=True):
                    if kw in ml:
                        canonical = metric_map[kw]
                        break
                if not canonical:
                    continue
                row_data: dict = {}
                for i, tc in enumerate(time_cols):
                    val = cells[i + 1].strip() if (i + 1) < len(cells) else ""
                    row_data[tc] = "" if is_junk(val) else val
                montee[canonical] = row_data

            if montee:
                form["montee_en_charge"] = montee
            continue

        # ══ 8.  COLLABORATEURS CLÉS ═══════════════════════════════════════════
        # Identified by: R0 has "Fonction", "Nom", "Prénom" / "Suppléant"
        if (r0 and "fonction" in r0_txt
                and "nom" in r0_txt
                and ("prénom" in r0_txt or "prenom" in r0_txt
                     or "suppléant" in r0_txt or "suppleant" in r0_txt)):
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                fct = cells[0].strip()
                if is_junk(fct):
                    continue
                form["collaborateurs_cles"].append({
                    "fonction":   fct,
                    "nom":        cells[1].strip() if len(cells) > 1 else "",
                    "prenom":     cells[2].strip() if len(cells) > 2 else "",
                    "suppleants": cells[-1].strip() if len(cells) > 3 else "",
                })
            continue

        # ══ 9.  APPLICATIONS INFORMATIQUES ════════════════════════════════════
        # Identified by: R0[0]="Application", R0 has "Criticité" and "DMIA"
        if (r0 and "application" in r0[0].lower()
                and any("criticit" in c.lower() for c in r0)
                and any("dmia" in c.lower() for c in r0)):
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                app = cells[0].strip()
                if is_junk(app):
                    continue
                form["applications"].append({
                    "application": app,
                    "criticite":   cells[1].strip() if len(cells) > 1 else "",
                    "dmia":        cells[2].strip() if len(cells) > 2 else "",
                    "pmdt":        cells[3].strip() if len(cells) > 3 else "",
                    "commentaires": cells[4].strip() if len(cells) > 4 else "",
                })
            continue

        # ══ 10. ÉQUIPEMENTS / APP_AVAILABILITY ════════════════════════════════
        # Identified by: R0[0]="Désignation" + time columns (H+N / J+N pattern)
        if (r0 and "désignation" in r0[0].lower()
                and any(re.match(r"^[HhJj]\+?\d", c) for c in r0[1:])):
            time_cols_eq = [c.strip() for c in r0[1:] if c.strip()]
            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                desig = cells[0].strip()
                if is_junk(desig):
                    continue
                entry: dict = {"designation": desig}
                for i, tc in enumerate(time_cols_eq):
                    val = cells[i + 1].strip() if (i + 1) < len(cells) else ""
                    entry[tc] = "" if is_junk(val) else val
                form["app_availability"].append(entry)
            continue

        # ══ 11. DOCUMENTS ET FICHIERS CRITIQUES ═══════════════════════════════
        # Repérage et colonnes par en-tête, pas par position : le nouveau
        # format place "Processus" en première colonne — le nom du document
        # passe alors en colonne 1 — et ajoute "Localisation". Avec l'ancienne
        # lecture positionnelle, aucun document n'était remonté.
        if (r0
                and ("document" in r0_txt or "fichier" in r0_txt)
                and len(table.columns) >= 3
                and "stockage" in r0_txt):

            def _doc_col(*keywords):
                for ci, h in enumerate(r0):
                    hl = h.lower()
                    if any(k in hl for k in keywords):
                        return ci
                return -1

            ci_doc   = _doc_col("document", "fichier", "data")
            ci_stock = _doc_col("stockage")
            ci_dupl  = _doc_col("duplication")
            ci_loc   = _doc_col("localisation")
            ci_proc  = _doc_col("processus")
            if ci_doc < 0:
                ci_doc = 0

            def _dc(cells, idx):
                return cells[idx].strip() if 0 <= idx < len(cells) else ""

            for row in table.rows[1:]:
                cells = row_cells(row)
                if not cells:
                    continue
                doc_name = _dc(cells, ci_doc)
                if is_junk(doc_name):
                    continue
                form["documents"].append({
                    "document":     doc_name,
                    "stockage":     _dc(cells, ci_stock),
                    "duplication":  _dc(cells, ci_dupl),
                    "localisation": _dc(cells, ci_loc),
                    "processus":    _dc(cells, ci_proc),
                })
            continue

    # ── Assign impact tables to activities (by R0 activity name) ──────────────
    for imp in impact_tables:
        aname = imp["act_name"].lower().strip()
        matched = False
        for act in form["activities"]:
            nl = act["name"].lower().strip()
            if nl == aname or aname in nl or nl in aname:
                act["impacts"] = imp["impacts"]
                matched = True
                break
        if not matched and aname:
            # Add as new activity (rare — impact table with no matching §4 row)
            form["activities"].append({
                "name":                 imp["act_name"],
                "description":          "",
                "ressources_utilisees": "",
                "periode_critique":     "",
                "criticite":            "",
                "dmia_exprimee":        "",
                "premieres_actions":    "",
                "impacts":              imp["impacts"],
            })

    return form


# ── DMIA extraction helpers ────────────────────────────────────────────────────

def extract_dmias_from_fiche(fiche_path: Path) -> list[dict]:
    """
    Open a BIA fiche .docx and extract activity/DMIA pairs from §5.3.
    Returns list of {activity_name, dmia_exprimee, dmia_minutes}.
    """
    from docx import Document
    from fiche_writer import parse_dmia_minutes

    doc = Document(str(fiche_path))
    results: list[dict] = []

    for table in doc.tables:
        if not table.rows:
            continue
        header = " ".join(c.text.strip() for c in table.rows[0].cells).lower()
        has_dmia = "dmia" in header
        has_activity_col = any(kw in header for kw in ("processus", "désignation", "designat", "activit"))
        if has_dmia and has_activity_col:
            # Find which column index is the activity name and which is the DMIA
            col_headers = [c.text.strip().lower() for c in table.rows[0].cells]
            act_idx, dmia_idx = 0, 1  # defaults
            for ci, ch in enumerate(col_headers):
                if any(kw in ch for kw in ("processus", "désignation", "designat", "activit")):
                    act_idx = ci
                if "dmia" in ch:
                    dmia_idx = ci
            for row in table.rows[1:]:
                cells = row.cells
                if len(cells) <= max(act_idx, dmia_idx):
                    continue
                activity = cells[act_idx].text.strip()
                dmia     = cells[dmia_idx].text.strip()
                skip_vals = ("xx", "processus", "désignation de l'activité", "")
                if activity and dmia and activity.lower() not in skip_vals:
                    results.append({
                        "activity_name": activity,
                        "dmia_exprimee": dmia,
                        "dmia_minutes":  parse_dmia_minutes(dmia),
                    })
            break   # only one §5.3 table

    return results


def extract_dmias_from_synthese(synthese_path: Path) -> list[dict]:
    """
    Open a BIA synthèse .xlsx and extract activity/DMIA data.

    Priority rule:
      DMIA Arbitrée (1)  >  DMIA Préconisée/Recommandée (2)
        >  DMIA Exprimée (3)  >  plain DMIA (4)

    Design goals:
      - Accept *any* Devoteam synthèse layout (Impact DMIA, dedicated DMIA
        sheets, Applications IT, Macro Process sheets, …)
      - Never mistake a title row for a header row
      - Never store non-duration values (equipment counts, score columns, etc.)
      - Robust against merged cells, deep headers (up to row 35), and duplicate
        (activity, dmia) pairs across sheets
    """
    import openpyxl
    from fiche_writer import parse_dmia_minutes

    wb = openpyxl.load_workbook(str(synthese_path), data_only=True)
    results: list[dict] = []
    seen: set = set()          # (activity_name.lower(), dmia_exprimee.lower())

    # ── column-header fingerprints ─────────────────────────────────────────────
    _NAME_PATTERNS = (
        "activit", "applicat", "processus", "macro process",
        "service", "composant", "fonction", "système", "systeme",
    )
    _DEPT_PATTERNS = (
        "niveau 1", "département", "departement",
        "direction", "entité", "entite", "métier", "metier",
    )
    # Values in a DMIA data cell that mean "no data — skip"
    _SKIP_DMIA = {
        "", "dmia", "xx", "x", "n.a.", "na", "n/a", "nd", "nc",
        "-", "--", "—", "pmdt", "s", "oui", "non", "?", "tbd",
        "à définir", "a definir", "non défini", "non defini",
        "immédiat", "immediat",  # parse_dmia_minutes handles these separately
    }
    # Cells that contain "dmia" but are section/file titles, not column headers
    _TITLE_KW = (
        "synthèse bia", "synthese bia", "bts_", "bpm_", "pca_",
        "maintien", "tableau", "rapport", "document",
    )

    def _dmia_priority(col_name: str) -> int:
        """Lower = higher priority."""
        n = col_name.lower()
        if "arbitr" in n:
            return 1
        if any(k in n for k in ("préconisé", "preconisé", "préconisee",
                                 "preconisee", "recommand")):
            return 2
        if "exprim" in n:
            return 3
        return 4

    def _is_title_cell(v: str) -> bool:
        vl = v.lower()
        return any(kw in vl for kw in _TITLE_KW)

    def _cell_text(row, col_1based: int) -> str:
        """Safe cell read — handles merged cells (value=None on non-masters)."""
        try:
            return str(row[col_1based - 1].value or "").strip()
        except IndexError:
            return ""

    for ws in wb.worksheets:
        hdr_row   = None
        dmia_cols : list[tuple[int, int]] = []   # [(priority, col_1based), …]
        act_col   : Optional[int] = None
        dept_col  : Optional[int] = None

        # ── locate header row (scan up to row 35) ─────────────────────────────
        # A valid header must contain BOTH a DMIA column AND an activity column
        # in the SAME row — any row with only one is a title/section label.
        for row in ws.iter_rows(min_row=1, max_row=35):
            tmp_dmia: list[tuple[int, int]] = []
            tmp_act = tmp_dept = None

            for cell in row:
                v  = str(cell.value or "").strip()
                vl = v.lower()
                if not v:
                    continue

                if "dmia" in vl and not _is_title_cell(v):
                    tmp_dmia.append((_dmia_priority(v), cell.column))

                if tmp_act is None and any(p in vl for p in _NAME_PATTERNS):
                    tmp_act = cell.column

                if tmp_dept is None and any(p in vl for p in _DEPT_PATTERNS):
                    tmp_dept = cell.column

            if tmp_dmia and tmp_act is not None:
                dmia_cols = sorted(tmp_dmia)   # ascending priority (1 = best)
                hdr_row   = row[0].row
                act_col   = tmp_act
                dept_col  = tmp_dept
                break

        if not dmia_cols or not hdr_row or act_col is None:
            continue

        # ── read data rows ─────────────────────────────────────────────────────
        for row in ws.iter_rows(min_row=hdr_row + 1):
            act_val  = _cell_text(row, act_col)
            dept_val = _cell_text(row, dept_col) if dept_col else ""

            # Skip blank / placeholder activity names
            if not act_val:
                continue
            if act_val.lower() in {
                "_", "-", "--", "xx", "x", "n/a", "na", "nd", "nc",
                "activité", "activite", "activity", "processus",
                "application", "nom", "libellé", "libelle", "désignation",
                "designation", "total", "sous-total", "sous total",
            }:
                continue
            # Skip rows that are clearly sub-headers or section titles
            # (all-caps short labels or repeating the header keyword)
            if len(act_val) < 3:
                continue

            # Pick highest-priority non-empty DMIA value from the columns
            dmia_val = ""
            for _prio, col in dmia_cols:
                raw = _cell_text(row, col)
                if raw and raw.lower() not in _SKIP_DMIA:
                    dmia_val = raw
                    break

            if not dmia_val:
                continue

            # ── CRITICAL FILTER: only keep rows whose DMIA parses to a valid
            # duration (>= 0 minutes).  This eliminates equipment quantities,
            # score columns, plain numbers, and any other non-duration junk
            # that slipped through the column-name check.
            mins = parse_dmia_minutes(dmia_val)
            if mins < 0:
                continue

            key = (act_val.lower(), dmia_val.lower())
            if key in seen:
                continue
            seen.add(key)

            results.append({
                "activity_name": act_val,
                "departement":   dept_val,
                "dmia_exprimee": dmia_val,
                "dmia_minutes":  mins,
            })

    return results


# ── Risk Assessment Functions ───────────────────────────────────────────────────

def _floor_label(floor_num: int) -> str:
    """Libellé par défaut d'un étage (RDC pour le niveau 1)."""
    return "Rez-de-chaussée" if floor_num == 1 else f"Étage {floor_num - 1}"


def _refresh_notes_count(con, floor_id: int) -> None:
    con.execute("""
        UPDATE assessment_floors
           SET notes_count = (SELECT COUNT(*) FROM assessment_notes WHERE floor_id = ?)
         WHERE id = ?
    """, (floor_id, floor_id))


def create_risk_assessment(sector: str, client: str, building_type: str,
                           num_floors: int, project_id: Optional[int] = None) -> int:
    """Crée une évaluation et ses étages ; retourne l'identifiant."""
    with _conn() as con:
        cursor = con.execute("""
            INSERT INTO risk_assessments (project_id, sector, client, building_type, num_floors)
            VALUES (?, ?, ?, ?, ?)
        """, (project_id, sector, client, building_type, num_floors))
        assessment_id = cursor.lastrowid

        for floor_num in range(1, num_floors + 1):
            con.execute("""
                INSERT INTO assessment_floors (assessment_id, floor_number, floor_label)
                VALUES (?, ?, ?)
            """, (assessment_id, floor_num, _floor_label(floor_num)))

        con.commit()
    return assessment_id


def list_risk_assessments(sector: Optional[str] = None,
                          client: Optional[str] = None) -> List[dict]:
    """Évaluations existantes, les plus récentes d'abord."""
    sql = """
        SELECT a.*,
               (SELECT COUNT(*) FROM assessment_notes n
                  JOIN assessment_floors f ON f.id = n.floor_id
                 WHERE f.assessment_id = a.id) AS notes_total,
               (SELECT COUNT(*) FROM assessment_photos p
                  JOIN assessment_notes n  ON n.id = p.note_id
                  JOIN assessment_floors f ON f.id = n.floor_id
                 WHERE f.assessment_id = a.id) AS photos_total
          FROM risk_assessments a
    """
    where, params = [], []
    if sector:
        where.append("a.sector = ?"); params.append(sector)
    if client:
        where.append("a.client = ?"); params.append(client)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY a.id DESC"
    with _conn() as con:
        return [dict(r) for r in con.execute(sql, params).fetchall()]


def get_risk_assessment(assessment_id: int) -> Optional[dict]:
    """Évaluation avec ses étages (identifiants réels des étages inclus)."""
    with _conn() as con:
        ra = con.execute(
            "SELECT * FROM risk_assessments WHERE id = ?", (assessment_id,)
        ).fetchone()
        if not ra:
            return None

        floors = con.execute("""
            SELECT id, floor_number, floor_label, notes_count
              FROM assessment_floors
             WHERE assessment_id = ?
             ORDER BY floor_number
        """, (assessment_id,)).fetchall()

        result = dict(ra)
        result['floors'] = [dict(f) for f in floors]
        return result


def get_floor(assessment_id: int, floor_number: int) -> Optional[dict]:
    """Étage d'une évaluation, résolu par son numéro (jamais par son rang)."""
    with _conn() as con:
        row = con.execute("""
            SELECT * FROM assessment_floors
             WHERE assessment_id = ? AND floor_number = ?
        """, (assessment_id, floor_number)).fetchone()
        return dict(row) if row else None


def rename_floor(assessment_id: int, floor_number: int, label: str) -> bool:
    with _conn() as con:
        cur = con.execute("""
            UPDATE assessment_floors SET floor_label = ?
             WHERE assessment_id = ? AND floor_number = ?
        """, (label, assessment_id, floor_number))
        con.commit()
        return cur.rowcount > 0


def list_floor_notes(floor_id: int) -> List[dict]:
    """Notes d'un étage, chacune avec ses photos et ses tags."""
    with _conn() as con:
        notes = con.execute("""
            SELECT * FROM assessment_notes WHERE floor_id = ? ORDER BY id
        """, (floor_id,)).fetchall()

        out = []
        for n in notes:
            photos = con.execute("""
                SELECT p.id, p.filename, p.width, p.height, p.exif_timestamp,
                       t.condition, t.risk_level
                  FROM assessment_photos p
             LEFT JOIN photo_tags t ON t.photo_id = p.id
                 WHERE p.note_id = ?
                 ORDER BY p.id
            """, (n['id'],)).fetchall()
            item = dict(n)
            item['photos'] = [dict(p) for p in photos]
            out.append(item)
        return out


def create_note(floor_id: int, component: str, note_text: str,
                condition: str = 'non_renseigne',
                risk_level: str = 'non_renseigne') -> int:
    """Crée une note rattachée à un composant d'un étage."""
    with _conn() as con:
        cursor = con.execute("""
            INSERT INTO assessment_notes (floor_id, component, note_text, status)
            VALUES (?, ?, ?, 'saisie')
        """, (floor_id, component, note_text))
        note_id = cursor.lastrowid
        _refresh_notes_count(con, floor_id)
        con.commit()
    return note_id


def delete_note(note_id: int) -> bool:
    """Supprime une note ; les photos et tags suivent par cascade."""
    with _conn() as con:
        row = con.execute(
            "SELECT floor_id FROM assessment_notes WHERE id = ?", (note_id,)
        ).fetchone()
        if not row:
            return False
        con.execute("DELETE FROM assessment_notes WHERE id = ?", (note_id,))
        _refresh_notes_count(con, row['floor_id'])
        con.commit()
        return True


def add_photo_to_note(note_id: int, filename: str, original_path: str,
                      thumbnail_path: str, meta: dict,
                      condition: str = 'non_renseigne',
                      risk_level: str = 'non_renseigne') -> int:
    """Enregistre une photo et son étiquetage (étage / composant / état)."""
    with _conn() as con:
        cursor = con.execute("""
            INSERT INTO assessment_photos
                (note_id, filename, filesize, mime_type, original_path,
                 thumbnail_path, width, height, orientation, exif_timestamp,
                 is_compressed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            note_id,
            filename,
            meta.get('filesize'),
            meta.get('mime_type', 'image/jpeg'),
            original_path,
            thumbnail_path,
            meta.get('width'),
            meta.get('height'),
            meta.get('orientation', 1),
            meta.get('timestamp'),
        ))
        photo_id = cursor.lastrowid

        note = con.execute(
            "SELECT floor_id, component FROM assessment_notes WHERE id = ?", (note_id,)
        ).fetchone()
        floor = con.execute(
            "SELECT assessment_id, floor_number FROM assessment_floors WHERE id = ?",
            (note['floor_id'],)
        ).fetchone()

        con.execute("""
            INSERT INTO photo_tags
                (photo_id, assessment_id, floor_number, component_type,
                 condition, risk_level)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (photo_id, floor['assessment_id'], floor['floor_number'],
              note['component'], condition, risk_level))

        con.commit()
    return photo_id


def get_photo(photo_id: int) -> Optional[dict]:
    """Photo avec les chemins de fichiers, pour la servir."""
    with _conn() as con:
        row = con.execute(
            "SELECT * FROM assessment_photos WHERE id = ?", (photo_id,)
        ).fetchone()
        return dict(row) if row else None


def update_photo_tags(photo_id: int, condition: str, risk_level: str) -> None:
    """Met à jour l'état et le niveau de risque associés à une photo."""
    with _conn() as con:
        con.execute("""
            UPDATE photo_tags SET condition = ?, risk_level = ? WHERE photo_id = ?
        """, (condition, risk_level, photo_id))
        con.commit()


# ── Types de bâtiment personnalisés ───────────────────────────────────────────

def list_custom_building_types() -> List[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT label, icon FROM assessment_building_types ORDER BY label"
        ).fetchall()
        return [dict(r) for r in rows]


def add_custom_building_type(label: str, icon: str = 'fa-building') -> None:
    with _conn() as con:
        con.execute("""
            INSERT OR IGNORE INTO assessment_building_types (label, icon) VALUES (?, ?)
        """, (label, icon))
        con.commit()


# ── Export pour l'agent de synthèse ───────────────────────────────────────────

def get_assessment_export(assessment_id: int) -> Optional[dict]:
    """Données de l'évaluation structurées pour la génération de synthèse.

    Chaque photo est livrée avec son étiquetage complet — étage, composant,
    catégorie, état, niveau de risque — afin que l'agent puisse la rattacher
    au bon constat sans ambiguïté.
    """
    with _conn() as con:
        ra = con.execute(
            "SELECT * FROM risk_assessments WHERE id = ?", (assessment_id,)
        ).fetchone()
        if not ra:
            return None

        floors_data = []
        floors = con.execute("""
            SELECT * FROM assessment_floors WHERE assessment_id = ? ORDER BY floor_number
        """, (assessment_id,)).fetchall()

        total_notes = total_photos = 0

        for floor_row in floors:
            notes = con.execute("""
                SELECT * FROM assessment_notes WHERE floor_id = ? ORDER BY id
            """, (floor_row['id'],)).fetchall()

            # Regroupement par composant : un composant peut porter plusieurs notes.
            par_composant: dict = {}
            for note_row in notes:
                total_notes += 1
                photos = con.execute("""
                    SELECT p.id, p.filename, p.width, p.height, p.exif_timestamp,
                           p.upload_timestamp, t.condition, t.risk_level
                      FROM assessment_photos p
                 LEFT JOIN photo_tags t ON t.photo_id = p.id
                     WHERE p.note_id = ? ORDER BY p.id
                """, (note_row['id'],)).fetchall()
                total_photos += len(photos)

                entry = par_composant.setdefault(note_row['component'], [])
                entry.append({
                    'note_id':    note_row['id'],
                    'texte':      note_row['note_text'],
                    'cree_le':    note_row['created_at'],
                    'photos': [{
                        'photo_id':      p['id'],
                        'fichier':       p['filename'],
                        'url':           f"/api/risk-assessment/{assessment_id}/photos/{p['id']}",
                        'largeur':       p['width'],
                        'hauteur':       p['height'],
                        'prise_le':      p['exif_timestamp'] or p['upload_timestamp'],
                        'etage':         floor_row['floor_number'],
                        'etage_libelle': floor_row['floor_label'],
                        'composant':     note_row['component'],
                        'etat':          p['condition'],
                        'niveau_risque': p['risk_level'],
                    } for p in photos],
                })

            floors_data.append({
                'numero':   floor_row['floor_number'],
                'libelle':  floor_row['floor_label'],
                'composants': [
                    {'composant': comp, 'notes': items}
                    for comp, items in par_composant.items()
                ],
            })

        meta = dict(ra)
        return {
            'evaluation': {
                'id':             meta['id'],
                'secteur':        meta['sector'],
                'client':         meta['client'],
                'type_batiment':  meta['building_type'],
                'nombre_etages':  meta['num_floors'],
                'date':           meta['assessment_date'],
                'statut':         meta['status'],
                'total_notes':    total_notes,
                'total_photos':   total_photos,
            },
            'etages': floors_data,
        }
