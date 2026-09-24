"""
llm_fallback.py — Optional local LLM fallback for BIA fiche extraction.

Uses Ollama (http://localhost:11434) with a small model (qwen2.5:3b recommended).
Called only when rule-based parsing fails or produces low-confidence results.
If Ollama is not running, every public function returns gracefully with None/False.
"""
from __future__ import annotations

import json
import http.client
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from bia_etl import BIAFiche

OLLAMA_HOST = "127.0.0.1"
OLLAMA_PORT = 11434
DEFAULT_MODEL = "qwen2.5:3b"


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

def is_ollama_available(model: str = DEFAULT_MODEL) -> bool:
    """Return True if Ollama is reachable and the model is pulled."""
    try:
        conn = http.client.HTTPConnection(OLLAMA_HOST, OLLAMA_PORT, timeout=2)
        conn.request("GET", "/api/tags")
        resp = conn.getresponse()
        if resp.status != 200:
            return False
        data = json.loads(resp.read())
        base = model.split(":")[0].lower()
        return any(base in m.get("name", "").lower() for m in data.get("models", []))
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Core Ollama call
# ─────────────────────────────────────────────────────────────────────────────

def _call_ollama(prompt: str, model: str = DEFAULT_MODEL, timeout: int = 90) -> Optional[str]:
    """
    Call Ollama's /api/generate endpoint with JSON output mode.
    Returns the raw response string, or None on any error.
    """
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.0,   # deterministic — we want facts, not creativity
            "num_predict": 2048,
            "top_p": 0.9,
        },
    }).encode("utf-8")

    try:
        conn = http.client.HTTPConnection(OLLAMA_HOST, OLLAMA_PORT, timeout=timeout)
        conn.request(
            "POST", "/api/generate", body=payload,
            headers={"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        data = json.loads(resp.read())
        return data.get("response", "").strip()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Table → plain text helper
# ─────────────────────────────────────────────────────────────────────────────

def _table_to_text(table, max_rows: int = 30) -> str:
    """Convert a python-docx table to a pipe-separated text block."""
    lines = []
    for i, row in enumerate(table.rows):
        if i >= max_rows:
            break
        cells = [c.text.strip().replace("\n", " / ") for c in row.cells]
        lines.append(" | ".join(cells))
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Per-table extraction prompts
# ─────────────────────────────────────────────────────────────────────────────

_EXTRACT_PROMPTS: dict[str, str] = {

    "activities": """\
Extract the list of business activities from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"activities": [{{"name": "activity name", "resources": "resources used", "critical_period": "critical period", "criticality": "digit 1-4", "volume": "volume or empty"}}]}}

Rules: only extract rows that are actual activities (skip header row). Use "" for missing fields.""",

    "exchanges_internal": """\
Extract internal information exchanges from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"exchanges": [{{"correspondent": "name", "info_type": "information type", "criticality": "A/B/C/D or empty", "tr_type": "T or R or empty", "si_resources": "IT resources or empty"}}]}}

Rules: ie_type is always "I" for internal. Use "" for missing fields.""",

    "exchanges_external": """\
Extract external information exchanges from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"exchanges": [{{"correspondent": "name", "info_type": "information type", "criticality": "A/B/C/D or empty", "tr_type": "T or R or empty", "si_resources": "IT resources or empty"}}]}}

Rules: ie_type is always "E" for external. Use "" for missing fields.""",

    "applications": """\
Extract the list of IT applications from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"applications": [{{"name": "app name", "criticality": "V/C/MC/PC or empty", "dmia": "DMIA value", "pmdt": "PMDT value or empty", "workaround": "workaround text or empty"}}]}}

DMIA/PMDT values look like: J+1, J+2, H+4, 4H, etc. Use "" for missing fields.""",

    "ramp_up": """\
Extract staffing ramp-up data from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"ramp_up": [{{"label": "row label (Effectif/Positions/Télétravail)", "nominal": "normal value", "h0": "", "h1": "", "h2": "", "h4": "", "j1": "", "j2": "", "j3": "", "j4": "", "j5": "", "j10": "", "j15": "", "j30": ""}}]}}

Time horizon columns: H0, H+1, H+2, H+4, J+1...J+30. Use "" for missing.""",

    "key_people": """\
Extract key personnel from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"key_people": [{{"function": "role", "last_name": "last name", "first_name": "first name", "position": "position or empty", "seniority": "seniority or empty", "replacements": "possible replacements or empty"}}]}}

Use "" for missing fields.""",

    "equipment": """\
Extract IT equipment/hardware from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"equipment": [{{"designation": "equipment name", "h0": "", "h1": "", "h2": "", "h4": "", "j1": "", "j2": "", "j3": "", "j5": "", "j10": "", "j15": ""}}]}}

Quantity columns follow time horizons. Use "" for missing fields.""",

    "docs": """\
Extract critical documents from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"docs": [{{"name": "document name", "storage_type": "Electronique/Papier/Les deux", "duplication": "Oui/Non", "duplication_method": "method or empty"}}]}}

Use "" for missing fields.""",

    "identification": """\
Extract the entity/department identification from this BIA table (French document).

Table:
{table_text}

Return JSON:
{{"entity_name": "full entity name", "division": "division or empty", "unite": "unit or empty", "department": "department or empty"}}

The entity_name should be the most specific non-empty value (department > unit > division).""",
}


# ─────────────────────────────────────────────────────────────────────────────
# Table classification
# ─────────────────────────────────────────────────────────────────────────────

_TABLE_TYPES = [
    "activities", "exchanges_internal", "exchanges_external",
    "applications", "ramp_up", "key_people", "equipment", "docs",
    "identification", "other",
]

def classify_table(table_text: str, headers: list[str], model: str = DEFAULT_MODEL) -> Optional[str]:
    """
    Ask the LLM to classify what type of BIA table this is.
    Returns one of the _TABLE_TYPES strings, or None on failure.
    """
    prompt = f"""\
Classify this BIA (Business Impact Analysis) table from a French document.

Headers: {headers}
First rows:
{table_text}

Choose exactly one type:
- "activities" — list of business activities with criticality 1-4
- "exchanges_internal" — internal information exchanges
- "exchanges_external" — external information exchanges
- "applications" — IT applications list with DMIA/PMDT
- "ramp_up" — staffing/capacity over time horizons (H0, H+1, J+1...)
- "key_people" — key personnel with names and roles
- "equipment" — IT equipment/hardware quantities
- "docs" — critical documents list
- "identification" — entity/department identification (name, structure)
- "other" — none of the above

Return JSON: {{"type": "one_of_the_above"}}"""

    response = _call_ollama(prompt, model, timeout=30)
    if not response:
        return None
    try:
        data = json.loads(response)
        t = data.get("type", "other")
        return t if t in _TABLE_TYPES else "other"
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Extract from a single table
# ─────────────────────────────────────────────────────────────────────────────

def extract_table(table_text: str, table_type: str, model: str = DEFAULT_MODEL) -> Optional[dict]:
    """
    Extract structured data from a table of the given type.
    Returns a parsed dict or None on failure.
    """
    template = _EXTRACT_PROMPTS.get(table_type)
    if not template:
        return None

    prompt = template.format(table_text=table_text)
    response = _call_ollama(prompt, model, timeout=60)
    if not response:
        return None
    try:
        return json.loads(response)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Merge LLM result into BIAFiche
# ─────────────────────────────────────────────────────────────────────────────

def merge_llm_result(fiche: "BIAFiche", table_type: str, data: dict) -> None:
    """
    Merge LLM-extracted data into an existing BIAFiche.
    Only fills in fields that are currently empty — never overwrites.
    """
    from bia_etl import Activity, Exchange, ITApplication, RampUpRow, KeyPerson, OtherEquipment, CriticalDoc

    if table_type == "identification":
        if not fiche.entity_name:
            fiche.entity_name = data.get("entity_name", "")
        if not fiche.division:
            fiche.division = data.get("division", "")
        if not fiche.unite:
            fiche.unite = data.get("unite", "")
        if not fiche.department:
            fiche.department = data.get("department", "")

    elif table_type == "activities" and not fiche.activities:
        for a in data.get("activities", []):
            if a.get("name"):
                fiche.activities.append(Activity(
                    name=a.get("name", ""),
                    resources=a.get("resources", ""),
                    critical_period=a.get("critical_period", ""),
                    criticality=a.get("criticality", ""),
                    volume=a.get("volume", ""),
                ))

    elif table_type in ("exchanges_internal", "exchanges_external") and not fiche.exchanges:
        ie = "I" if table_type == "exchanges_internal" else "E"
        for e in data.get("exchanges", []):
            if e.get("correspondent"):
                fiche.exchanges.append(Exchange(
                    correspondent=e.get("correspondent", ""),
                    ie_type=ie,
                    info_type=e.get("info_type", ""),
                    criticality=e.get("criticality", ""),
                    tr_type=e.get("tr_type", ""),
                    si_resources=e.get("si_resources", ""),
                ))

    elif table_type == "applications" and not fiche.it_applications:
        for a in data.get("applications", []):
            if a.get("name"):
                fiche.it_applications.append(ITApplication(
                    name=a.get("name", ""),
                    criticality=a.get("criticality", ""),
                    dmia=a.get("dmia", ""),
                    pmdt=a.get("pmdt", ""),
                    workaround=a.get("workaround", ""),
                ))

    elif table_type == "ramp_up" and not fiche.ramp_up:
        for r in data.get("ramp_up", []):
            if r.get("label"):
                fiche.ramp_up.append(RampUpRow(
                    label=r.get("label", ""),
                    nominal=r.get("nominal", ""),
                    h0=r.get("h0", ""), h1=r.get("h1", ""),
                    h2=r.get("h2", ""), h4=r.get("h4", ""),
                    j1=r.get("j1", ""), j2=r.get("j2", ""),
                    j3=r.get("j3", ""), j4=r.get("j4", ""),
                    j5=r.get("j5", ""), j10=r.get("j10", ""),
                    j15=r.get("j15", ""), j30=r.get("j30", ""),
                ))

    elif table_type == "key_people" and not fiche.key_people:
        for p in data.get("key_people", []):
            if p.get("last_name") or p.get("first_name"):
                fiche.key_people.append(KeyPerson(
                    function=p.get("function", ""),
                    last_name=p.get("last_name", ""),
                    first_name=p.get("first_name", ""),
                    position=p.get("position", ""),
                    seniority=p.get("seniority", ""),
                    replacements=p.get("replacements", ""),
                ))

    elif table_type == "equipment" and not fiche.other_equipment:
        for e in data.get("equipment", []):
            if e.get("designation"):
                fiche.other_equipment.append(OtherEquipment(
                    designation=e.get("designation", ""),
                    h0=e.get("h0", ""), h1=e.get("h1", ""),
                    h2=e.get("h2", ""), h4=e.get("h4", ""),
                    j1=e.get("j1", ""), j2=e.get("j2", ""),
                    j3=e.get("j3", ""), j5=e.get("j5", ""),
                    j10=e.get("j10", ""), j15=e.get("j15", ""),
                ))

    elif table_type == "docs" and not fiche.critical_docs:
        for d in data.get("docs", []):
            if d.get("name"):
                fiche.critical_docs.append(CriticalDoc(
                    name=d.get("name", ""),
                    storage_type=d.get("storage_type", ""),
                    duplication=d.get("duplication", ""),
                    duplication_method=d.get("duplication_method", ""),
                ))


# ─────────────────────────────────────────────────────────────────────────────
# Main public API
# ─────────────────────────────────────────────────────────────────────────────

def enhance_fiche(fiche: "BIAFiche", docx_path, model: str = DEFAULT_MODEL, verbose: bool = False) -> "BIAFiche":
    """
    Enhance a partially-extracted BIAFiche using LLM on tables that the
    rule-based parser couldn't classify.

    Only runs if Ollama is available. Only fills fields that are currently empty.
    Returns the (possibly enriched) fiche.
    """
    if not is_ollama_available(model):
        if verbose:
            print(f"  [LLM] Ollama not available — skipping enhancement")
        return fiche

    from docx import Document
    from bia_etl import (
        _is_identification_table, _is_activity_list_table, _is_dmia_table,
        _is_exchange_table, _is_ramp_up_table, _is_key_people_table,
        _is_app_table, _is_other_eqt_table, _is_doc_table,
    )

    known_checkers = [
        _is_identification_table, _is_activity_list_table, _is_dmia_table,
        _is_exchange_table, _is_ramp_up_table, _is_key_people_table,
        _is_app_table, _is_other_eqt_table, _is_doc_table,
    ]

    doc = Document(str(docx_path))
    enhanced = 0

    for table in doc.tables:
        # Skip tables the rule-based parser already handled
        if any(check(table) for check in known_checkers):
            continue
        if not table.rows or len(table.rows) < 2:
            continue

        table_text = _table_to_text(table, max_rows=6)
        headers = [c.text.strip() for c in table.rows[0].cells]

        table_type = classify_table(table_text, headers, model)
        if not table_type or table_type == "other":
            continue

        full_text = _table_to_text(table)
        data = extract_table(full_text, table_type, model)
        if data:
            merge_llm_result(fiche, table_type, data)
            enhanced += 1
            if verbose:
                print(f"  [LLM] Enhanced '{table_type}' from unclassified table")

    if verbose and enhanced == 0:
        print(f"  [LLM] No unclassified tables found to enhance")

    return fiche
