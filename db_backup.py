"""
db_backup.py — Sauvegarde continue de projects.db

La base est suivie en continu : dès qu'une écriture a lieu, une copie est
rafraîchie. Conçu après la perte réelle de la base le 03/09/2026.

Trois décisions structurantes :

1. **Détection par date de modification du fichier, pas par appel de code.**
   Trois modules écrivent dans cette base (`projects_db`, `questionnaires_db`,
   `risk_analysis`) et certains ouvrent leur propre connexion. Suivre le mtime
   du fichier attrape *tous* les écrivains — y compris une modification faite
   à la main en dehors de l'application — là où brancher un hook sur chaque
   point d'écriture laisserait forcément passer un cas.

2. **Une copie de référence que rien ne peut supprimer.** Les copies tournantes
   suivent fidèlement la base : si celle-ci est vidée, le vide finit par se
   propager dans toutes. La copie `reference` ne recule jamais — elle n'est
   remplacée que par une base contenant au moins autant de lignes. C'est elle
   qui aurait sauvé les 155 projets.

3. **Les suppressions volontaires ne sont pas bloquées.** Supprimer un projet
   depuis l'interface est légitime et doit se refléter dans les copies
   tournantes. Seule la copie de référence est protégée.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "projects.db"

# Surchargeable : BIA_BACKUP_DIR=D:\sauvegardes pour sortir du dossier projet.
BACKUP_DIR = Path(os.environ.get("BIA_BACKUP_DIR", BASE_DIR / "backups"))
REFERENCE = BACKUP_DIR / "projects-reference.db"

KEEP = 15            # copies horodatées conservées
POLL_SECONDS = 5     # fréquence de vérification du mtime
QUIET_SECONDS = 3    # délai de calme avant copie (regroupe les écritures)

_GUARDED_TABLES = ("projects", "activity_dmia", "fiche_equipment",
                   "synthese_fiche_links", "questionnaires", "risk_files",
                   "risk_assessments")

_watcher: Optional[threading.Thread] = None
_stop = threading.Event()
_last_event: dict = {}


def _counts(db: Path) -> dict[str, int]:
    """Nombre de lignes par table surveillée. Table ou base absente => 0."""
    if not db.exists():
        return {t: 0 for t in _GUARDED_TABLES}
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
    except sqlite3.Error:
        return {t: 0 for t in _GUARDED_TABLES}
    out: dict[str, int] = {}
    try:
        for t in _GUARDED_TABLES:
            try:
                out[t] = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            except sqlite3.Error:
                out[t] = 0
    finally:
        con.close()
    return out


def _total(counts: dict[str, int]) -> int:
    return sum(counts.values())


def _copy(dest: Path) -> None:
    """Copie cohérente via l'API backup de sqlite3 (gère verrous et WAL)."""
    src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
    dst = sqlite3.connect(dest)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()


def list_backups() -> list[Path]:
    """Copies horodatées, de la plus récente à la plus ancienne."""
    if not BACKUP_DIR.exists():
        return []
    return sorted(BACKUP_DIR.glob("projects-2*.db"), reverse=True)


def _prune() -> None:
    for old in list_backups()[KEEP:]:
        try:
            old.unlink()
        except OSError:
            pass


def sync(reason: str = "") -> dict:
    """
    Rafraîchit la sauvegarde à partir de l'état actuel de la base.

    Écrit une copie horodatée, puis met à jour la copie de référence
    uniquement si la base n'a pas perdu de lignes par rapport à elle.
    """
    if not DB_PATH.exists():
        return {"status": "ignore", "raison": "projects.db absent"}

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    current = _counts(DB_PATH)

    # Nom unique : deux syncs dans la même seconde ne doivent pas se marcher
    # dessus, et surtout ne doivent pas court-circuiter la copie de référence
    # ci-dessous — c'est elle qui protège les données.
    stamp = time.strftime("%Y%m%d-%H%M%S")
    dest = BACKUP_DIR / f"projects-{stamp}.db"
    n = 2
    while dest.exists():
        dest = BACKUP_DIR / f"projects-{stamp}-{n}.db"
        n += 1
    _copy(dest)
    _prune()

    # ── copie de référence : ne recule jamais ────────────────────────────────
    ref_counts = _counts(REFERENCE) if REFERENCE.exists() else None
    ref_action = ""
    if ref_counts is None:
        _copy(REFERENCE)
        ref_action = "créée"
    else:
        perdues = {t: (ref_counts[t], current[t])
                   for t in _GUARDED_TABLES if current[t] < ref_counts[t]}
        if perdues:
            ref_action = "conservée (la base a moins de lignes)"
        else:
            _copy(REFERENCE)
            ref_action = "mise à jour"

    return {"status": "ok", "fichier": dest.name, "lignes": current,
            "reference": ref_action, "motif": reason or "automatique",
            "taille": dest.stat().st_size}


def restore(backup_name: str) -> dict:
    """
    Restaure une sauvegarde. La base courante est d'abord mise de côté :
    une restauration ne doit jamais être elle-même une perte de données.
    """
    src = REFERENCE if backup_name == REFERENCE.name else BACKUP_DIR / backup_name
    if not src.exists():
        return {"status": "erreur", "raison": f"sauvegarde introuvable : {backup_name}"}

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    secours = ""
    if DB_PATH.exists():
        secours = f"avant-restauration-{stamp}.db"
        shutil.copy2(DB_PATH, BACKUP_DIR / secours)

    shutil.copy2(src, DB_PATH)
    _last_event["mtime"] = DB_PATH.stat().st_mtime   # ne pas se resauvegarder aussitôt
    return {"status": "ok", "restaure_depuis": backup_name,
            "lignes": _counts(DB_PATH), "copie_de_securite": secours}


def status() -> dict:
    backups = list_backups()
    ref = None
    if REFERENCE.exists():
        ref = {"fichier": REFERENCE.name,
               "date": time.strftime("%Y-%m-%d %H:%M:%S",
                                     time.localtime(REFERENCE.stat().st_mtime)),
               "lignes": _counts(REFERENCE)}
    return {
        "dossier": str(BACKUP_DIR),
        "surveillance_active": bool(_watcher and _watcher.is_alive()),
        "base_actuelle": _counts(DB_PATH),
        "reference": ref,
        "nombre": len(backups),
        "conserve": KEEP,
        "sauvegardes": [
            {"fichier": b.name,
             "date": time.strftime("%Y-%m-%d %H:%M:%S",
                                   time.localtime(b.stat().st_mtime)),
             "taille": b.stat().st_size,
             "lignes": _counts(b)}
            for b in backups
        ],
    }


# ── surveillance continue ────────────────────────────────────────────────────

def _watch_loop() -> None:
    while not _stop.is_set():
        try:
            if DB_PATH.exists():
                mtime = DB_PATH.stat().st_mtime
                known = _last_event.get("mtime")
                if known is None:
                    _last_event["mtime"] = mtime
                elif mtime != known:
                    # Attendre que les écritures se calment avant de copier,
                    # pour regrouper une rafale en une seule sauvegarde.
                    if time.time() - mtime >= QUIET_SECONDS:
                        res = sync(reason="modification détectée")
                        _last_event["mtime"] = DB_PATH.stat().st_mtime
                        if res.get("status") == "ok":
                            print(f"[sauvegarde] {res['fichier']} "
                                  f"— référence {res['reference']}")
        except Exception as exc:
            print(f"[sauvegarde] erreur de surveillance : {exc}")
        _stop.wait(POLL_SECONDS)


def start_watcher() -> bool:
    """Démarre la surveillance en tâche de fond. Idempotent."""
    global _watcher
    if _watcher and _watcher.is_alive():
        return False
    _stop.clear()
    _watcher = threading.Thread(target=_watch_loop, name="db-backup-watcher",
                                daemon=True)
    _watcher.start()
    return True


def stop_watcher() -> None:
    _stop.set()


if __name__ == "__main__":
    import json, sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "sync"
    if cmd == "restore":
        print(json.dumps(restore(sys.argv[2]), ensure_ascii=False, indent=2))
    elif cmd == "status":
        print(json.dumps(status(), ensure_ascii=False, indent=2))
    else:
        print(json.dumps(sync(reason="manuel (ligne de commande)"),
                         ensure_ascii=False, indent=2))
