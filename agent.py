# -*- coding: utf-8 -*-
"""
agent.py — l'assistant de SmartPCA
===================================

Il répond aux questions sur l'application et sur ce qu'elle contient, ouvre
les pages et les fichiers à la demande, et présente une page en surlignant
ses éléments un à un.

Le partage des rôles est explicite, et c'est ce qui rend l'assistant fiable
avec un modèle de trois milliards de paramètres :

  * **le code décide des actions.** Aller à une page, ouvrir un fichier,
    lancer une visite guidée : ces intentions sont reconnues par des règles.
    Elles doivent marcher à tous les coups, et un petit modèle ne tient pas
    cette promesse — mesuré dans ce même projet, qwen2.5:3b répondait « Non »
    à « Combien existe-t-il de points d'accès ? ».
  * **le modèle rédige.** Il reformule des faits qu'on lui fournit : fiche
    descriptive de la page, chiffres tirés de la base. Il n'a rien à
    inventer, et quand les faits manquent il est tenu de le dire.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

import httpx

import agent_kb as kb

OLLAMA_BASE = "http://localhost:11434"
MODELE_DEFAUT = "qwen2.5:3b"


# ─────────────────────────────────────────────────────────────────────────────
# Outils de texte
# ─────────────────────────────────────────────────────────────────────────────

def norme(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().lower()


_VIDES = {"le", "la", "les", "de", "des", "du", "un", "une", "et", "ou", "a",
          "au", "aux", "en", "est", "sont", "je", "tu", "il", "on", "me",
          "moi", "mon", "ma", "mes", "ce", "cette", "que", "qui", "quoi",
          "pour", "sur", "dans", "avec", "comment", "quel", "quelle", "ou",
          "veux", "peux", "peut", "fais", "fait", "faire", "aller", "montre",
          "montrer", "dis", "dire", "page", "svp", "stp", "merci"}


def mots(s: str) -> set:
    return {m for m in re.split(r"[^a-z0-9]+", norme(s))
            if len(m) > 2 and m not in _VIDES}


# ─────────────────────────────────────────────────────────────────────────────
# Reconnaissance d'intention — en code, pas par le modèle
# ─────────────────────────────────────────────────────────────────────────────

_RE_NAV = re.compile(
    r"\b(va|vas|aller|amene|emmene|ouvre|ouvrir|affiche|montre|montrer|"
    r"navigue|rends-toi|conduis|dirige)\b", re.I)
_RE_DEMO = re.compile(
    r"\b(demo|demonstration|visite guidee|fais une demo|montre moi comment|"
    r"explique moi la page|guide|tutoriel|fais le tour|presente)\b", re.I)
_RE_FERMER = re.compile(r"\b(ferme|fermer|quitte|quitter|annule)\b", re.I)
_RE_COMMENT = re.compile(r"\b(comment|comment faire|procedure|etapes?|marche a suivre)\b", re.I)
_RE_QUOI = re.compile(r"\b(c'est quoi|qu'est ce|quest ce|a quoi (ca |cela )?sert|"
                      r"que fait|a quoi sert|role|utilite)\b", re.I)


def trouver_page(texte: str) -> Optional[str]:
    """
    Page désignée par une formulation libre.

    Le score compte les mots du synonyme présents dans la phrase, pondérés par
    la longueur du synonyme : « analyse des risques » doit l'emporter sur
    « risques » quand les deux apparaissent.
    """
    t = norme(texte)
    meilleur, score_max = None, 0
    for cle, syns in kb.SYNONYMES.items():
        for syn in syns:
            sn = norme(syn)
            if sn and sn in t:
                score = len(sn.split()) * 10 + len(sn)
                if score > score_max:
                    meilleur, score_max = cle, score
    if meilleur:
        return meilleur

    # Deuxième passe : recoupement de vocabulaire avec le titre de la page.
    mt = mots(texte)
    if not mt:
        return None
    for cle, fiche in kb.tout_le_catalogue().items():
        inter = mt & mots(fiche["titre"])
        if len(inter) >= 2:
            return cle
    return None


# Une phrase qui interroge ne demande pas un deplacement. « Combien de
# projets dans la base ? » contient « base de projets » : sans ce garde-fou,
# la question devenait un ordre de navigation.
_RE_INTERROGE = re.compile(
    r"^\s*(combien|quel|quelle|quels|quelles|qui|quand|pourquoi|"
    r"est-ce|y a-t-il|liste|donne|affiche-moi la liste)\b", re.I)


def analyser(message: str, page_courante: str = "") -> dict:
    """
    Détermine ce que l'utilisateur demande.

    Renvoie {'intention', 'cible'} où l'intention vaut 'naviguer', 'demo',
    'fermer', 'expliquer' ou 'question'.
    """
    cible = trouver_page(message)

    # Les motifs sont ecrits sans accent : on compare donc sur la forme
    # normalisee. « demo » ne reconnaissait pas « demo » accentue, et la
    # demande de visite guidee partait vers le modele comme une question.
    m = norme(message)

    if _RE_FERMER.search(m) and not cible:
        return {"intention": "fermer", "cible": None}

    if _RE_DEMO.search(m):
        return {"intention": "demo", "cible": cible or page_courante}

    # « Ouvre la page X » navigue ; « ouvre le fichier Y » relève de la base.
    if _RE_NAV.search(m) and cible and not _RE_INTERROGE.match(m):
        return {"intention": "naviguer", "cible": cible}

    if (_RE_COMMENT.search(m) or _RE_QUOI.search(m)):
        return {"intention": "expliquer", "cible": cible or page_courante}

    if cible and len(mots(message)) <= 4 and not _RE_INTERROGE.match(m):
        # Formulation courte qui ne nomme qu'une page : on y va.
        return {"intention": "naviguer", "cible": cible}

    return {"intention": "question", "cible": cible or page_courante}


# ─────────────────────────────────────────────────────────────────────────────
# Faits : ce que contient la base
# ─────────────────────────────────────────────────────────────────────────────

def faits_base(message: str) -> list[str]:
    """
    Chiffres et noms tirés de la base, choisis selon la question posée.

    On ne verse que ce qui est pertinent : un petit modèle noyé sous le
    contexte répond à côté.
    """
    faits: list[str] = []
    t = norme(message)
    try:
        import projects_db as db
    except Exception:
        return faits

    try:
        with db._conn() as con:
            if any(k in t for k in ("projet", "fiche", "synthese", "base",
                                    "client", "combien", "biblio", "secteur")):
                n = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
                nc = con.execute("SELECT COUNT(DISTINCT client) FROM projects").fetchone()[0]
                na = con.execute("SELECT COUNT(*) FROM activity_dmia").fetchone()[0]
                faits.append(f"La base contient {n} documents enregistres, "
                             f"{nc} clients distincts et {na} activites indexees.")
                rows = con.execute(
                    "SELECT client, COUNT(*) c FROM projects GROUP BY client "
                    "ORDER BY c DESC LIMIT 8").fetchall()
                if rows:
                    faits.append("Repartition par client : " +
                                 ", ".join(f"{r[0]} ({r[1]})" for r in rows) + ".")

            if any(k in t for k in ("dmia", "duree", "interruption", "delai")):
                rows = con.execute(
                    "SELECT activity_name, dmia_exprimee FROM activity_dmia "
                    "WHERE dmia_minutes >= 0 ORDER BY dmia_minutes LIMIT 6").fetchall()
                if rows:
                    faits.append("Exemples de DMIA les plus courtes : " +
                                 " ; ".join(f"{r[0][:50]} = {r[1]}" for r in rows) + ".")

            if any(k in t for k in ("releve", "visite", "risque", "photo", "batiment")):
                n = con.execute("SELECT COUNT(*) FROM risk_assessments").fetchone()[0]
                faits.append(f"{n} releve(s) de visite de site enregistre(s).")
    except Exception:
        pass

    try:
        import questionnaires_db as q
        if "questionnaire" in t:
            lst = q.list_questionnaires()
            faits.append(f"{len(lst)} questionnaire(s) enregistre(s).")
    except Exception:
        pass

    return faits


def repondre_comptage(message: str) -> Optional[str]:
    """
    Reponse exacte aux questions de denombrement, formulee sans modele.

    Renvoie None si la question n'en est pas une.
    """
    t = norme(message)
    if not re.search(r"\b(combien|nombre de|total|y a-t-il)\b", t):
        return None
    try:
        import projects_db as db
        with db._conn() as con:
            n_doc = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
            n_fic = con.execute(
                "SELECT COUNT(*) FROM projects WHERE file_type LIKE 'fiche%'").fetchone()[0]
            n_syn = con.execute(
                "SELECT COUNT(*) FROM projects WHERE file_type = 'synthese'").fetchone()[0]
            n_cli = con.execute("SELECT COUNT(DISTINCT client) FROM projects").fetchone()[0]
            n_act = con.execute("SELECT COUNT(*) FROM activity_dmia").fetchone()[0]
            n_rel = con.execute("SELECT COUNT(*) FROM risk_assessments").fetchone()[0]
    except Exception:
        return None

    if "client" in t:
        return "La base compte %d clients distincts." % n_cli
    if "activite" in t or "dmia" in t:
        return "%d activites sont indexees avec leur DMIA." % n_act
    if "releve" in t or "visite" in t:
        return "%d releves de visite de site sont enregistres." % n_rel
    if "synthese" in t:
        return "%d syntheses sont enregistrees." % n_syn
    if "fiche" in t:
        return "%d fiches sont enregistrees." % n_fic
    return ("La base contient %d documents : %d fiches et %d syntheses, "
            "repartis sur %d clients, pour %d activites indexees."
            % (n_doc, n_fic, n_syn, n_cli, n_act))


def chercher_fichiers(message: str, limite: int = 8) -> list[dict]:
    """Documents de la base dont le nom correspond à la demande."""
    mt = mots(message)
    if not mt:
        return []
    try:
        import projects_db as db
        with db._conn() as con:
            rows = con.execute(
                "SELECT id, project_name, client, sector, file_type FROM projects"
            ).fetchall()
    except Exception:
        return []

    notes = []
    for r in rows:
        cible = mots(r["project_name"]) | mots(r["client"])
        inter = len(mt & cible)
        if inter:
            notes.append((inter, dict(r)))
    notes.sort(key=lambda t: t[0], reverse=True)
    return [d for _, d in notes[:limite]]


# ─────────────────────────────────────────────────────────────────────────────
# Rédaction par le modèle local
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEME = (
    "Tu es l'assistant de SmartPCA, l'outil de plan de continuite d'activite "
    "de Devoteam. Tu reponds en francais, brievement : trois phrases au plus, "
    "sans liste a puces, sans formule de politesse.\n"
    "Tu ne disposes que des informations fournies sous CONTEXTE. Si elles ne "
    "permettent pas de repondre, dis-le en une phrase et indique ou "
    "l'utilisateur trouvera l'information dans l'application.\n"
    "N'invente aucun chiffre, aucun nom de client, aucun nom de fichier."
)


def disponible(modele: str = MODELE_DEFAUT) -> bool:
    try:
        r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=4)
        if r.status_code != 200:
            return False
        noms = [m.get("name", "") for m in r.json().get("models", [])]
        return any(n == modele or n.split(":")[0] == modele.split(":")[0] for n in noms)
    except Exception:
        return False


def rediger(question: str, contexte: str, modele: str = MODELE_DEFAUT,
            timeout: float = 60.0) -> str:
    charge = {
        "model": modele,
        "messages": [
            {"role": "system", "content": _SYSTEME},
            {"role": "user", "content": f"CONTEXTE\n{contexte}\n\nQUESTION\n{question}"},
        ],
        "stream": False,
        "options": {"temperature": 0.25, "repeat_penalty": 1.2, "num_predict": 220},
    }
    r = httpx.post(f"{OLLAMA_BASE}/api/chat", json=charge, timeout=timeout)
    r.raise_for_status()
    return (r.json().get("message") or {}).get("content", "").strip()


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────

def repondre(message: str, page_courante: str = "", modele: str = MODELE_DEFAUT) -> dict:
    """
    Traite un message et renvoie la réponse et l'action éventuelle.

    Forme de la réponse :
      {'texte', 'action': {'type': 'naviguer'|'demo'|'ouvrir'|'fermer', ...},
       'fichiers': [...], 'modele_utilise': bool}
    """
    message = (message or "").strip()
    if not message:
        return {"texte": "Posez-moi une question sur l'application ou sur vos "
                         "donnees.", "action": None, "fichiers": []}

    intention = analyser(message, page_courante)
    cle = intention["cible"]
    catalogue = kb.tout_le_catalogue()
    fiche = catalogue.get(cle) if cle else None

    # ── Fermer ────────────────────────────────────────────────────────────
    if intention["intention"] == "fermer":
        return {"texte": "C'est ferme.", "action": {"type": "fermer"},
                "fichiers": [], "modele_utilise": False}

    # ── Visite guidée ─────────────────────────────────────────────────────
    if intention["intention"] == "demo":
        if not fiche or not fiche.get("demo"):
            return {"texte": "Je n'ai pas de visite guidee pour cette page. "
                             "Demandez-moi plutot a quoi elle sert.",
                    "action": None, "fichiers": [], "modele_utilise": False}
        etapes = [{"selecteur": s, "titre": t, "texte": c}
                  for s, t, c in fiche["demo"]]
        return {
            "texte": f"Je vous presente « {fiche['titre']} » en {len(etapes)} points.",
            "action": {"type": "demo", "page": cle, "etapes": etapes},
            "fichiers": [], "modele_utilise": False,
        }

    # ── Navigation ────────────────────────────────────────────────────────
    if intention["intention"] == "naviguer" and fiche:
        return {
            "texte": f"J'ouvre « {fiche['titre']} ». {fiche['resume']}",
            "action": {"type": "naviguer", "page": cle,
                       "titre": fiche["titre"],
                       "groupe": fiche.get("groupe", "")},
            "fichiers": [], "modele_utilise": False,
        }

    # ── Comptages : reponse directe, sans passer par le modele ────────────
    # Un chiffre est un fait. Le modele, a qui on fournissait pourtant le
    # bon total, repondait « 565 activites indexees, pas specifiquement des
    # projets ». Ce qui doit etre exact est calcule, pas redige.
    rep_directe = repondre_comptage(message)
    if rep_directe:
        return {"texte": rep_directe, "action": None, "fichiers": [],
                "modele_utilise": False}

    # ── Fichiers de la base ───────────────────────────────────────────────
    fichiers = []
    if re.search(r"\b(fichier|document|docx|xlsx|fiche de|synthese de|ouvre|"
                 r"trouve|cherche|recherche)\b", message, re.I):
        fichiers = chercher_fichiers(message)

    # ── Question libre : on fournit les faits, le modèle rédige ───────────
    morceaux = []
    if fiche:
        morceaux.append(
            "Page « %s »%s.\n%s\n%s\n%s" % (
                fiche["titre"],
                f" (phase {fiche['groupe']})" if fiche.get("groupe") else "",
                fiche.get("resume", ""),
                fiche.get("a_quoi_ca_sert", ""),
                ("Mode d'emploi : " + fiche["comment"]) if fiche.get("comment") else "",
            ))
    morceaux += faits_base(message)
    if fichiers:
        morceaux.append("Documents de la base correspondant a la demande : " +
                        " ; ".join(f"{f['project_name']} ({f['client']})"
                                   for f in fichiers[:5]) + ".")

    contexte = "\n\n".join(m for m in morceaux if m and m.strip())
    if not contexte:
        contexte = ("Aucune information precise ne correspond a cette question. "
                    "L'application couvre la synthese BIA, le rapport BIA et la "
                    "gestion des risques.")

    if disponible(modele):
        try:
            texte = rediger(message, contexte, modele)
            if texte:
                return {"texte": texte, "action": None, "fichiers": fichiers,
                        "modele_utilise": True}
        except Exception:
            pass

    # Repli sans modèle : les faits bruts valent mieux que rien.
    return {"texte": contexte, "action": None, "fichiers": fichiers,
            "modele_utilise": False}
