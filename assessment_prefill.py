"""
assessment_prefill.py — du relevé de visite aux réponses du questionnaire
==========================================================================

Après une visite de site, le consultant a des observations rattachées à un
niveau et à un composant ; il a par ailleurs un questionnaire dont chaque
question attend une réponse. Les deux parlent de la même chose, et personne
ne devrait avoir à les rapprocher à la main.

Ce module propose des réponses. Il n'en écrit aucune : le consultant valide,
corrige ou écarte chaque proposition. C'est délibéré — une réponse de
questionnaire part devant un comité des risques, et la signature reste celle
du consultant.

Comment l'appariement se fait
-----------------------------
Le catalogue de composants du relevé a été construit à partir de ces mêmes
questionnaires : chaque composant porte la référence de sa rubrique (SAB,
ENV, INC, EAU, AEB, DC…). Les questions portent la même référence dans leur
colonne « Ref. », sous la forme ``SAB.07``. L'appariement se fait donc sur
cette référence, pas sur une ressemblance de texte — il est exact et
explicable, et un consultant peut vérifier pourquoi telle observation a été
rattachée à telle question.

À l'intérieur d'une rubrique, les observations sont ensuite classées par
proximité de vocabulaire avec la question, afin que la plus pertinente soit
présentée en premier au modèle.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field, asdict
from typing import Optional

import httpx

OLLAMA_BASE = "http://localhost:11434"

# Réponses admises dans la colonne « Réponses » des questionnaires bâtiment.
REPONSES_FERMEES = ("Oui", "Non", "N.A.")

# En deçà, le rapprochement ne tient qu'à un mot passe-partout — « sécurité »
# ou « accès » figurent dans la moitié des questions d'une rubrique. Proposer
# sur cette base revenait à soumettre au consultant des réponses tirées d'un
# équipement voisin, qu'il validait sans les relire.
SEUIL_PERTINENCE = 3


# ─────────────────────────────────────────────────────────────────────────────
# Normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _sans_accents(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", txt or "")
                   if not unicodedata.combining(c))


def _norm(txt: str) -> str:
    return _sans_accents(str(txt or "")).lower().strip()


_VIDES = {
    "le", "la", "les", "un", "une", "des", "du", "de", "d", "et", "ou", "a",
    "au", "aux", "en", "est", "sont", "il", "elle", "y", "ce", "cet", "cette",
    "pour", "par", "sur", "dans", "avec", "sans", "que", "qui", "quel",
    "quelle", "existe", "t", "l", "se", "son", "sa", "ses", "leur", "plus",
    "combien", "quels", "quelles", "vous", "votre", "etes", "avez",
}


def _mots(txt: str) -> set:
    """Mots porteurs de sens, pour mesurer la proximité question/observation."""
    return {m for m in re.split(r"[^a-z0-9]+", _norm(txt))
            if len(m) > 2 and m not in _VIDES}


def _radicaux(txt: str) -> set:
    """
    Mots ramenés à leurs cinq premières lettres.

    « agents » et « agent », « portique » et « portiques » doivent se
    reconnaître : sans cela une question au pluriel ne retrouve pas
    l'observation au singulier.
    """
    return {m[:5] for m in _mots(txt)}


def pertinence(question: str, obs: "Observation") -> int:
    """
    À quel point une observation éclaire cette question.

    Partager une rubrique ne suffit pas : la rubrique SAB couvre aussi bien
    le portique que le scanner à bagages. Sans ce second filtre, une question
    sur un équipement absent du relevé recevait l'état d'un équipement voisin
    — une réponse plausible et fausse, que le consultant valide sans y
    regarder. Le libellé du composant pèse double : c'est lui qui dit de quoi
    l'observation parle.
    """
    q = _radicaux(question)
    return (2 * len(q & _radicaux(obs.composant))
            + len(q & _radicaux(obs.texte)))


def ref_rubrique(ref: str) -> str:
    """``SAB.07`` → ``SAB``. Une référence sans point reste telle quelle."""
    return _norm(ref).split(".")[0].strip().upper()


# Une question ouverte attend un fait, pas un Oui/Non. Un modèle de petite
# taille ne fait pas la différence de façon fiable : on la tranche ici, sur la
# forme interrogative, et on la lui impose.
_OUVERTES = (
    "combien", "quel", "quels", "quelle", "quelles", "comment", "ou se",
    "a quelle", "a quel", "de quelle", "de quel", "lesquels", "lesquelles",
    "qui est", "quoi", "pourquoi", "preciser", "precisez", "decrire",
    "decrivez", "lister", "listez", "nature", "type de",
)


def question_fermee(libelle: str) -> bool:
    """Vrai si la question appelle un Oui / Non / N.A."""
    q = _norm(libelle)
    # Un mot interrogatif ouvert en tête de l'une des phrases l'emporte :
    # « Combien … ? Lesquels ? » reste une question ouverte.
    for segment in re.split(r"[?.;]", q):
        seg = segment.strip()
        if seg and any(seg.startswith(m) for m in _OUVERTES):
            return False
    if any(m in q for m in ("combien", "lesquels", "lesquelles")):
        return False
    return bool(re.search(r"(existe|est-ce|avez-vous|etes-vous|disposez|"
                          r"-t-il|-t-elle|-il |-elle |y a-t-il)", q))


# ─────────────────────────────────────────────────────────────────────────────
# Structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Observation:
    """Une observation du relevé, telle qu'elle sera montrée au consultant."""
    note_id: int = 0
    niveau: str = ""
    composant: str = ""
    rubrique: str = ""          # ENV, SAB, INC…
    texte: str = ""
    etat: str = ""
    risque: str = ""
    photos: int = 0


@dataclass
class Proposition:
    """Une réponse proposée pour une question, avec ce qui la justifie."""
    ref: str = ""
    categorie: str = ""
    question: str = ""
    ligne: int = 0              # index de la ligne dans le questionnaire
    reponse: str = ""
    commentaire: str = ""
    observations: list = field(default_factory=list)   # [Observation]
    origine: str = "regle"      # "regle" ou "modele"
    score: int = 0              # force du rattachement, montrée au consultant

    def to_dict(self) -> dict:
        d = asdict(self)
        d["observations"] = [asdict(o) if not isinstance(o, dict) else o
                             for o in self.observations]
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Lecture du relevé
# ─────────────────────────────────────────────────────────────────────────────

def observations_du_releve(export: dict, catalogue: list[dict]) -> list[Observation]:
    """
    Aplatit l'export d'un relevé en observations, chacune portant la rubrique
    de son composant.

    `catalogue` est la liste de composants du type de bâtiment concerné, telle
    que la fournit risk_components.components_for(). La correspondance se fait
    sur le libellé du composant, qui est ce que le relevé enregistre.
    """
    par_label = {_norm(c.get("label", "")): (c.get("ref") or "").upper()
                 for c in catalogue}

    out: list[Observation] = []
    for etage in export.get("etages", []) or []:
        niveau = etage.get("libelle") or f"Niveau {etage.get('numero', '')}"
        for groupe in etage.get("composants", []) or []:
            composant = groupe.get("composant", "")
            rub = par_label.get(_norm(composant), "")
            for n in groupe.get("notes", []) or []:
                texte = (n.get("texte") or "").strip()
                if not texte:
                    continue
                photos = n.get("photos") or []
                # L'état et le risque viennent de l'observation ; pour les
                # relevés antérieurs à la migration ils n'existent que sur
                # la première photo.
                etat = n.get("etat") or ""
                risque = n.get("niveau_risque") or ""
                if photos and isinstance(photos[0], dict):
                    etat = etat or photos[0].get("etat") or ""
                    risque = risque or photos[0].get("niveau_risque") or ""
                out.append(Observation(
                    note_id=n.get("note_id", 0), niveau=str(niveau),
                    composant=str(composant), rubrique=rub, texte=texte,
                    etat=_lisible(etat), risque=_lisible(risque),
                    photos=len(photos),
                ))
    return out


def _lisible(v: str) -> str:
    """'non_renseigne' → '' ; 'bon_etat' → 'bon etat'."""
    v = (v or "").strip()
    if not v or _norm(v) in ("non_renseigne", "non renseigne", "none"):
        return ""
    return v.replace("_", " ")


# ─────────────────────────────────────────────────────────────────────────────
# Appariement
# ─────────────────────────────────────────────────────────────────────────────

def apparier(questions: list[dict], observations: list[Observation],
             col_ref: str, col_question: str,
             col_categorie: str = "") -> list[Proposition]:
    """
    Rattache les observations aux questions par leur rubrique.

    Une question sans observation dans sa rubrique n'est pas retournée : le
    consultant ne doit voir que ce que le relevé permet effectivement de
    renseigner, pas la totalité du questionnaire.
    """
    par_rubrique: dict[str, list[Observation]] = {}
    for o in observations:
        if o.rubrique:
            par_rubrique.setdefault(o.rubrique, []).append(o)

    props: list[Proposition] = []
    for idx, q in enumerate(questions):
        ref = str(q.get(col_ref, "") or "").strip()
        rub = ref_rubrique(ref)
        candidates = par_rubrique.get(rub, [])
        if not candidates:
            continue

        libelle = str(q.get(col_question, "") or "").strip()
        if not libelle:
            continue

        # Les plus proches d'abord : le modèle lit mieux un contexte ordonné.
        notees = sorted(((pertinence(libelle, o), o) for o in candidates),
                        key=lambda t: t[0], reverse=True)
        # Aucune observation ne parle du sujet de la question : le relevé ne
        # la couvre pas. On n'en propose rien — mieux vaut une case vide
        # qu'une réponse inventée à partir d'un équipement voisin.
        if not notees or notees[0][0] < SEUIL_PERTINENCE:
            continue
        classees = [o for score, o in notees if score > 0][:4]
        score_max = notees[0][0]

        props.append(Proposition(
            score=score_max,
            ref=ref,
            categorie=str(q.get(col_categorie, "") or "").strip() if col_categorie else "",
            question=libelle,
            ligne=idx,
            observations=classees,
        ))
    return props


# ─────────────────────────────────────────────────────────────────────────────
# Rédaction
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEME = (
    "Tu es consultant en plan de continuité d'activité. On te donne une "
    "question d'un questionnaire d'état des lieux d'un bâtiment, et les "
    "observations relevées sur place par le consultant pour la rubrique "
    "concernée.\n"
    "Rédige UNIQUEMENT un objet JSON, sans texte autour, de la forme :\n"
    '{"reponse": "...", "commentaire": "..."}\n'
    "Règles :\n"
    "- \"reponse\" vaut exactement Oui, Non ou N.A. si la question appelle "
    "un oui/non ; sinon une réponse factuelle de quelques mots.\n"
    "- \"commentaire\" reformule ce qui a été constaté, en une ou deux "
    "phrases, au présent, sans tournure commerciale.\n"
    "- Ne t'appuie que sur les observations fournies. Si elles ne permettent "
    "pas de répondre, mets \"reponse\": \"\" et explique en commentaire ce "
    "qu'il reste à vérifier.\n"
    "- N'invente rien qui ne soit dans les observations. Mais ce qui y est "
    "dit compte comme constaté : si une observation mentionne deux entrées, "
    "alors il y a deux entrées.\n"
    "\n"
    "Exemple.\n"
    "Question (SAB.01) : Combien existe-t-il de points d'accès ? Lesquels ?\n"
    "Observation : [Rez-de-chaussée · Entrée] Deux accès : l'entrée "
    "principale tenue par un agent, et une porte de livraison à l'arrière.\n"
    "Réponse attendue : "
    '{"reponse": "Deux : entrée principale et porte de livraison", '
    '"commentaire": "Le bâtiment compte deux points d\'accès. L\'entrée '
    "principale est tenue par un agent ; la porte de livraison se situe à "
    "l'arrière.\"}"
)


def _contexte(p: Proposition) -> str:
    if question_fermee(p.question):
        consigne = ("Cette question est fermée : \"reponse\" doit valoir "
                    "exactement Oui, Non ou N.A.")
    else:
        consigne = ("Cette question est ouverte : \"reponse\" doit être un "
                    "fait bref tiré des observations (un nombre, une liste, "
                    "un nom). N'y mets jamais Oui ni Non.")
    lignes = [f"Question ({p.ref}) : {p.question}", consigne, "",
              "Observations sur place :"]
    for o in p.observations:
        bits = [f"- [{o.niveau} · {o.composant}] {o.texte}"]
        extra = [x for x in (o.etat and f"état : {o.etat}",
                             o.risque and f"risque : {o.risque}") if x]
        if extra:
            bits.append("  (" + " ; ".join(extra) + ")")
        lignes.append("\n".join(bits))
    return "\n".join(lignes)


def _extraire_json(txt: str) -> Optional[dict]:
    """Récupère le premier objet JSON d'une réponse de modèle."""
    m = re.search(r"\{.*?\}", txt or "", re.S)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
        return d if isinstance(d, dict) else None
    except json.JSONDecodeError:
        return None


def rediger_par_regle(p: Proposition) -> Proposition:
    """
    Proposition sans modèle : le constat brut, recopié et attribué.

    Sert de repli quand Ollama n'est pas joignable, et de garde-fou — une
    proposition est toujours rendue, jamais une case vide sans explication.
    """
    morceaux = []
    for o in p.observations[:2]:
        bout = o.texte.strip().rstrip(".")
        detail = " ; ".join(x for x in (o.etat, o.risque) if x)
        morceaux.append(f"{o.composant} ({o.niveau}) : {bout}"
                        + (f" — {detail}" if detail else ""))
    p.commentaire = ". ".join(morceaux) + ("." if morceaux else "")
    p.reponse = ""
    p.origine = "regle"
    return p


def rediger_par_modele(p: Proposition, model: str,
                       timeout: float = 90.0) -> Proposition:
    """Demande au modèle local une réponse et un commentaire."""
    charge = {
        "model": model,
        "messages": [
            {"role": "system", "content": _SYSTEME},
            {"role": "user", "content": _contexte(p)},
        ],
        "stream": False,
        "options": {"temperature": 0.2, "repeat_penalty": 1.25,
                    "num_predict": 260},
    }
    r = httpx.post(f"{OLLAMA_BASE}/api/chat", json=charge, timeout=timeout)
    r.raise_for_status()
    contenu = (r.json().get("message") or {}).get("content", "")

    d = _extraire_json(contenu)
    if not d:
        return rediger_par_regle(p)

    reponse = str(d.get("reponse", "")).strip()
    fermee = question_fermee(p.question)
    if fermee:
        # Recale sur la forme attendue.
        for admise in REPONSES_FERMEES:
            if _norm(reponse) in (_norm(admise), _norm(admise).rstrip(".")):
                reponse = admise
                break
    elif _norm(reponse).rstrip(".") in ("oui", "non", "n.a", "na"):
        # Un Oui/Non ne répond pas à « Combien » : mieux vaut laisser le
        # champ vide, que le consultant verra, qu'une réponse plausible
        # et fausse qu'il validera sans y regarder.
        reponse = ""
    p.reponse = reponse
    p.commentaire = str(d.get("commentaire", "")).strip()
    p.origine = "modele"
    if not p.commentaire:
        return rediger_par_regle(p)
    return p


def ollama_disponible(model: str = "") -> bool:
    try:
        r = httpx.get(f"{OLLAMA_BASE}/api/tags", timeout=4)
        if r.status_code != 200:
            return False
        if not model:
            return True
        noms = [m.get("name", "") for m in r.json().get("models", [])]
        return any(n == model or n.split(":")[0] == model.split(":")[0]
                   for n in noms)
    except Exception:
        return False
