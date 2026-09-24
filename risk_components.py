"""
Catalogue des composants — Analyse des risques (visites de sites)

Les composants sont dérivés des questionnaires « État des lieux des actifs
critiques » utilisés en mission :

  • GAT Assurances_PCA_Questionnaire_Siège_V0.1.xlsx
      – onglet « Siège »       → catégories ENV / NAT / SAB / EVA / VID /
                                  INC / EAU / AEB / DIV / PRES
      – onglet « Data Center » → catégorie DC
  • ASTREE Assurances_SMCA_Risques Agences_V1.0.xlsx
      – onglet « Préalables Agences » → repli / continuité des agences

Chaque composant garde le préfixe de référence du questionnaire (`ref`) afin
que les notes et photos prises sur le terrain restent rattachables aux
questions d'origine lors de la génération de la synthèse.
"""
from typing import Dict, List

# ── Catégories (colonne « Catégorie » des questionnaires) ──────────────────────
CATEGORIES: Dict[str, str] = {
    "SAB":  "Sécurité & accès bâtiment",
    "EVA":  "Procédure d'évacuation",
    "VID":  "Vidéo-surveillance",
    "INC":  "Détection & extinction incendie",
    "EAU":  "Détection des fuites d'eau",
    "AEB":  "Alimentation électrique",
    "DC":   "Data Center",
    "DIV":  "Divers",
    "ENV":  "Environnement",
}


def _c(ref: str, label: str, icon: str) -> dict:
    return {"ref": ref, "label": label, "icon": icon, "category": CATEGORIES[ref]}


# ── Composants par catégorie ──────────────────────────────────────────────────
_SECURITE_ACCES = [
    _c("SAB", "Point d'accès / Entrée",        "fa-door-open"),
    _c("SAB", "Poste d'accueil",               "fa-bell-concierge"),
    _c("SAB", "Portique de sécurité",          "fa-person-through-window"),
    _c("SAB", "Scanner à bagages",             "fa-suitcase-rolling"),
    _c("SAB", "Contrôle d'accès par badge",    "fa-id-badge"),
    _c("SAB", "Alarme anti-intrusion",         "fa-bell"),
    _c("SAB", "Fenêtres",                      "fa-window-maximize"),
    _c("SAB", "Faux plafond",                  "fa-grip-lines"),
    _c("SAB", "Faux plancher",                 "fa-grip-lines"),
    _c("SAB", "Salle technique",               "fa-screwdriver-wrench"),
    _c("SAB", "Salle d'archives",              "fa-boxes-stacked"),
    _c("SAB", "Sous-sol / Parking",            "fa-square-parking"),
    _c("SAB", "Ascenseur",                     "fa-elevator"),
    _c("SAB", "Signalétique",                  "fa-signs-post"),
]

_EVACUATION = [
    _c("EVA", "Issue de secours",              "fa-person-running"),
    _c("EVA", "Escalier de secours",           "fa-stairs"),
    _c("EVA", "Plan d'évacuation",             "fa-map"),
    _c("EVA", "Point de rassemblement",        "fa-people-group"),
]

_VIDEO = [
    _c("VID", "Caméra de surveillance",        "fa-video"),
    _c("VID", "Poste central de sécurité",     "fa-desktop"),
]

_INCENDIE = [
    _c("INC", "Extincteur",                    "fa-fire-extinguisher"),
    _c("INC", "RIA (robinet incendie armé)",   "fa-faucet"),
    _c("INC", "Porte coupe-feu",               "fa-door-closed"),
    _c("INC", "Détecteur d'incendie",          "fa-fire"),
    _c("INC", "Extinction automatique",        "fa-spray-can-sparkles"),
    _c("INC", "Alarme incendie",               "fa-bullhorn"),
    _c("INC", "Stockage déchets / cartons",    "fa-dumpster"),
]

_EAU = [
    _c("EAU", "Détecteur de fuite d'eau",      "fa-droplet"),
]

_ELECTRICITE = [
    _c("AEB", "Groupe électrogène",            "fa-gas-pump"),
    _c("AEB", "Onduleur",                      "fa-car-battery"),
    _c("AEB", "Armoire électrique / TGBT",     "fa-bolt"),
    _c("AEB", "Tableau électrique d'étage",    "fa-plug"),
]

_DIVERS = [
    _c("DIV", "Coffre-fort",                   "fa-vault"),
    _c("DIV", "Paratonnerre",                  "fa-bolt-lightning"),
    _c("DIV", "Salle de crise",                "fa-headset"),
    _c("DIV", "Kitchenette / Salle de repos",  "fa-mug-hot"),
    _c("DIV", "État général du bâtiment",      "fa-building-circle-check"),
    _c("DIV", "Équipement désuet",             "fa-triangle-exclamation"),
]

_DATA_CENTER = [
    _c("DC",  "Baie / Rack",                   "fa-server"),
    _c("DC",  "Contrôle d'accès salle serveur", "fa-fingerprint"),
    _c("DC",  "Climatisation",                 "fa-snowflake"),
    _c("DC",  "Détecteur de température",      "fa-temperature-half"),
    _c("DC",  "Détecteur de fumée",            "fa-smog"),
    _c("DC",  "Extinction gaz inerte",         "fa-wind"),
    _c("DC",  "Faux plancher technique",       "fa-grip-lines"),
    _c("DC",  "Câblage courant fort / faible", "fa-ethernet"),
    _c("DC",  "Coupure d'urgence (EPO)",       "fa-hand"),
    _c("DC",  "Supervision environnementale",  "fa-gauge-high"),
    _c("DC",  "Multiprise",                    "fa-plug-circle-bolt"),
]

_ENVIRONNEMENT = [
    _c("ENV", "Abords / Voirie",               "fa-road"),
    _c("ENV", "Façade extérieure",             "fa-building"),
    _c("ENV", "Toiture / Terrasse",            "fa-house-chimney"),
]

# ── Composants par type de bâtiment ───────────────────────────────────────────
# Le Siège reprend l'intégralité du questionnaire (Siège + Data Center).
_SIEGE = (_ENVIRONNEMENT + _SECURITE_ACCES + _EVACUATION + _VIDEO
          + _INCENDIE + _EAU + _ELECTRICITE + _DATA_CENTER + _DIVERS)

# L'agence : périmètre réduit, orienté guichet / repli (questionnaire ASTREE).
_AGENCE = (
    _ENVIRONNEMENT
    + [
        _c("SAB", "Point d'accès / Entrée",        "fa-door-open"),
        _c("SAB", "Espace guichet / Accueil",      "fa-bell-concierge"),
        _c("SAB", "Contrôle d'accès par badge",    "fa-id-badge"),
        _c("SAB", "Alarme anti-intrusion",         "fa-bell"),
        _c("SAB", "Fenêtres",                      "fa-window-maximize"),
        _c("SAB", "Réserve / Archives agence",     "fa-boxes-stacked"),
    ]
    + _EVACUATION + _VIDEO + _INCENDIE + _EAU + _ELECTRICITE
    + [
        _c("DC",  "Baie réseau / Routeur",         "fa-network-wired"),
        _c("DC",  "Climatisation",                 "fa-snowflake"),
        _c("DIV", "Coffre-fort",                   "fa-vault"),
        _c("DIV", "DAB / GAB",                     "fa-money-bill-transfer"),
        _c("DIV", "État général du bâtiment",      "fa-building-circle-check"),
    ]
)

# L'archive : conservation documentaire (SAB.23 → SAB.27 du questionnaire).
_ARCHIVE = [
    _c("SAB", "Rayonnage / Étagères",          "fa-boxes-stacked"),
    _c("SAB", "Contrôle d'accès",              "fa-id-badge"),
    _c("SAB", "Porte du local",                "fa-door-closed"),
    _c("SAB", "Fenêtres",                      "fa-window-maximize"),
    _c("INC", "Extincteur",                    "fa-fire-extinguisher"),
    _c("INC", "Détecteur d'incendie",          "fa-fire"),
    _c("INC", "Extinction automatique",        "fa-spray-can-sparkles"),
    _c("EAU", "Détecteur de fuite d'eau",      "fa-droplet"),
    _c("DIV", "Aération / Hygrométrie",        "fa-wind"),
    _c("DIV", "Proximité point d'eau",         "fa-faucet-drip"),
    _c("AEB", "Tableau électrique d'étage",    "fa-plug"),
    _c("VID", "Caméra de surveillance",        "fa-video"),
]

_DATA_CENTER_SEUL = (
    _DATA_CENTER
    + [
        _c("SAB", "Porte de la salle",             "fa-door-closed"),
        _c("SAB", "Faux plafond",                  "fa-grip-lines"),
        _c("INC", "Extincteur",                    "fa-fire-extinguisher"),
        _c("INC", "Porte coupe-feu",               "fa-door-closed"),
        _c("EAU", "Détecteur de fuite d'eau",      "fa-droplet"),
        _c("AEB", "Groupe électrogène",            "fa-gas-pump"),
        _c("AEB", "Onduleur",                      "fa-car-battery"),
        _c("VID", "Caméra de surveillance",        "fa-video"),
    ]
)

# ── Types de bâtiment livrés en standard ──────────────────────────────────────
BUILDING_TYPES: List[dict] = [
    {"label": "Bâtiment - Siège",  "icon": "fa-building",            "components": _SIEGE},
    {"label": "Bâtiment - Agence", "icon": "fa-building-columns",    "components": _AGENCE},
    {"label": "Archive",           "icon": "fa-box-archive",         "components": _ARCHIVE},
    {"label": "Data Center",       "icon": "fa-server",              "components": _DATA_CENTER_SEUL},
    {"label": "Site de repli",     "icon": "fa-arrows-rotate",       "components": _SIEGE},
    {"label": "Entrepôt",          "icon": "fa-warehouse",           "components": _ARCHIVE + _SECURITE_ACCES},
    {"label": "Bureau",            "icon": "fa-briefcase",           "components": _AGENCE},
]

_DEFAULT_COMPONENTS = _SIEGE


def _dedup(components: List[dict]) -> List[dict]:
    """Supprime les doublons (même libellé) en conservant l'ordre."""
    seen, out = set(), []
    for c in components:
        if c["label"] not in seen:
            seen.add(c["label"])
            out.append(c)
    return out


def building_types() -> List[dict]:
    """Types de bâtiment standard, sans la liste des composants."""
    return [{"label": b["label"], "icon": b["icon"]} for b in BUILDING_TYPES]


def components_for(building_type: str) -> List[dict]:
    """Composants proposés pour un type de bâtiment.

    Un type personnalisé (ajouté par l'utilisateur) reçoit le catalogue
    complet du Siège, le plus large, à charge pour le consultant de ne
    renseigner que ce qui est pertinent.
    """
    for b in BUILDING_TYPES:
        if b["label"] == building_type:
            return _dedup(b["components"])
    return _dedup(_DEFAULT_COMPONENTS)


def components_grouped(building_type: str) -> List[dict]:
    """Composants regroupés par catégorie, pour l'affichage en sections."""
    groups: Dict[str, List[dict]] = {}
    for c in components_for(building_type):
        groups.setdefault(c["category"], []).append(c)
    return [{"category": cat, "components": items} for cat, items in groups.items()]
