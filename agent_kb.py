# -*- coding: utf-8 -*-
"""
agent_kb.py — ce que l'assistant sait de l'application
=======================================================

Deux sources distinctes, et c'est volontaire :

  * ce fichier décrit les pages, leurs fonctions et la manière de s'en
    servir. C'est une connaissance stable, écrite à la main, que personne
    n'a à deviner ;
  * la base de données fournit les chiffres et les noms, interrogés en
    direct à chaque question.

L'assistant ne « sait » donc rien qu'il ait inventé : il reformule ce qui
est ici ou ce qui vient de la base. Un modèle de trois milliards de
paramètres n'a pas la fiabilité nécessaire pour décider seul d'ouvrir un
fichier ou de changer de page — ces actions sont décidées par le code, le
modèle ne rédige que la réponse.
"""
from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# Les pages de l'application
# ─────────────────────────────────────────────────────────────────────────────
# `page`    : identifiant interne, celui que dvGo() attend
# `url`     : pour les pages autonomes, qui ne vivent pas dans l'application
# `demo`    : étapes de la visite guidée — un sélecteur et ce qu'il faut en dire

PAGES = {
    "page-accueil": {
        "titre": "Accueil",
        "groupe": "",
        "resume": "Présente SmartPCA et les trois phases d'une mission PCA : "
                  "Business Impact Analysis, Analyse des Risques, Stratégie.",
        "a_quoi_ca_sert": "C'est le point d'entrée. Les chiffres affichés viennent "
                          "de la base : nombre de fiches capitalisées, activités "
                          "indexées, formats de fiche reconnus.",
        "demo": [
            (".dv-headline", "Le titre",
             "La promesse de l'outil : mener un PCA du cadrage à la mise en place "
             "sans ressaisir les données d'une étape à l'autre."),
            (".home-metrics", "Les chiffres",
             "Ils sont lus dans la base à chaque chargement : ce sont vos projets "
             "réels, pas des valeurs d'exemple."),
            (".dv-grid-3", "Les trois phases",
             "La méthode de la mission. Les modules de l'application se rattachent "
             "à ces phases, et se trouvent dans la barre du haut."),
        ],
    },

    "page-generer-fiches": {
        "titre": "Générer les fiches BIA",
        "groupe": "Synthèse BIA",
        "resume": "Produit une fiche BIA par département à partir d'une fiche de "
                  "recensement (.xlsx) et d'un modèle Word.",
        "a_quoi_ca_sert": "Évite de créer à la main des dizaines de documents "
                          "identiques. Les colonnes du recensement sont détectées "
                          "automatiquement.",
        "comment": "Déposez le recensement et le modèle .docx, vérifiez la "
                   "correspondance des colonnes, puis générez. Les fiches peuvent "
                   "être enregistrées dans la base ou envoyées directement à l'étape 01.",
        "demo": [
            ("#page-generer-fiches .tdb-card", "Le tableau de bord de l'étape",
             "Il rappelle ce que l'étape attend en entrée et ce qu'elle produit."),
            ("#page-generer-fiches .drop-zone", "Les fichiers d'entrée",
             "Le recensement (.xlsx) donne la liste des entités ; le modèle (.docx) "
             "donne la forme de la fiche."),
        ],
    },

    "page-charger-fiches": {
        "titre": "Charger les fiches BIA",
        "groupe": "Synthèse BIA",
        "resume": "Charge les fiches BIA remplies par le client, depuis la base de "
                  "projets ou depuis le poste.",
        "a_quoi_ca_sert": "C'est l'entrée de la chaîne : au moins une fiche est "
                          "nécessaire pour générer la synthèse.",
        "comment": "Glissez les .docx dans la zone de dépôt, ou sélectionnez un "
                   "secteur puis des fiches déjà enregistrées. Les fiches chargées "
                   "alimentent automatiquement les étapes 02 et 03.",
        "demo": [
            ("#cf-zone-fiches", "Déposer des fiches",
             "Les fiches .docx du client, en lot. Quatre formats sont reconnus, "
             "dont l'état des lieux."),
            ("#cf-db-list", "Reprendre depuis la base",
             "Plutôt que de rechercher les fichiers, on reprend des fiches déjà "
             "capitalisées lors d'une mission précédente."),
            ("#cf-loaded-card", "Les fiches retenues",
             "Ce qui part vers la synthèse. Le compteur débloque l'étape suivante."),
        ],
    },

    "page-generer-synthese": {
        "titre": "Générer la synthèse BIA",
        "groupe": "Synthèse BIA",
        "resume": "Consolide les fiches chargées en une seule synthèse Excel.",
        "a_quoi_ca_sert": "C'est le gain de temps principal : consolider des dizaines "
                          "de documents à la main prend des jours.",
        "comment": "Déposez la synthèse modèle (.xlsx), lancez la génération, puis "
                   "téléchargez le résultat. Chaque feuille est remplie à partir des "
                   "fiches : activités, impacts, DMIA, échanges, applications, documents.",
        "demo": [
            ("#gen-loaded-bar", "Les fiches en entrée",
             "Ce qui a été retenu à l'étape 01."),
            ("#s1-zone-synthese", "La synthèse modèle",
             "Le classeur dont la structure sera remplie. Ses feuilles sont "
             "reconnues par leur nom."),
            ("#s1-progress-panel", "L'avancement",
             "Fiche par fiche, avec le détail de ce qui est écrit dans chaque feuille."),
        ],
    },

    "page-sync": {
        "titre": "Synchronisation bidirectionnelle",
        "groupe": "Synthèse BIA",
        "resume": "Compare les DMIA de la synthèse à celles des fiches et réécrit "
                  "les fiches quand la synthèse change.",
        "a_quoi_ca_sert": "Évite la divergence entre le tableau de pilotage et les "
                          "fiches signées par les métiers.",
        "comment": "Chargez la synthèse et les fiches, consultez les écarts, puis "
                   "appliquez les corrections aux fiches concernées.",
        "demo": [
            ("#page-sync .tdb-card", "Le principe",
             "La synthèse fait foi : ses DMIA arbitrées sont réécrites dans les "
             "fiches .docx correspondantes."),
        ],
    },

    "page-organigramme": {
        "titre": "Génération de l'organigramme",
        "groupe": "Synthèse BIA",
        "resume": "Transforme l'organigramme du client en structure exploitable.",
        "a_quoi_ca_sert": "Un organigramme reçu en image devient une arborescence "
                          "manipulable, au lieu d'être ressaisi.",
        "comment": "Importez la synthèse (.xlsx) ou une image d'organigramme, puis "
                   "choisissez le moteur d'analyse.",
        "demo": [
            ("#page-organigramme .tdb-card", "Les moteurs d'analyse",
             "Plusieurs approches sont proposées : vision, OCR, hybride. Le choix "
             "dépend de la qualité du document d'origine."),
        ],
    },

    "page-rapport": {
        "titre": "Tableau de bord BIA",
        "groupe": "Rapport BIA",
        "resume": "Vue consolidée de la mission à partir de la synthèse : courbe de "
                  "reprise, applications par lot, équipements.",
        "a_quoi_ca_sert": "Donne l'état de la mission d'un coup d'œil et produit le "
                          "rapport PowerPoint complet.",
        "comment": "Déposez la synthèse (.xlsx). Une seconde synthèse peut être "
                   "ajoutée pour comparer deux versions.",
        "demo": [
            ("#rp-upload-zone", "La synthèse en entrée",
             "Tout le tableau de bord est calculé à partir de ce classeur."),
            ("#rp-btn-dash", "Afficher le tableau de bord",
             "Les graphiques sont calculés à la volée, rien n'est lu dans des "
             "cellules de résultat."),
            ("#rp-btn-pptx", "Export PowerPoint",
             "Le livrable complet : activités, applications par lot de reprise, "
             "collaborateurs clés, documents critiques."),
        ],
    },

    "page-montee-en-charge": {
        "titre": "Montée en charge",
        "groupe": "Rapport BIA",
        "resume": "Reprise des effectifs et des positions de travail par palier.",
        "a_quoi_ca_sert": "C'est la donnée que le client réclame en premier en "
                          "comité : combien de personnes, à quel horizon.",
        "comment": "Déposez la synthèse (.xlsx) ; le diagramme se construit à partir "
                   "des feuilles de montée en charge.",
        "demo": [
            ("#page-montee-en-charge .tdb-card", "Lecture du diagramme",
             "Chaque palier correspond à un horizon de reprise, de H+1 à J+30."),
        ],
    },

    "page-collecte-specifiques": {
        "titre": "Questionnaires spécifiques",
        "groupe": "Gestion des risques",
        "resume": "Les questionnaires de référence du cabinet, éditables et versionnés.",
        "a_quoi_ca_sert": "Remplace le fichier Excel recopié de mission en mission. "
                          "Sept types sont prévus.",
        "comment": "Choisissez un type pour consulter ou modifier son questionnaire "
                   "de référence : ajout de lignes et de colonnes, renommage, édition.",
        "demo": [
            ("#qzs-screen-type", "Les sept types",
             "Bâtiment siège, bâtiment agence, RH, fournisseurs, cyber, gouvernance "
             "de crise, télétravail."),
        ],
    },

    "page-collecte-reponses": {
        "titre": "Réponses",
        "groupe": "Gestion des risques",
        "resume": "Compare le questionnaire rempli par le client au questionnaire "
                  "de référence.",
        "a_quoi_ca_sert": "Fait ressortir les questions sans réponse et celles qui "
                          "manquent, au lieu de relire deux cents lignes.",
        "comment": "Choisissez un type, chargez la réponse du client, puis comparez.",
        "demo": [
            ("#qzr-screen-type", "Choix du type",
             "La comparaison se fait toujours contre le questionnaire de référence "
             "du même type."),
        ],
    },

    "page-risques": {
        "titre": "Cartographie des risques",
        "groupe": "Gestion des risques",
        "resume": "Charge une cartographie, recalcule les scores et propose des "
                  "plans d'actions par IA locale.",
        "a_quoi_ca_sert": "Les scores bruts et nets sont recalculés par le moteur, "
                          "jamais lus dans les cellules — les classeurs arrivent "
                          "souvent avec des formules pointant vers un fichier disparu.",
        "comment": "Chargez le classeur, vérifiez les constats et les éléments de "
                   "maîtrise, puis lancez la génération des plans d'actions.",
        "demo": [
            ("#page-risques .tdb-card", "Le classeur de cartographie",
             "Il doit comporter au minimum les colonnes Constats et Éléments de "
             "maîtrise ; les scores sont recalculés."),
        ],
    },

    "page-risques-assessment": {
        "titre": "Analyse des risques",
        "groupe": "Gestion des risques",
        "resume": "Relevé photo par niveau lors des visites de site, puis "
                  "pré-remplissage des questionnaires à partir des observations.",
        "a_quoi_ca_sert": "Une photo ne peut pas exister sans être rattachée à un "
                          "niveau et à un composant : le contexte est structurel, "
                          "plus une affaire de mémoire au retour au bureau.",
        "comment": "Renseignez le contexte de la visite, démarrez le relevé, "
                   "choisissez un niveau puis un composant, et saisissez "
                   "l'observation avec ses photos.",
        "demo": [
            ("#ra-card-setup", "Le contexte de la visite",
             "Secteur, client, nombre de niveaux et type de bâtiment. Le type "
             "détermine la liste des composants proposés."),
            ("#ra-building-types", "Le type de bâtiment",
             "Siège, agence, archive, data center… Chaque type a son catalogue de "
             "composants, extrait des questionnaires de référence."),
            ("#ra-start-btn", "Démarrer le relevé",
             "Crée les niveaux. C'est après ce bouton qu'apparaissent la grille des "
             "niveaux et la saisie des observations."),
            ("#ra-floors-grid", "Les niveaux",
             "Cliquez un niveau pour voir ses composants, puis un composant pour "
             "saisir l'observation et prendre des photos."),
            ("#ra-card-prefill", "Pré-remplir un questionnaire",
             "Les observations sont rattachées aux questions par leur rubrique. "
             "L'IA locale rédige une proposition par question ; rien n'est écrit "
             "tant que vous n'avez pas validé."),
        ],
    },
}

# Pages autonomes, hors application à onglets
PAGES_AUTONOMES = {
    "/projects": {
        "titre": "Base de projets",
        "resume": "Toutes les fiches et synthèses enregistrées, par secteur et par "
                  "client, avec l'explorateur DMIA.",
        "a_quoi_ca_sert": "La mémoire du cabinet : les missions passées cessent "
                          "d'être des fichiers dormants sur un partage réseau.",
        "comment": "Ajoutez des fichiers par secteur et client ; cherchez une "
                   "activité dans l'explorateur DMIA pour voir ce qui a été retenu "
                   "ailleurs.",
        "demo": [
            ("#panel-left", "Statistiques et explorateur",
             "Le nombre de projets, d'activités et de clients, puis la recherche "
             "de DMIA par nom d'activité."),
            ("#panel-right", "La bibliothèque",
             "Les documents groupés par secteur et par client."),
        ],
    },
    "/fiche-selector": {
        "titre": "Éditeur de fiches — sélection",
        "resume": "Point d'entrée de l'éditeur : secteur, puis nouvelle fiche ou "
                  "fiche existante.",
        "a_quoi_ca_sert": "Permet d'ouvrir une fiche déjà enregistrée sans la "
                          "rechercher sur le disque.",
        "comment": "Choisissez le secteur, puis « Charger une fiche existante » "
                   "pour parcourir la base.",
        "demo": [
            (".sec-card", "Le secteur",
             "Il filtre la bibliothèque présentée ensuite."),
        ],
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Reconnaissance d'une page à partir d'une formulation libre
# ─────────────────────────────────────────────────────────────────────────────

SYNONYMES = {
    "page-accueil":              ["accueil", "home", "page d'accueil", "début"],
    "page-generer-fiches":       ["generer les fiches", "créer les fiches",
                                  "fabriquer les fiches", "recensement", "etape 00"],
    "page-charger-fiches":       ["charger les fiches", "importer les fiches",
                                  "deposer les fiches", "etape 01", "upload fiche"],
    "page-generer-synthese":     ["generer la synthese", "consolider", "synthese bia",
                                  "faire la synthese", "etape 02"],
    "page-sync":                 ["synchronisation", "sync", "bidirectionnelle",
                                  "comparer dmia", "etape 03"],
    "page-organigramme":         ["organigramme", "structure", "hierarchie", "etape 04"],
    "page-rapport":              ["tableau de bord", "rapport bia", "dashboard",
                                  "powerpoint", "pptx", "courbe de reprise"],
    "page-montee-en-charge":     ["montee en charge", "effectifs", "palier", "reprise rh"],
    "page-collecte-specifiques": ["questionnaire", "questionnaires specifiques",
                                  "questions de reference", "modele de questionnaire"],
    "page-collecte-reponses":    ["reponses", "comparaison client", "reponse client"],
    "page-risques":              ["cartographie", "cartographie des risques",
                                  "plan d'actions", "plans d'actions", "scoring"],
    "page-risques-assessment":   ["analyse des risques", "releve", "visite de site",
                                  "photo", "photos", "niveau", "composant",
                                  "pre-remplir", "prefill"],
    "/projects":                 ["base de projets", "bibliotheque", "explorateur dmia",
                                  "projets enregistres"],
    "/fiche-selector":           ["editeur de fiche", "editeur de fiches",
                                  "ouvrir une fiche", "modifier une fiche"],
}


def tout_le_catalogue() -> dict:
    """Pages internes et autonomes réunies, pour la recherche."""
    out = dict(PAGES)
    out.update(PAGES_AUTONOMES)
    return out
