# BIA Automatique — Guide Utilisateur

> Ce guide explique ce que fait chaque partie de l'application, ce que vous lui donnez,
> ce que vous obtenez en retour, et comment l'application se souvient de vos travaux passés.
> Aucune connaissance technique n'est nécessaire.

---

## La Base de Données — C'est quoi ?

L'application dispose d'une **base de données locale** (un fichier appelé `projects.db`
stocké sur votre ordinateur, dans le même dossier que l'application). Considérez-la
comme la **mémoire** de l'application.

Chaque fois que vous sauvegardez un projet, importez un fichier ou remplissez une fiche,
l'application enregistre ces informations. Cela lui permet de :

- Se souvenir de tous vos projets et de leurs fichiers.
- Vous suggérer des valeurs DMIA basées sur ce que d'autres clients ont déjà validé.
- Vous alerter lorsqu'une DMIA que vous avez saisie est plus longue (moins stricte)
  que ce qui a été retenu pour des activités similaires chez d'autres clients.

La base de données conserve deux types d'informations :
1. **Les projets** — le nom, le secteur, le client et le type de chaque fichier sauvegardé.
2. **Les paires Activité / DMIA** — pour chaque projet, la liste des activités et leurs
   valeurs DMIA. C'est ce « capital de connaissance » qui alimente le système de suggestions.

---

## Étape 00 — Générer les fiches BIA

**Ce que ça fait** : À partir de votre fichier de recensement (la liste de vos
départements), l'application génère automatiquement une fiche BIA vierge par
département, pré-remplie avec le nom du département et les informations du client.
Vous obtenez un ensemble de fichiers Word prêts à être envoyés — un par équipe.

---

**Ce que vous lui donnez**

| Quoi | Format | Obligatoire ? |
|---|---|---|
| Fiche de recensement | Fichier Excel (`.xlsx`) — liste de vos structures/départements | Oui |
| Modèle de fiche BIA | Fichier Word (`.docx`) — le modèle de fiche vierge que vous utilisez | Oui |
| Nom du client | Champ texte | Non — l'application essaiera de le détecter automatiquement |
| Version | `1.0` ou `2.0` | Non — `2.0` par défaut |
| Clé API OpenAI | Champ texte | Non — uniquement nécessaire pour la détection automatique du nom du client |

---

**Ce que vous obtenez**

Une **archive ZIP** contenant un fichier `.docx` par département trouvé dans votre
recensement. Par exemple, si votre recensement liste 15 départements, vous obtenez
un ZIP avec 15 fichiers Word, chacun nommé d'après son département et prêt à être envoyé.

---

**Base de données — ce qui est enregistré**

Après avoir reçu le ZIP, vous pouvez cliquer sur **« Enregistrer dans la base »**. Cela va :
- Sauvegarder chaque fichier Word dans la bibliothèque de projets sur votre ordinateur
  (dans un dossier : `projects_data / secteur / client / fiche /`).
- Enregistrer chaque fichier dans la base de données avec le nom du département et
  les valeurs DMIA (une fois que les fiches sont remplies et réimportées).

**Rien n'est sauvegardé automatiquement** — seulement quand vous cliquez sur le bouton.

---

**Base de données — ce qu'elle utilise**

Cette étape ne **lit pas** la base de données. Elle y écrit uniquement si vous le demandez.

---

## Étape 01 — Charger les fiches BIA

**Ce que ça fait** : C'est le point d'entrée pour vos fiches BIA remplies. Vous déposez
ici les fichiers Word afin que l'application sache quelles fiches traiter à l'étape suivante.

---

**Ce que vous lui donnez**

Une ou plusieurs fiches BIA remplies au format `.docx`. Celles-ci peuvent provenir de :
- Fichiers que vous importez directement en les faisant glisser dans la zone de dépôt.
- Le ZIP généré à l'Étape 00 — en cliquant sur « Utiliser dans l'étape 01 » sur la
  page précédente, les fichiers sont chargés ici automatiquement, sans avoir à les
  re-importer.

---

**Ce que vous obtenez**

Aucun fichier n'est généré ici. Cette étape conserve simplement les fiches en mémoire
pour que l'Étape 02 puisse les utiliser.

---

**Base de données — ce qui est enregistré / utilisé**

Cette étape n'interagit pas avec la base de données. C'est uniquement une zone de transit.

---

## Étape 02 — Générer la Synthèse BIA

**Ce que ça fait** : C'est le cœur de l'application. Elle lit toutes les fiches BIA
chargées à l'Étape 01, en extrait toutes les données (activités, scores d'impact,
valeurs DMIA, applications IT, collaborateurs clés, etc.) et écrit tout dans un seul
fichier Excel Synthèse BIA — automatiquement.

---

**Ce que vous lui donnez**

| Quoi | Format | Obligatoire ? |
|---|---|---|
| Fiches BIA | Les fichiers chargés à l'Étape 01 | Oui |
| Modèle de synthèse | Fichier Excel (`.xlsx`) — le modèle Synthèse BIA vierge | Oui |
| Fiche de recensement | Fichier Excel (`.xlsx`) | Fortement recommandé |
| Correspondance des colonnes | Sélecteurs (affichés si l'application n'est pas sûre) | Parfois |
| Modèle Ollama | Champ texte | Non — pour une amélioration optionnelle par IA |

**Pourquoi le recensement est-il recommandé ?**
Sans lui, l'application utilise les lignes déjà présentes dans votre modèle de synthèse.
Avec lui, l'application construit d'abord la structure organisationnelle depuis le
recensement (une ligne par département), ce qui garantit que chaque département de vos
fiches trouvera sa place dans la synthèse.

---

**Ce que vous obtenez**

Un fichier **Excel Synthèse BIA** (`.xlsx`) rempli, prêt à être relu. Tous les onglets
sont alimentés : Activités, Impact DMIA, Applications IT, Échanges, Montée en charge,
Collaborateurs Clés, Documents critiques, etc.

---

**Base de données — ce qui est enregistré**

Cette étape **n'enregistre rien** dans la base de données.

---

**Base de données — ce qu'elle utilise**

Cette étape **ne lit pas non plus** la base de données. La correspondance entre les
fiches et les lignes de la synthèse se fait entièrement par comparaison de noms —
grâce à une recherche floue intelligente (voir la section dédiée plus bas).

---

## Étape 03 — Synchronisation

**Ce que ça fait** : Compare les valeurs DMIA de votre Synthèse BIA avec les valeurs
DMIA de vos fiches individuelles. Elle signale tout écart — par exemple, lorsqu'une
DMIA d'activité a été modifiée dans la synthèse lors de l'arbitrage, mais que la fiche
d'origine affiche toujours l'ancienne valeur.

---

**Ce que vous lui donnez**

| Quoi | Format | Notes |
|---|---|---|
| Synthèse BIA | Fichier Excel (`.xlsx`) | La référence — contient les DMIA arbitrées |
| Fiches BIA | Fichiers `.docx` ou projets sauvegardés en base | Les fiches à comparer |

Vous pouvez choisir les fiches de deux façons :
- Importer directement des fichiers `.docx`.
- Sélectionner des projets déjà sauvegardés dans la base de données.

---

**Ce que vous obtenez**

Un tableau des **écarts** — une ligne par divergence, indiquant :
- Quel département et quelle activité est en conflit.
- La DMIA dans la Synthèse versus la DMIA dans la fiche.
- De quelle fiche provient le conflit.

Vous pouvez marquer chaque écart comme « Résolu » une fois traité.

---

**Base de données — ce qu'elle utilise**

Lorsque vous sélectionnez des projets depuis la base, l'application :
- Retrouve le chemin du fichier `.docx` sauvegardé pour le relire à la volée.
- Utilise les paires activité/DMIA déjà stockées pour accélérer la comparaison.

---

## Étape 04 — Organigramme Interactif

**Ce que ça fait** : Génère un organigramme visuel de la structure de votre client,
coloré selon le niveau de criticité de chaque département (basé sur leur DMIA la plus
critique). Vous pouvez explorer la hiérarchie, cliquer sur les départements pour voir
leurs activités, et naviguer librement dans le schéma.

---

**Ce que vous lui donnez**

Un fichier **Synthèse BIA** Excel (`.xlsx`).

---

**Ce que vous obtenez**

Un arbre interactif directement dans le navigateur :

| Couleur | Signification |
|---|---|
| 🔴 Rouge | DMIA ≤ 2 jours — très critique, redémarrage requis sous 48 heures |
| 🟠 Orange | DMIA 3 à 5 jours — modérément critique |
| 🟢 Vert | DMIA > 5 jours — moins urgent |
| ⚫ Gris | Aucune donnée DMIA trouvée pour ce département |

**Fonctionnalités disponibles :**
- Cliquer sur un nœud → développer ou réduire sa hiérarchie.
- Cliquer sur un département (feuille) → ouvre un panneau listant toutes ses activités/applications avec leurs DMIA.
- Bouton « Tous les DMIA » → tableau récapitulatif de tous les départements et leur DMIA.
- Basculer entre la vue **Activités** et la vue **Applications IT**.
- Zoom à la molette, déplacement en maintenant le clic.
- Mode plein écran.
- Export du schéma en image `.svg`.

---

**Base de données — ce qui est enregistré / utilisé**

Cette étape n'utilise pas la base de données. Le schéma est construit entièrement
à partir du fichier Excel importé.

---

## Éditeur de Fiches

**Ce que ça fait** : Un formulaire web pour créer ou modifier une fiche BIA directement
dans le navigateur — sans avoir besoin de Word. Vous remplissez toutes les sections de la
fiche (activités, impacts, DMIA, échanges, collaborateurs clés, applications IT, etc.)
et l'application génère le document Word pour vous.

---

**Ce que vous lui donnez**

Vous remplissez les sections du formulaire directement dans le navigateur :
- Nom du département, responsable, détails de l'organisation.
- Liste des activités — nom, description, criticité.
- Pour chaque activité : scores d'impact, DMIA exprimée, premières actions.
- Échanges fonctionnels (internes et externes).
- Montée en charge (effectifs à chaque horizon temporel).
- Collaborateurs clés, applications IT, autres équipements, documents critiques.

Vous pouvez aussi **ouvrir une fiche existante** depuis la base de données (soit une
session éditeur précédemment sauvegardée, soit un fichier `.docx` importé — l'application
analysera le document Word et pré-remplira le formulaire automatiquement).

---

**Ce que vous obtenez**

Un document **Word** (`.docx`) de la fiche BIA complète, généré et téléchargé
automatiquement lorsque vous cliquez sur « Générer le document ».

---

**Base de données — ce qui est enregistré**

Lorsque vous cliquez sur **« Enregistrer »**, l'application sauvegarde :
- Une fiche projet (secteur, client, nom) dans la base de données.
- Toutes les activités et leurs valeurs DMIA dans la table des activités — afin
  qu'elles contribuent aux suggestions DMIA pour les futures sessions.
- Un fichier JSON sur le disque contenant l'intégralité de l'état du formulaire,
  pour que vous puissiez revenir le modifier ultérieurement.

---

**Base de données — ce qu'elle utilise et pourquoi**

L'éditeur exploite la base de données de deux façons pendant votre travail :

**1. Suggestions DMIA**
Dès que vous tapez un nom d'activité, l'application interroge la base pour trouver
des activités similaires issues de projets passés et vous suggère leurs valeurs
DMIA validées. Celles-ci apparaissent sous forme de petites étiquettes cliquables
à côté du champ DMIA.

*Pourquoi est-ce utile ?* Au lieu de décider de zéro si « Gestion de la trésorerie »
a une DMIA de 4H ou J+1, vous pouvez voir ce que d'autres clients (dans le même
secteur ou sur l'ensemble des secteurs) ont déjà validé — et cliquer pour réutiliser
cette valeur.

Les suggestions sont classées de la DMIA la plus courte à la plus longue
(la plus critique en premier).

**2. Alertes DMIA**
Si vous saisissez une DMIA plus longue que ce que la base a enregistré pour une
activité similaire chez un autre client, un badge d'avertissement apparaît.
Par exemple : si vous saisissez « J+5 » pour « Paiements fournisseurs » mais que
la base indique qu'une compagnie d'assurance a validé « 4H » pour la même activité —
vous verrez une alerte vous invitant à vérifier si J+5 est bien acceptable.

*Pourquoi est-ce utile ?* C'est un filet de sécurité qui assure la cohérence entre
les clients et les projets, et qui détecte les cas où une DMIA aurait été fixée trop
facilement sans savoir qu'un précédent plus strict existait ailleurs.

---

## Projets

**Ce que ça fait** : Une bibliothèque de tous les fichiers BIA que vous avez sauvegardés
via l'application. Les fichiers sont organisés par secteur → client → type de fichier.

---

**Ce que vous lui donnez**

Vous pouvez :
- Consulter et rechercher des projets sauvegardés.
- Importer de nouveaux fichiers `.docx` (fiches) ou `.xlsx` (synthèses) directement
  dans la bibliothèque d'un client.
- Supprimer un projet (supprime son enregistrement en base et toutes ses données DMIA).

---

**Ce que vous obtenez**

Lorsque vous importez un fichier ici :
- Le fichier est sauvegardé dans la bibliothèque de projets sur le disque.
- L'application extrait toutes les paires activité/DMIA du fichier et les stocke
  en base, enrichissant ainsi le capital de suggestions pour les futures sessions.

---

**Base de données — ce qui est enregistré / utilisé**

- **Enregistre** : Une fiche projet par fichier + toutes ses paires activité/DMIA.
- **Lit** : Affiche la liste complète des projets groupés par secteur et client,
  avec le nombre d'activités stockées par projet.

---

## La Recherche Floue — Comment ça marche ? (En clair)

Lors du traitement des fiches à l'Étape 02, l'application doit identifier quelle
ligne de la Synthèse Excel correspond à quel département. Le problème est que le
nom dans la fiche et le nom dans la synthèse ne sont souvent pas exactement identiques :

- La fiche peut indiquer **« Département Informatique »** mais la synthèse dit **« Informatique »**.
- Ou la synthèse contient une faute de frappe : **« ingenieurue »** au lieu de **« ingénierie »**.
- Ou les accents sont absents : **« Reglementaire »** contre **« Réglementaire »**.

Pour résoudre cela, l'application utilise un algorithme de **recherche floue**. Au lieu
de demander « ces deux noms sont-ils identiques ? », elle demande « à quel point
ces deux noms se ressemblent-ils ? » et attribue un score de similarité de 0 à 100.

L'application exige un score supérieur à **88 sur 100** pour considérer deux noms
comme correspondants. C'est assez strict pour éviter de confondre des noms proches
(comme « GAT Invest » et « GAT Vie »), mais suffisamment souple pour gérer les fautes
de frappe et les accents manquants.

Si l'application ne trouve pas de correspondance suffisamment fiable, elle signale
le nom du département comme **« absent du recensement »** dans les avertissements —
pour que vous sachiez quelles fiches n'ont pas été intégrées dans la synthèse et pourquoi.

---

## Tableau Récapitulatif

| Fonctionnalité | Ce que vous donnez | Ce que vous obtenez | Enregistre en base | Lit la base |
|---|---|---|---|---|
| Générer les fiches (Étape 00) | Recensement + Modèle | ZIP de fiches vierges | Uniquement si vous cliquez « Enregistrer » | Non |
| Charger les fiches (Étape 01) | Fichiers `.docx` remplis | — (zone de transit) | Non | Non |
| Générer la Synthèse (Étape 02) | Fiches + Modèle Synthèse + Recensement | Synthèse `.xlsx` remplie | Non | Non |
| Synchronisation (Étape 03) | Synthèse + Fiches (ou projets base) | Rapport d'écarts | Non | Oui — pour charger les fiches sauvegardées |
| Organigramme (Étape 04) | Synthèse `.xlsx` | Arbre interactif coloré | Non | Non |
| Éditeur de fiches | Formulaire (navigateur) | Fiche `.docx` générée | Oui — à la sauvegarde | Oui — pour suggestions et alertes DMIA |
| Projets | Fichiers à importer | Fichier sauvegardé en bibliothèque | Oui — automatiquement | Oui — pour afficher la liste des projets |
