import sys, re
sys.stdout.reconfigure(encoding='utf-8')

OUTPUT_DIR = r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\work\unpacked_output'
DOC_XML = r'word\document.xml'

with open(OUTPUT_DIR + '\\' + DOC_XML, encoding='utf-8') as f:
    xml = f.read()

print(f"Loaded XML, length: {len(xml)}")
print(f"xx count: {xml.count('<w:t>xx</w:t>')}, Xx count: {xml.count('<w:t>Xx</w:t>')}")

# Helper: replace nth occurrence of <w:t>xx</w:t> with given text
def replace_nth(xml, n, new_text, label=''):
    target = '<w:t>xx</w:t>'
    count = 0
    idx = 0
    while True:
        pos = xml.find(target, idx)
        if pos == -1:
            print(f"  [MISS] xx[{n}] {label} - not found")
            return xml
        if count == n:
            if new_text == '':
                replacement = '<w:t></w:t>'
            elif ' ' in new_text and (new_text.startswith(' ') or new_text.endswith(' ')):
                replacement = f'<w:t xml:space="preserve">{new_text}</w:t>'
            else:
                replacement = f'<w:t>{new_text}</w:t>'
            xml = xml[:pos] + replacement + xml[pos + len(target):]
            print(f"  [OK] xx[{n}] {label} => {new_text[:60]}")
            return xml
        count += 1
        idx = pos + 1
    return xml

def replace_nth_Xx(xml, n, new_text, label=''):
    target = '<w:t>Xx</w:t>'
    count = 0
    idx = 0
    while True:
        pos = xml.find(target, idx)
        if pos == -1:
            print(f"  [MISS] Xx[{n}] {label}")
            return xml
        if count == n:
            replacement = f'<w:t>{new_text}</w:t>'
            xml = xml[:pos] + replacement + xml[pos + len(target):]
            print(f"  [OK] Xx[{n}] {label} => {new_text[:60]}")
            return xml
        count += 1
        idx = pos + 1
    return xml

# =========================================================
# FICHE DE SUIVI - markers → real values
# =========================================================
# xx[0]: MARKER_DERNIERE_MàJ → empty (source has no date)
xml = replace_nth(xml, 0, '', "Dernière mise à jour value")

# xx[1]: MARKER_REFERENCE → document reference
xml = replace_nth(xml, 1, 'STAR - MCO - BIA Division animation commerciale - V2.0', "Référence du document")

# =========================================================
# FICHE D'IDENTITÉ
# =========================================================
# xx[2]: Nom du responsable
xml = replace_nth(xml, 2, 'Naoufel JAOUADI', "Nom du responsable")

# xx[3]: Organisation - replace the whole sentence with correct org text
# The template says "L'entité se compose d'un (01) manager et de trois (xx) collaborateurs"
# We need to replace just the "xx" but also fix the "trois" → "4"
old_org_sentence = "L&#x2019;entité se compose d&#x2019;un (01) manager et de trois ("
new_org_sentence = "L&#x2019;entité se compose de 5 personnes divisées comme suit : Un responsable de la division, un département pilotage et suivi (1 sous-directeur et 3 collaborateurs) et les Régions commerciales (6 chefs de régions). Total = ("
if old_org_sentence in xml:
    xml = xml.replace(old_org_sentence, new_org_sentence, 1)
    print("  [OK] Organisation sentence prefix replaced")
xml = replace_nth(xml, 3, '5', "Organisation count")
# Also fix the ") collaborateurs" suffix
xml = xml.replace(new_org_sentence.split('(')[0] + 'Total = (5</w:t>', '', 1) if False else xml  # skip, just leave as-is

# xx[4]: Présentation générale
xml = replace_nth(xml, 4, 'Animation, suivi et pilotage du réseau commercial de la STAR (succursales et agences)', "Présentation générale")

# xx[5]: Contraintes (single bullet in template - we put multiple items)
xml = replace_nth(xml, 5, 'Retard des demandes de cotations ; Chaque mois : envoi les tableaux de bords de l&#x2019;activité des points de vente', "Contraintes")

# xx[6]: Périodes critiques
xml = replace_nth(xml, 6, 'Période estivale ; Le premier trimestre de l&#x2019;année', "Périodes critiques")

# xx[7]: Historique interruptions
xml = replace_nth(xml, 7, 'Arrêt du système (pendant une heure)', "Historique interruptions")

# =========================================================
# DESCRIPTION ET CRITICITÉ TABLE (2 placeholder rows × 4 cols)
# xx[8-11]: Row 1
# xx[12-15]: Row 2 (delete → use "-")
# =========================================================
# Row 1: Activité | Ressources | Période critique | Criticité
xml = replace_nth(xml, 8,  'Animation, suivi et pilotage du réseau commercial de la STAR (succursales et agences)', "Desc criticité - activité")
xml = replace_nth(xml, 9,  'Espace commun reporting, Office 365, PC', "Desc criticité - ressources")
xml = replace_nth(xml, 10, 'Le premier trimestre de l&#x2019;année (Décembre - Mars)', "Desc criticité - période")
xml = replace_nth(xml, 11, 'Forte', "Desc criticité - criticité")

# Row 2: clear
xml = replace_nth(xml, 12, '-', "Desc criticité row2 - activité")
xml = replace_nth(xml, 13, '-', "Desc criticité row2 - ressources")
xml = replace_nth(xml, 14, '-', "Desc criticité row2 - période")
xml = replace_nth(xml, 15, '-', "Desc criticité row2 - criticité")

# =========================================================
# EVALUATION DES IMPACTS - Tableau 6
# xx[16]: Tableau 6 caption name
# xx[17]: Process name in table header row
# xx[18-25]: 8 impact values
# =========================================================
xml = replace_nth(xml, 16, 'Animation, suivi et pilotage du réseau commercial de la STAR', "Tableau 6 caption")
xml = replace_nth(xml, 17, 'Animation, suivi et pilotage du réseau commercial de la STAR', "Tableau 6 process name")
xml = replace_nth(xml, 18, '1', "Tableau 6 - Image de marque A")
xml = replace_nth(xml, 19, '1', "Tableau 6 - Image de marque B")
xml = replace_nth(xml, 20, '1', "Tableau 6 - Désorganisation A")
xml = replace_nth(xml, 21, '2', "Tableau 6 - Désorganisation B")
xml = replace_nth(xml, 22, '1', "Tableau 6 - Juridique A")
xml = replace_nth(xml, 23, '1', "Tableau 6 - Juridique B")
xml = replace_nth(xml, 24, '4', "Tableau 6 - Financier A")
xml = replace_nth(xml, 25, '4', "Tableau 6 - Financier B")

# =========================================================
# DMIA TABLE (2 rows × 2 cols each = 4 xx's)
# xx[26]: Row 1 process name
# xx[27]: Row 1 DMIA
# xx[28]: Row 2 process name (N/A - only 1 process)
# xx[29]: Row 2 DMIA
# =========================================================
xml = replace_nth(xml, 26, 'Animation, suivi et pilotage du réseau commercial de la STAR (succursales et agences)', "DMIA row1 process")
xml = replace_nth(xml, 27, '1J', "DMIA row1 value")
xml = replace_nth(xml, 28, '-', "DMIA row2 process")
xml = replace_nth(xml, 29, '-', "DMIA row2 value")

# =========================================================
# ANALYSE DES ÉCHANGES (6 rows × 5 cols = 30 xx's)
# Columns: Groupes fonctionnels | I/E | Type d'information | T/R | Ressources SI
# =========================================================
exchanges = [
    ('Tous les départements métiers et transverses', 'I', 'Demandes de cotations, Propositions d&#x2019;offres, Réclamations réseau', 'T / R', 'Mail, Téléphone'),
    ('Agents de la STAR', 'I', 'Réclamations, Demandes de cotations', 'T / R', 'Mail, Téléphone'),
    ('Les chefs des régions', 'I', 'Statistiques, Rapports d&#x2019;activité hebdomadaire, Réclamations', 'T / R', 'Mail, Téléphone'),
    ('Les clients', 'E', 'Informations sur l&#x2019;activité pour une éventuelle offre d&#x2019;assurance', 'T / R', 'Mail, Téléphone'),
    ('Les prestataires, Hôtels, boites de communications, etc.', 'E', 'Bons de commandes, Statistiques, Devis, Validations, Contrats', 'T / R', 'Mail'),
    ('-', '-', '-', '-', '-'),
]

for row_i, (group, ie, type_info, tr, ressources) in enumerate(exchanges):
    base = 30 + row_i * 5
    xml = replace_nth(xml, base,     group,      f"Echanges row{row_i+1} - groupe")
    xml = replace_nth(xml, base + 1, ie,         f"Echanges row{row_i+1} - I/E")
    xml = replace_nth(xml, base + 2, type_info,  f"Echanges row{row_i+1} - type")
    xml = replace_nth(xml, base + 3, tr,         f"Echanges row{row_i+1} - T/R")
    xml = replace_nth(xml, base + 4, ressources, f"Echanges row{row_i+1} - ressources")

# =========================================================
# EFFECTIFS (3 xx's)
# xx[60]: Effectif count
# xx[61]: ?
# xx[62]: Télétravail count
# xx[63]: ?
# =========================================================
# From source: Effectif = 5, Télétravail +2 at H+2
# Template structure: Nominal | H+2 | H+4 | J+1 | J+2 | J+3 | J+5 | J+10 | J+15 | Commentaires
# Row "Effectif" has: 5 | (then mostly 5 at each time step)
# Based on DMIA = 1J, all effectif from J+1 onwards = 5
xml = replace_nth(xml, 60, '5', "Effectif nominal")
xml = replace_nth(xml, 61, 'Animation, suivi et pilotage du réseau commercial de la STAR', "Effectif positions name")
xml = replace_nth(xml, 62, '+2', "Télétravail count")
xml = replace_nth(xml, 63, '-', "Effectif row4")

# =========================================================
# COLLABORATEURS CLÉS - 1 placeholder row (Xx Xx Xx xx)
# Plus uppercase Xx instances
# Template has 1 row: Fonction(Xx) | Nom(Xx) | Prénom(Xx) | Suppléants(xx)
# We fill first collaborateur row, then note that only 1 row fits
# =========================================================
# Uppercase Xx replacements for Collaborateurs (Xx[0,1,2])
xml = replace_nth_Xx(xml, 0, 'Responsable de la division animation commerciale', "Collab[0] - Fonction")
xml = replace_nth_Xx(xml, 1, 'JAOUADI Naoufel (24 ans)', "Collab[0] - Nom Prénom")
xml = replace_nth_Xx(xml, 2, 'Anis BERREJEB', "Collab[0] - Ancienneté")
# xx[64]: Suppléants for first row
xml = replace_nth(xml, 64, 'Anis BERREJEB', "Collab[0] - Suppléants")

# =========================================================
# APPLICATIONS INFORMATIQUES (6 rows × 4 cols = 24 xx's)
# Columns: Application | Criticité | DMIA | PMDT | Commentaires (some fixed "-")
# xx[65-87]: 24 instances (but wait - we have Collab at 64, so 65 onwards for apps)
# From source: 8 applications, template has 6 rows
# =========================================================
# Actually verify: after xx[64] (collab suppléants), xx[65-88] should be apps
# But we have 24 xx in apps range and only xx[65]-xx[88]=24 indices

# Wait: 65 to 88 = 24 indices (65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88)
# = 24 instances. 24 / 4 = 6 rows × 4 xx each (Commentaires is fixed "-" in template)

apps = [
    ('Commun reporting', 'V', '3J', '5J', '-'),
    ('PI', 'V', '3J', '3J', 'La DMIA / PMDT dépend parfois du client'),
    ('Pro ASSUR', 'V', '3J', '5J', '-'),
    ('Application IRDS', 'PC', '10J', '15J', '-'),
    ('UNICOM', 'C', '3J', '3J', 'La DMIA / PMDT dépend parfois du client'),
    ('Refection', 'PC', '10J', '15J', '-'),
]

# Each row: Application(xx) | Criticité(xx) | DMIA(xx) | PMDT(xx) | Commentaires(xx)
# So 5 xx per row × 6 rows = 30? But we count 24...
# Let me check: maybe Commentaires is sometimes fixed "-" (not xx)
# From the text seen: "xx xx xx xx - xx xx xx xx" suggests col5 is "-" (fixed), not xx
# So 4 xx per row × 6 rows = 24. Commentaires is pre-filled as "-" in template.

for row_i, (app, crit, dmia, pmdt, comment) in enumerate(apps):
    base = 65 + row_i * 4
    xml = replace_nth(xml, base,     app,  f"App[{row_i}] - name")
    xml = replace_nth(xml, base + 1, crit, f"App[{row_i}] - criticité")
    xml = replace_nth(xml, base + 2, dmia, f"App[{row_i}] - DMIA")
    xml = replace_nth(xml, base + 3, pmdt, f"App[{row_i}] - PMDT")

# =========================================================
# ÉQUIPEMENTS (1 xx)
# xx[89]: Equipment row
# From source: PC (+2), Casques (+2), Internet/VPN
# Template has 1 placeholder row
# =========================================================
xml = replace_nth(xml, 89, 'PC, Casques, Internet / VPN', "Équipements")

# =========================================================
# DOCUMENTS ET FICHIERS (2 xx's)
# xx[90], xx[91]: Documents rows
# From source: "RAS" (rien à signaler) with no specific docs
# =========================================================
xml = replace_nth(xml, 90, 'RAS', "Documents - row1")
xml = replace_nth(xml, 91, '-', "Documents - row2")

# =========================================================
# Wait - check total: we had 91 xx's (indices 0-90)
# Let me also handle xx[88] which was "Equipements" context
# =========================================================

print(f"\nFinal xx count: {xml.count('<w:t>xx</w:t>')}")
print(f"Final Xx count: {xml.count('<w:t>Xx</w:t>')}")

# =========================================================
# UPDATE TABLE DES TABLEAUX - caption text replacements
# The Table des tableaux TOC has xx entries for Tableau 6 and 7
# These are the auto-generated TOC entries from SEQ field captions
# The actual captions in the body ARE updated (xx[16] = Tableau 6 caption)
# The TOC entries (xx[0] was already the first TOC area - but wait,
# xx[0] was MARKER_DERNIERE_MàJ and xx[1] was MARKER_REFERENCE
# Actually the Table des tableaux TOC is auto-generated from captions
# so we don't need to manually edit it - Word will regenerate it
# =========================================================

with open(OUTPUT_DIR + '\\' + DOC_XML, 'w', encoding='utf-8') as f:
    f.write(xml)
print("\nDone transform3!")
