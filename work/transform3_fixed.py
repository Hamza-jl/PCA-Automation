"""
Complete replacement script - always replaces index 0 (next remaining xx) in sequence.
This is correct because each replacement removes one xx, so index 0 is always the next one.
"""
import sys, re
sys.stdout.reconfigure(encoding='utf-8')

OUTPUT_DIR = r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\work\unpacked_output'
DOC_XML = r'word\document.xml'

with open(OUTPUT_DIR + '\\' + DOC_XML, encoding='utf-8') as f:
    xml = f.read()

print(f"Starting: {xml.count('<w:t>xx</w:t>')} xx, {xml.count('<w:t>Xx</w:t>')} Xx")

def next_xx(xml, value, label):
    """Replace the first (next) <w:t>xx</w:t> in file."""
    target = '<w:t>xx</w:t>'
    pos = xml.find(target)
    if pos == -1:
        print(f"  [MISS] {label}")
        return xml
    if value == '':
        repl = '<w:t></w:t>'
    else:
        repl = f'<w:t>{value}</w:t>'
    xml = xml[:pos] + repl + xml[pos + len(target):]
    print(f"  [OK] {label} => {value[:60]}")
    return xml

def next_Xx(xml, value, label):
    """Replace the first <w:t>Xx</w:t> in file."""
    target = '<w:t>Xx</w:t>'
    pos = xml.find(target)
    if pos == -1:
        print(f"  [MISS Xx] {label}")
        return xml
    repl = f'<w:t>{value}</w:t>'
    xml = xml[:pos] + repl + xml[pos + len(target):]
    print(f"  [OK Xx] {label} => {value[:60]}")
    return xml

# =============================================
# FIRST: Fix the organisation sentence prefix
# The template says "L'entité se compose d'un (01) manager et de trois (xx) collaborateurs"
# We replace the whole sentence across three runs with source content
# =============================================
old_org = "L&#x2019;entité se compose d&#x2019;un (01) manager et de trois ("
new_org = "L&#x2019;entité se compose de 5 personnes divisées comme suit : Un responsable de la division, un département pilotage et suivi (1 sous-directeur et 3 collaborateurs) et les Régions commerciales (6 chefs de régions). ("
if old_org in xml:
    xml = xml.replace(old_org, new_org, 1)
    print("  [OK] Organisation sentence prefix fixed")
# Also fix the closing part
xml = xml.replace(") collaborateurs", ")", 1)
print("  [OK] Organisation sentence suffix fixed")

# =============================================
# ORDER OF xx's IN THE FILE (91 total after transforms 1 & 2):
# 0: Fiche de suivi - Dernière mise à jour value
# 1: Fiche de suivi - Référence du document value
# 2: Fiche d'identité - Nom du responsable
# 3: Fiche d'identité - Organisation count (now in "Total = (" context)
# 4: Fiche d'identité - Présentation générale
# 5: Fiche d'identité - Contraintes
# 6: Fiche d'identité - Périodes critiques
# 7: Fiche d'identité - Historique
# 8-11: Description criticité row1 (4 cols)
# 12-15: Description criticité row2 (4 cols, fill with "-")
# 16: Evaluation - Tableau 6 caption name
# 17: Evaluation - Tableau 6 table process name
# 18-25: Evaluation - Tableau 6 impact values (8 values)
# 26-29: DMIA rows (2 rows × 2 cols)
# 30-59: Analyse des échanges (6 rows × 5 cols)
# 60-63: Effectifs (4 values)
# 64: Collaborateurs Xx[2] = suppléants (lowercase xx)
# 65-88: Applications (6 rows × 4 cols = 24)
# 89: Equipements
# 90-91: Documents (but 90 is last at index 90)
# =============================================

print("\n--- FICHE DE SUIVI ---")
xml = next_xx(xml, '', "Dernière mise à jour value (blank)")
xml = next_xx(xml, 'STAR - MCO - BIA Division animation commerciale - V2.0', "Référence du document")

print("\n--- FICHE D'IDENTITÉ ---")
xml = next_xx(xml, 'Naoufel JAOUADI', "Nom du responsable")
xml = next_xx(xml, '5', "Organisation count")
xml = next_xx(xml, 'Animation, suivi et pilotage du réseau commercial de la STAR (succursales et agences)', "Présentation générale")
xml = next_xx(xml, 'Retard des demandes de cotations ; Chaque mois : envoi les tableaux de bords de l&#x2019;activité des points de vente', "Contraintes")
xml = next_xx(xml, 'Période estivale ; Le premier trimestre de l&#x2019;année', "Périodes critiques")
xml = next_xx(xml, 'Arrêt du système (pendant une heure)', "Historique interruptions")

print("\n--- DESCRIPTION ET CRITICITÉ ---")
xml = next_xx(xml, 'Animation, suivi et pilotage du réseau commercial de la STAR (succursales et agences)', "Activité")
xml = next_xx(xml, 'Espace commun reporting, Office 365, PC', "Ressources")
xml = next_xx(xml, 'Le premier trimestre de l&#x2019;année (Décembre - Mars)', "Période critique")
xml = next_xx(xml, 'Forte', "Criticité")
# Row 2 (empty row in template, no matching process)
xml = next_xx(xml, '-', "Row2 - Activité")
xml = next_xx(xml, '-', "Row2 - Ressources")
xml = next_xx(xml, '-', "Row2 - Période")
xml = next_xx(xml, '-', "Row2 - Criticité")

print("\n--- ÉVALUATION DES IMPACTS - Tableau 6 ---")
xml = next_xx(xml, 'Animation, suivi et pilotage du réseau commercial de la STAR', "Tableau 6 caption name")
xml = next_xx(xml, 'Animation, suivi et pilotage du réseau commercial de la STAR', "Tableau 6 process header")
xml = next_xx(xml, '1', "Image de marque A")
xml = next_xx(xml, '1', "Image de marque B")
xml = next_xx(xml, '1', "Désorganisation A")
xml = next_xx(xml, '2', "Désorganisation B")
xml = next_xx(xml, '1', "Juridique A")
xml = next_xx(xml, '1', "Juridique B")
xml = next_xx(xml, '4', "Financier A")
xml = next_xx(xml, '4', "Financier B")

print("\n--- DMIA ---")
xml = next_xx(xml, 'Animation, suivi et pilotage du réseau commercial de la STAR (succursales et agences)', "DMIA row1 - processus")
xml = next_xx(xml, '1J', "DMIA row1 - DMIA value")
xml = next_xx(xml, '-', "DMIA row2 - processus")
xml = next_xx(xml, '-', "DMIA row2 - value")

print("\n--- ANALYSE DES ÉCHANGES (6 rows × 5 cols) ---")
exchanges = [
    ('Tous les départements métiers et transverses', 'I', 'Demandes de cotations, Propositions d&#x2019;offres, Réclamations réseau', 'T / R', 'Mail, Téléphone'),
    ('Agents de la STAR', 'I', 'Réclamations, Demandes de cotations', 'T / R', 'Mail, Téléphone'),
    ('Les chefs des régions', 'I', 'Statistiques, Rapports d&#x2019;activité hebdomadaire, Réclamations', 'T / R', 'Mail, Téléphone'),
    ('Les clients', 'E', 'Informations sur l&#x2019;activité pour une éventuelle offre d&#x2019;assurance', 'T / R', 'Mail, Téléphone'),
    ('Les prestataires, Hôtels, boites de communications, etc.', 'E', 'Bons de commandes, Statistiques, Devis, Validations, Contrats', 'T / R', 'Mail'),
    ('-', '-', '-', '-', '-'),
]
for r, (g, ie, t, tr, rs) in enumerate(exchanges):
    xml = next_xx(xml, g,  f"Échange row{r+1} - groupe")
    xml = next_xx(xml, ie, f"Échange row{r+1} - I/E")
    xml = next_xx(xml, t,  f"Échange row{r+1} - type")
    xml = next_xx(xml, tr, f"Échange row{r+1} - T/R")
    xml = next_xx(xml, rs, f"Échange row{r+1} - ressources")

print("\n--- EFFECTIFS ---")
# Template has some xx's in effectif table
# From source: Effectif = 5, Process name row, Télétravail = +2
# Depending on template structure, we may have 4 xx's here
xml = next_xx(xml, '5', "Effectif nominal")
xml = next_xx(xml, 'Animation, suivi et pilotage du réseau commercial de la STAR', "Effectif - processus")
xml = next_xx(xml, '+2', "Télétravail")
xml = next_xx(xml, '-', "Effectif row 4")

print("\n--- COLLABORATEURS CLÉS ---")
# Template has: Xx(Fonction), Xx(Nom+Prénom), Xx(Ancienneté?), xx(Suppléants)
# But we only have 1 xx (lowercase) - the others are Xx (uppercase)
xml = next_Xx(xml, 'Responsable de la division animation commerciale', "Collab - Fonction")
xml = next_Xx(xml, 'JAOUADI Naoufel', "Collab - Nom Prénom")
xml = next_Xx(xml, '24 ans', "Collab - Ancienneté")
xml = next_xx(xml, 'Anis BERREJEB', "Collab - Suppléants")

print("\n--- APPLICATIONS INFORMATIQUES (6 rows × 4 cols) ---")
apps = [
    ('Commun reporting', 'V', '3J', '5J'),
    ('PI', 'V', '3J', '3J'),
    ('Pro ASSUR', 'V', '3J', '5J'),
    ('Application IRDS', 'PC', '10J', '15J'),
    ('UNICOM', 'C', '3J', '3J'),
    ('Refection', 'PC', '10J', '15J'),
]
for r, (app, crit, dmia, pmdt) in enumerate(apps):
    xml = next_xx(xml, app,  f"App row{r+1} - application")
    xml = next_xx(xml, crit, f"App row{r+1} - criticité")
    xml = next_xx(xml, dmia, f"App row{r+1} - DMIA")
    xml = next_xx(xml, pmdt, f"App row{r+1} - PMDT")

print("\n--- ÉQUIPEMENTS ---")
xml = next_xx(xml, 'PC, Casques, Internet / VPN', "Équipements")

print("\n--- DOCUMENTS ET FICHIERS ---")
xml = next_xx(xml, 'RAS', "Documents row1")
xml = next_xx(xml, '-', "Documents row2")

print(f"\nFinal: {xml.count('<w:t>xx</w:t>')} xx remaining, {xml.count('<w:t>Xx</w:t>')} Xx remaining")

with open(OUTPUT_DIR + '\\' + DOC_XML, 'w', encoding='utf-8') as f:
    f.write(xml)
print("Done! Saved.")
