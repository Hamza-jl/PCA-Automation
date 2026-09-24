import sys, shutil, re
sys.stdout.reconfigure(encoding='utf-8')

TEMPLATE_DIR = r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\work\unpacked_template'
OUTPUT_DIR   = r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\work\unpacked_output'
DOC_XML      = r'word\document.xml'

# Copy template to output
shutil.copytree(TEMPLATE_DIR, OUTPUT_DIR, dirs_exist_ok=True)
print("Copied template to output directory")

with open(OUTPUT_DIR + '\\' + DOC_XML, encoding='utf-8') as f:
    xml = f.read()

print(f"Loaded XML, length: {len(xml)}")

def replace_once(xml, old, new, label):
    if old in xml:
        xml = xml.replace(old, new, 1)
        print(f"  [OK] {label}")
    else:
        print(f"  [MISS] {label}")
    return xml

# ========= 1. COVER PAGE - entity name =========
xml = replace_once(xml,
    '<w:highlight w:val="yellow"/>\n              </w:rPr>\n            </w:pPr>\n            <w:r>\n              <w:rPr>\n                <w:rFonts w:ascii="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat" w:cs="Montserrat"/>\n                <w:b/>\n                <w:bCs/>\n                <w:color w:val="808080"/>\n                <w:sz w:val="24"/>\n                <w:szCs w:val="24"/>\n                <w:highlight w:val="yellow"/>\n              </w:rPr>\n              <w:t>xx</w:t>',
    '<w:highlight w:val="yellow"/>\n              </w:rPr>\n            </w:pPr>\n            <w:r>\n              <w:rPr>\n                <w:rFonts w:ascii="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat" w:cs="Montserrat"/>\n                <w:b/>\n                <w:bCs/>\n                <w:color w:val="808080"/>\n                <w:sz w:val="24"/>\n                <w:szCs w:val="24"/>\n              </w:rPr>\n              <w:t>Division animation commerciale</w:t>',
    "Cover entity name"
)

# ========= 2. FICHE DE SUIVI - entity cell =========
# The entity cell in fiche de suivi has "xx" with yellow highlight after "Entité" header
xml = replace_once(xml,
    '<w:highlight w:val="yellow"/>\n              </w:rPr>\n            </w:pPr>\n            <w:r>\n              <w:rPr>\n                <w:rFonts w:ascii="Montserrat" w:hAnsi="Montserrat"/>\n                <w:szCs w:val="18"/>\n                <w:highlight w:val="yellow"/>\n              </w:rPr>\n              <w:t>xx</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n      </w:tr>\n      <w:tr w:rsidR="00367467"',
    '<w:highlight w:val="yellow"/>\n              </w:rPr>\n            </w:pPr>\n            <w:r>\n              <w:rPr>\n                <w:rFonts w:ascii="Montserrat" w:hAnsi="Montserrat"/>\n                <w:szCs w:val="18"/>\n              </w:rPr>\n              <w:t>Division animation commerciale</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n      </w:tr>\n      <w:tr w:rsidR="00367467"',
    "Fiche de suivi entity"
)

# ========= 3. ATTENDEES =========
xml = replace_once(xml, '<w:t>Mr Lazher HEDFI</w:t>', '<w:t>Naoufel Jawadi</w:t>', "Attendee 1 name")
xml = replace_once(xml, '<w:t>Risk manager</w:t>', '<w:t xml:space="preserve"></w:t>', "Attendee 1 function")
xml = replace_once(xml, '<w:t>Mr Achraf HAMDANI</w:t>', '<w:t>Anis BERREJEB</w:t>', "Attendee 2 name")
xml = replace_once(xml, '<w:t xml:space="preserve">Senior Risk manager </w:t>', '<w:t xml:space="preserve"></w:t>', "Attendee 2 function")
xml = replace_once(xml, '<w:t>Mme Asma BOUZAIENE</w:t>', '<w:t>Asma BOUZAIEN</w:t>', "Attendee 3 name")
xml = replace_once(xml, '<w:t>Senior Risk manager</w:t>', '<w:t xml:space="preserve"></w:t>', "Attendee 3 function (first occurrence)")
xml = replace_once(xml, '<w:t>Mr Houcem LIMAM</w:t>', '<w:t>Aymen Salhi</w:t>', "Attendee 4 name")
xml = replace_once(xml, '<w:t>Senior Risk manager</w:t>', '<w:t xml:space="preserve"></w:t>', "Attendee 4 function")
xml = replace_once(xml, '<w:t>Mme Fatèn TAGHOUTI</w:t>', '<w:t>Houcem LIMAM</w:t>', "Attendee 5 name")
xml = replace_once(xml, '<w:t>Directrice du département Risk &amp; Cyber Advisory - Devoteam</w:t>', '<w:t xml:space="preserve"></w:t>', "Attendee 5 function")
xml = replace_once(xml, '<w:t>Mme Rania CHERIF</w:t>', '<w:t>Rania CHERIF</w:t>', "Attendee 6 name")

# ========= 4. DERNIÈRE MISE À JOUR =========
# Line 1437 - first "xx" after version info
# Find the "Dernière mise à jour" row and replace its xx
xml = replace_once(xml,
    'Dernière mise à jour</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>',
    'Dernière mise à jour</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>',
    "Check derniere mise a jour"
)

# ========= 5. RÉFÉRENCE DU DOCUMENT =========
xml = replace_once(xml,
    'Référence du document</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>\n            <w:tcBorders>\n              <w:top w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n              <w:left w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n              <w:bottom w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n              <w:right w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n            </w:tcBorders>\n            <w:shd w:val="clear" w:color="auto" w:fill="F2F2F2" w:themeFill="background1" w:themeFillShade="F2"/>\n            <w:vAlign w:val="center"/>\n          </w:tcPr>\n          <w:p',
    'Référence du document</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>\n            <w:tcBorders>\n              <w:top w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n              <w:left w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n              <w:bottom w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n              <w:right w:val="single" w:sz="4" w:space="0" w:color="D9D9D9" w:themeColor="background1" w:themeShade="D9"/>\n            </w:tcBorders>\n            <w:shd w:val="clear" w:color="auto" w:fill="F2F2F2" w:themeFill="background1" w:themeFillShade="F2"/>\n            <w:vAlign w:val="center"/>\n          </w:tcPr>\n          <w:p',
    "Reference doc check"
)

# Simple replacement of the 2nd "xx" (reference doc) - we know it's after "Dernière mise à jour" xx
# After first xx (Derniere mise a jour), second xx is Référence
count_xx = xml.count('<w:t>xx</w:t>')
print(f"Remaining <w:t>xx</w:t> count: {count_xx}")

# Let's do numbered replacements for remaining xx placeholders
# Find all remaining xx and print context
positions = []
idx = 0
while True:
    pos = xml.find('<w:t>xx</w:t>', idx)
    if pos == -1:
        break
    positions.append(pos)
    idx = pos + 1

print(f"Found {len(positions)} remaining <w:t>xx</w:t> instances")
for i, pos in enumerate(positions[:5]):
    snippet = xml[max(0,pos-200):pos+50]
    # extract last text before the xx
    texts = re.findall(r'<w:t[^>]*>([^<]+)</w:t>', snippet)
    print(f"  xx[{i}] context texts: {texts[-3:] if len(texts)>=3 else texts}")

with open(OUTPUT_DIR + '\\' + DOC_XML, 'w', encoding='utf-8') as f:
    f.write(xml)
print("Done phase 1!")
