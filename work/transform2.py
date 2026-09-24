import sys, shutil, re
sys.stdout.reconfigure(encoding='utf-8')

OUTPUT_DIR = r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\work\unpacked_output'
DOC_XML = r'word\document.xml'

with open(OUTPUT_DIR + '\\' + DOC_XML, encoding='utf-8') as f:
    xml = f.read()

print(f"Loaded XML, length: {len(xml)}")
print(f"Total <w:t>xx</w:t>: {xml.count('<w:t>xx</w:t>')}")

def replace_once(xml, old, new, label):
    if old in xml:
        xml = xml.replace(old, new, 1)
        print(f"  [OK] {label}")
    else:
        print(f"  [MISS] {label}")
    return xml

# ========= 1. Cover entity - unique context with 10-space indent =========
# Pattern: the xx run in cover page has <w:highlight w:val="yellow"/> inside rPr
# with specific color/size and is right before </w:p>\n    <w:p w14:paraId="43A37C3D"
xml = replace_once(xml,
    '          <w:highlight w:val="yellow"/>\n        </w:rPr>\n        <w:t>xx</w:t>\n      </w:r>\n    </w:p>\n    <w:p w14:paraId="43A37C3D"',
    '        </w:rPr>\n        <w:t>Division animation commerciale</w:t>\n      </w:r>\n    </w:p>\n    <w:p w14:paraId="43A37C3D"',
    "Cover entity name"
)

# ========= 2. Fiche de suivi entity cell =========
# Unique context: highlight+yellow, szCs=18, followed by </tc></tr><tr rsidR="00367467"
xml = replace_once(xml,
    '                <w:highlight w:val="yellow"/>\n              </w:rPr>\n              <w:t>xx</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n      </w:tr>\n      <w:tr w:rsidR="00367467"',
    '              </w:rPr>\n              <w:t>Division animation commerciale</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n      </w:tr>\n      <w:tr w:rsidR="00367467"',
    "Fiche de suivi entity"
)

# ========= 3. Dernière mise à jour - the xx with yellow highlight after "Dernière mise à jour" =========
# Line 1437 - find the specific cell
# Its context: szCs=18 highlight=yellow, preceded by "Dernière mise à jour" label
xml = replace_once(xml,
    'Dernière mise à jour</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>\n            <w:tcBorders>\n              <w:top w:val="single" w:sz="4" w:space="0" w:color="D9D9D9"',
    'MARKER_DERNIERE_MàJ_FOUND</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>\n            <w:tcBorders>\n              <w:top w:val="single" w:sz="4" w:space="0" w:color="D9D9D9"',
    "Derniere maj marker"
)

# ========= 4. Référence du document - the xx after "Référence du document" label =========
xml = replace_once(xml,
    'Référence du document</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>\n            <w:tcBorders>\n              <w:top w:val="single" w:sz="4" w:space="0" w:color="D9D9D9"',
    'MARKER_REFERENCE_FOUND</w:t>\n            </w:r>\n          </w:p>\n        </w:tc>\n        <w:tc>\n          <w:tcPr>\n            <w:tcW w:w="7877" w:type="dxa"/>\n            <w:gridSpan w:val="2"/>\n            <w:tcBorders>\n              <w:top w:val="single" w:sz="4" w:space="0" w:color="D9D9D9"',
    "Reference doc marker"
)

print(f"After cover/fiche replacements, remaining xx: {xml.count('<w:t>xx</w:t>')}")

# Now let's find all remaining xx positions and their context
positions = []
idx = 0
while True:
    pos = xml.find('<w:t>xx</w:t>', idx)
    if pos == -1:
        break
    positions.append(pos)
    idx = pos + 1

print(f"\nRemaining xx instances: {len(positions)}")
for i, pos in enumerate(positions[:20]):
    # Get 300 chars before for context
    before = xml[max(0,pos-400):pos]
    # Find last meaningful text label
    labels = re.findall(r'<w:t[^>]*>([^<]{5,})</w:t>', before)
    print(f"  xx[{i:2d}] @ char {pos}, nearby labels: {labels[-2:] if len(labels)>=2 else labels}")

with open(OUTPUT_DIR + '\\' + DOC_XML, 'w', encoding='utf-8') as f:
    f.write(xml)
print("\nDone phase 2 - analysis complete!")
