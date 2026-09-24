import sys, re
sys.stdout.reconfigure(encoding='utf-8')

OUTPUT_DIR = r'C:\Users\jelassi.hamza\Desktop\Devoteam\BIA_Implementation\work\unpacked_output'
DOC_XML = r'word\document.xml'

with open(OUTPUT_DIR + '\\' + DOC_XML, encoding='utf-8') as f:
    xml = f.read()

positions = []
idx = 0
while True:
    pos = xml.find('<w:t>xx</w:t>', idx)
    if pos == -1:
        break
    positions.append(pos)
    idx = pos + 1

print(f"Total xx: {len(positions)}")
for i, pos in enumerate(positions):
    # Get 500 chars before for context
    before = xml[max(0,pos-600):pos]
    # Find all text content in the last 600 chars
    texts = re.sub(r'<[^>]+>', ' ', before)
    texts = re.sub(r'\s+', ' ', texts).strip()
    # Get last 150 chars of text
    print(f"\nxx[{i:2d}] @ char {pos}")
    print(f"  Context: {texts[-200:]}")
