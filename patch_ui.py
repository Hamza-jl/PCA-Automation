"""
patch_ui.py
───────────
1. Replace sidebar "D" logo-mark with the real Devoteam logo image
2. Darken invisible secondary sidebar text (opacity 0.15-0.28 → 0.48-0.62)
3. Montserrat everywhere — replace Cormorant Garamond references
"""
import pathlib, re

SRC = pathlib.Path("static/index.html")
html = SRC.read_text(encoding="utf-8")

# ─── 1. Logo image ────────────────────────────────────────────────────────────
# Replace the <div class="sb-logo-mark">D</div> with an <img>
html = html.replace(
    '<div class="sb-logo-mark">D</div>',
    '<img class="sb-logo-img" src="devoteam_logo.png" alt="Devoteam" />',
    1
)

# Add CSS for the logo image (right after .sb-logo-mark rule)
OLD_LOGO_MARK_CSS = """.sb-logo-mark {
      width: 30px; height: 30px;
      border-radius: 7px;
      background: var(--poppy);
      display: flex; align-items: center; justify-content: center;
      color: #fff;
      font-size: 0.85rem;
      font-weight: 800;
      font-family: 'Montserrat', sans-serif;
      flex-shrink: 0;
    }"""

NEW_LOGO_MARK_CSS = """.sb-logo-mark {
      width: 30px; height: 30px;
      border-radius: 7px;
      background: var(--poppy);
      display: flex; align-items: center; justify-content: center;
      color: #fff;
      font-size: 0.85rem;
      font-weight: 800;
      font-family: 'Montserrat', sans-serif;
      flex-shrink: 0;
    }
    .sb-logo-img {
      width: 34px; height: 34px;
      object-fit: contain;
      flex-shrink: 0;
      border-radius: 0;
      display: block;
    }"""

html = html.replace(OLD_LOGO_MARK_CSS, NEW_LOGO_MARK_CSS, 1)

# ─── 2. Darken invisible secondary sidebar text ───────────────────────────────
# Each rule: old opacity → new opacity
opacity_fixes = [
    # sb-context  (logo subtitle "Résilience 360°")
    ("color: rgba(26,24,20,0.28);\n      font-family: 'Montserrat', sans-serif; white-space: nowrap;",
     "color: rgba(26,24,20,0.58);\n      font-family: 'Montserrat', sans-serif; white-space: nowrap;"),
    # sb-track-label  ("PROGRESSION PCA")
    ("color: rgba(26,24,20,0.2); font-family: 'Montserrat', sans-serif;",
     "color: rgba(26,24,20,0.50); font-family: 'Montserrat', sans-serif;"),
    # sb-nav-label  ("PHASES DU PROJET")
    ("color: rgba(26,24,20,0.18); font-family: 'Montserrat', sans-serif;",
     "color: rgba(26,24,20,0.48); font-family: 'Montserrat', sans-serif;"),
    # sb-phase-desc  (phase sub-descriptions)
    ("color: rgba(26,24,20,0.25);\n      margin-top: 0.22rem;",
     "color: rgba(26,24,20,0.54);\n      margin-top: 0.22rem;"),
    # sb-status-text  (footer status)
    ("color: rgba(26,24,20,0.28);\n      font-family: 'Montserrat', sans-serif;\n    }\n    .sb-version",
     "color: rgba(26,24,20,0.58);\n      font-family: 'Montserrat', sans-serif;\n    }\n    .sb-version"),
    # sb-version  (bottom version string)
    ("font-size: 0.52rem; color: rgba(26,24,20,0.15);",
     "font-size: 0.52rem; color: rgba(26,24,20,0.42);"),
]

for old, new in opacity_fixes:
    if old in html:
        html = html.replace(old, new, 1)
    else:
        print(f"  WARN: could not find: {repr(old[:60])}")

# ─── 3. Montserrat everywhere — replace Cormorant Garamond ───────────────────
# Update Google Fonts import — remove Cormorant, keep Montserrat with wider weight range
html = html.replace(
    "family=Cormorant+Garamond:ital,wght@0,300;0,400;0,600;0,700;1,300;1,400;1,600&family=Montserrat:wght@200;300;400;500;600;700",
    "family=Montserrat:ital,wght@0,200;0,300;0,400;0,500;0,600;0,700;0,800;1,300;1,400",
    1
)

# Replace all Cormorant Garamond font-family declarations
html = re.sub(
    r"font-family:\s*'Cormorant Garamond',\s*serif",
    "font-family: 'Montserrat', sans-serif",
    html
)
# Also catch any shorthand font properties that reference Cormorant
html = html.replace("'Cormorant Garamond'", "'Montserrat'")

# Ensure body uses Montserrat (in case it's overridden)
html = re.sub(
    r"(body\s*\{[^}]*?)font-family:\s*[^;]+;",
    r"\1font-family: 'Montserrat', sans-serif;",
    html, count=1
)

# Hero/loader large italic text: Cormorant was often used for decorative italic
# Make sure the font-style:italic hero text looks good with Montserrat weights
html = re.sub(
    r"(font-style:\s*italic[^}]*?font-weight:\s*)300",
    r"\g<1>300",
    html
)

# ─── Write ────────────────────────────────────────────────────────────────────
SRC.write_text(html, encoding="utf-8")
print(f"Done — {len(html):,} chars written to {SRC}")

# Sanity
checks = [
    ("sb-logo-img",                  "Logo img class"),
    ("devoteam_logo.png",            "Logo src path"),
    ("rgba(26,24,20,0.58)",          "Darkened sb-context"),
    ("rgba(26,24,20,0.50)",          "Darkened sb-track-label"),
    ("rgba(26,24,20,0.48)",          "Darkened sb-nav-label"),
    ("rgba(26,24,20,0.54)",          "Darkened sb-phase-desc"),
    ("rgba(26,24,20,0.42)",          "Darkened sb-version"),
    ("Cormorant+Garamond",           "Cormorant in GFonts (should be 0)"),
    ("'Cormorant Garamond'",         "Cormorant in CSS (should be 0)"),
]
print()
for pat, label in checks:
    found = pat in html
    if "should be 0" in label:
        status = "OK" if not found else "FAIL"
    else:
        status = "OK" if found else "FAIL"
    print(f"  {status}: {label}")
