"""
retheme_beige.py — Flip the entire dark theme to a very light beige theme.
Dark backgrounds → light beige. Light text → dark text.
"""
import re, pathlib

SRC  = pathlib.Path("static/index.html")
html = SRC.read_text(encoding="utf-8")

# ── 1. Dark background hex swaps ─────────────────────────────────────────────
html = html.replace("#080810", "#FAF8F4")   # page bg
html = html.replace("#0F0F1A", "#F0EDE6")   # surface
html = html.replace("#0f0f1a", "#F0EDE6")
html = html.replace("#13131f", "#ECEAE4")   # panel
html = html.replace("#13131F", "#ECEAE4")
html = html.replace("#0c0c1c", "#ECEAE4")   # anim-border-inner
html = html.replace("#0C0C1C", "#ECEAE4")
html = html.replace("#07070f", "#E8E5DE")   # sidebar bg
html = html.replace("#07070F", "#E8E5DE")
html = html.replace("#0a0a18", "#E8E5DE")   # mobile menu bg
html = html.replace("#0A0A18", "#E8E5DE")

# ── 2. Text colour swaps ──────────────────────────────────────────────────────
html = html.replace("#F0EEE8", "#1A1814")   # main text → dark
html = html.replace("#f0eee8", "#1A1814")

# ── 3. rgba text / glass overlay swaps ───────────────────────────────────────
# Light text rgba(240,238,232,X) → dark text rgba(26,24,20,X)
def swap_rgba_rgb(html, old_rgb, new_rgb):
    pattern = re.compile(
        r'rgba\(\s*' + re.escape(old_rgb) + r'\s*,\s*([0-9.]+)\s*\)'
    )
    return pattern.sub(lambda m: f'rgba({new_rgb},{m.group(1)})', html)

html = swap_rgba_rgb(html, "240,238,232", "26,24,20")   # light text → dark text
html = swap_rgba_rgb(html, "240, 238, 232", "26,24,20") # with spaces

# White glass overlays rgba(255,255,255,X) → dark glass rgba(26,24,20,X)
html = swap_rgba_rgb(html, "255,255,255", "26,24,20")
html = swap_rgba_rgb(html, "255, 255, 255", "26,24,20")

# Dark bg tints rgba(8,8,16,X) → light bg rgba(250,248,244,X)
html = swap_rgba_rgb(html, "8,8,16", "250,248,244")
html = swap_rgba_rgb(html, "8, 8, 16", "250,248,244")

# Mobile menu bg rgba(10,10,22,X) → rgba(245,242,236,X)
html = swap_rgba_rgb(html, "10,10,22", "245,242,236")
html = swap_rgba_rgb(html, "10, 10, 22", "245,242,236")

# ── 4. Tailwind text-white/* → text-neutral-900/* ────────────────────────────
html = re.sub(r'\btext-white/(\d+)', r'text-neutral-900/\1', html)
# Also plain text-white (without opacity)
html = re.sub(r'\btext-white\b(?!/)', r'text-neutral-900', html)

# ── 5. Tailwind config colour values ─────────────────────────────────────────
html = html.replace(
    "dark:    '#FAF8F4',",   # already replaced above — handle original dark value
    "dark:    '#FAF8F4',"
)
# Update dark in tailwind config (it may be stored differently)
html = re.sub(
    r"(dark\s*:\s*['\"])#(?:080810|FAF8F4)(['\"])",
    r"\g<1>#FAF8F4\g<2>",
    html
)
html = re.sub(
    r"(surface\s*:\s*['\"])#(?:0f0f1a|0F0F1A|F0EDE6)(['\"])",
    r"\g<1>#F0EDE6\g<2>",
    html
)
html = re.sub(
    r"(panel\s*:\s*['\"])#(?:13131f|13131F|ECEAE4)(['\"])",
    r"\g<1>#ECEAE4\g<2>",
    html
)

# ── 6. CSS variable swaps ─────────────────────────────────────────────────────
# Sidebar bg + border
html = html.replace("--sb-bg: #E8E5DE", "--sb-bg: #E8E5DE")  # already swapped
html = html.replace("--sb-border: rgba(255,255,255,0.055)", "--sb-border: rgba(26,24,20,0.1)")
html = html.replace("--sb-border: rgba(255, 255, 255, 0.055)", "--sb-border: rgba(26,24,20,0.1)")

# ── 7. Hero grid: invert from red-on-dark to dark-on-light ───────────────────
# The animated grid uses Red Poppy rgba(242,72,94,X) — reduce opacity significantly for light bg
html = re.sub(
    r'rgba\(242,72,94,([0-9.]+)\)',
    lambda m: f'rgba(242,72,94,{min(float(m.group(1))*0.35, 0.08):.2f})',
    html
)

# ── 8. Loader bg (if still explicitly dark) ───────────────────────────────────
html = html.replace("background: #080810", "background: #FAF8F4")
html = html.replace("background:#080810", "background:#FAF8F4")

# ── 9. Scrollbar thumb/track colours ─────────────────────────────────────────
# Common dark scroll styles
html = html.replace("scrollbar-color: rgba(242,72,94,0.5) #13131f",
                    "scrollbar-color: rgba(242,72,94,0.5) #ECEAE4")
html = html.replace("scrollbar-color: rgba(242,72,94,0.5) #13131F",
                    "scrollbar-color: rgba(242,72,94,0.5) #ECEAE4")

# ── 10. sb-nav-toggle hamburger bars: was rgba(240,238,232,.55) already swapped ─
# rgba was caught by step 3. No extra action needed.

# ── Write ─────────────────────────────────────────────────────────────────────
SRC.write_text(html, encoding="utf-8")
print(f"Done. File: {len(html):,} chars")

# ── Sanity checks ─────────────────────────────────────────────────────────────
checks = [
    ("#FAF8F4",      "Page bg beige hex"),
    ("#F0EDE6",      "Surface beige"),
    ("#ECEAE4",      "Panel beige"),
    ("#E8E5DE",      "Sidebar bg beige"),
    ("#1A1814",      "Dark text hex"),
    ("26,24,20",     "Dark rgba"),
    ("250,248,244",  "Light bg rgba"),
    ("text-neutral-900", "Tailwind dark text class"),
]
print()
for pattern, label in checks:
    found = pattern in html
    print(f"  {'OK' if found else 'FAIL'}: {label}")

# Verify no old dark backgrounds remain
dark_refs = (html.count("#080810") + html.count("#0f0f1a") + html.count("#0F0F1A") +
             html.count("#13131f") + html.count("#07070f") + html.count("#0c0c1c"))
print(f"\n  Old dark bg refs remaining: {dark_refs} (should be 0)")

old_white_text = html.count("rgba(255,255,255,") + html.count("rgba(255, 255, 255,")
print(f"  Old white rgba refs remaining: {old_white_text} (should be 0)")

tw_white = len(re.findall(r'\btext-white\b', html))
print(f"  text-white Tailwind classes remaining: {tw_white} (should be 0)")
