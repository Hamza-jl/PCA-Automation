"""
retheme.py — Swap all gold/amber accent colours to Devoteam Red Poppy
and add collapsible sidebar + navbar toggle button.
"""
import re, pathlib

SRC  = pathlib.Path("static/index.html")
html = SRC.read_text(encoding="utf-8")

# ── 1. Hex colour swaps ────────────────────────────────────────────────────────
# Primary gold → Red Poppy
html = html.replace("#C5A059", "#F2485E")
# Darker gold → darker red
html = html.replace("#a8863e", "#d63448")
# Lighter gold → lighter red
html = html.replace("#e8c87a", "#f57c83")
# Gold in gradient stops (just in case any remain)
html = html.replace("#C5a059", "#F2485E")

# ── 2. rgba triplet swap ──────────────────────────────────────────────────────
html = html.replace("197,160,89", "242,72,94")

# ── 3. Tailwind config colours ────────────────────────────────────────────────
html = html.replace(
    "gold:    '#C5A059',\n            'gold-d':'#a8863e',",
    "gold:    '#F2485E',\n            'gold-d':'#d63448',"
)
# Also fix any lingering tailwind hex in config
html = html.replace("'#C5A059'", "'#F2485E'")
html = html.replace("'#a8863e'", "'#d63448'")

# ── 4. Loader text colour (inline style on #loader-text) ─────────────────────
# Already done via #C5A059 swap above.

# ── 5. Hero gradient swap ─────────────────────────────────────────────────────
# "linear-gradient(135deg, #C5A059 0%, #e8c87a 45%, #C5A059 70%, #a8863e 100%)"
# Already covered by individual hex swaps above.

# ── 6. Add sidebar desktop-collapsible CSS ────────────────────────────────────
SIDEBAR_TOGGLE_CSS = """
    /* ── Sidebar desktop toggle ─────────────────────────────────── */
    /* Default (sidebar visible on desktop) */
    @media (min-width: 769px) {
      body:not(.sb-hidden) #sidebar {
        transform: translateX(0);
      }
      body.sb-hidden #sidebar {
        transform: translateX(-100%);
      }
      body.sb-hidden {
        padding-left: 0 !important;
      }
      body.sb-hidden #navbar {
        left: 0 !important;
      }
      /* sidebar transition on desktop too */
      #sidebar {
        transition: transform 0.38s cubic-bezier(0.4,0,0.2,1) !important;
      }
    }

    /* ── Navbar sidebar-toggle button ───────────────────────────── */
    #sb-nav-toggle {
      display: flex; flex-direction: column; gap: 5px;
      cursor: pointer; background: none; border: none;
      padding: .35rem .4rem; margin-right: .5rem;
      border-radius: .4rem;
      transition: background .2s;
    }
    #sb-nav-toggle:hover { background: rgba(255,255,255,.06); }
    #sb-nav-toggle span {
      display: block; width: 20px; height: 1.5px;
      background: rgba(240,238,232,.55);
      border-radius: 1px;
      transition: all .3s;
    }
    /* Animate to X when open on mobile */
    body.sb-open #sb-nav-toggle span:nth-child(1) { transform: translateY(6.5px) rotate(45deg); }
    body.sb-open #sb-nav-toggle span:nth-child(2) { opacity: 0; }
    body.sb-open #sb-nav-toggle span:nth-child(3) { transform: translateY(-6.5px) rotate(-45deg); }

    /* Hide mobile FAB on desktop */
    @media (min-width: 769px) { #sb-fab { display: none !important; } }
"""

# Inject before the closing </style>
html = html.replace("    @property --ba {", SIDEBAR_TOGGLE_CSS + "\n    @property --ba {", 1)

# ── 7. Add toggle button to navbar ───────────────────────────────────────────
OLD_NAVBAR_LOGO = '<div class="nav-logo">BIA <span>Automatique</span></div>'
NEW_NAVBAR_LOGO = (
    '<button id="sb-nav-toggle" onclick="toggleSidebar()" aria-label="Menu">'
    '<span></span><span></span><span></span>'
    '</button>'
    '<div class="nav-logo">BIA <span>Automatique</span></div>'
)
html = html.replace(OLD_NAVBAR_LOGO, NEW_NAVBAR_LOGO, 1)

# ── 8. Add toggleSidebar JS (desktop + mobile unified toggle) ─────────────────
OLD_SB_JS = "window.sbOpen = function () {"
NEW_SB_JS = """window.toggleSidebar = function () {
    if (isMobile()) {
      // mobile: use overlay behaviour
      if (sidebar.classList.contains('sb-open')) { sbClose(); }
      else { sbOpen(); }
    } else {
      // desktop: hide/show sidebar, shift content
      const hidden = document.body.classList.toggle('sb-hidden');
      localStorage.setItem('sbHidden', hidden ? '1' : '0');
    }
  };

  // Restore desktop state from localStorage
  if (!isMobile() && localStorage.getItem('sbHidden') === '1') {
    document.body.classList.add('sb-hidden');
  }

  window.sbOpen = function () {"""

html = html.replace(OLD_SB_JS, NEW_SB_JS, 1)

# ── 9. Patch sbOpen/sbClose to also manage body.sb-open class (for hamburger X) ─
html = html.replace(
    "sidebar.classList.add('sb-open');\n    overlay.classList.add('sb-active');\n    document.body.style.overflow = 'hidden';",
    "sidebar.classList.add('sb-open');\n    overlay.classList.add('sb-active');\n    document.body.classList.add('sb-open');\n    document.body.style.overflow = 'hidden';"
)
html = html.replace(
    "sidebar.classList.remove('sb-open');\n    overlay.classList.remove('sb-active');\n    document.body.style.overflow = '';",
    "sidebar.classList.remove('sb-open');\n    overlay.classList.remove('sb-active');\n    document.body.classList.remove('sb-open');\n    document.body.style.overflow = '';"
)

# ── Write ─────────────────────────────────────────────────────────────────────
SRC.write_text(html, encoding="utf-8")
print(f"Done. File: {len(html):,} chars")

# Quick sanity
checks = [
    ("#F2485E",          "Red Poppy hex"),
    ("d63448",           "Dark red"),
    ("242,72,94",        "Red rgba triplet"),
    ("sb-hidden",        "sb-hidden class"),
    ("sb-nav-toggle",    "Navbar toggle btn"),
    ("toggleSidebar",    "Toggle function"),
    ("sb-open",          "Mobile sb-open class"),
]
import re
for pattern, label in checks:
    found = pattern in html
    print(f"  {'OK' if found else 'FAIL'}: {label}")

# Verify no old gold remains (outside sidebar CSS which intentionally uses var(--poppy))
old_gold = html.count("#C5A059") + html.count("197,160,89") + html.count("#a8863e")
print(f"\n  Old gold refs remaining: {old_gold} (should be 0)")
