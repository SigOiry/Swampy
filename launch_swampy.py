"""
Swampy entry point.

Run this from the repo root:
    python launch_swampy.py [options]

It checks for updates first, then delegates to app/launch_swampy.py.
"""

import os
import runpy
import sys

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_APP_DIR = os.path.join(_REPO_ROOT, "app")

# Make repo root importable (for updater) and app/ importable (for all
# the app modules: gui_swampy, sambuca, output_calculation, …)
for _p in (_REPO_ROOT, _APP_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Always run from the repo root so that relative paths inside the app
# (Data/, Output/, …) resolve correctly.
os.chdir(_REPO_ROOT)

# ------------------------------------------------------------------
# Update check — runs before any app code is loaded
# ------------------------------------------------------------------
import updater

if not updater.check_and_prompt():
    # Updater relaunched the process after applying the update.
    sys.exit(0)

# ------------------------------------------------------------------
# Launch the app
# ------------------------------------------------------------------
runpy.run_path(
    os.path.join(_APP_DIR, "launch_swampy.py"),
    run_name="__main__",
)
