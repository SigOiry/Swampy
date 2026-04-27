"""
Swampy auto-updater.

Checks GitHub Releases for a newer version and, if the user agrees,
pulls the update via git and refreshes the conda environment before
relaunching the app.

Usage (called from launch_swampy.py):
    import updater
    if not updater.check_and_prompt():
        sys.exit(0)   # updater relaunched the process — nothing left to do
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import tkinter as tk
import webbrowser
from tkinter import ttk
from urllib import request as _urllib_request
from urllib.error import URLError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GITHUB_REPO = "SigOiry/Swampy"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
CONDA_ENV_NAME = "SwampySim"
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_VERSION_FILE = os.path.join(_REPO_ROOT, "version.txt")
_ENV_YML = os.path.join(_REPO_ROOT, "environment.yml")

# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def _read_local_version():
    try:
        with open(_VERSION_FILE) as f:
            return f.read().strip().lstrip("v")
    except OSError:
        return "0.0.0"


def _version_tuple(v):
    """'1.2.3' → (1, 2, 3) for numeric comparison."""
    try:
        return tuple(int(x) for x in v.strip().lstrip("v").split("."))
    except Exception:
        return (0,)


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------

def _fetch_latest_release():
    """
    Returns (tag_name, changelog, release_url) on success, or None on any
    network / parse error. tag_name is exactly as GitHub returns it, e.g.
    'v1.2.0'.  Timeout is 3 seconds so a missing connection doesn't delay
    startup noticeably.
    """
    try:
        req = _urllib_request.Request(
            GITHUB_API_URL,
            headers={"User-Agent": "Swampy-updater/1.0"},
        )
        with _urllib_request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
        tag = data.get("tag_name", "")
        body = data.get("body") or "No changelog provided."
        url = data.get("html_url", f"https://github.com/{GITHUB_REPO}/releases")
        return tag, body, url
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Conda executable discovery
# ---------------------------------------------------------------------------

def _find_conda():
    """
    Return a path to a usable conda (or conda.bat) executable, or None.
    Checks activated conda variables, shell PATH, the running Python location,
    and well-known Windows / Unix install locations.
    """
    seen = set()
    candidates = []

    def _add(path):
        if not path:
            return
        path = os.path.abspath(os.path.expanduser(os.path.expandvars(path)))
        key = os.path.normcase(path)
        if key not in seen:
            seen.add(key)
            candidates.append(path)

    def _add_root(root):
        if not root:
            return
        root = os.path.abspath(os.path.expanduser(os.path.expandvars(root)))
        for rel in (
            ("condabin", "conda.bat"),
            ("condabin", "conda"),
            ("Library", "bin", "conda.bat"),
            ("Library", "bin", "conda.exe"),
            ("Scripts", "conda.exe"),
            ("Scripts", "conda.bat"),
            ("bin", "conda"),
        ):
            _add(os.path.join(root, *rel))

    def _add_prefix_and_base(prefix):
        if not prefix:
            return
        prefix = os.path.abspath(os.path.expanduser(os.path.expandvars(prefix)))
        _add_root(prefix)
        parent = os.path.dirname(prefix)
        if os.path.basename(parent).lower() == "envs":
            _add_root(os.path.dirname(parent))

    for var in ("CONDA_EXE", "_CONDA_EXE", "CONDA_BAT"):
        _add(os.environ.get(var))

    for name in ("conda", "conda.bat", "conda.exe"):
        _add(shutil.which(name))

    for var in (
        "CONDA_ROOT",
        "MAMBA_ROOT_PREFIX",
        "CONDA_PREFIX",
        "CONDA_PREFIX_1",
        "CONDA_PREFIX_2",
        "CONDA_PREFIX_3",
    ):
        _add_prefix_and_base(os.environ.get(var))

    if sys.executable:
        _add_prefix_and_base(os.path.dirname(sys.executable))

    home = os.path.expanduser("~")
    install_names = (
        "miniconda3", "Miniconda3",
        "anaconda3", "Anaconda3",
        "miniforge3", "Miniforge3",
        "mambaforge", "Mambaforge",
    )
    install_parents = [
        home,
        os.environ.get("LOCALAPPDATA", os.path.join(home, "AppData", "Local")),
        os.environ.get("ProgramData", r"C:\ProgramData"),
        "/opt",
        "/usr/local",
    ]
    for parent in install_parents:
        for name in install_names:
            _add_root(os.path.join(parent, name))

    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


# ---------------------------------------------------------------------------
# Update logic (runs in a background thread)
# ---------------------------------------------------------------------------

def _md5(path):
    try:
        return hashlib.md5(open(path, "rb").read()).hexdigest()
    except OSError:
        return None


def _run_update_thread(tag, log_cb, done_cb):
    """
    Execute git + conda environment refresh in a daemon thread.

    log_cb(str)        — called from the thread; UI must schedule to main thread.
    done_cb(bool)      — called on completion with success=True/False.
    """

    def _cmd(args, shell=False):
        log_cb(f"$ {' '.join(str(a) for a in args)}")
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
            shell=shell,
        )
        if result.stdout.strip():
            log_cb(result.stdout.strip())
        if result.stderr.strip():
            log_cb(result.stderr.strip())
        if result.returncode != 0:
            raise RuntimeError(
                f"Command failed (exit {result.returncode}): {' '.join(str(a) for a in args)}"
            )

    def _work():
        try:
            old_env_hash = _md5(_ENV_YML)
            conda = _find_conda()
            if conda is None:
                raise RuntimeError(
                    "conda not found before applying the update. Start Swampy from "
                    "Anaconda Prompt / Conda PowerShell, or make sure conda is installed. "
                    f"Then run conda env update -n {CONDA_ENV_NAME} -f environment.yml --prune "
                    "manually if needed."
                )
            log_cb(f"Using conda executable: {conda}")

            log_cb("Fetching tags from origin…")
            _cmd(["git", "fetch", "--tags", "origin"])

            log_cb(f"Checking out {tag}…")
            _cmd(["git", "reset", "--hard", tag])

            new_env_hash = _md5(_ENV_YML)

            if old_env_hash != new_env_hash:
                log_cb("environment.yml changed — updating conda environment with prune…")
            else:
                log_cb("Refreshing conda environment from environment.yml with prune…")

            if not os.path.isfile(_ENV_YML):
                raise RuntimeError("environment.yml not found; cannot update conda environment.")

            _cmd(
                [conda, "env", "update",
                 "-n", CONDA_ENV_NAME,
                 "-f", _ENV_YML,
                 "--prune"],
                shell=(conda.lower().endswith((".bat", ".cmd"))),
            )

            log_cb("Update complete.")
            done_cb(True)
        except Exception as exc:
            log_cb(f"\nERROR: {exc}")
            done_cb(False)

    threading.Thread(target=_work, daemon=True).start()


# ---------------------------------------------------------------------------
# Tkinter dialogs
# ---------------------------------------------------------------------------

def _show_update_dialog(local_ver, new_ver, changelog, release_url):
    """
    Modal dialog asking the user what to do.
    Returns 'update' | 'skip'.
    """
    root = tk.Tk()
    root.title("Swampy — Update Available")
    root.resizable(False, False)
    root.attributes("-topmost", True)

    # Header
    tk.Label(
        root,
        text="A new version of Swampy is available!",
        font=("Helvetica", 13, "bold"),
        pady=8,
    ).pack(padx=20, pady=(15, 0))
    tk.Label(
        root,
        text=f"Installed: v{local_ver}         Available: v{new_ver.lstrip('v')}",
        font=("Helvetica", 10),
    ).pack()

    # Changelog box
    frame = tk.LabelFrame(root, text="What's new", padx=5, pady=5)
    frame.pack(padx=15, pady=12, fill="both", expand=True)

    scroll = tk.Scrollbar(frame)
    scroll.pack(side="right", fill="y")

    txt = tk.Text(
        frame,
        height=12,
        width=64,
        wrap="word",
        yscrollcommand=scroll.set,
        relief="flat",
        bg=root.cget("bg"),
        font=("Helvetica", 9),
    )
    txt.insert("1.0", changelog)
    txt.config(state="disabled")
    txt.pack(side="left", fill="both", expand=True)
    scroll.config(command=txt.yview)

    # Result holder
    choice = tk.StringVar(value="skip")

    def on_update():
        choice.set("update")
        root.destroy()

    def on_skip():
        root.destroy()

    def on_github():
        webbrowser.open(release_url)

    # Buttons
    btn_row = tk.Frame(root)
    btn_row.pack(pady=(0, 15))

    tk.Button(
        btn_row, text="Update Now", command=on_update, width=14,
        bg="#2e7d32", fg="white", font=("Helvetica", 10, "bold"),
        relief="flat", cursor="hand2",
    ).pack(side="left", padx=6)

    tk.Button(
        btn_row, text="Skip", command=on_skip, width=10,
        font=("Helvetica", 10), relief="flat", cursor="hand2",
    ).pack(side="left", padx=6)

    tk.Button(
        btn_row, text="View on GitHub", command=on_github, width=14,
        font=("Helvetica", 10), relief="flat", cursor="hand2",
    ).pack(side="left", padx=6)

    # Centre on screen
    root.update_idletasks()
    w = root.winfo_reqwidth()
    h = root.winfo_reqheight()
    x = (root.winfo_screenwidth() - w) // 2
    y = (root.winfo_screenheight() - h) // 2
    root.geometry(f"+{x}+{y}")

    root.protocol("WM_DELETE_WINDOW", on_skip)
    root.mainloop()
    return choice.get()


def _show_progress_window(tag):
    """
    Show a progress/log window, run the update in a thread, then relaunch.
    Blocks until the window is closed (either by success-relaunch or user
    closing after a failure).
    """
    root = tk.Tk()
    root.title("Swampy — Updating…")
    root.resizable(True, False)
    root.attributes("-topmost", True)

    tk.Label(
        root,
        text=f"Downloading and applying update {tag.lstrip('v')} …",
        font=("Helvetica", 11),
    ).pack(padx=20, pady=(12, 4))

    # Log area
    log_frame = tk.Frame(root)
    log_frame.pack(padx=10, fill="both", expand=True)

    log_scroll = tk.Scrollbar(log_frame)
    log_scroll.pack(side="right", fill="y")

    log_box = tk.Text(
        log_frame,
        height=16,
        width=72,
        state="disabled",
        bg="#1e1e1e",
        fg="#d4d4d4",
        font=("Courier", 9),
        yscrollcommand=log_scroll.set,
    )
    log_box.pack(side="left", fill="both", expand=True)
    log_scroll.config(command=log_box.yview)

    # Progress bar
    bar = ttk.Progressbar(root, mode="indeterminate", length=500)
    bar.pack(padx=10, pady=6, fill="x")
    bar.start(12)

    # Status label
    status_var = tk.StringVar(value="Starting…")
    status_lbl = tk.Label(root, textvariable=status_var, font=("Helvetica", 9))
    status_lbl.pack(pady=(0, 4))

    # Centre on screen
    root.update_idletasks()
    w = root.winfo_reqwidth()
    h = root.winfo_reqheight()
    x = (root.winfo_screenwidth() - w) // 2
    y = (root.winfo_screenheight() - h) // 2
    root.geometry(f"+{x}+{y}")

    # Callbacks (safe to call from the worker thread via root.after)
    def _append_log(msg):
        def _do():
            log_box.config(state="normal")
            log_box.insert("end", msg + "\n")
            log_box.see("end")
            log_box.config(state="disabled")
            status_var.set(msg[:80])
        root.after(0, _do)

    def _on_done(success):
        def _do():
            bar.stop()
            if success:
                status_var.set("Done! Relaunching Swampy…")
                root.after(1500, _relaunch)
            else:
                status_var.set("Update failed. See log above.")
                bar.config(mode="determinate", value=0)
                tk.Button(
                    root, text="Close", command=root.destroy,
                    width=10, relief="flat",
                ).pack(pady=6)
        root.after(0, _do)

    def _relaunch():
        root.destroy()
        launcher = os.path.join(_REPO_ROOT, "launch_swampy.py")
        subprocess.Popen([sys.executable, launcher] + sys.argv[1:])
        sys.exit(0)

    root.protocol("WM_DELETE_WINDOW", lambda: None)  # disable close during update

    _run_update_thread(tag, _append_log, _on_done)
    root.mainloop()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def check_and_prompt():
    """
    Call this at startup (before opening the main app window).

    Returns True  → caller should proceed to launch the app normally.
    Returns False → this function relaunched the process; caller must exit.
    """
    local = _read_local_version()
    release = _fetch_latest_release()

    if release is None:
        # Offline or API error — launch normally, silently.
        return True

    tag, changelog, release_url = release
    remote = tag.lstrip("v")

    if _version_tuple(remote) <= _version_tuple(local):
        return True  # already up to date

    # Ask the user
    choice = _show_update_dialog(local, tag, changelog, release_url)

    if choice == "update":
        _show_progress_window(tag)
        return False  # _show_progress_window either relaunched or failed

    return True  # user chose to skip
