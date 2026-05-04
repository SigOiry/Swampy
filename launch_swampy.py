"""
Swampy entry point.

Run this from the repo root:
    python launch_swampy.py [options]

It checks that Swampy is started from the expected Conda environment,
checks for updates, then delegates to app/launch_swampy.py.
"""

import os
import runpy
import sys

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
_APP_DIR = os.path.join(_REPO_ROOT, "app")


def _prepare_import_paths():
    # Make repo root importable (for updater/auth) and app/ importable (for
    # all the app modules: gui_swampy, sambuca, output_calculation, ...)
    for path in (_REPO_ROOT, _APP_DIR):
        if path not in sys.path:
            sys.path.insert(0, path)


def _show_dialog(method_name, title, message):
    import tkinter as tk
    from tkinter import messagebox

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        return getattr(messagebox, method_name)(title, message, parent=root)
    finally:
        root.destroy()


def _ask_yes_no(title, message):
    return _show_dialog("askyesno", title, message)


def _show_warning(title, message):
    _show_dialog("showwarning", title, message)


def _show_info(title, message):
    _show_dialog("showinfo", title, message)


def _show_error(title, message):
    _show_dialog("showerror", title, message)


def _build_wrong_env_message(active_env, expected_env, legacy_env_name=None, include_delete_prompt=False):
    detected_env = active_env or "No active Conda environment detected"
    lines = [
        f"Swampy must be started from the '{expected_env}' Conda environment.",
        "",
        f"Detected environment: {detected_env}",
        f"Required environment: {expected_env}",
        "",
        "Close this launcher, activate the correct environment, then run:",
        "python launch_swampy.py",
    ]

    if legacy_env_name and include_delete_prompt:
        lines.extend(
            [
                "",
                f"Do you want to delete the old '{legacy_env_name}' environment now?",
            ]
        )

    return "\n".join(lines)


def _ensure_required_conda_env(updater_module):
    expected_env = updater_module.CONDA_ENV_NAME
    legacy_env = getattr(updater_module, "LEGACY_CONDA_ENV_NAME", None)
    active_env = updater_module.get_active_conda_env_name()

    if active_env == expected_env:
        return True

    conda = updater_module.get_conda_executable()
    known_envs = {}
    if conda:
        try:
            known_envs = updater_module.find_conda_envs(conda=conda)
        except Exception:
            known_envs = {}

    legacy_exists = bool(legacy_env) and (active_env == legacy_env or legacy_env in known_envs)
    title = "Swampy - Conda Environment Required"

    if legacy_exists:
        message = _build_wrong_env_message(
            active_env,
            expected_env,
            legacy_env_name=legacy_env,
            include_delete_prompt=True,
        )
        if _ask_yes_no(title, message):
            try:
                if active_env == legacy_env:
                    updater_module.schedule_conda_env_removal_after_exit(legacy_env, conda=conda)
                    _show_info(
                        title,
                        f"The '{legacy_env}' environment will be deleted after this launcher closes.\n\n"
                        f"Activate '{expected_env}' before starting Swampy again.",
                    )
                else:
                    updater_module.remove_conda_env(legacy_env, conda=conda)
                    _show_info(
                        title,
                        f"The '{legacy_env}' environment was deleted.\n\n"
                        f"Activate '{expected_env}' before starting Swampy again.",
                    )
            except Exception as exc:
                _show_error(
                    title,
                    f"Unable to remove '{legacy_env}'.\n\n{exc}",
                )
        return False

    _show_warning(title, _build_wrong_env_message(active_env, expected_env))
    return False


def main():
    _prepare_import_paths()

    # Always run from the repo root so that relative paths inside the app
    # (Data/, Output/, ...) resolve correctly.
    os.chdir(_REPO_ROOT)

    import updater

    if not _ensure_required_conda_env(updater):
        return 1

    import auth

    if not auth.ensure_app_authorized(_REPO_ROOT):
        return 1

    # Update check - runs before any app code is loaded.
    if not updater.check_and_prompt():
        # Updater relaunched the process after applying the update.
        return 0

    # Launch the app.
    runpy.run_path(
        os.path.join(_APP_DIR, "launch_swampy.py"),
        run_name="__main__",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
