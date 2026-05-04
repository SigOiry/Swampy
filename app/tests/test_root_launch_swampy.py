import importlib.util
import types
from pathlib import Path


def _load_root_launcher():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "launch_swampy.py"
    spec = importlib.util.spec_from_file_location("root_launch_swampy", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_ensure_required_conda_env_allows_expected_env():
    root_launch = _load_root_launcher()
    updater_stub = types.SimpleNamespace(
        CONDA_ENV_NAME="Swampy2026",
        LEGACY_CONDA_ENV_NAME="SwampySim",
        get_active_conda_env_name=lambda: "Swampy2026",
        get_conda_executable=lambda: None,
        find_conda_envs=lambda conda=None: {},
    )

    assert root_launch._ensure_required_conda_env(updater_stub) is True


def test_ensure_required_conda_env_schedules_legacy_cleanup(monkeypatch):
    root_launch = _load_root_launcher()
    calls = []
    infos = []

    updater_stub = types.SimpleNamespace(
        CONDA_ENV_NAME="Swampy2026",
        LEGACY_CONDA_ENV_NAME="SwampySim",
        get_active_conda_env_name=lambda: "SwampySim",
        get_conda_executable=lambda: r"C:\Miniconda3\condabin\conda.bat",
        find_conda_envs=lambda conda=None: {"SwampySim": r"C:\Miniconda3\envs\SwampySim"},
        schedule_conda_env_removal_after_exit=lambda env_name, conda=None: calls.append(
            ("schedule", env_name, conda)
        ),
        remove_conda_env=lambda env_name, conda=None: calls.append(("remove", env_name, conda)),
    )

    monkeypatch.setattr(root_launch, "_ask_yes_no", lambda title, message: True)
    monkeypatch.setattr(root_launch, "_show_info", lambda title, message: infos.append(message))
    monkeypatch.setattr(root_launch, "_show_warning", lambda title, message: infos.append(message))
    monkeypatch.setattr(root_launch, "_show_error", lambda title, message: infos.append(message))

    assert root_launch._ensure_required_conda_env(updater_stub) is False
    assert calls == [("schedule", "SwampySim", r"C:\Miniconda3\condabin\conda.bat")]
    assert any("will be deleted after this launcher closes" in message for message in infos)


def test_ensure_required_conda_env_shows_warning_when_no_legacy_env(monkeypatch):
    root_launch = _load_root_launcher()
    warnings = []

    updater_stub = types.SimpleNamespace(
        CONDA_ENV_NAME="Swampy2026",
        LEGACY_CONDA_ENV_NAME="SwampySim",
        get_active_conda_env_name=lambda: "base",
        get_conda_executable=lambda: r"C:\Miniconda3\condabin\conda.bat",
        find_conda_envs=lambda conda=None: {"Swampy2026": r"C:\Miniconda3\envs\Swampy2026"},
    )

    monkeypatch.setattr(root_launch, "_show_warning", lambda title, message: warnings.append(message))

    assert root_launch._ensure_required_conda_env(updater_stub) is False
    assert warnings
    assert "Required environment: Swampy2026" in warnings[0]
