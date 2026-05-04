import os
from pathlib import Path

import updater


def test_get_active_conda_env_name_prefers_conda_default_env(monkeypatch):
    monkeypatch.setenv("CONDA_DEFAULT_ENV", updater.CONDA_ENV_NAME)
    monkeypatch.delenv("CONDA_PREFIX", raising=False)

    assert updater.get_active_conda_env_name() == updater.CONDA_ENV_NAME


def test_get_active_conda_env_name_infers_env_from_python_path(monkeypatch, tmp_path):
    monkeypatch.delenv("CONDA_DEFAULT_ENV", raising=False)
    monkeypatch.delenv("CONDA_PREFIX", raising=False)

    python_relpath = "python.exe" if os.name == "nt" else os.path.join("bin", "python")
    python_path = tmp_path / "miniconda3" / "envs" / updater.CONDA_ENV_NAME / Path(python_relpath)

    assert (
        updater.get_active_conda_env_name(python_executable=str(python_path))
        == updater.CONDA_ENV_NAME
    )


def test_build_target_env_launch_command_uses_target_env_python(monkeypatch, tmp_path):
    env_prefix = tmp_path / "miniconda3" / "envs" / updater.CONDA_ENV_NAME
    python_relpath = "python.exe" if os.name == "nt" else os.path.join("bin", "python")
    python_path = env_prefix / Path(python_relpath)
    python_path.parent.mkdir(parents=True, exist_ok=True)
    python_path.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        updater,
        "find_conda_envs",
        lambda conda=None: {updater.CONDA_ENV_NAME: str(env_prefix)},
    )

    command = updater.build_target_env_launch_command(["-f", "run.xml"], conda="conda")

    assert command[0] == str(python_path)
    assert command[1] == str(Path(updater.__file__).resolve().parent / "launch_swampy.py")
    assert command[2:] == ["-f", "run.xml"]
