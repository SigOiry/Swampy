"""Lightweight install-bound access control for SWAMpy.

This is deterrence, not strong anti-tamper protection: the repository is
public, so a determined user can inspect and modify the code. The goal here is
to require an admin-defined password on first use and to bind the local
validation marker to one machine and one installation path.
"""

from __future__ import annotations

import getpass
import hashlib
import hmac
import json
import os
import platform
import socket
import sys
import time
import uuid
from datetime import datetime, timezone


APP_NAME = "SWAMpy"
AUTH_CONFIG_FILENAME = "auth_config.json"
AUTH_CONFIG_VERSION = 1
AUTH_TOKEN_VERSION = 1
DEFAULT_KDF = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 300_000
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_FAILURE_DELAY_SECONDS = 1.5


def _repo_root(repo_root=None):
    return os.path.abspath(repo_root or os.path.dirname(__file__))


def _config_path(repo_root=None):
    return os.path.join(_repo_root(repo_root), AUTH_CONFIG_FILENAME)


def _normalise_install_path(repo_root=None):
    return os.path.normcase(os.path.abspath(_repo_root(repo_root)))


def _token_dir():
    appdata = os.environ.get("APPDATA")
    if appdata:
        return os.path.join(appdata, APP_NAME)
    return os.path.join(os.path.expanduser("~"), f".{APP_NAME.lower()}_auth")


def _install_fingerprint(repo_root=None):
    return hashlib.sha256(_normalise_install_path(repo_root).encode("utf-8")).hexdigest()


def _token_path(repo_root=None):
    install_hash = _install_fingerprint(repo_root)[:16]
    return os.path.join(_token_dir(), f"swampy_auth_{install_hash}.json")


def _machine_material():
    parts = [
        socket.gethostname(),
        platform.system(),
        platform.machine(),
        platform.node(),
        f"{uuid.getnode():012x}",
    ]
    return "|".join(str(part or "") for part in parts)


def _machine_fingerprint():
    return hashlib.sha256(_machine_material().encode("utf-8")).hexdigest()


def _canonical_config_payload(config):
    return {
        "config_version": int(config.get("config_version", AUTH_CONFIG_VERSION)),
        "app_name": str(config.get("app_name", APP_NAME)),
        "kdf": str(config.get("kdf", DEFAULT_KDF)),
        "iterations": int(config.get("iterations", DEFAULT_ITERATIONS)),
        "salt": str(config.get("salt", "")),
        "password_hash": str(config.get("password_hash", "")),
    }


def _config_fingerprint(config):
    payload = json.dumps(_canonical_config_payload(config), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _derive_password_hash(password, salt_hex, iterations):
    if not isinstance(password, str):
        raise TypeError("Password must be a string.")
    salt = bytes.fromhex(str(salt_hex))
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        int(iterations),
    ).hex()


def build_auth_config(password, *, iterations=DEFAULT_ITERATIONS, salt_hex=None):
    if not isinstance(password, str) or password == "":
        raise ValueError("Password must be a non-empty string.")
    if salt_hex is None:
        salt_hex = os.urandom(16).hex()
    password_hash = _derive_password_hash(password, salt_hex, int(iterations))
    return {
        "config_version": AUTH_CONFIG_VERSION,
        "configured": True,
        "app_name": APP_NAME,
        "kdf": DEFAULT_KDF,
        "iterations": int(iterations),
        "salt": salt_hex,
        "password_hash": password_hash,
    }


def write_auth_config(password, output_path, *, iterations=DEFAULT_ITERATIONS):
    config = build_auth_config(password, iterations=iterations)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")
    return config


def _load_auth_config(repo_root=None):
    path = _config_path(repo_root)
    if not os.path.exists(path):
        raise RuntimeError(
            f"Missing '{AUTH_CONFIG_FILENAME}' at '{path}'. "
            "Create it locally with generate_auth_config.py before sharing the repo."
        )
    with open(path, "r", encoding="utf-8") as handle:
        try:
            config = json.load(handle)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid '{AUTH_CONFIG_FILENAME}': {exc}") from exc
    if not isinstance(config, dict):
        raise RuntimeError(f"Invalid '{AUTH_CONFIG_FILENAME}': expected a JSON object.")
    if not config.get("configured", False):
        raise RuntimeError(
            f"'{AUTH_CONFIG_FILENAME}' is still a placeholder. "
            "Run generate_auth_config.py locally to set the admin password."
        )
    required_fields = ("salt", "password_hash", "iterations", "kdf")
    missing = [field for field in required_fields if not config.get(field)]
    if missing:
        raise RuntimeError(
            f"Invalid '{AUTH_CONFIG_FILENAME}': missing required field(s): {', '.join(missing)}."
        )
    if str(config.get("kdf")) != DEFAULT_KDF:
        raise RuntimeError(
            f"Unsupported password KDF '{config.get('kdf')}'. Expected '{DEFAULT_KDF}'."
        )
    return config


def _password_matches(password, config):
    derived = _derive_password_hash(password, config["salt"], config["iterations"])
    return hmac.compare_digest(derived, str(config["password_hash"]))


def _build_validation_record(config, repo_root=None):
    config_fp = _config_fingerprint(config)
    machine_fp = _machine_fingerprint()
    install_fp = _install_fingerprint(repo_root)
    proof_payload = "|".join(
        (
            str(AUTH_TOKEN_VERSION),
            config_fp,
            machine_fp,
            install_fp,
            str(config["password_hash"]),
        )
    ).encode("utf-8")
    return {
        "token_version": AUTH_TOKEN_VERSION,
        "app_name": APP_NAME,
        "config_fingerprint": config_fp,
        "machine_fingerprint_hash": machine_fp,
        "install_fingerprint_hash": install_fp,
        "token_proof": hashlib.sha256(proof_payload).hexdigest(),
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _load_validation_record(repo_root=None):
    path = _token_path(repo_root)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _validation_record_matches(record, config, repo_root=None):
    if not isinstance(record, dict):
        return False
    expected = _build_validation_record(config, repo_root=repo_root)
    if str(record.get("app_name", "")) != APP_NAME:
        return False
    return (
        hmac.compare_digest(str(record.get("config_fingerprint", "")), expected["config_fingerprint"]) and
        hmac.compare_digest(str(record.get("machine_fingerprint_hash", "")), expected["machine_fingerprint_hash"]) and
        hmac.compare_digest(str(record.get("install_fingerprint_hash", "")), expected["install_fingerprint_hash"]) and
        hmac.compare_digest(str(record.get("token_proof", "")), expected["token_proof"])
    )


def _write_validation_record(record, repo_root=None):
    token_dir = _token_dir()
    os.makedirs(token_dir, exist_ok=True)
    with open(_token_path(repo_root), "w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2)
        handle.write("\n")


def is_authorized(repo_root=None):
    config = _load_auth_config(repo_root)
    record = _load_validation_record(repo_root)
    return _validation_record_matches(record, config, repo_root=repo_root)


def ensure_app_authorized(repo_root=None, *, max_attempts=DEFAULT_MAX_ATTEMPTS,
                          failure_delay_seconds=DEFAULT_FAILURE_DELAY_SECONDS):
    try:
        config = _load_auth_config(repo_root)
    except RuntimeError as exc:
        print(f"[ERROR]: {exc}")
        return False

    record = _load_validation_record(repo_root)
    if _validation_record_matches(record, config, repo_root=repo_root):
        return True

    print(f"[INFO]: {APP_NAME} access is locked for this installation.")
    print("[INFO]: Enter the admin password to validate this machine.")
    for attempt in range(1, int(max_attempts) + 1):
        try:
            password = getpass.getpass("Password: ")
        except (EOFError, KeyboardInterrupt):
            print("\n[ERROR]: Authentication cancelled.")
            return False
        if _password_matches(password, config):
            validation_record = _build_validation_record(config, repo_root=repo_root)
            _write_validation_record(validation_record, repo_root=repo_root)
            print("[INFO]: Access granted. This installation is now validated.")
            return True
        print(f"[ERROR]: Invalid password ({attempt}/{int(max_attempts)}).")
        if attempt < int(max_attempts):
            time.sleep(float(failure_delay_seconds))
    print("[ERROR]: Maximum password attempts exceeded. Exiting.")
    return False


__all__ = [
    "APP_NAME",
    "AUTH_CONFIG_FILENAME",
    "DEFAULT_ITERATIONS",
    "build_auth_config",
    "ensure_app_authorized",
    "is_authorized",
    "write_auth_config",
]
