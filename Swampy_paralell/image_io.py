# -*- coding: utf-8 -*-
"""Small helpers for detecting reflectance bands in supported image products."""

import math
import re

import numpy as np


WAVELENGTH_ATTRIBUTE_NAMES = (
    "wavelength",
    "wave_nm",
    "wavelength_nm",
    "central_wavelength",
    "center_wavelength",
    "centre_wavelength",
    "lambda",
    "wl",
)


def _coerce_float(value):
    try:
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if isinstance(value, np.ndarray):
            if value.size != 1:
                return None
            value = value.reshape(-1)[0]
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, (list, tuple)):
            if len(value) != 1:
                return None
            value = value[0]
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def looks_like_wavelength_var(var_name):
    """Return True when a variable name looks like a wavelength coordinate."""
    lowered = str(var_name).lower()
    return any(token in lowered for token in ("wave", "wl", "lambda", "wavelength", "band"))


def extract_wavelength(var_name, variable=None):
    """Return a central wavelength from variable metadata or its name."""
    if variable is not None:
        for attr_name in WAVELENGTH_ATTRIBUTE_NAMES:
            try:
                value = getattr(variable, attr_name)
            except Exception:
                continue
            parsed = _coerce_float(value)
            if parsed is not None:
                return parsed

    matches = re.findall(r"(\d+(?:\.\d+)?)", str(var_name))
    if not matches:
        return None
    values = []
    for match in matches:
        parsed = _coerce_float(match)
        if parsed is not None:
            values.append(parsed)
    if not values:
        return None
    plausible_nm = [value for value in values if 300.0 <= value <= 2500.0]
    if plausible_nm:
        return plausible_nm[0]
    return values[0]


def is_polymer_reflectance_variable(var_name):
    """Return True for Polymer water-reflectance layers such as Rw443."""
    lowered = str(var_name).lower()
    return re.match(r"^rw[_-]?\d", lowered) is not None


def is_rrs_band_variable(var_name):
    """Return True if the variable name looks like a reflectance band."""
    lowered = str(var_name).lower()
    if is_polymer_reflectance_variable(lowered):
        return True
    return lowered.startswith(("rrs", "rho", "reflectance", "band"))


def is_auxiliary_scene_variable(var_name):
    """Return True for scene variables that are not spectral reflectance bands."""
    lowered = str(var_name).lower()
    tokens = (
        "flag",
        "mask",
        "quality",
        "class",
        "cloud",
        "glint",
        "angle",
        "uncert",
        "bit",
        "logchl",
        "logfb",
        "gli",
        "nir",
    )
    return any(token in lowered for token in tokens)


def band_sort_key(var_name):
    """Sort spectral layers by wavelength when one is available."""
    value = extract_wavelength(var_name)
    if value is not None:
        return value
    return str(var_name).lower()


def stable_dimension_name(dim_name, fallback):
    """Replace netCDF4 HDF fake dimensions with stable row/column names."""
    text = str(dim_name or "").strip()
    if not text or re.fullmatch(r"fakedim\d+", text.lower()):
        return fallback
    return text
