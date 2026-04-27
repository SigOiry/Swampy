# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 15:14:44 2019

@author: marco
"""

import ctypes
import copy
import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
import xml.etree.ElementTree as ET
from tkinter import BooleanVar, StringVar, W
from tkinter import messagebox
from tkinter import ttk
from tkinter.filedialog import askdirectory, askopenfilename, askopenfilenames

import numpy as np

try:
    import siop_config
except ImportError:  # pragma: no cover - fallback when imported as a package
    from app import siop_config

try:
    import sensor_config
except ImportError:  # pragma: no cover - fallback when imported as a package
    from app import sensor_config

try:
    import image_io
except ImportError:  # pragma: no cover - fallback when imported as a package
    from app import image_io


PLOT_COLORS = [
    "#0f6cbd",
    "#198754",
    "#cc5500",
    "#6c757d",
]

_SENTINEL_RGB_PREVIEW_MAX_PIXELS = 8_000_000


def _xml_find_text(node, path, default=None):
    if node is None:
        return default
    found = node.find(path)
    if found is None or found.text is None:
        return default
    text = str(found.text).strip()
    return text if text != "" else default


def _xml_find_items(node, path):
    parent = node.find(path) if node is not None else None
    if parent is None:
        return []
    values = []
    for child in parent.findall("./item"):
        if child.text is None:
            continue
        text = str(child.text).strip()
        if text != "":
            values.append(text)
    return values


def _parse_bool_text(value, default=False):
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _paths_equivalent(path_a, path_b):
    if not path_a or not path_b:
        return False
    try:
        return os.path.normcase(os.path.normpath(os.path.abspath(path_a))) == os.path.normcase(os.path.normpath(os.path.abspath(path_b)))
    except Exception:
        return False


def _resolve_bundled_resource(repo_root, path):
    if not path:
        return path
    if os.path.exists(path):
        return path

    basename = os.path.basename(path)
    candidate_map = {
        "new_input_sub.xml": [
            os.path.join(repo_root, "Data", "Templates", "new_input_sub.xml"),
            os.path.join(repo_root, "Data", "Test", "new_input_sub.xml"),
        ],
        "new_inputs_posidonia_sand_sargassum.xml": [
            os.path.join(repo_root, "Data", "Templates", "new_inputs_posidonia_sand_sargassum.xml"),
        ],
        "swampy_s2_5_bands_filter_nedr.xml": [
            os.path.join(repo_root, "Data", "SRF", "swampy_s2_5_bands_filter_nedr.xml"),
            os.path.join(repo_root, "Test", "swampy_s2_5_bands_filter_nedr.xml"),
            os.path.join(repo_root, "Data", "Test", "swampy_s2_5_bands_filter_nedr.xml"),
        ],
        "swampy_s2_6_bands_filter_nedr.xml": [
            os.path.join(repo_root, "Data", "SRF", "swampy_s2_6_bands_filter_nedr.xml"),
        ],
        "swampy_prisma_63_bands_filter_nedr.xml": [
            os.path.join(repo_root, "Data", "SRF", "swampy_prisma_63_bands_filter_nedr.xml"),
        ],
        "E4_2024.tif": [
            os.path.join(repo_root, "Data", "Bathy", "E4_2024.tif"),
            os.path.join(repo_root, "Bathy", "E4_2024.tif"),
        ],
    }
    for candidate in candidate_map.get(basename, []):
        if os.path.exists(candidate):
            return candidate
    generic_candidates = [
        os.path.join(repo_root, "Data", "SRF", basename),
        os.path.join(repo_root, "Data", "Templates", basename),
        os.path.join(repo_root, "Data", "Bathy", basename),
        os.path.join(repo_root, "Test", basename),
        os.path.join(repo_root, "Data", "Test", basename),
    ]
    for candidate in generic_candidates:
        if os.path.exists(candidate):
            return candidate
    return path


def _match_clean_name(name, candidates):
    if not name:
        return None
    clean_target = siop_config.clean_label(name).lower()
    for candidate in candidates:
        if siop_config.clean_label(candidate).lower() == clean_target:
            return candidate
    return name if name in candidates else None


def _match_band_indices_by_center(template, centers, max_delta_nm=1.0):
    selected = []
    used = set()
    for center in centers:
        try:
            target_center = float(center)
        except (TypeError, ValueError):
            continue
        best_index = None
        best_delta = None
        for band in template["bands"]:
            index = int(band["index"])
            if index in used:
                continue
            delta = abs(float(band["center"]) - target_center)
            if best_delta is None or delta < best_delta:
                best_index = index
                best_delta = delta
        if best_index is not None and best_delta is not None and best_delta <= max_delta_nm:
            selected.append(best_index)
            used.add(best_index)
    return selected


def _format_sensor_centers(centers, limit=8):
    labels = [f"{center:.0f}" if float(center).is_integer() else f"{center:.1f}" for center in centers]
    if len(labels) <= limit:
        return ", ".join(labels)
    return ", ".join(labels[:limit]) + ", ..."


def _display_input_selection(files):
    files = [str(path) for path in files if str(path).strip()]
    if not files:
        return ""
    if len(files) == 1:
        return files[0]
    return f"{files[0]} (+{len(files) - 1} more)"


def _looks_like_wavelength_var(var_name):
    return image_io.looks_like_wavelength_var(var_name)


def _extract_wavelength(var_name, variable=None):
    return image_io.extract_wavelength(var_name, variable)


def _band_sort_key(var_name):
    return image_io.band_sort_key(var_name)


def _is_rrs_band_variable(var_name):
    return image_io.is_rrs_band_variable(var_name)


def _is_auxiliary_scene_variable(var_name):
    return image_io.is_auxiliary_scene_variable(var_name)


def _format_band_wavelength(wavelength):
    if wavelength is None:
        return ""
    try:
        value = float(wavelength)
    except (TypeError, ValueError):
        return ""
    if not np.isfinite(value):
        return ""
    if float(value).is_integer():
        return f"{int(round(value))} nm"
    return f"{value:.1f} nm"


def _build_image_band_label(index, wavelength=None, var_name=None, source_kind="3d"):
    wavelength_text = _format_band_wavelength(wavelength)
    if source_kind == "stacked_2d" and var_name:
        return f"{var_name} ({wavelength_text})" if wavelength_text else str(var_name)
    label = f"Band {int(index) + 1}"
    if wavelength_text:
        label += f" ({wavelength_text})"
    return label


def _find_matching_wavelength_vector(variables, variable_name, variable, spectral_axis):
    band_count = int(variable.shape[spectral_axis])
    raw_dims = getattr(variable, "dimensions", ()) or ()
    spectral_dim = raw_dims[spectral_axis] if spectral_axis < len(raw_dims) else None

    candidate_names = []
    if spectral_dim and spectral_dim in variables:
        candidate_names.append(spectral_dim)
    for cand_name, cand_var in variables.items():
        if cand_name == variable_name:
            continue
        if getattr(cand_var, "ndim", 0) != 1:
            continue
        try:
            cand_length = int(cand_var.shape[0])
        except Exception:
            continue
        if cand_length != band_count:
            continue
        std_name = getattr(cand_var, "standard_name", "").lower() if hasattr(cand_var, "standard_name") else ""
        axis_name = getattr(cand_var, "axis", "").lower() if hasattr(cand_var, "axis") else ""
        if (
            cand_name == spectral_dim
            or _looks_like_wavelength_var(cand_name)
            or "wavelength" in std_name
            or axis_name == "z"
        ):
            candidate_names.append(cand_name)

    seen = set()
    for cand_name in candidate_names:
        if cand_name in seen:
            continue
        seen.add(cand_name)
        try:
            values = np.asarray(variables[cand_name][:], dtype="float32").reshape(-1)
        except Exception:
            continue
        if values.size != band_count or not np.all(np.isfinite(values)):
            continue
        return [float(value) for value in values], cand_name
    return [None] * band_count, None


def _load_input_image_band_info(path):
    from netCDF4 import Dataset

    latlon_names = {'lat', 'latitude', 'lon', 'longitude'}
    with Dataset(path, 'r') as dataset:
        variables = dataset.variables
        three_d_candidates = []
        two_d_candidates = []
        wavelength_vector = None
        wavelength_name = None

        for var_name, variable in variables.items():
            lowered = str(var_name).lower()
            if lowered in latlon_names:
                continue
            shape = getattr(variable, 'shape', ())
            if len(shape) == 1 and _looks_like_wavelength_var(var_name):
                try:
                    wavelength_vector = np.asarray(variable[:], dtype='float32').reshape(-1)
                    wavelength_name = str(var_name)
                except Exception:
                    pass
                continue
            if len(shape) == 3 and all(int(size) > 0 for size in shape):
                three_d_candidates.append((str(var_name), variable))
            elif len(shape) == 2 and _is_rrs_band_variable(var_name) and not _is_auxiliary_scene_variable(var_name):
                wave = _extract_wavelength(var_name, variable)
                sort_key = wave if wave is not None else _band_sort_key(var_name)
                two_d_candidates.append((sort_key, str(var_name), variable, wave))

        if three_d_candidates:
            last_valid_result = None
            for var_name, variable in three_d_candidates:
                try:
                    spectral_axis = _identify_spectral_axis(getattr(variable, 'dimensions', ()), variable.shape)
                    band_count = int(variable.shape[spectral_axis])
                except Exception:
                    continue
                wavelengths, wavelength_var_name = _find_matching_wavelength_vector(
                    variables,
                    var_name,
                    variable,
                    spectral_axis,
                )
                if all(value is None for value in wavelengths) and wavelength_vector is not None and len(wavelength_vector) == band_count:
                    wavelengths = [float(value) for value in wavelength_vector]
                    wavelength_var_name = wavelength_name
                bands = []
                for band_index in range(band_count):
                    wavelength = wavelengths[band_index] if band_index < len(wavelengths) else None
                    bands.append({
                        "index": band_index,
                        "label": _build_image_band_label(band_index, wavelength=wavelength, source_kind="3d"),
                        "wavelength": wavelength,
                        "source_name": var_name,
                    })
                last_valid_result = {
                    "path": path,
                    "source_name": var_name,
                    "source_kind": "3d",
                    "wavelength_var_name": wavelength_var_name,
                    "bands": bands,
                    "labels": [band["label"] for band in bands],
                    "wavelengths": [band["wavelength"] for band in bands],
                    "band_count": len(bands),
                    "is_hyperspectral": len(bands) > 20,
                }
            if last_valid_result is not None:
                return last_valid_result

        if two_d_candidates:
            two_d_candidates.sort(key=lambda item: (item[0], item[1].lower()))
            bands = []
            for band_index, (_sort_key, var_name, _variable, wavelength) in enumerate(two_d_candidates):
                bands.append({
                    "index": band_index,
                    "label": _build_image_band_label(
                        band_index,
                        wavelength=wavelength,
                        var_name=var_name,
                        source_kind="stacked_2d",
                    ),
                    "wavelength": wavelength,
                    "source_name": var_name,
                })
            return {
                "path": path,
                "source_name": "stacked_rrs",
                "source_kind": "stacked_2d",
                "wavelength_var_name": wavelength_name,
                "bands": bands,
                "labels": [band["label"] for band in bands],
                "wavelengths": [band["wavelength"] for band in bands],
                "band_count": len(bands),
                "is_hyperspectral": len(bands) > 20,
            }

    raise RuntimeError("Unable to find valid reflectance bands in the selected input image.")


def _image_band_info_matches_mapping(image_band_info, mapping):
    if not image_band_info or not mapping:
        return False
    source_labels = list(mapping.get("source_band_labels") or [])
    source_wavelengths = list(mapping.get("source_band_wavelengths") or [])
    if source_labels and list(image_band_info.get("labels") or []) == source_labels:
        return True
    current_wavelengths = np.asarray(image_band_info.get("wavelengths") or [], dtype=object)
    stored_wavelengths = np.asarray(source_wavelengths or [], dtype=object)
    if current_wavelengths.size and stored_wavelengths.size and current_wavelengths.size == stored_wavelengths.size:
        current_numeric = np.array([
            np.nan if value is None else float(value) for value in current_wavelengths
        ], dtype=float)
        stored_numeric = np.array([
            np.nan if value in (None, "") else float(value) for value in stored_wavelengths
        ], dtype=float)
        valid_mask = np.isfinite(current_numeric) & np.isfinite(stored_numeric)
        if np.any(valid_mask) and np.array_equal(np.isnan(current_numeric), np.isnan(stored_numeric)):
            return np.allclose(current_numeric[valid_mask], stored_numeric[valid_mask], atol=0.5)
    return False


def _mapping_lookup_from_payload(mapping):
    sensor_band_indices = list(mapping.get("sensor_band_indices") or [])
    image_band_indices = list(mapping.get("image_band_indices") or [])
    lookup = {}
    for sensor_index, image_index in zip(sensor_band_indices, image_band_indices):
        try:
            lookup[int(sensor_index)] = int(image_index)
        except (TypeError, ValueError):
            continue
    return lookup


def _build_sensor_band_mapping_payload(template, selected_indices, image_band_info, lookup, mode, tolerance_nm):
    if template is None or not image_band_info:
        return None
    source_bands = list(image_band_info.get("bands") or [])
    band_by_index = {int(band["index"]): band for band in template.get("bands", [])}
    sensor_band_indices = []
    sensor_band_centers = []
    image_band_indices = []
    image_band_labels = []
    image_band_wavelengths = []
    for sensor_index in selected_indices:
        sensor_index = int(sensor_index)
        if sensor_index not in lookup:
            continue
        source_index = int(lookup[sensor_index])
        if source_index < 0 or source_index >= len(source_bands):
            continue
        sensor_band = band_by_index.get(sensor_index)
        if sensor_band is None:
            continue
        source_band = source_bands[source_index]
        sensor_band_indices.append(sensor_index)
        sensor_band_centers.append(float(sensor_band["center"]))
        image_band_indices.append(source_index)
        image_band_labels.append(source_band["label"])
        image_band_wavelengths.append(source_band.get("wavelength"))
    if not image_band_indices:
        return None
    return {
        "mode": str(mode),
        "tolerance_nm": float(tolerance_nm),
        "source_kind": image_band_info.get("source_kind", ""),
        "source_name": image_band_info.get("source_name", ""),
        "source_band_labels": list(image_band_info.get("labels") or []),
        "source_band_wavelengths": list(image_band_info.get("wavelengths") or []),
        "sensor_band_indices": sensor_band_indices,
        "sensor_band_centers": sensor_band_centers,
        "image_band_indices": image_band_indices,
        "image_band_labels": image_band_labels,
        "image_band_wavelengths": image_band_wavelengths,
    }


def _clone_sensor_band_mapping_config(mapping):
    if not mapping:
        return None
    cloned = dict(mapping)
    for key in (
        "source_band_labels",
        "source_band_wavelengths",
        "sensor_band_indices",
        "sensor_band_centers",
        "image_band_indices",
        "image_band_labels",
        "image_band_wavelengths",
    ):
        cloned[key] = list(mapping.get(key) or [])
    return cloned


def _auto_match_sensor_band_lookup(template, selected_sensor_indices, image_band_info, tolerance_nm=10.0):
    if not image_band_info:
        return {}, []
    tolerance_nm = max(0.0, float(tolerance_nm))
    source_bands = list(image_band_info.get("bands") or [])
    source_wavelengths = [band.get("wavelength") for band in source_bands]
    lookup = {}
    unmatched = []
    used_source_indices = set()

    has_source_wavelengths = any(value is not None for value in source_wavelengths)
    if has_source_wavelengths:
        numeric_wavelengths = np.array([
            np.nan if value is None else float(value) for value in source_wavelengths
        ], dtype=float)
        for sensor_index in selected_sensor_indices:
            sensor_band = next((band for band in template["bands"] if int(band["index"]) == int(sensor_index)), None)
            if sensor_band is None:
                unmatched.append(int(sensor_index))
                continue
            target_center = float(sensor_band["center"])
            candidate_order = np.argsort(np.abs(numeric_wavelengths - target_center))
            chosen_source_index = None
            for candidate in candidate_order:
                if int(candidate) in used_source_indices:
                    continue
                delta = abs(float(numeric_wavelengths[int(candidate)]) - target_center)
                if not np.isfinite(delta) or delta > tolerance_nm:
                    continue
                chosen_source_index = int(candidate)
                break
            if chosen_source_index is None:
                unmatched.append(int(sensor_index))
                continue
            lookup[int(sensor_index)] = chosen_source_index
            used_source_indices.add(chosen_source_index)
        return lookup, unmatched

    return {}, [int(sensor_index) for sensor_index in selected_sensor_indices]


def _identify_spectral_axis(dim_names, shape):
    spectral_tokens = ('band', 'wavelength', 'wave', 'lambda', 'wl', 'spec')
    for idx, name in enumerate(dim_names):
        lowered = str(name).lower()
        if any(token in lowered for token in spectral_tokens):
            return idx
    return int(np.argmin(shape))


def _normalize_rrs_axes(rrs_arr, dim_names):
    if rrs_arr.ndim != 3:
        raise ValueError("Expected a 3D RRS array.")
    if not dim_names or len(dim_names) != 3:
        dim_names = tuple(f"dim_{i}" for i in range(rrs_arr.ndim))
    else:
        dim_names = tuple(dim_names)
    spectral_axis = _identify_spectral_axis(dim_names, rrs_arr.shape)
    spatial_axes = [idx for idx in range(rrs_arr.ndim) if idx != spectral_axis]
    ordered = np.transpose(rrs_arr, axes=[spectral_axis] + spatial_axes)
    return ordered


def _load_coordinate_variable(nc_vars, primary_names, std_names=('latitude',)):
    for cand in primary_names:
        if cand in nc_vars:
            try:
                data = np.asarray(nc_vars[cand][:])
                return cand, data
            except Exception:
                continue
    for var_name, var in nc_vars.items():
        std_name = getattr(var, 'standard_name', '').lower() if hasattr(var, 'standard_name') else ''
        if std_name in std_names:
            try:
                data = np.asarray(var[:])
                return var_name, data
            except Exception:
                continue
    return None, None


def _prepare_preview_coordinate_grids(variables, height, width):
    lat_name, lat_array = _load_coordinate_variable(
        variables,
        ('lat', 'latitude', 'Lat', 'Latitude', 'LAT', 'LATITUDE'),
        ('latitude',),
    )
    lon_name, lon_array = _load_coordinate_variable(
        variables,
        ('lon', 'longitude', 'Lon', 'Longitude', 'LON', 'LONGITUDE'),
        ('longitude',),
    )
    if lat_array is None or lon_array is None:
        return None, None, lat_name, lon_name

    lat_array = np.asarray(lat_array, dtype='float32')
    lon_array = np.asarray(lon_array, dtype='float32')

    if lat_array.ndim == 1 and lon_array.ndim == 1:
        lon_grid, lat_grid = np.meshgrid(lon_array, lat_array)
        return lat_grid.astype('float32', copy=False), lon_grid.astype('float32', copy=False), lat_name, lon_name

    if lat_array.ndim == 2 and lon_array.ndim == 2:
        expected_shape = (height, width)
        if lat_array.shape != expected_shape or lon_array.shape != expected_shape:
            if lat_array.shape[::-1] == expected_shape and lon_array.shape[::-1] == expected_shape:
                lat_array = np.transpose(lat_array)
                lon_array = np.transpose(lon_array)
        if lat_array.shape == expected_shape and lon_array.shape == expected_shape:
            return lat_array, lon_array, lat_name, lon_name

    return None, None, lat_name, lon_name


def _load_vector_mask_geometries(path):
    import fiona
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape
    from shapely.ops import transform as shapely_transform
    from pyproj import Transformer

    def _transform_with_optional_point_buffer(geometry, src_crs, dst_crs, point_buffer_m=50.0):
        geom_type = str(geometry.get("type") or "")
        if geom_type in {"Point", "MultiPoint"}:
            source_geom = shape(geometry)
            to_metric = Transformer.from_crs(src_crs, "EPSG:3857", always_xy=True)
            to_target = Transformer.from_crs("EPSG:3857", dst_crs, always_xy=True)
            buffered_geom = shapely_transform(to_metric.transform, source_geom).buffer(float(point_buffer_m))
            return mapping(shapely_transform(to_target.transform, buffered_geom))
        return transform_geom(src_crs, dst_crs, geometry, precision=8)

    geometries = []
    with fiona.open(path, 'r') as src:
        src_crs = src.crs_wkt or src.crs
        if not src_crs:
            raise RuntimeError("The shapefile has no CRS information.")
        for feature in src:
            geometry = feature.get('geometry')
            if not geometry:
                continue
            transformed = _transform_with_optional_point_buffer(geometry, src_crs, "EPSG:4326")
            geometries.append(transformed)
    if not geometries:
        raise RuntimeError("The shapefile does not contain any valid geometry.")
    return geometries


def _iter_geometry_line_parts(geometry):
    geom_type = str(geometry.get("type", ""))
    coords = geometry.get("coordinates")
    if not coords:
        return
    if geom_type == "Polygon":
        for ring in coords:
            yield ring
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                yield ring
    elif geom_type == "LineString":
        yield coords
    elif geom_type == "MultiLineString":
        for line in coords:
            yield line
    elif geom_type == "GeometryCollection":
        for sub_geom in geometry.get("geometries", []):
            yield from _iter_geometry_line_parts(sub_geom)


def _load_preview_band_from_netcdf(path, sensor_name=None, prefer_rgb_preview=True):
    from netCDF4 import Dataset

    latlon_names = {'lat', 'latitude', 'lon', 'longitude'}
    sensor_name_text = str(sensor_name).strip().lower()
    is_sentinel2 = sensor_name_text == "sentinel-2"
    preferred_band_index = 1 if is_sentinel2 else 0

    def _select_sentinel_rgb_indices(wavelengths, band_count):
        if band_count < 3:
            return None
        finite_waves = []
        for index, value in enumerate(wavelengths):
            try:
                wave = float(value)
            except (TypeError, ValueError):
                continue
            if np.isfinite(wave):
                finite_waves.append((index, wave))
        targets = (665.0, 560.0, 490.0)
        if finite_waves:
            used = set()
            chosen = []
            for target in targets:
                ranked = sorted(
                    ((abs(wave - target), index) for index, wave in finite_waves if index not in used),
                    key=lambda item: item[0],
                )
                if not ranked:
                    return None
                delta, best_index = ranked[0]
                if delta > 60.0:
                    return None
                used.add(best_index)
                chosen.append(int(best_index))
            return tuple(chosen)
        if band_count >= 4:
            return (3, 2, 1)
        return None

    def _build_preview_info(preview, source_name, lat_grid, lon_grid, lat_name, lon_name,
                            preview_band_index=None, preview_mode="grayscale", preview_description=None):
        return {
            "source_name": source_name,
            "height": int(preview.shape[0]),
            "width": int(preview.shape[1]),
            "lat_grid": lat_grid,
            "lon_grid": lon_grid,
            "lat_name": lat_name,
            "lon_name": lon_name,
            "preview_band_index": preview_band_index,
            "preview_mode": preview_mode,
            "preview_description": preview_description or source_name,
        }

    with Dataset(path, 'r') as dataset:
        variables = dataset.variables
        three_d_candidates = []
        two_d_candidates = []
        for var_name, variable in variables.items():
            lowered = str(var_name).lower()
            if lowered in latlon_names:
                continue
            shape = getattr(variable, 'shape', ())
            if len(shape) == 3 and all(int(size) > 0 for size in shape):
                three_d_candidates.append((str(var_name), variable))
            elif len(shape) == 2 and _is_rrs_band_variable(var_name) and not _is_auxiliary_scene_variable(var_name):
                wave = _extract_wavelength(var_name, variable)
                sort_key = wave if wave is not None else _band_sort_key(var_name)
                two_d_candidates.append((sort_key, str(var_name), variable, wave))

        if three_d_candidates:
            for var_name, variable in three_d_candidates:
                try:
                    raw_dims = getattr(variable, 'dimensions', ())
                    spectral_axis = _identify_spectral_axis(raw_dims, variable.shape)
                    spatial_shape = [int(size) for axis_index, size in enumerate(variable.shape) if axis_index != spectral_axis]
                    preview_pixels = int(spatial_shape[0]) * int(spatial_shape[1]) if len(spatial_shape) == 2 else 0
                    selection = [slice(None)] * 3
                    band_count = int(variable.shape[spectral_axis])
                    wavelengths, wavelength_var_name = _find_matching_wavelength_vector(
                        variables,
                        var_name,
                        variable,
                        spectral_axis,
                    )
                    if (
                        prefer_rgb_preview
                        and is_sentinel2
                        and preview_pixels > 0
                        and preview_pixels <= _SENTINEL_RGB_PREVIEW_MAX_PIXELS
                    ):
                        rgb_indices = _select_sentinel_rgb_indices(wavelengths, band_count)
                        if rgb_indices is not None:
                            rgb_layers = []
                            for band_index in rgb_indices:
                                rgb_selection = [slice(None)] * 3
                                rgb_selection[spectral_axis] = int(band_index)
                                layer = np.asarray(variable[tuple(rgb_selection)], dtype='float32')
                                if layer.ndim != 2:
                                    rgb_layers = []
                                    break
                                rgb_layers.append(layer)
                            if len(rgb_layers) == 3:
                                preview = np.stack(rgb_layers, axis=-1)
                                lat_grid, lon_grid, lat_name, lon_name = _prepare_preview_coordinate_grids(
                                    variables,
                                    int(preview.shape[0]),
                                    int(preview.shape[1]),
                                )
                                return preview, _build_preview_info(
                                    preview,
                                    var_name,
                                    lat_grid,
                                    lon_grid,
                                    lat_name,
                                    lon_name,
                                    preview_band_index=int(rgb_indices[0]),
                                    preview_mode="rgb",
                                    preview_description="Sentinel-2 RGB composite (bands 4-3-2)",
                                )
                    selection[spectral_axis] = min(max(preferred_band_index, 0), max(0, band_count - 1))
                    preview = np.asarray(variable[tuple(selection)], dtype='float32')
                    if preview.ndim == 2:
                        lat_grid, lon_grid, lat_name, lon_name = _prepare_preview_coordinate_grids(
                            variables,
                            int(preview.shape[0]),
                            int(preview.shape[1]),
                        )
                        preview_description = (
                            "Sentinel-2 grayscale preview (band 2)"
                            if is_sentinel2
                            else f"Grayscale preview ({var_name})"
                        )
                        return preview, _build_preview_info(
                            preview,
                            var_name,
                            lat_grid,
                            lon_grid,
                            lat_name,
                            lon_name,
                            preview_band_index=min(max(preferred_band_index, 0), max(0, band_count - 1)),
                            preview_mode="grayscale",
                            preview_description=preview_description,
                        )
                except Exception:
                    continue

        if two_d_candidates:
            two_d_candidates.sort(key=lambda item: (item[0], item[1].lower()))
            if prefer_rgb_preview and is_sentinel2:
                preview_height = int(two_d_candidates[0][2].shape[0])
                preview_width = int(two_d_candidates[0][2].shape[1])
                preview_pixels = preview_height * preview_width
                if preview_pixels <= _SENTINEL_RGB_PREVIEW_MAX_PIXELS:
                    rgb_indices = _select_sentinel_rgb_indices(
                        [item[3] for item in two_d_candidates],
                        len(two_d_candidates),
                    )
                    if rgb_indices is not None:
                        rgb_layers = []
                        rgb_names = []
                        for band_index in rgb_indices:
                            _, var_name, variable, _wave = two_d_candidates[int(band_index)]
                            layer = np.asarray(variable[:], dtype='float32')
                            if layer.ndim != 2:
                                rgb_layers = []
                                break
                            rgb_layers.append(layer)
                            rgb_names.append(var_name)
                        if len(rgb_layers) == 3:
                            preview = np.stack(rgb_layers, axis=-1)
                            lat_grid, lon_grid, lat_name, lon_name = _prepare_preview_coordinate_grids(
                                variables,
                                int(preview.shape[0]),
                                int(preview.shape[1]),
                            )
                            return preview, _build_preview_info(
                                preview,
                                ", ".join(rgb_names),
                                lat_grid,
                                lon_grid,
                                lat_name,
                                lon_name,
                                preview_band_index=int(rgb_indices[0]),
                                preview_mode="rgb",
                                preview_description="Sentinel-2 RGB composite (bands 4-3-2)",
                            )
            candidate_index = min(max(preferred_band_index, 0), max(0, len(two_d_candidates) - 1))
            _sort_key, var_name, variable, _wave = two_d_candidates[candidate_index]
            preview = np.asarray(variable[:], dtype='float32')
            if preview.ndim == 2:
                lat_grid, lon_grid, lat_name, lon_name = _prepare_preview_coordinate_grids(
                    variables,
                    int(preview.shape[0]),
                    int(preview.shape[1]),
                )
                preview_description = (
                    "Sentinel-2 grayscale preview (band 2)"
                    if is_sentinel2
                    else f"Grayscale preview ({var_name})"
                )
                return preview, _build_preview_info(
                    preview,
                    var_name,
                    lat_grid,
                    lon_grid,
                    lat_name,
                    lon_name,
                    preview_band_index=candidate_index,
                    preview_mode="grayscale",
                    preview_description=preview_description,
                )

    raise RuntimeError("Unable to find a valid 2D or 3D reflectance layer for preview.")


def _preview_image_from_array(preview_data, max_dim=1400):
    from PIL import Image

    preview = np.asarray(preview_data, dtype='float32')
    if preview.ndim == 3 and preview.shape[-1] >= 3:
        preview = preview[..., :3]
        scaled = np.zeros(preview.shape, dtype='uint8')
        for channel_index in range(3):
            channel = np.asarray(preview[..., channel_index], dtype='float32')
            finite_mask = np.isfinite(channel)
            if np.any(finite_mask):
                valid = channel[finite_mask]
                vmin = float(np.nanpercentile(valid, 2))
                vmax = float(np.nanpercentile(valid, 98))
                if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
                    vmin = float(np.nanmin(valid))
                    vmax = float(np.nanmax(valid))
            else:
                vmin, vmax = 0.0, 1.0
            if vmax > vmin:
                clipped = np.clip(channel, vmin, vmax)
                if np.any(finite_mask):
                    channel_scaled = np.round(
                        ((clipped[finite_mask] - vmin) / (vmax - vmin)) * 255.0
                    )
                    scaled[..., channel_index][finite_mask] = channel_scaled.astype('uint8')
        image = Image.fromarray(scaled, mode='RGB')
    else:
        finite_mask = np.isfinite(preview)
        if np.any(finite_mask):
            valid = preview[finite_mask]
            vmin = float(np.nanpercentile(valid, 2))
            vmax = float(np.nanpercentile(valid, 98))
            if not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
                vmin = float(np.nanmin(valid))
                vmax = float(np.nanmax(valid))
        else:
            vmin, vmax = 0.0, 1.0
        scaled = np.zeros(preview.shape, dtype='uint8')
        if vmax > vmin:
            clipped = np.clip(preview, vmin, vmax)
            if np.any(finite_mask):
                preview_scaled = np.round(((clipped[finite_mask] - vmin) / (vmax - vmin)) * 255.0)
                scaled[finite_mask] = preview_scaled.astype('uint8')
        image = Image.fromarray(scaled, mode='L')
    width, height = image.size
    largest_dim = max(width, height)
    if largest_dim > max_dim and largest_dim > 0:
        scale = float(max_dim) / float(largest_dim)
        resized_size = (
            max(1, int(round(width * scale))),
            max(1, int(round(height * scale))),
        )
        resampling = getattr(Image, "Resampling", Image)
        image = image.resize(resized_size, resampling.BILINEAR)
    return image


def _write_preview_image_asset(preview_data, output_dir, base_name="leaflet_crop_preview", max_dim=1400):
    image = _preview_image_from_array(preview_data, max_dim=max_dim)
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    webp_path = os.path.join(output_dir, f"{base_name}.webp")
    png_path = os.path.join(output_dir, f"{base_name}.png")
    try:
        image.save(webp_path, format='WEBP', quality=78, method=6)
        return webp_path
    except Exception:
        image.save(png_path, format='PNG', optimize=True)
        return png_path


def _open_leaflet_crop_window(repo_root, request_payload):
    helper_script = os.path.join(repo_root, "app", "leaflet_crop_window.py")
    if not os.path.isfile(helper_script):
        raise RuntimeError(f"Missing Leaflet crop helper script:\n{helper_script}")

    with tempfile.TemporaryDirectory(prefix="swampy_leaflet_crop_") as temp_dir:
        request_payload = dict(request_payload)
        preview_data = request_payload.pop("preview_data", None)
        if preview_data is not None:
            preview_max_dim = int(request_payload.pop("preview_max_dim", 1400))
            request_payload["image_url"] = _write_preview_image_asset(
                preview_data,
                temp_dir,
                max_dim=max(256, preview_max_dim),
            )
        request_path = os.path.join(temp_dir, "request.json")
        response_path = os.path.join(temp_dir, "response.json")
        helper_log_path = os.path.join(temp_dir, "leaflet_crop_helper.log")
        with open(request_path, "w", encoding="utf-8") as request_file:
            json.dump(request_payload, request_file)
        with open(helper_log_path, "w", encoding="utf-8") as helper_log:
            process = subprocess.Popen(
                [sys.executable, helper_script, request_path, response_path],
                cwd=repo_root,
                stdout=helper_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            while process.poll() is None:
                try:
                    root_widget = tk._default_root
                    if root_widget is not None:
                        root_widget.update_idletasks()
                        root_widget.update()
                except Exception:
                    try:
                        process.terminate()
                    except Exception:
                        pass
                    raise
                time.sleep(0.05)
            result = process
        if result.returncode != 0:
            details = ""
            if os.path.isfile(helper_log_path):
                try:
                    with open(helper_log_path, "r", encoding="utf-8", errors="replace") as helper_log:
                        details = helper_log.read().strip()
                except Exception:
                    details = ""
            if details:
                raise RuntimeError(details)
            raise RuntimeError("Leaflet crop window exited with an error.")
        if not os.path.isfile(response_path):
            return None
        with open(response_path, "r", encoding="utf-8") as response_file:
            response = json.load(response_file)
    if response.get("cancelled", False):
        return None
    return response.get("selection")


def _infer_output_folder_from_output_file(output_file):
    if not output_file:
        return ""
    try:
        output_file = os.path.abspath(str(output_file))
        run_dir = os.path.dirname(output_file)
        run_dir_name = os.path.basename(run_dir).lower()
        if run_dir_name.startswith("swampy_run_"):
            return os.path.dirname(run_dir)
        return run_dir
    except Exception:
        return os.path.dirname(str(output_file))


def _draw_spectra_preview(canvas, spectral_library, selected_names, hovered_name=None):
    canvas.delete("all")
    width = max(canvas.winfo_width(), 620)
    height = max(canvas.winfo_height(), 300)
    left, top, right, bottom = 58, 18, 18, 42
    plot_width = width - left - right
    plot_height = height - top - bottom

    ordered_names = []
    for name in selected_names:
        if name in spectral_library["spectra"] and name not in ordered_names:
            ordered_names.append(name)
    if hovered_name and hovered_name in spectral_library["spectra"] and hovered_name not in ordered_names:
        ordered_names.append(hovered_name)

    if not ordered_names:
        canvas.create_text(
            width / 2,
            height / 2,
            text="Hover a spectrum to preview it.",
            fill="#666666",
            font=("Segoe UI", 10),
        )
        return

    series = [(name, spectral_library["spectra"][name][1]) for name in ordered_names]
    wavelengths = spectral_library["wavelengths"]
    x_min = min(wavelengths)
    x_max = max(wavelengths)
    y_min = min(min(values) for _name, values in series)
    y_max = max(max(values) for _name, values in series)
    if y_max <= y_min:
        y_max = y_min + 1.0
    padding = (y_max - y_min) * 0.05
    if padding == 0:
        padding = 0.05
    y_min -= padding
    y_max += padding

    canvas.create_rectangle(left, top, left + plot_width, top + plot_height, outline="#cccccc", width=1)

    for tick_fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + plot_height - (tick_fraction * plot_height)
        tick_value = y_min + (tick_fraction * (y_max - y_min))
        canvas.create_line(left - 5, y, left, y, fill="#444444")
        canvas.create_text(left - 8, y, text=f"{tick_value:.2f}", anchor="e", fill="#444444", font=("Segoe UI", 8))

    for tick_fraction in (0.0, 0.5, 1.0):
        x = left + (tick_fraction * plot_width)
        tick_value = x_min + (tick_fraction * (x_max - x_min))
        canvas.create_line(x, top + plot_height, x, top + plot_height + 5, fill="#444444")
        canvas.create_text(x, top + plot_height + 16, text=f"{tick_value:.0f}", anchor="n", fill="#444444", font=("Segoe UI", 8))

    canvas.create_text(left + (plot_width / 2), height - 8, text="Wavelength (nm)", fill="#444444", font=("Segoe UI", 9))
    canvas.create_text(15, top + (plot_height / 2), text="Reflectance", angle=90, fill="#444444", font=("Segoe UI", 9))

    def scale_x(value):
        if x_max == x_min:
            return left + (plot_width / 2)
        return left + ((value - x_min) / (x_max - x_min) * plot_width)

    def scale_y(value):
        return top + ((y_max - value) / (y_max - y_min) * plot_height)

    color_map = {}
    for index, name in enumerate(selected_names):
        color_map[name] = PLOT_COLORS[index % len(PLOT_COLORS)]
    if hovered_name and hovered_name not in color_map:
        color_map[hovered_name] = PLOT_COLORS[len(selected_names) % len(PLOT_COLORS)]

    legend_y = top + 10
    for name, values in series:
        points = []
        for wavelength, value in zip(wavelengths, values):
            points.extend((scale_x(wavelength), scale_y(value)))
        is_hover_only = hovered_name == name and name not in selected_names
        line_width = 3 if is_hover_only else 2
        canvas.create_line(points, fill=color_map.get(name, "#0f6cbd"), width=line_width, smooth=False)
        canvas.create_line(left + 10, legend_y, left + 34, legend_y, fill=color_map.get(name, "#0f6cbd"), width=line_width)
        suffix = " (hover)" if is_hover_only else ""
        canvas.create_text(left + 42, legend_y, text=f"{name}{suffix}", anchor="w", fill="#222222", font=("Segoe UI", 9))
        legend_y += 16


def gui():
    root = tk.Tk()
    root.title("SWAMpy | Input Configuration")

    try:
        style = ttk.Style()
        if sys.platform.startswith("win"):
            style.theme_use("vista")
        else:
            style.theme_use("clam")
        style.configure("TLabel", padding=4)
        style.configure("TEntry", padding=2)
        style.configure("TButton", padding=6)
        style.configure("TLabelframe", padding=10)
        style.configure("TLabelframe.Label", font=("Segoe UI", 10, "bold"))
    except Exception:
        pass

    cancelled = False
    compiled_siop = None
    compiled_sensor = None

    def on_close():
        nonlocal cancelled
        cancelled = True
        root.destroy()

    def _parse_geometry_size(geometry):
        if not geometry:
            return None
        match = re.match(r"^\s*(\d+)x(\d+)", str(geometry))
        if not match:
            return None
        return int(match.group(1)), int(match.group(2))

    def _get_screen_info(window):
        """
        Return (left, top, width, height) of the work area for the monitor that
        currently contains *window*.  On Windows this uses ctypes to query the
        real monitor geometry so multi-monitor setups with different resolutions
        are handled correctly.  Falls back to winfo_screen* on other platforms.
        """
        if sys.platform.startswith("win"):
            try:
                window.update_idletasks()

                class _RECT(ctypes.Structure):
                    _fields_ = [("left", ctypes.c_long), ("top", ctypes.c_long),
                                 ("right", ctypes.c_long), ("bottom", ctypes.c_long)]

                class _MONITORINFO(ctypes.Structure):
                    _fields_ = [("cbSize", ctypes.c_ulong),
                                 ("rcMonitor", _RECT),
                                 ("rcWork", _RECT),
                                 ("dwFlags", ctypes.c_ulong)]

                MONITOR_DEFAULTTONEAREST = 0x00000002
                hwnd = window.winfo_id()
                hmon = ctypes.windll.user32.MonitorFromWindow(hwnd, MONITOR_DEFAULTTONEAREST)
                mi = _MONITORINFO()
                mi.cbSize = ctypes.sizeof(_MONITORINFO)
                ctypes.windll.user32.GetMonitorInfoW(hmon, ctypes.byref(mi))
                w = mi.rcWork.right - mi.rcWork.left
                h = mi.rcWork.bottom - mi.rcWork.top
                return mi.rcWork.left, mi.rcWork.top, w, h
            except Exception:
                pass
        return 0, 0, window.winfo_screenwidth(), window.winfo_screenheight()

    def apply_window_size(window, preferred_size=None, minsize=(700, 240),
                          width_ratio=None, height_ratio=None,
                          max_width_ratio=0.96, max_height_ratio=0.92):
        _, _, screen_width, screen_height = _get_screen_info(window)
        preferred_width = preferred_size[0] if preferred_size else None
        preferred_height = preferred_size[1] if preferred_size else None

        if width_ratio is not None:
            ratio_width = int(screen_width * float(width_ratio))
            preferred_width = max(preferred_width or 0, ratio_width)
        if height_ratio is not None:
            ratio_height = int(screen_height * float(height_ratio))
            preferred_height = max(preferred_height or 0, ratio_height)

        target_width = preferred_width if preferred_width is not None else window.winfo_reqwidth()
        target_height = preferred_height if preferred_height is not None else window.winfo_reqheight()

        max_width = max(640, int(screen_width * max_width_ratio))
        max_height = max(420, int(screen_height * max_height_ratio))
        target_width = max(minsize[0], target_width)
        target_height = max(minsize[1], target_height)
        target_width = min(target_width, max_width)
        target_height = min(target_height, max_height)

        effective_min_width = min(int(minsize[0]), target_width)
        effective_min_height = min(int(minsize[1]), target_height)
        window.geometry(f"{int(target_width)}x{int(target_height)}")
        window.minsize(effective_min_width, effective_min_height)

    def center_window(window, max_width_ratio=0.96, max_height_ratio=0.92):
        window.update_idletasks()
        scr_left, scr_top, screen_width, screen_height = _get_screen_info(window)
        max_width = max(640, int(screen_width * max_width_ratio))
        max_height = max(420, int(screen_height * max_height_ratio))
        width = min(window.winfo_width(), max_width)
        height = min(window.winfo_height(), max_height)
        window.geometry(f"{width}x{height}")
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        x_pos = scr_left + int((screen_width - width) / 2)
        y_pos = scr_top + int((screen_height - height) / 3)
        window.geometry(f"{width}x{height}+{x_pos}+{y_pos}")

    apply_window_size(
        root,
        preferred_size=(1180, 820),
        minsize=(960, 700),
        width_ratio=0.9,
        height_ratio=0.88,
        max_width_ratio=0.94,
        max_height_ratio=0.9,
    )

    cwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_template_path = _resolve_bundled_resource(
        cwd,
        os.path.join(cwd, "Data", "Templates", "new_input_sub.xml"),
    )
    default_library_path = os.path.join(cwd, "Data", "spectral_library", "Spectral_Library.csv")

    try:
        template_config = siop_config.load_template_config(default_template_path)
        spectral_library = siop_config.load_spectral_library(default_library_path)
    except Exception as exc:
        messagebox.showerror("Missing defaults", f"Unable to load the default Water & Bottom Settings.\n\n{exc}")
        root.destroy()
        return None

    def load_available_sensor_templates():
        sensor_template_paths = sensor_config.default_sensor_templates(cwd)
        loaded_templates = {}
        load_errors = {}
        for sensor_name, template_path in sensor_template_paths.items():
            try:
                loaded_templates[sensor_name] = sensor_config.load_sensor_template(template_path, sensor_name)
            except Exception as exc:
                load_errors[sensor_name] = str(exc)
        return loaded_templates, load_errors

    sensor_templates, sensor_load_errors = load_available_sensor_templates()

    if not sensor_templates:
        messagebox.showerror("Missing defaults", "Unable to load any bundled sensor templates from Data/SRF.")
        root.destroy()
        return None

    now = datetime.datetime.now()
    year = str(now.year)
    month = f"{now.month:02d}"
    day = f"{now.day:02d}"
    hour = f"{now.hour:02d}"
    minute = f"{now.minute:02d}"
    second = f"{now.second:02d}"

    input_files = []
    input_image_var = StringVar(value="")
    output_folder_var = StringVar(value=os.path.join(cwd, "Output"))
    crop_selection = None
    deep_water_selection = None
    crop_summary_var = StringVar(value="Full scene")
    deep_water_summary_var = StringVar(value="No deep-water polygons selected")
    deep_water_use_sd_var = BooleanVar(value=False)
    io_change_state = {
        "modified_since_load": False,
        "suspend_tracking": False,
        "last_input_value": "",
        "last_output_value": output_folder_var.get(),
    }

    selected_target_names = []
    scalar_values = dict(template_config["scalar_fields"])
    spectrum_override_paths = {"a_water": "", "a_ph_star": ""}
    sensor_state = {
        "sensor_name": "Sentinel-2" if "Sentinel-2" in sensor_templates else next(iter(sensor_templates)),
        "selected_indices": {
            sensor_name: sensor_config.default_selected_band_indices(template)
            for sensor_name, template in sensor_templates.items()
        },
        "band_mapping_configs": {},
    }
    image_band_info_cache = {
        "path": None,
        "info": None,
        "error": "",
    }

    def select_file_im():
        files = askopenfilenames(
            parent=root,
            title="Choose one or more input images (.nc/.hdf)",
            filetypes=[
                ("NetCDF/HDF files", "*.nc *.hdf *.h5"),
                ("NetCDF files", "*.nc"),
                ("HDF files", "*.hdf *.h5"),
                ("All files", "*.*"),
            ],
        )
        nonlocal input_files
        if files:
            input_files = list(files)
            input_image_var.set(_display_input_selection(input_files))
        else:
            input_files = []
            input_image_var.set("")
        image_band_info_cache.update({"path": None, "info": None, "error": ""})
        _auto_update_sensor_mapping_for_current_image(show_warning=True)

    def select_folder():
        folder = askdirectory(parent=root, title="Choose the output folder")
        if folder:
            output_folder_var.set(folder)

    def build_current_siop():
        return siop_config.build_siop_config(
            template_config,
            spectral_library,
            selected_target_names,
            scalar_values,
            spectrum_override_paths=spectrum_override_paths,
        )

    def build_current_sensor():
        sensor_name = sensor_state["sensor_name"]
        if sensor_name not in sensor_templates:
            raise ValueError(sensor_load_errors.get(sensor_name, f"No template is available for {sensor_name}."))
        return sensor_config.build_sensor_config(
            sensor_templates[sensor_name],
            sensor_state["selected_indices"].get(sensor_name, []),
            band_mapping=sensor_state["band_mapping_configs"].get(sensor_name),
        )

    siop_summary_var = StringVar()
    sensor_summary_var = StringVar()
    sub3_saved_bounds = {"min": "0", "max": "1"}
    anomaly_search_settings = {
        "export_local_moran_raster": False,
        "export_suspicious_binary_raster": False,
        "export_interpolated_rasters": False,
    }

    def update_substrate_ui():
        try:
            current_siop = build_current_siop()
            substrate_names = current_siop["substrate_names"]
            actual_count = len(current_siop["actual_selected_targets"])
        except Exception:
            current_siop = None
            substrate_names = ["Substrate 1", "Substrate 2", siop_config.UNUSED_SUBSTRATE_NAME]
            actual_count = len(selected_target_names)

        label_sub1.configure(text=substrate_names[0])
        label_sub2.configure(text=substrate_names[1])
        label_sub3.configure(text=substrate_names[2])

        summary_names = selected_target_names
        if current_siop is not None:
            summary_names = current_siop["actual_selected_targets"]

        if actual_count <= 1:
            if sub3_min_var.get() == "0" and sub3_max_var.get() == "0":
                sub3_min_var.set(sub3_saved_bounds["min"])
                sub3_max_var.set(sub3_saved_bounds["max"])
            try:
                sub3_min_entry.state(["!disabled"])
                sub3_max_entry.state(["!disabled"])
            except Exception:
                sub3_min_entry.configure(state="normal")
                sub3_max_entry.configure(state="normal")
            if actual_count == 0:
                siop_summary_var.set("No target spectra selected. Select at least two target spectra.")
            else:
                siop_summary_var.set(f"1 target spectrum selected: {summary_names[0]}. Select at least two target spectra.")
        elif actual_count == 2:
            if not (sub3_min_var.get() == "0" and sub3_max_var.get() == "0"):
                sub3_saved_bounds["min"] = sub3_min_var.get()
                sub3_saved_bounds["max"] = sub3_max_var.get()
            sub3_min_var.set("0")
            sub3_max_var.set("0")
            try:
                sub3_min_entry.state(["disabled"])
                sub3_max_entry.state(["disabled"])
            except Exception:
                sub3_min_entry.configure(state="disabled")
                sub3_max_entry.configure(state="disabled")
            siop_summary_var.set(
                f"{actual_count} target spectra selected: {', '.join(summary_names)}. "
                "Third substrate is fixed to zero."
            )
        else:
            if sub3_min_var.get() == "0" and sub3_max_var.get() == "0":
                sub3_min_var.set(sub3_saved_bounds["min"])
                sub3_max_var.set(sub3_saved_bounds["max"])
            try:
                sub3_min_entry.state(["!disabled"])
                sub3_max_entry.state(["!disabled"])
            except Exception:
                sub3_min_entry.configure(state="normal")
                sub3_max_entry.configure(state="normal")
            siop_summary_var.set(f"{actual_count} target spectra selected: {', '.join(summary_names)}.")
        update_run_button_state()

    def update_sensor_ui():
        try:
            current_sensor = build_current_sensor()
            centers = [band["center"] for band in current_sensor["bands"]]
            mapping_config = current_sensor.get("band_mapping") or {}
            mapping_suffix = ""
            if mapping_config.get("image_band_indices"):
                mapping_mode = str(mapping_config.get("mode", "manual")).strip().lower()
                mapping_count = len(mapping_config.get("image_band_indices") or [])
                unmatched_count = max(0, len(centers) - mapping_count)
                if mapping_mode == "manual":
                    mapping_suffix = f" Explicit input-band mapping set for {mapping_count} band(s)."
                else:
                    mapping_suffix = f" Input bands auto-linked for {mapping_count} band(s)."
                if unmatched_count:
                    mapping_suffix += f" WARNING: {unmatched_count} selected band(s) are unmatched."
            elif _current_input_file_list():
                mapping_suffix = " WARNING: selected sensor bands are not linked to input image bands."
            sensor_summary_var.set(
                f"{current_sensor['sensor_name']}: {len(centers)} band(s) selected "
                f"({_format_sensor_centers(centers)}).{mapping_suffix}"
            )
        except Exception as exc:
            sensor_name = sensor_state["sensor_name"]
            sensor_summary_var.set(f"{sensor_name}: {exc}")
        update_run_button_state()

    run_button = None
    versions_button = None
    saved_run_versions = []
    active_run_versions = []
    run_button_enabled_bg = "#b9f6ca"
    run_button_disabled_bg = "#d9eadf"
    run_button_active_bg = "#8ee8a5"
    run_button_fg = "#0f3d22"

    def _set_widget_enabled(widget, enabled):
        try:
            if enabled:
                widget.state(["!disabled"])
            else:
                widget.state(["disabled"])
        except Exception:
            widget.configure(state="normal" if enabled else "disabled")

    def _current_input_file_list():
        text = input_image_var.get().strip()
        if input_files:
            return [path for path in input_files if str(path).strip()]
        return [text] if text else []

    def _get_current_image_band_info():
        current_files = _current_input_file_list()
        image_path = current_files[0] if current_files else ""
        if not image_path or not os.path.isfile(image_path):
            image_band_info_cache.update({"path": image_path, "info": None, "error": ""})
            return None, ""
        if image_band_info_cache["path"] == image_path:
            return image_band_info_cache["info"], image_band_info_cache["error"]
        try:
            info = _load_input_image_band_info(image_path)
            image_band_info_cache.update({"path": image_path, "info": info, "error": ""})
            return info, ""
        except Exception as exc:
            error = str(exc)
            image_band_info_cache.update({"path": image_path, "info": None, "error": error})
            return None, error

    def _sensor_band_labels_for_indices(template, indices, limit=8):
        band_by_index = {int(band["index"]): band for band in template.get("bands", [])} if template else {}
        labels = []
        for sensor_index in indices:
            band = band_by_index.get(int(sensor_index))
            labels.append(band["label"] if band is not None else f"Band {int(sensor_index) + 1}")
        if len(labels) > limit:
            return ", ".join(labels[:limit]) + f", and {len(labels) - limit} more"
        return ", ".join(labels)

    def _mapping_missing_sensor_indices(template, selected_indices, mapping):
        lookup = _mapping_lookup_from_payload(mapping or {})
        return [int(sensor_index) for sensor_index in selected_indices if int(sensor_index) not in lookup]

    def _auto_update_sensor_mapping_for_current_image(show_warning=False):
        image_band_info, image_band_error = _get_current_image_band_info()
        sensor_name = sensor_state["sensor_name"]
        template = sensor_templates.get(sensor_name)
        selected_indices = list(sensor_state["selected_indices"].get(sensor_name, []))

        if image_band_error:
            sensor_state["band_mapping_configs"].pop(sensor_name, None)
            update_sensor_ui()
            if show_warning:
                messagebox.showwarning(
                    "Band matching unavailable",
                    "The selected input image could not be inspected for sensor-band matching.\n\n"
                    f"{image_band_error}",
                    parent=root,
                )
            return

        if template is None or not image_band_info or not selected_indices:
            sensor_state["band_mapping_configs"].pop(sensor_name, None)
            update_sensor_ui()
            return

        lookup, unmatched = _auto_match_sensor_band_lookup(
            template,
            selected_indices,
            image_band_info,
            tolerance_nm=10.0,
        )
        mapping = _build_sensor_band_mapping_payload(
            template,
            selected_indices,
            image_band_info,
            lookup,
            "auto",
            10.0,
        )
        if mapping:
            sensor_state["band_mapping_configs"][sensor_name] = mapping
        else:
            sensor_state["band_mapping_configs"].pop(sensor_name, None)
        update_sensor_ui()

        if show_warning and unmatched:
            messagebox.showwarning(
                "Incomplete band matching",
                "Some selected sensor bands do not have a matching input image band within 10 nm.\n\n"
                f"Unmatched: {_sensor_band_labels_for_indices(template, unmatched)}\n\n"
                "Open Sensor Configuration to change the selected bands or adjust the mapping manually.",
                parent=root,
            )

    def _normalise_crop_selection(selection):
        if not selection:
            return None
        normalized = dict(selection)
        bbox = normalized.get("bbox")
        if bbox:
            try:
                min_lon = float(min(bbox["min_lon"], bbox["max_lon"]))
                max_lon = float(max(bbox["min_lon"], bbox["max_lon"]))
                min_lat = float(min(bbox["min_lat"], bbox["max_lat"]))
                max_lat = float(max(bbox["min_lat"], bbox["max_lat"]))
            except Exception:
                bbox = None
            else:
                if max_lon > min_lon and max_lat > min_lat:
                    bbox = {
                        "min_lon": min_lon,
                        "max_lon": max_lon,
                        "min_lat": min_lat,
                        "max_lat": max_lat,
                    }
                else:
                    bbox = None
        else:
            bbox = None
        mask_path = str(normalized.get("mask_path") or "").strip()
        normalized["bbox"] = bbox
        normalized["mask_path"] = mask_path
        if not bbox and not mask_path:
            return None
        return normalized

    def _normalise_deep_water_selection(selection):
        if not selection:
            return None
        normalized = dict(selection)
        polygons = normalized.get("polygons") or []
        valid_polygons = []
        for geometry in polygons:
            if not isinstance(geometry, dict):
                continue
            geom_type = str(geometry.get("type") or "")
            coordinates = geometry.get("coordinates")
            if geom_type not in {"Polygon", "MultiPolygon"} or not coordinates:
                continue
            valid_polygons.append(geometry)
        if not valid_polygons:
            return None
        normalized["polygons"] = valid_polygons
        normalized["mask_path"] = ""
        normalized["bbox"] = None
        return normalized

    def _format_crop_summary(selection):
        if not selection:
            return "Full scene"
        parts = []
        bbox = selection.get("bbox")
        if bbox:
            parts.append(
                "BBox "
                f"lon {bbox['min_lon']:.5f} to {bbox['max_lon']:.5f}, "
                f"lat {bbox['min_lat']:.5f} to {bbox['max_lat']:.5f}"
            )
        mask_path = str(selection.get("mask_path") or "").strip()
        if mask_path:
            parts.append(f"Mask {os.path.basename(mask_path)}")
        return " | ".join(parts) if parts else "Full scene"

    def _format_deep_water_summary(selection):
        if not selection:
            return "No deep-water polygons selected"
        polygon_count = len(selection.get("polygons") or [])
        if polygon_count <= 0:
            return "No deep-water polygons selected"
        mode_text = "mean ± sd bounds" if deep_water_use_sd_var.get() else "fixed values"
        return (
            f"{polygon_count} deep-water polygon(s) selected ({mode_text}). "
            "CHL, CDOM and NAP parameter bounds below are disabled and inferred from those pixels."
        )

    def _set_crop_selection(selection):
        nonlocal crop_selection
        crop_selection = _normalise_crop_selection(selection)
        crop_summary_var.set(_format_crop_summary(crop_selection))
        update_crop_button_state()

    def _set_deep_water_selection(selection):
        nonlocal deep_water_selection
        deep_water_selection = _normalise_deep_water_selection(selection)
        deep_water_summary_var.set(_format_deep_water_summary(deep_water_selection))
        _update_deep_water_parameter_controls()

    def clear_crop_selection():
        _set_crop_selection(None)

    def clear_deep_water_selection():
        _set_deep_water_selection(None)

    crop_button = None

    def update_crop_button_state(*_args):
        if crop_button is None:
            return
        _set_widget_enabled(crop_button, bool(_current_input_file_list()))

    def _refresh_deep_water_summary(*_args):
        deep_water_summary_var.set(_format_deep_water_summary(deep_water_selection))
        _update_deep_water_parameter_controls()

    def _run_with_loading_dialog(title_text, message_text, worker_func):
        result_holder = {"done": False}

        loading_popup = tk.Toplevel(root)
        loading_popup.title(title_text)
        apply_window_size(
            loading_popup,
            preferred_size=(420, 150),
            minsize=(380, 140),
            width_ratio=0.32,
            height_ratio=0.18,
            max_width_ratio=0.38,
            max_height_ratio=0.24,
        )
        loading_popup.transient(root)
        loading_popup.grab_set()
        loading_popup.resizable(False, False)
        loading_popup.protocol("WM_DELETE_WINDOW", lambda: None)
        loading_popup.columnconfigure(0, weight=1)
        loading_popup.rowconfigure(0, weight=1)

        container = ttk.Frame(loading_popup, padding=14)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)

        ttk.Label(
            container,
            text=message_text,
            wraplength=340,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        progress = ttk.Progressbar(container, mode="indeterminate", length=320)
        progress.grid(row=1, column=0, sticky="ew", pady=(14, 0))
        progress.start(10)

        def _worker():
            try:
                result_holder["result"] = worker_func()
            except Exception as exc:
                result_holder["error"] = exc
            finally:
                result_holder["done"] = True

        def _poll():
            if result_holder.get("done"):
                try:
                    progress.stop()
                finally:
                    if loading_popup.winfo_exists():
                        loading_popup.destroy()
                return
            loading_popup.after(75, _poll)

        threading.Thread(target=_worker, daemon=True).start()
        center_window(loading_popup, max_width_ratio=0.5, max_height_ratio=0.3)
        loading_popup.after(75, _poll)
        root.wait_window(loading_popup)

        if "error" in result_holder:
            raise result_holder["error"]
        return result_holder.get("result")

    def _sync_input_files_with_entry(*_args):
        nonlocal input_files
        if input_files and input_image_var.get() != _display_input_selection(input_files):
            input_files = []
            sensor_state["band_mapping_configs"].clear()
            update_sensor_ui()
        update_crop_button_state()
        update_run_button_state()

    def _track_input_field_change(*_args):
        current_value = input_image_var.get()
        if current_value == io_change_state["last_input_value"]:
            return
        io_change_state["last_input_value"] = current_value
        if not io_change_state["suspend_tracking"]:
            io_change_state["modified_since_load"] = True
            image_band_info_cache.update({"path": None, "info": None, "error": ""})
            if sensor_state["band_mapping_configs"]:
                sensor_state["band_mapping_configs"].clear()
                update_sensor_ui()

    def _track_output_field_change(*_args):
        current_value = output_folder_var.get()
        if current_value == io_change_state["last_output_value"]:
            return
        io_change_state["last_output_value"] = current_value
        if not io_change_state["suspend_tracking"]:
            io_change_state["modified_since_load"] = True

    def _prepare_crop_window_request(first_image):
        preview_data, preview_info = _load_preview_band_from_netcdf(first_image, sensor_state["sensor_name"])

        lat_grid = preview_info.get("lat_grid")
        lon_grid = preview_info.get("lon_grid")
        if lat_grid is None or lon_grid is None:
            raise RuntimeError("The crop tool requires latitude and longitude coordinates in the input image.")

        finite_coord_mask = np.isfinite(lat_grid) & np.isfinite(lon_grid)
        if not np.any(finite_coord_mask):
            raise RuntimeError("The crop tool requires valid latitude and longitude coordinates.")

        lon_min = float(np.nanmin(lon_grid[finite_coord_mask]))
        lon_max = float(np.nanmax(lon_grid[finite_coord_mask]))
        lat_min = float(np.nanmin(lat_grid[finite_coord_mask]))
        lat_max = float(np.nanmax(lat_grid[finite_coord_mask]))

        existing_selection = _normalise_crop_selection(crop_selection) or {"bbox": None, "mask_path": ""}
        existing_source_path = str(existing_selection.get("source_path") or "").strip()
        if existing_source_path and not _paths_equivalent(existing_source_path, first_image):
            existing_selection = {"bbox": None, "mask_path": ""}

        existing_mask_geometries = []
        existing_mask_path = str(existing_selection.get("mask_path") or "").strip()
        if existing_mask_path:
            try:
                existing_mask_geometries = _load_vector_mask_geometries(existing_mask_path)
            except Exception:
                existing_selection["mask_path"] = ""

        return {
            "title": "Crop area",
            "image_name": os.path.basename(first_image),
            "source_name": preview_info["source_name"],
            "sensor_name": sensor_state["sensor_name"],
            "preview_description": preview_info.get("preview_description", preview_info["source_name"]),
            "preview_data": preview_data,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "lat_min": lat_min,
            "lat_max": lat_max,
            "selection": {
                "bbox": existing_selection.get("bbox"),
                "mask_path": existing_selection.get("mask_path", ""),
                "mask_geometries": existing_mask_geometries,
            },
        }

    def _prepare_deep_water_window_request(first_image):
        preview_data, preview_info = _load_preview_band_from_netcdf(
            first_image,
            sensor_state["sensor_name"],
            prefer_rgb_preview=True,
        )

        lat_grid = preview_info.get("lat_grid")
        lon_grid = preview_info.get("lon_grid")
        if lat_grid is None or lon_grid is None:
            raise RuntimeError("The deep-water selector requires latitude and longitude coordinates in the input image.")

        finite_coord_mask = np.isfinite(lat_grid) & np.isfinite(lon_grid)
        if not np.any(finite_coord_mask):
            raise RuntimeError("The deep-water selector requires valid latitude and longitude coordinates.")

        lon_min = float(np.nanmin(lon_grid[finite_coord_mask]))
        lon_max = float(np.nanmax(lon_grid[finite_coord_mask]))
        lat_min = float(np.nanmin(lat_grid[finite_coord_mask]))
        lat_max = float(np.nanmax(lat_grid[finite_coord_mask]))

        existing_selection = _normalise_deep_water_selection(deep_water_selection) or {"polygons": []}
        existing_source_path = str(existing_selection.get("source_path") or "").strip()
        if existing_source_path and not _paths_equivalent(existing_source_path, first_image):
            existing_selection = {"polygons": []}

        return {
            "mode": "polygons",
            "title": "Deep-water polygons",
            "subtitle": (
                f"{preview_info.get('preview_description', preview_info['source_name'])}. "
                "Draw one or several polygons over optically deep water. These pixels will be used to estimate CHL, CDOM and NAP."
            ),
            "image_name": os.path.basename(first_image),
            "source_name": preview_info["source_name"],
            "sensor_name": sensor_state["sensor_name"],
            "preview_description": preview_info.get("preview_description", preview_info["source_name"]),
            "preview_data": preview_data,
            "preview_max_dim": 1000,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "lat_min": lat_min,
            "lat_max": lat_max,
            "allow_mask_import": False,
            "allow_rectangle": False,
            "allow_polygon": True,
            "selection": {
                "polygons": list(existing_selection.get("polygons") or []),
            },
        }

    def open_crop_popup():
        current_files = _current_input_file_list()
        if not current_files:
            messagebox.showinfo("No input image", "Select at least one input image before defining a crop area.", parent=root)
            return

        first_image = current_files[0]
        if not os.path.isfile(first_image):
            messagebox.showerror("Missing input image", f"Input image not found:\n{first_image}", parent=root)
            return

        try:
            request_payload = _run_with_loading_dialog(
                "Loading crop tool",
                "Preparing the preview and crop interface...",
                lambda: _prepare_crop_window_request(first_image),
            )
            selection = _open_leaflet_crop_window(cwd, request_payload)
        except Exception as exc:
            messagebox.showerror(
                "Leaflet crop unavailable",
                f"Unable to open the Leaflet crop window.\n\n{exc}",
                parent=root,
            )
            return

        if selection is None:
            return

        selection["source_path"] = first_image
        _set_crop_selection(selection)

    def open_deep_water_popup():
        current_files = _current_input_file_list()
        if not current_files:
            messagebox.showinfo("No input image", "Select at least one input image before defining deep-water polygons.", parent=root)
            return

        first_image = current_files[0]
        if not os.path.isfile(first_image):
            messagebox.showerror("Missing input image", f"Input image not found:\n{first_image}", parent=root)
            return

        try:
            request_payload = _run_with_loading_dialog(
                "Loading deep-water selector",
                "Preparing the preview and polygon interface...",
                lambda: _prepare_deep_water_window_request(first_image),
            )
            selection = _open_leaflet_crop_window(cwd, request_payload)
        except Exception as exc:
            messagebox.showerror(
                "Deep-water selector unavailable",
                f"Unable to open the deep-water selection window.\n\n{exc}",
                parent=root,
            )
            return

        if selection is None:
            return

        selection["source_path"] = first_image
        _set_deep_water_selection(selection)

    def _get_sensor_mapping_validation_error():
        image_band_info, image_band_error = _get_current_image_band_info()
        if image_band_error:
            return f"Unable to inspect input image bands for sensor matching: {image_band_error}"
        if not image_band_info:
            return None

        sensor_name = sensor_state["sensor_name"]
        template = sensor_templates.get(sensor_name)
        if template is None:
            return None

        selected_indices = list(sensor_state["selected_indices"].get(sensor_name, []))
        mapping = sensor_state["band_mapping_configs"].get(sensor_name)
        if mapping and not _image_band_info_matches_mapping(image_band_info, mapping):
            return "Sensor band mapping was created for a different input image. Re-open Sensor Configuration or reselect the image."

        missing_indices = _mapping_missing_sensor_indices(template, selected_indices, mapping)
        if missing_indices:
            return (
                "Some selected sensor bands are not matched to input image bands: "
                f"{_sensor_band_labels_for_indices(template, missing_indices)}."
            )

        image_band_indices = list((mapping or {}).get("image_band_indices") or [])
        if len(set(image_band_indices)) != len(image_band_indices):
            return "Sensor band mapping uses the same input image band more than once."
        return None

    def _get_form_validation_error():
        current_files = _current_input_file_list()
        if not current_files:
            return "Please select at least one input image (.nc/.hdf)."
        for path in current_files:
            if not os.path.isfile(path):
                return f"Input image not found: {path}"

        if not output_folder_var.get().strip():
            return "Please choose an output folder."

        if crop_selection:
            mask_path = str(crop_selection.get("mask_path") or "").strip()
            if mask_path and not os.path.isfile(mask_path):
                return f"Shapefile mask not found: {mask_path}"

        if bathy_mode.get() == "input":
            selected_bathy = bathy_path_var.get().strip() or _resolve_bundled_resource(
                cwd,
                os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"),
            )
            if not selected_bathy or not os.path.exists(selected_bathy):
                return "Please choose a valid bathymetry file."
            try:
                float(bathy_correction.get())
                float(bathy_tolerance.get())
            except ValueError:
                return "Bathymetry correction and tolerance must be numeric."

        try:
            build_current_siop()
        except Exception as exc:
            return f"Invalid Water & Bottom Settings: {exc}"

        try:
            build_current_sensor()
        except Exception as exc:
            return f"Invalid sensor setup: {exc}"

        sensor_mapping_error = _get_sensor_mapping_validation_error()
        if sensor_mapping_error:
            return sensor_mapping_error

        numeric_pairs = [
            (chl_min_var, chl_max_var, "CHL"),
            (cdom_min_var, cdom_max_var, "CDOM"),
            (nap_min_var, nap_max_var, "NAP"),
            (depth_min_var, depth_max_var, "Depth"),
            (sub1_min_var, sub1_max_var, label_sub1.cget("text")),
            (sub2_min_var, sub2_max_var, label_sub2.cget("text")),
            (sub3_min_var, sub3_max_var, label_sub3.cget("text")),
        ]
        for vmin, vmax, name in numeric_pairs:
            try:
                vmin_f = float(vmin.get())
                vmax_f = float(vmax.get())
            except ValueError:
                return f"{name} bounds must be numeric."
            if vmax_f < vmin_f:
                return f"{name} max must be greater than or equal to min."

        if allow_split.get():
            chunk_value = chunk_rows.get().strip()
            if chunk_value:
                try:
                    rows_int = int(float(chunk_value))
                    if rows_int <= 0:
                        raise ValueError
                except ValueError:
                    return "Rows per chunk must be a positive number."

        return None

    def update_run_button_state(*_args):
        if run_button is None:
            return
        enabled = _get_form_validation_error() is None
        _set_widget_enabled(run_button, enabled)
        try:
            run_button.configure(
                bg=run_button_enabled_bg if enabled else run_button_disabled_bg,
                activebackground=run_button_active_bg if enabled else run_button_disabled_bg,
                fg=run_button_fg if enabled else "#6f7f73",
                activeforeground=run_button_fg,
                cursor="hand2" if enabled else "arrow",
            )
        except Exception:
            pass

    def open_feature_popup(title, description, settings_builder=None, apply_callback=None,
                           geometry="760x300", minsize=(700, 240), max_height_ratio=0.6):
        popup = tk.Toplevel(root)
        popup.title(title)
        preferred_size = _parse_geometry_size(geometry)
        apply_window_size(
            popup,
            preferred_size=preferred_size,
            minsize=minsize,
            width_ratio=0.62,
            height_ratio=0.34,
            max_width_ratio=0.78,
            max_height_ratio=max_height_ratio,
        )
        popup.transient(root)
        popup.grab_set()
        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)

        container = ttk.Frame(popup, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)

        ttk.Label(
            container,
            text=description,
            wraplength=max(popup.winfo_width() - 60, 620),
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        if settings_builder is not None:
            settings_frame = ttk.Labelframe(container, text="Settings")
            settings_frame.grid(row=1, column=0, sticky="ew")
            settings_frame.columnconfigure(0, weight=1)
            settings_frame.columnconfigure(1, weight=1)
            settings_builder(settings_frame)

        actions = ttk.Frame(container)
        actions.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        actions.columnconfigure(0, weight=1)
        if apply_callback is None:
            ttk.Button(actions, text="Close", command=popup.destroy).grid(row=0, column=1, sticky="e")
        else:
            ttk.Button(actions, text="Cancel", command=popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))

            def apply_and_close():
                try:
                    result = apply_callback()
                except Exception:
                    return
                if result is False:
                    return
                popup.destroy()

            ttk.Button(actions, text="Apply", command=apply_and_close).grid(row=0, column=2, sticky="e", padx=(8, 0))

        center_window(popup)
        popup.wait_window()

    def open_above_rrs_popup():
        open_feature_popup(
            "Above-water reflectance",
            "Use this when the input reflectance is above-water remote-sensing reflectance (Rrs). "
            "When enabled, the workflow converts the input to below-surface reflectance before the inversion. "
            "Leave it disabled if the image already contains below-surface reflectance.",
            geometry="720x220",
            minsize=(660, 200),
        )

    def open_reflectance_input_popup():
        open_feature_popup(
            "Reflectance input (÷π)",
            "Enable this when the input image contains hemispherical reflectance (ρ) rather than "
            "remote-sensing reflectance (Rrs). "
            "The two quantities are related by  Rrs = ρ / π, so when this option is on the workflow "
            "divides every band by π before any further processing. "
            "Leave it disabled if the image already contains Rrs values.",
            geometry="760x240",
            minsize=(700, 220),
        )

    def open_shallow_water_popup():
        open_feature_popup(
            "Shallow water adjustment",
            "This option applies a second depth adjustment after the main optimisation. "
            "For pixels that still behave as optically deep, the workflow reduces the fitted depth toward the shallowest value that keeps the bottom effectively undetectable. "
            "Use it when you want a shallow-water-oriented depth product. Disable it if you prefer to keep the raw fitted depth.",
            geometry="760x240",
            minsize=(700, 220),
        )

    def open_anomaly_search_popup():
        export_local_moran_var = BooleanVar(value=bool(anomaly_search_settings["export_local_moran_raster"]))
        export_suspicious_binary_var = BooleanVar(value=bool(anomaly_search_settings["export_suspicious_binary_raster"]))
        export_interpolated_var = BooleanVar(value=bool(anomaly_search_settings["export_interpolated_rasters"]))

        def build_settings(settings_frame):
            settings_frame.columnconfigure(0, weight=1)
            ttk.Checkbutton(
                settings_frame,
                text="Export edge / plateau debug rasters",
                variable=export_local_moran_var,
            ).grid(row=0, column=0, sticky="w", pady=2)
            ttk.Checkbutton(
                settings_frame,
                text="Export suspicious/not-suspicious raster",
                variable=export_suspicious_binary_var,
            ).grid(row=1, column=0, sticky="w", pady=2)
            ttk.Checkbutton(
                settings_frame,
                text="Export interpolated depth / CHL / CDOM / NAP rasters",
                variable=export_interpolated_var,
            ).grid(row=2, column=0, sticky="w", pady=2)

        def apply_anomaly_search_changes():
            anomaly_search_settings["export_local_moran_raster"] = bool(export_local_moran_var.get())
            anomaly_search_settings["export_suspicious_binary_raster"] = bool(export_suspicious_binary_var.get())
            anomaly_search_settings["export_interpolated_rasters"] = bool(export_interpolated_var.get())
            return True

        open_feature_popup(
            "False-deep bathymetry correction",
            "This stage detects suspicious false-deep areas as deeper / lower-SDI plateaus that begin at sharp boundaries.\n\n"
            "It looks for places where depth increases suddenly and SDI drops suddenly, then grows inward over connected pixels that remain deep and low in SDI. Suspicious pixels are then corrected by linearly interpolating depth, CHL, CDOM, and NAP from non-suspicious pixels and re-optimising only the substrate fractions.",
            settings_builder=build_settings,
            apply_callback=apply_anomaly_search_changes,
            geometry="760x260",
            minsize=(700, 220),
            max_height_ratio=0.4,
        )

    def open_initial_guess_popup():
        feature_enabled = bool(optimize_initial_guesses_flag.get())
        five_var = BooleanVar(value=bool(five_initial_guess_testing_flag.get()))
        debug_var = BooleanVar(value=bool(initial_guess_debug_flag.get()))

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "These settings are used only when 'Optimise initial guesses' is enabled on the main page."
                    if feature_enabled else
                    "Enable 'Optimise initial guesses' on the main page to use the options below."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            five_check = ttk.Checkbutton(
                settings_frame,
                text="Use 5 initial guess testing",
                variable=five_var,
            )
            five_check.grid(row=1, column=0, sticky="w", pady=(0, 4))
            debug_check = ttk.Checkbutton(
                settings_frame,
                text="Export initial guess debug GeoTIFF",
                variable=debug_var,
            )
            debug_check.grid(row=2, column=0, sticky="w")
            _set_widget_enabled(five_check, feature_enabled)
            _set_widget_enabled(debug_check, feature_enabled)

        def apply_initial_guess_changes():
            if not optimize_initial_guesses_flag.get():
                five_initial_guess_testing_flag.set(False)
                initial_guess_debug_flag.set(False)
                return
            five_initial_guess_testing_flag.set(bool(five_var.get()))
            initial_guess_debug_flag.set(bool(debug_var.get()))

        open_feature_popup(
            "Initial guess optimisation",
            "Before the main optimisation, the workflow can test several starting points for each pixel and keep the combination that gives the lowest forward-model error. "
            "Standard mode tests exactly 3 values per variable: 25%, mean, and 75% of the user bounds. "
            "If 'Use 5 initial guess testing' is enabled, it tests exactly 5 values: min, 25%, mean, 75%, and max.",
            settings_builder=build_settings,
            apply_callback=apply_initial_guess_changes,
            geometry="780x320",
            minsize=(720, 260),
        )

    def open_relaxed_constraints_popup():
        feature_enabled = bool(relaxed.get())
        fully_relaxed_var = BooleanVar(value=bool(fully_relaxed_flag.get()))

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "Standard relaxed mode keeps the internal substrate-cover sum between 0.5 and 2.0, "
                    "but the exported target-cover maps are standardized back between 0 and 1 for the user.\n\n"
                    "If only two target spectra are active, SWAMpy reduces the problem to one cover variable "
                    "(x for target 1 and 1-x for target 2) to speed up the optimisation.\n\n"
                    "Fully relaxed mode removes the cross-target cover constraint completely and exports the raw fitted target values."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            fully_relaxed_check = ttk.Checkbutton(
                settings_frame,
                text="Use fully relaxed substrate mode",
                variable=fully_relaxed_var,
            )
            fully_relaxed_check.grid(row=1, column=0, sticky="w")
            _set_widget_enabled(fully_relaxed_check, feature_enabled)

        def apply_relaxed_changes():
            if not relaxed.get():
                fully_relaxed_flag.set(False)
                return
            fully_relaxed_flag.set(bool(fully_relaxed_var.get()))

        open_feature_popup(
            "Relaxed substrate constraints",
            "When enabled, the workflow stops enforcing a strict convex substrate mixture and uses a relaxed treatment instead.",
            settings_builder=build_settings,
            apply_callback=apply_relaxed_changes,
            geometry="800x340",
            minsize=(740, 280),
        )

    def open_post_processing_popup():
        open_feature_popup(
            "Output spectral parameters",
            "This computes and exports extra spectral products after the main inversion, including modeled reflectance, deep-water reflectance, spectral kd, substrate reflectance, absorption, and backscattering. "
            "It is useful for diagnostics, but it adds runtime and creates additional output files.",
            geometry="780x230",
            minsize=(720, 210),
        )

    def open_modeled_reflectance_popup():
        open_feature_popup(
            "Output modeled reflectance",
            "This exports the final fitted modeled reflectance in the selected sensor bands as a standalone multiband product. "
            "When enabled, SWAMpy writes both a GeoTIFF and a NetCDF file with georeferencing and wavelength metadata so the result can be opened easily in SNAP, ENVI, or GIS software. "
            "The product uses the final inversion result and the same sensor-band selection as the run. "
            "If 'Above RRS' is enabled, the export is converted to above-water reflectance so it matches the input convention.",
            geometry="800x250",
            minsize=(740, 220),
        )

    def open_split_popup():
        chunk_var = StringVar(value=chunk_rows.get())

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "Image splitting processes the scene in row chunks to reduce peak memory usage. "
                    "Leave rows per chunk blank to let the workflow choose automatically."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            ttk.Label(settings_frame, text="Rows per chunk").grid(row=1, column=0, sticky="w")
            ttk.Entry(settings_frame, textvariable=chunk_var, width=12).grid(row=1, column=1, sticky="w")

        def apply_split_changes():
            value = chunk_var.get().strip()
            if value:
                try:
                    rows_int = int(float(value))
                    if rows_int <= 0:
                        raise ValueError
                except ValueError:
                    messagebox.showerror("Invalid value", "Rows per chunk must be a positive number.", parent=root)
                    raise
            chunk_rows.set(value)

        def apply_split_changes_and_handle():
            try:
                apply_split_changes()
            except Exception:
                raise

        open_feature_popup(
            "Image splitting",
            "Enable this for large scenes or low-memory machines. "
            "The workflow will process the image chunk by chunk instead of loading the whole scene at once. "
            "Smaller chunks reduce memory use but add overhead.",
            settings_builder=build_settings,
            apply_callback=apply_split_changes_and_handle,
            geometry="760x280",
            minsize=(700, 240),
        )

    def open_sensor_popup():
        nonlocal sensor_templates, sensor_load_errors

        popup = tk.Toplevel(root)
        popup.title("Sensor Configuration")
        apply_window_size(
            popup,
            preferred_size=(1120, 860),
            minsize=(920, 700),
            width_ratio=0.9,
            height_ratio=0.88,
            max_width_ratio=0.94,
            max_height_ratio=0.92,
        )
        popup.transient(root)
        popup.grab_set()

        local_sensor_name = tk.StringVar(value=sensor_state["sensor_name"])
        local_selected_indices = {
            sensor_name: list(indices)
            for sensor_name, indices in sensor_state["selected_indices"].items()
        }
        local_mapping_state = {
            sensor_name: _clone_sensor_band_mapping_config(mapping)
            for sensor_name, mapping in sensor_state["band_mapping_configs"].items()
            if mapping
        }
        current_input_files = _current_input_file_list()
        current_image_path = current_input_files[0] if current_input_files else ""
        current_image_band_info = None
        current_image_band_error = ""
        if current_image_path and os.path.isfile(current_image_path):
            try:
                current_image_band_info = _load_input_image_band_info(current_image_path)
            except Exception as exc:
                current_image_band_error = str(exc)

        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)

        popup_container = ttk.Frame(popup, padding=12)
        popup_container.grid(row=0, column=0, sticky="nsew")
        popup_container.columnconfigure(0, weight=0)
        popup_container.columnconfigure(1, weight=1)
        popup_container.rowconfigure(0, weight=1)
        popup_container.rowconfigure(1, weight=0)
        popup_container.rowconfigure(2, weight=0)

        sensor_frame = ttk.Labelframe(popup_container, text="Sensor")
        sensor_frame.grid(row=0, column=0, sticky="nsw", padx=(0, 10), pady=(0, 8))
        sensor_frame.columnconfigure(0, weight=1)

        sensor_choice_frame = ttk.Frame(sensor_frame)
        sensor_choice_frame.grid(row=0, column=0, sticky="nsew")
        sensor_choice_frame.columnconfigure(0, weight=1)

        sensor_actions = ttk.Frame(sensor_frame)
        sensor_actions.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        sensor_actions.columnconfigure(2, weight=1)

        sensor_error_var = StringVar()
        ttk.Label(sensor_frame, textvariable=sensor_error_var, wraplength=240, justify="left").grid(
            row=2, column=0, sticky="w", pady=(6, 0)
        )

        selection_frame = ttk.Labelframe(popup_container, text="Bands")
        selection_frame.grid(row=0, column=1, sticky="nsew", pady=(0, 8))
        selection_frame.columnconfigure(0, weight=1)
        selection_frame.rowconfigure(1, weight=1)
        selection_frame.rowconfigure(2, weight=0)

        selection_status_var = StringVar()
        ttk.Label(selection_frame, textvariable=selection_status_var, wraplength=620, justify="left").grid(
            row=0, column=0, sticky="w", padx=4, pady=(0, 4)
        )

        bands_listbox = tk.Listbox(
            selection_frame,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            height=24,
            width=24,
        )
        bands_listbox.grid(row=1, column=0, sticky="nsew", padx=(4, 0), pady=(0, 8))
        bands_scroll = ttk.Scrollbar(selection_frame, orient="vertical", command=bands_listbox.yview)
        bands_scroll.grid(row=1, column=1, sticky="ns", pady=(0, 8))
        bands_listbox.configure(yscrollcommand=bands_scroll.set)

        quick_frame = ttk.Frame(selection_frame)
        quick_frame.grid(row=2, column=0, columnspan=2, sticky="ew", padx=4)
        for col_index in range(5):
            quick_frame.columnconfigure(col_index, weight=0)
        quick_frame.columnconfigure(5, weight=1)

        range_min_var = StringVar(value="400")
        range_max_var = StringVar(value="700")
        range_step_var = StringVar(value="1")

        smart_frame = ttk.Labelframe(selection_frame, text="Smart Selection")
        smart_frame.grid(row=3, column=0, columnspan=2, sticky="ew", padx=4, pady=(8, 0))
        for col_index in range(7):
            smart_frame.columnconfigure(col_index, weight=0)

        ttk.Label(smart_frame, text="Min nm").grid(row=0, column=0, sticky="w")
        ttk.Entry(smart_frame, textvariable=range_min_var, width=8).grid(row=0, column=1, sticky="w", padx=(4, 10))
        ttk.Label(smart_frame, text="Max nm").grid(row=0, column=2, sticky="w")
        ttk.Entry(smart_frame, textvariable=range_max_var, width=8).grid(row=0, column=3, sticky="w", padx=(4, 10))
        ttk.Label(smart_frame, text="Every nth band").grid(row=0, column=4, sticky="w")
        ttk.Entry(smart_frame, textvariable=range_step_var, width=6).grid(row=0, column=5, sticky="w", padx=(4, 10))

        mapping_frame = ttk.Labelframe(popup_container, text="Input Image Band Mapping")
        mapping_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(0, 8))
        mapping_frame.columnconfigure(0, weight=1)
        mapping_frame.rowconfigure(2, weight=1)

        mapping_status_var = StringVar()
        ttk.Label(mapping_frame, textvariable=mapping_status_var, wraplength=980, justify="left").grid(
            row=0, column=0, sticky="w", padx=4, pady=(0, 6)
        )

        mapping_controls = ttk.Frame(mapping_frame)
        mapping_controls.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 6))
        mapping_controls.columnconfigure(5, weight=1)

        mapping_tolerance_var = StringVar(value="10")
        ttk.Label(mapping_controls, text="Tolerance (nm)").grid(row=0, column=0, sticky="w")
        mapping_tolerance_entry = ttk.Entry(mapping_controls, textvariable=mapping_tolerance_var, width=8)
        mapping_tolerance_entry.grid(row=0, column=1, sticky="w", padx=(6, 10))
        auto_match_button = ttk.Button(mapping_controls, text="Auto-match")
        auto_match_button.grid(row=0, column=2, sticky="w")

        mapping_detail_frame = ttk.Frame(mapping_frame)
        mapping_detail_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))
        mapping_detail_frame.columnconfigure(0, weight=1)
        mapping_detail_frame.rowconfigure(0, weight=1)

        manual_mapping_frame = ttk.Frame(mapping_detail_frame)
        manual_mapping_frame.columnconfigure(1, weight=1)

        mapping_tree = ttk.Treeview(
            mapping_detail_frame,
            columns=("sensor_band", "image_band", "delta_nm"),
            show="headings",
            height=10,
        )
        mapping_tree.heading("sensor_band", text="Sensor band")
        mapping_tree.heading("image_band", text="Input image band")
        mapping_tree.heading("delta_nm", text="Delta (nm)")
        mapping_tree.column("sensor_band", width=150, anchor="w")
        mapping_tree.column("image_band", width=420, anchor="w")
        mapping_tree.column("delta_nm", width=90, anchor="center")

        def _ensure_local_selection_defaults():
            for sensor_name, template in sensor_templates.items():
                local_selected_indices.setdefault(
                    sensor_name,
                    sensor_config.default_selected_band_indices(template),
                )
            for sensor_name in list(local_selected_indices.keys()):
                if sensor_name not in sensor_templates:
                    del local_selected_indices[sensor_name]
            if sensor_templates and local_sensor_name.get() not in sensor_templates:
                local_sensor_name.set(next(iter(sensor_templates)))

        def current_template():
            sensor_name = local_sensor_name.get()
            return sensor_templates.get(sensor_name)

        def current_selected_sensor_indices():
            template = current_template()
            if template is None:
                return []
            return list(local_selected_indices.get(template["sensor_name"], []))

        def _get_current_local_mapping():
            return local_mapping_state.get(local_sensor_name.get()) or {}

        def _set_current_local_mapping(mapping):
            sensor_name = local_sensor_name.get()
            if not sensor_name:
                return
            if mapping:
                local_mapping_state[sensor_name] = mapping
            else:
                local_mapping_state.pop(sensor_name, None)

        def _build_mapping_payload_from_lookup(template, selected_indices, lookup, mode, tolerance_nm):
            return _build_sensor_band_mapping_payload(
                template,
                selected_indices,
                current_image_band_info,
                lookup,
                mode,
                tolerance_nm,
            )

        def _ensure_mapping_state_for_current_sensor(force_auto=False):
            template = current_template()
            if template is None or not current_image_band_info:
                return
            sensor_name = template["sensor_name"]
            selected_indices = current_selected_sensor_indices()
            mapping = _clone_sensor_band_mapping_config(local_mapping_state.get(sensor_name))
            if mapping and not _image_band_info_matches_mapping(current_image_band_info, mapping):
                mapping = None

            if mapping is None:
                tolerance_nm = 10.0
                lookup = {}
                mode = "manual"
            else:
                tolerance_nm = float(mapping.get("tolerance_nm", 10.0))
                lookup = _mapping_lookup_from_payload(mapping)
                mode = str(mapping.get("mode", "manual")).strip().lower() or "manual"

            if force_auto or not mapping or mode == "auto":
                lookup, unmatched = _auto_match_sensor_band_lookup(
                    template,
                    selected_indices,
                    current_image_band_info,
                    tolerance_nm=tolerance_nm,
                )
                mode = "auto"
                mapping = _build_mapping_payload_from_lookup(template, selected_indices, lookup, mode, tolerance_nm)
                if unmatched and not mapping:
                    _set_current_local_mapping(None)
                    return
                _set_current_local_mapping(mapping)
                mapping_tolerance_var.set(str(int(tolerance_nm)) if float(tolerance_nm).is_integer() else f"{tolerance_nm:g}")
                return

            filtered_lookup = {int(key): int(value) for key, value in lookup.items() if int(key) in selected_indices}
            auto_lookup, _unmatched = _auto_match_sensor_band_lookup(
                template,
                selected_indices,
                current_image_band_info,
                tolerance_nm=tolerance_nm,
            )
            used_source_indices = set(filtered_lookup.values())
            for sensor_index in selected_indices:
                sensor_index = int(sensor_index)
                if sensor_index in filtered_lookup:
                    continue
                auto_source_index = auto_lookup.get(sensor_index)
                if auto_source_index is None or int(auto_source_index) in used_source_indices:
                    continue
                filtered_lookup[sensor_index] = int(auto_source_index)
                used_source_indices.add(int(auto_source_index))
            mapping = _build_mapping_payload_from_lookup(template, selected_indices, filtered_lookup, mode, tolerance_nm)
            _set_current_local_mapping(mapping)
            mapping_tolerance_var.set(str(int(tolerance_nm)) if float(tolerance_nm).is_integer() else f"{tolerance_nm:g}")

        def _render_mapping_ui():
            for child in manual_mapping_frame.winfo_children():
                child.destroy()
            manual_mapping_frame.grid_remove()
            mapping_tree.grid_remove()

            if current_image_band_error:
                mapping_status_var.set(
                    f"Unable to inspect the selected input image for band linking.\n{current_image_band_error}"
                )
                _set_widget_enabled(auto_match_button, False)
                _set_widget_enabled(mapping_tolerance_entry, False)
                return

            if not current_image_band_info:
                mapping_status_var.set(
                    "Select an input image before configuring explicit band linking. Without it, SWAMpy will fall back to wavelength-based alignment at run time."
                )
                _set_widget_enabled(auto_match_button, False)
                _set_widget_enabled(mapping_tolerance_entry, False)
                return

            template = current_template()
            if template is None:
                mapping_status_var.set("No sensor template is available for band linking.")
                _set_widget_enabled(auto_match_button, False)
                _set_widget_enabled(mapping_tolerance_entry, False)
                return

            _set_widget_enabled(auto_match_button, True)
            _set_widget_enabled(mapping_tolerance_entry, True)

            selected_indices = current_selected_sensor_indices()
            mapping = _get_current_local_mapping()
            lookup = _mapping_lookup_from_payload(mapping)
            mode = str(mapping.get("mode", "manual")).strip().lower() if mapping else "manual"
            matched_count = sum(1 for sensor_index in selected_indices if int(sensor_index) in lookup)
            unmatched_count = max(0, len(selected_indices) - matched_count)
            source_count = int(current_image_band_info.get("band_count", 0))
            source_name = current_image_band_info.get("source_name", "input image")
            status_text = (
                f"Detected {source_count} input image band(s) from '{source_name}'. "
                f"{matched_count} selected sensor band(s) are linked and {unmatched_count} remain unmatched. "
                f"Mode: {mode}."
            )
            if unmatched_count:
                missing_indices = _mapping_missing_sensor_indices(template, selected_indices, mapping)
                status_text += (
                    "\nWARNING: the following selected sensor band(s) do not have a matching input image band: "
                    f"{_sensor_band_labels_for_indices(template, missing_indices)}."
                )
            mapping_status_var.set(status_text)

            use_summary_view = bool(current_image_band_info.get("is_hyperspectral")) or len(selected_indices) > 15
            if use_summary_view:
                mapping_tree.grid(row=0, column=0, sticky="nsew")
                for item in mapping_tree.get_children():
                    mapping_tree.delete(item)
                for sensor_index in selected_indices:
                    sensor_band = next((band for band in template["bands"] if int(band["index"]) == int(sensor_index)), None)
                    if sensor_band is None:
                        continue
                    source_index = lookup.get(int(sensor_index))
                    if source_index is None or source_index >= len(current_image_band_info["bands"]):
                        image_band_label = "Unmatched"
                        delta_text = ""
                    else:
                        source_band = current_image_band_info["bands"][int(source_index)]
                        image_band_label = source_band["label"]
                        source_wavelength = source_band.get("wavelength")
                        if source_wavelength is None:
                            delta_text = ""
                        else:
                            delta_text = f"{abs(float(source_wavelength) - float(sensor_band['center'])):.1f}"
                    mapping_tree.insert(
                        "",
                        "end",
                        values=(sensor_band["label"], image_band_label, delta_text),
                    )
                return

            manual_mapping_frame.grid(row=0, column=0, sticky="nsew")
            manual_mapping_frame.columnconfigure(1, weight=1)
            source_bands = list(current_image_band_info.get("bands") or [])
            source_options = ["-- Not linked --"] + [band["label"] for band in source_bands]
            source_label_to_index = {band["label"]: int(band["index"]) for band in source_bands}

            for row_index, sensor_index in enumerate(selected_indices):
                sensor_band = next((band for band in template["bands"] if int(band["index"]) == int(sensor_index)), None)
                if sensor_band is None:
                    continue
                ttk.Label(manual_mapping_frame, text=sensor_band["label"]).grid(row=row_index, column=0, sticky="w", padx=(0, 8), pady=2)
                selected_source_index = lookup.get(int(sensor_index))
                initial_label = "-- Not linked --"
                if selected_source_index is not None and 0 <= int(selected_source_index) < len(source_bands):
                    initial_label = source_bands[int(selected_source_index)]["label"]
                combo_var = StringVar(value=initial_label)
                combo = ttk.Combobox(
                    manual_mapping_frame,
                    values=source_options,
                    state="readonly",
                    textvariable=combo_var,
                )
                combo.grid(row=row_index, column=1, sticky="ew", pady=2)
                delta_var = StringVar(value="")
                ttk.Label(manual_mapping_frame, textvariable=delta_var, width=12).grid(row=row_index, column=2, sticky="w", padx=(8, 0), pady=2)

                def on_combo_change(*_args, sensor_band_index=int(sensor_index), local_var=combo_var, local_delta_var=delta_var, sensor_center=float(sensor_band["center"])):
                    selected_label = local_var.get()
                    current_mapping = _clone_sensor_band_mapping_config(_get_current_local_mapping()) or {}
                    current_lookup = _mapping_lookup_from_payload(current_mapping)
                    if selected_label == "-- Not linked --":
                        current_lookup.pop(sensor_band_index, None)
                        local_delta_var.set("")
                    else:
                        source_index = source_label_to_index[selected_label]
                        current_lookup[sensor_band_index] = int(source_index)
                        source_wavelength = source_bands[int(source_index)].get("wavelength")
                        if source_wavelength is None:
                            local_delta_var.set("")
                        else:
                            local_delta_var.set(f"{abs(float(source_wavelength) - sensor_center):.1f} nm")
                    tolerance_text = mapping_tolerance_var.get().strip() or "10"
                    try:
                        tolerance_nm = float(tolerance_text)
                    except ValueError:
                        tolerance_nm = 10.0
                    updated_mapping = _build_mapping_payload_from_lookup(
                        template,
                        selected_indices,
                        current_lookup,
                        "manual",
                        tolerance_nm,
                    )
                    _set_current_local_mapping(updated_mapping)

                combo_var.trace_add("write", on_combo_change)
                if initial_label != "-- Not linked --":
                    initial_wavelength = source_bands[int(selected_source_index)].get("wavelength")
                    if initial_wavelength is not None:
                        delta_var.set(f"{abs(float(initial_wavelength) - float(sensor_band['center'])):.1f} nm")

        def auto_match_current_sensor_bands():
            template = current_template()
            if template is None or not current_image_band_info:
                return
            tolerance_text = mapping_tolerance_var.get().strip() or "10"
            try:
                tolerance_nm = float(tolerance_text)
            except ValueError:
                messagebox.showerror("Invalid tolerance", "Tolerance must be numeric.", parent=popup)
                return
            current_mapping = _clone_sensor_band_mapping_config(_get_current_local_mapping()) or {}
            current_mapping["tolerance_nm"] = tolerance_nm
            _set_current_local_mapping(current_mapping)
            _ensure_mapping_state_for_current_sensor(force_auto=True)
            _render_mapping_ui()

        def sync_current_selection():
            template = current_template()
            if template is None:
                return
            local_selected_indices[template["sensor_name"]] = list(bands_listbox.curselection())

        def update_selection_status():
            template = current_template()
            if template is None:
                selection_status_var.set("No template is available for this sensor in the current workspace.")
                sensor_error_var.set(sensor_load_errors.get(local_sensor_name.get(), ""))
                return
            selected = [template["bands"][idx]["center"] for idx in local_selected_indices.get(template["sensor_name"], [])]
            if selected:
                selection_status_var.set(
                    f"{template['sensor_name']}: {len(selected)} band(s) selected ({_format_sensor_centers(selected)})."
                )
            else:
                selection_status_var.set(f"{template['sensor_name']}: no bands selected.")
            sensor_error_var.set("")

        def refresh_sensor_buttons():
            for child in sensor_choice_frame.winfo_children():
                child.destroy()
            if not sensor_templates and not sensor_load_errors:
                ttk.Label(sensor_choice_frame, text="No sensor templates are available.").grid(row=0, column=0, sticky="w")
                return

            row_index = 0
            for sensor_name in sensor_templates.keys():
                ttk.Radiobutton(
                    sensor_choice_frame,
                    text=sensor_name,
                    value=sensor_name,
                    variable=local_sensor_name,
                ).grid(row=row_index, column=0, sticky="w", pady=(0, 4))
                row_index += 1

            for sensor_name, error_text in sensor_load_errors.items():
                ttk.Radiobutton(
                    sensor_choice_frame,
                    text=f"{sensor_name} (template unavailable)",
                    value=sensor_name,
                    variable=local_sensor_name,
                    state="disabled",
                ).grid(row=row_index, column=0, sticky="w", pady=(0, 4))
                row_index += 1

        def refresh_band_list():
            _ensure_local_selection_defaults()
            refresh_sensor_buttons()
            template = current_template()
            bands_listbox.delete(0, tk.END)
            if template is None:
                smart_frame.grid_remove()
                update_selection_status()
                _render_mapping_ui()
                return

            for band in template["bands"]:
                bands_listbox.insert(tk.END, band["label"])

            selected_indices = local_selected_indices.get(template["sensor_name"], [])
            for idx in selected_indices:
                if 0 <= idx < len(template["bands"]):
                    bands_listbox.selection_set(idx)

            if sensor_config.supports_smart_selection(template):
                smart_frame.grid()
            else:
                smart_frame.grid_remove()
            update_selection_status()
            _ensure_mapping_state_for_current_sensor()
            _render_mapping_ui()

        def apply_selection(indices):
            template = current_template()
            if template is None:
                return
            deduped = sorted({int(idx) for idx in indices if 0 <= int(idx) < len(template["bands"])})
            local_selected_indices[template["sensor_name"]] = deduped
            bands_listbox.selection_clear(0, tk.END)
            for idx in deduped:
                bands_listbox.selection_set(idx)
            update_selection_status()
            _ensure_mapping_state_for_current_sensor()
            _render_mapping_ui()

        def select_all_bands():
            template = current_template()
            if template is None:
                return
            apply_selection(range(len(template["bands"])))

        def clear_bands():
            apply_selection([])

        def apply_range_selection():
            template = current_template()
            if template is None:
                return
            try:
                min_nm = float(range_min_var.get().strip())
                max_nm = float(range_max_var.get().strip())
                step = int(float(range_step_var.get().strip()))
            except ValueError:
                messagebox.showerror("Invalid value", "PRISMA range and step values must be numeric.", parent=popup)
                return
            if step <= 0:
                messagebox.showerror("Invalid value", "Every nth band must be positive.", parent=popup)
                return
            if max_nm < min_nm:
                messagebox.showerror("Invalid value", "Max nm must be greater than or equal to Min nm.", parent=popup)
                return
            apply_selection(sensor_config.select_bands_by_range(template, min_nm, max_nm, step=step))

        def select_visible_all():
            template = current_template()
            if template is None:
                return
            apply_selection(sensor_config.select_bands_by_range(template, 400.0, 700.0, step=1))

        def select_visible_every_other():
            template = current_template()
            if template is None:
                return
            apply_selection(sensor_config.select_bands_by_range(template, 400.0, 700.0, step=2))

        def reset_sensor_defaults():
            template = current_template()
            if template is None:
                return
            apply_selection(sensor_config.default_selected_band_indices(template))

        def _sync_outer_sensor_state_after_template_change():
            for sensor_name in list(sensor_state["selected_indices"].keys()):
                if sensor_name not in sensor_templates:
                    del sensor_state["selected_indices"][sensor_name]
            for sensor_name in list(sensor_state["band_mapping_configs"].keys()):
                if sensor_name not in sensor_templates:
                    del sensor_state["band_mapping_configs"][sensor_name]
            for sensor_name, template in sensor_templates.items():
                sensor_state["selected_indices"][sensor_name] = list(
                    local_selected_indices.get(
                        sensor_name,
                        sensor_config.default_selected_band_indices(template),
                    )
                )
            if sensor_templates and sensor_state["sensor_name"] not in sensor_templates:
                if local_sensor_name.get() in sensor_templates:
                    sensor_state["sensor_name"] = local_sensor_name.get()
                else:
                    sensor_state["sensor_name"] = next(iter(sensor_templates))
            update_sensor_ui()

        def _refresh_sensor_templates_from_disk(preferred_sensor_name=None):
            nonlocal sensor_templates, sensor_load_errors

            previous_selection = dict(local_selected_indices)
            sensor_templates, sensor_load_errors = load_available_sensor_templates()
            for sensor_name, template in sensor_templates.items():
                if sensor_name not in local_selected_indices:
                    local_selected_indices[sensor_name] = previous_selection.get(
                        sensor_name,
                        sensor_config.default_selected_band_indices(template),
                    )
            for sensor_name in list(local_selected_indices.keys()):
                if sensor_name not in sensor_templates:
                    del local_selected_indices[sensor_name]

            if preferred_sensor_name and preferred_sensor_name in sensor_templates:
                local_sensor_name.set(preferred_sensor_name)
            elif sensor_templates and local_sensor_name.get() not in sensor_templates:
                local_sensor_name.set(next(iter(sensor_templates)))

            _sync_outer_sensor_state_after_template_change()
            refresh_band_list()

        def open_add_sensor_popup():
            add_popup = tk.Toplevel(popup)
            add_popup.title("Add sensor")
            apply_window_size(
                add_popup,
                preferred_size=(860, 320),
                minsize=(720, 230),
                width_ratio=0.68,
                height_ratio=0.34,
                max_width_ratio=0.74,
                max_height_ratio=0.4,
            )
            add_popup.transient(popup)
            add_popup.grab_set()
            add_popup.columnconfigure(0, weight=1)
            add_popup.rowconfigure(0, weight=1)

            name_var = StringVar()
            xml_path_var = StringVar()

            container = ttk.Frame(add_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(1, weight=1)
            container.rowconfigure(3, weight=1)

            ttk.Label(
                container,
                text=(
                    "Choose a sensor template XML containing the sensor response functions and fixed NEDR values. "
                    "The sensor will be copied into Data/SRF and will remain available in future runs."
                ),
                wraplength=700,
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            ttk.Label(container, text="Sensor name").grid(row=1, column=0, sticky="w")
            ttk.Entry(container, textvariable=name_var).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0))

            ttk.Label(container, text="Sensor XML").grid(row=2, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=xml_path_var).grid(row=2, column=1, sticky="ew", padx=(8, 6), pady=(10, 0))

            def choose_xml():
                path = askopenfilename(
                    parent=add_popup,
                    title="Choose sensor template XML",
                    filetypes=[("XML files", "*.xml"), ("All files", "*.*")],
                )
                if path:
                    xml_path_var.set(path)

            ttk.Button(container, text="Browse", command=choose_xml).grid(row=2, column=2, sticky="e", pady=(10, 0))

            def apply_add_sensor():
                xml_path = xml_path_var.get().strip()
                if not xml_path:
                    messagebox.showerror("Missing XML", "Choose a sensor XML file.", parent=add_popup)
                    return
                try:
                    add_result = sensor_config.add_sensor_template(cwd, name_var.get(), xml_path)
                except Exception as exc:
                    messagebox.showerror("Invalid sensor", str(exc), parent=add_popup)
                    return

                preferred_sensor_name = add_result["template"]["sensor_name"]
                _refresh_sensor_templates_from_disk(preferred_sensor_name=preferred_sensor_name)

                if add_result["backup_created"]:
                    messagebox.showinfo(
                        "Sensor added",
                        f"Added sensor '{preferred_sensor_name}'.\n\nA backup of the original templates was created in:\n{add_result['backup_dir']}",
                        parent=popup,
                    )
                else:
                    messagebox.showinfo(
                        "Sensor added",
                        f"Added sensor '{preferred_sensor_name}'.",
                        parent=popup,
                    )
                add_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(14, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=add_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="Add", command=apply_add_sensor).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(add_popup)
            add_popup.wait_window()

        def open_remove_sensor_popup():
            remove_popup = tk.Toplevel(popup)
            remove_popup.title("Remove sensors")
            apply_window_size(
                remove_popup,
                preferred_size=(640, 520),
                minsize=(500, 380),
                width_ratio=0.5,
                height_ratio=0.52,
                max_width_ratio=0.58,
                max_height_ratio=0.62,
            )
            remove_popup.transient(popup)
            remove_popup.grab_set()
            remove_popup.columnconfigure(0, weight=1)
            remove_popup.rowconfigure(0, weight=1)

            container = ttk.Frame(remove_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(0, weight=1)
            container.rowconfigure(1, weight=1)

            ttk.Label(
                container,
                text="Select one or several sensors to remove from the app.",
                wraplength=440,
                justify="left",
            ).grid(row=0, column=0, sticky="w", pady=(0, 8))

            remove_listbox = tk.Listbox(
                container,
                selectmode=tk.MULTIPLE,
                exportselection=False,
                height=16,
            )
            remove_listbox.grid(row=1, column=0, sticky="nsew")
            remove_scroll = ttk.Scrollbar(container, orient="vertical", command=remove_listbox.yview)
            remove_scroll.grid(row=1, column=1, sticky="ns")
            remove_listbox.configure(yscrollcommand=remove_scroll.set)

            for sensor_name in sensor_templates.keys():
                remove_listbox.insert(tk.END, sensor_name)

            def apply_remove_sensor():
                selected_names = [remove_listbox.get(index) for index in remove_listbox.curselection()]
                if not selected_names:
                    messagebox.showerror("No sensors selected", "Select at least one sensor to remove.", parent=remove_popup)
                    return
                confirm_message = (
                    "You are about to permanently remove the following sensors from the app:\n\n"
                    + "\n".join(f"- {name}" for name in selected_names)
                    + "\n\nDo you want to continue?"
                )
                if not messagebox.askyesno("Confirm deletion", confirm_message, parent=remove_popup):
                    return
                try:
                    remove_result = sensor_config.remove_sensor_templates(cwd, sensor_templates, selected_names)
                except Exception as exc:
                    messagebox.showerror("Cannot remove sensors", str(exc), parent=remove_popup)
                    return

                remaining_names = [name for name in sensor_templates.keys() if name not in set(remove_result["removed_names"])]
                preferred_sensor_name = remaining_names[0] if remaining_names else None
                _refresh_sensor_templates_from_disk(preferred_sensor_name=preferred_sensor_name)

                if remove_result["backup_created"]:
                    messagebox.showinfo(
                        "Sensors removed",
                        f"Removed {len(remove_result['removed_names'])} sensor(s).\n\nA backup of the original templates was created in:\n{remove_result['backup_dir']}",
                        parent=popup,
                    )
                else:
                    messagebox.showinfo(
                        "Sensors removed",
                        f"Removed {len(remove_result['removed_names'])} sensor(s).",
                        parent=popup,
                    )
                remove_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=remove_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="OK", command=apply_remove_sensor).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(remove_popup)
            remove_popup.wait_window()

        ttk.Button(quick_frame, text="All", command=select_all_bands).grid(row=0, column=0, sticky="w", padx=(0, 6))
        ttk.Button(quick_frame, text="Clear", command=clear_bands).grid(row=0, column=1, sticky="w", padx=(0, 6))
        ttk.Button(quick_frame, text="Reset", command=reset_sensor_defaults).grid(row=0, column=2, sticky="w", padx=(0, 6))

        ttk.Button(sensor_actions, text="Add", command=open_add_sensor_popup).grid(row=0, column=0, sticky="w")
        ttk.Button(sensor_actions, text="Remove", command=open_remove_sensor_popup).grid(row=0, column=1, sticky="w", padx=(8, 0))

        ttk.Button(smart_frame, text="Apply Range", command=apply_range_selection).grid(row=0, column=6, sticky="w")
        ttk.Button(smart_frame, text="Visible 400-700", command=select_visible_all).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))
        ttk.Button(smart_frame, text="Visible Every 2nd", command=select_visible_every_other).grid(row=1, column=3, columnspan=3, sticky="w", padx=(10, 0), pady=(8, 0))

        def on_sensor_changed(*_args):
            refresh_band_list()

        def on_band_selection(_event=None):
            sync_current_selection()
            update_selection_status()
            _ensure_mapping_state_for_current_sensor()
            _render_mapping_ui()

        def apply_sensor_changes():
            sensor_name = local_sensor_name.get()
            if sensor_name not in sensor_templates:
                messagebox.showerror("Unavailable sensor", sensor_load_errors.get(sensor_name, "Template not available."), parent=popup)
                return
            current_mapping = _clone_sensor_band_mapping_config(local_mapping_state.get(sensor_name))
            if current_image_band_info:
                selected_indices = list(local_selected_indices.get(sensor_name, []))
                lookup = _mapping_lookup_from_payload(current_mapping or {})
                missing_sensor_indices = [sensor_index for sensor_index in selected_indices if int(sensor_index) not in lookup]
                if missing_sensor_indices:
                    missing_labels = []
                    for sensor_index in missing_sensor_indices:
                        band = next((band for band in sensor_templates[sensor_name]["bands"] if int(band["index"]) == int(sensor_index)), None)
                        missing_labels.append(band["label"] if band is not None else f"Band {sensor_index}")
                    messagebox.showerror(
                        "Incomplete band mapping",
                        "Link every selected sensor band to an input image band before applying the sensor setup.\n\n"
                        f"Missing: {', '.join(missing_labels)}",
                        parent=popup,
                    )
                    return
                image_band_indices = list((current_mapping or {}).get("image_band_indices") or [])
                if len(set(image_band_indices)) != len(image_band_indices):
                    messagebox.showerror(
                        "Invalid band mapping",
                        "Each selected sensor band must be linked to a different input image band.",
                        parent=popup,
                    )
                    return
            sensor_state["sensor_name"] = sensor_name
            for key in list(sensor_state["selected_indices"].keys()):
                if key not in sensor_templates:
                    del sensor_state["selected_indices"][key]
            for key, indices in local_selected_indices.items():
                if key in sensor_templates:
                    sensor_state["selected_indices"][key] = list(indices)
            if current_mapping and current_mapping.get("image_band_indices"):
                sensor_state["band_mapping_configs"][sensor_name] = current_mapping
            else:
                sensor_state["band_mapping_configs"].pop(sensor_name, None)
            try:
                sensor_config.build_sensor_config(
                    sensor_templates[sensor_name],
                    sensor_state["selected_indices"][sensor_name],
                    band_mapping=sensor_state["band_mapping_configs"].get(sensor_name),
                )
            except Exception as exc:
                messagebox.showerror("Invalid sensor setup", str(exc), parent=popup)
                return
            update_sensor_ui()
            popup.destroy()

        local_sensor_name.trace_add("write", on_sensor_changed)
        bands_listbox.bind("<<ListboxSelect>>", on_band_selection)
        auto_match_button.configure(command=auto_match_current_sensor_bands)

        popup_actions = ttk.Frame(popup_container)
        popup_actions.grid(row=2, column=0, columnspan=2, sticky="ew")
        popup_actions.columnconfigure(0, weight=1)
        ttk.Button(popup_actions, text="Cancel", command=popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Button(popup_actions, text="Apply", command=apply_sensor_changes).grid(row=0, column=2, sticky="e", padx=(8, 0))

        _ensure_local_selection_defaults()
        refresh_band_list()
        center_window(popup)
        popup.wait_window()

    def open_siop_popup():
        nonlocal spectral_library, selected_target_names

        popup = tk.Toplevel(root)
        popup.title("Water & Bottom Settings")
        apply_window_size(
            popup,
            preferred_size=(1260, 900),
            minsize=(1000, 720),
            width_ratio=0.92,
            height_ratio=0.9,
            max_width_ratio=0.95,
            max_height_ratio=0.93,
        )
        popup.transient(root)
        popup.grab_set()

        local_scalar_values = dict(scalar_values)
        local_selected_names = list(selected_target_names)
        local_spectrum_paths = dict(spectrum_override_paths)

        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)

        popup_container = ttk.Frame(popup, padding=12)
        popup_container.grid(row=0, column=0, sticky="nsew")
        popup_container.columnconfigure(0, weight=0)
        popup_container.columnconfigure(1, weight=1)
        popup_container.rowconfigure(0, weight=1)
        popup_container.rowconfigure(1, weight=0)

        targets_frame = ttk.Labelframe(popup_container, text="Target Spectra")
        targets_frame.grid(row=0, column=0, sticky="nsw", padx=(0, 10), pady=(0, 8))
        targets_frame.columnconfigure(0, weight=1)
        targets_frame.rowconfigure(1, weight=1)

        ttk.Label(
            targets_frame,
            text="Select 2 or 3 spectra. Hover a name to preview it. Spectra are grouped by tag.",
            wraplength=260,
        ).grid(row=0, column=0, sticky="w")

        spectra_listbox = tk.Listbox(
            targets_frame,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            width=32,
            height=20,
        )
        spectra_listbox.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        for name in spectral_library["names"]:
            spectra_listbox.insert(tk.END, name)

        listbox_scroll = ttk.Scrollbar(targets_frame, orient="vertical", command=spectra_listbox.yview)
        listbox_scroll.grid(row=1, column=1, sticky="ns", pady=(6, 0))
        spectra_listbox.configure(yscrollcommand=listbox_scroll.set)

        library_actions = ttk.Frame(targets_frame)
        library_actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        library_actions.columnconfigure(2, weight=1)

        preview_frame = ttk.Labelframe(popup_container, text="Preview & Advanced Parameters")
        preview_frame.grid(row=0, column=1, sticky="nsew", pady=(0, 8))
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(0, weight=1)
        preview_frame.rowconfigure(1, weight=0)
        preview_frame.rowconfigure(2, weight=0)
        preview_frame.rowconfigure(3, weight=0)

        preview_canvas = tk.Canvas(preview_frame, bg="white", highlightthickness=1, highlightbackground="#d7d7d7")
        preview_canvas.grid(row=0, column=0, sticky="nsew", padx=4, pady=(4, 8))

        absorption_status_var = StringVar()

        def refresh_absorption_summary():
            has_water_override = bool(local_spectrum_paths.get("a_water", "").strip())
            has_chl_override = bool(local_spectrum_paths.get("a_ph_star", "").strip())
            if has_water_override and has_chl_override:
                absorption_status_var.set("Custom water and chlorophyll absorption CSV files selected.")
            elif has_water_override:
                absorption_status_var.set("Custom pure-water absorption CSV selected.")
            elif has_chl_override:
                absorption_status_var.set("Custom chlorophyll absorption CSV selected.")
            else:
                absorption_status_var.set(
                    f"Using the default absorption spectra from {os.path.basename(default_template_path)}."
                )

        def open_absorption_popup():
            absorption_popup = tk.Toplevel(popup)
            absorption_popup.title("Modify absorption of chl and water")
            apply_window_size(
                absorption_popup,
                preferred_size=(960, 320),
                minsize=(780, 260),
                width_ratio=0.72,
                height_ratio=0.34,
                max_width_ratio=0.8,
                max_height_ratio=0.4,
            )
            absorption_popup.transient(popup)
            absorption_popup.grab_set()
            absorption_popup.columnconfigure(0, weight=1)
            absorption_popup.rowconfigure(0, weight=1)

            draft_paths = dict(local_spectrum_paths)
            a_water_path_var = StringVar(value=draft_paths.get("a_water", ""))
            a_ph_star_path_var = StringVar(value=draft_paths.get("a_ph_star", ""))

            def choose_spectrum_csv(target_key, target_var, title_text):
                path = askopenfilename(
                    parent=absorption_popup,
                    title=title_text,
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                )
                if path:
                    draft_paths[target_key] = path
                    target_var.set(path)

            def clear_spectrum_csv(target_key, target_var):
                draft_paths[target_key] = ""
                target_var.set("")

            absorption_container = ttk.Frame(absorption_popup, padding=12)
            absorption_container.grid(row=0, column=0, sticky="nsew")
            absorption_container.columnconfigure(1, weight=1)
            absorption_container.rowconfigure(3, weight=1)

            ttk.Label(
                absorption_container,
                text="CSV format: wavelength, value. One header row is allowed.",
                wraplength=820,
            ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 8))

            ttk.Label(
                absorption_container,
                text="Absorption coefficient of pure seawater [m^-1]",
            ).grid(row=1, column=0, sticky="w")
            ttk.Entry(
                absorption_container,
                textvariable=a_water_path_var,
            ).grid(row=1, column=1, sticky="ew", padx=(0, 6))
            ttk.Button(
                absorption_container,
                text="Browse",
                command=lambda: choose_spectrum_csv(
                    "a_water",
                    a_water_path_var,
                    "Choose pure seawater absorption CSV",
                ),
            ).grid(row=1, column=2, sticky="e", padx=(0, 6))
            ttk.Button(
                absorption_container,
                text="Default",
                command=lambda: clear_spectrum_csv("a_water", a_water_path_var),
            ).grid(row=1, column=3, sticky="e")

            ttk.Label(
                absorption_container,
                text="Chlorophyll-specific absorption spectrum [m^2 mg^-1]",
            ).grid(row=2, column=0, sticky="w", pady=(8, 0))
            ttk.Entry(
                absorption_container,
                textvariable=a_ph_star_path_var,
            ).grid(row=2, column=1, sticky="ew", padx=(0, 6), pady=(8, 0))
            ttk.Button(
                absorption_container,
                text="Browse",
                command=lambda: choose_spectrum_csv(
                    "a_ph_star",
                    a_ph_star_path_var,
                    "Choose chlorophyll-specific absorption CSV",
                ),
            ).grid(row=2, column=2, sticky="e", padx=(0, 6), pady=(8, 0))
            ttk.Button(
                absorption_container,
                text="Default",
                command=lambda: clear_spectrum_csv("a_ph_star", a_ph_star_path_var),
            ).grid(row=2, column=3, sticky="e", pady=(8, 0))

            def apply_absorption_changes():
                local_spectrum_paths["a_water"] = a_water_path_var.get().strip()
                local_spectrum_paths["a_ph_star"] = a_ph_star_path_var.get().strip()
                refresh_absorption_summary()
                absorption_popup.destroy()

            absorption_actions = ttk.Frame(absorption_container)
            absorption_actions.grid(row=3, column=0, columnspan=4, sticky="sew", pady=(14, 0))
            absorption_actions.columnconfigure(0, weight=1)

            ttk.Button(
                absorption_actions,
                text="Cancel",
                command=absorption_popup.destroy,
            ).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(
                absorption_actions,
                text="Apply",
                command=apply_absorption_changes,
            ).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(absorption_popup)
            absorption_popup.wait_window()

        absorption_frame = ttk.Frame(preview_frame)
        absorption_frame.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 8))
        absorption_frame.columnconfigure(1, weight=1)

        ttk.Button(
            absorption_frame,
            text="Modify absorption of chl and water",
            command=open_absorption_popup,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            absorption_frame,
            textvariable=absorption_status_var,
            justify="left",
            wraplength=430,
        ).grid(row=0, column=1, sticky="w", padx=(10, 0))

        scalar_vars = {}
        for key, _label, _required in siop_config.SIOP_SCALAR_FIELDS:
            scalar_vars[key] = StringVar(value=local_scalar_values.get(key, ""))

        scalar_summary_var = StringVar()

        def refresh_scalar_summary():
            missing_required = []
            invalid_values = []
            for key, label, required in siop_config.SIOP_SCALAR_FIELDS:
                text = scalar_vars[key].get().strip()
                if not text:
                    if required:
                        missing_required.append(label)
                    continue
                try:
                    float(text)
                except ValueError:
                    invalid_values.append(label)
            if invalid_values:
                scalar_summary_var.set(f"{len(invalid_values)} numeric parameter(s) need valid numbers.")
            elif missing_required:
                scalar_summary_var.set(f"{len(missing_required)} required numeric parameter(s) are empty.")
            else:
                scalar_summary_var.set(f"{len(siop_config.SIOP_SCALAR_FIELDS)} numeric parameter(s) configured.")

        def open_numeric_parameters_popup():
            numeric_popup = tk.Toplevel(popup)
            numeric_popup.title("Water & Bottom Numeric Parameters")
            apply_window_size(
                numeric_popup,
                preferred_size=(980, 520),
                minsize=(820, 420),
                width_ratio=0.72,
                height_ratio=0.54,
                max_width_ratio=0.82,
                max_height_ratio=0.68,
            )
            numeric_popup.transient(popup)
            numeric_popup.grab_set()
            numeric_popup.columnconfigure(0, weight=1)
            numeric_popup.rowconfigure(0, weight=1)

            draft_vars = {
                key: StringVar(value=scalar_vars[key].get())
                for key, _label, _required in siop_config.SIOP_SCALAR_FIELDS
            }
            status_var = StringVar()

            container = ttk.Frame(numeric_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            for column_index in range(4):
                container.columnconfigure(column_index, weight=1 if column_index in (1, 3) else 0)
            container.rowconfigure(8, weight=1)

            ttk.Label(
                container,
                text="These values control the water IOP and backscattering model used by the inversion.",
                justify="left",
                wraplength=780,
            ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 10))

            for idx, (key, label, required) in enumerate(siop_config.SIOP_SCALAR_FIELDS):
                row = 1 + idx // 2
                col = (idx % 2) * 2
                label_text = f"{label}{' *' if required else ''}"
                ttk.Label(
                    container,
                    text=label_text,
                    justify="left",
                    wraplength=250,
                ).grid(row=row, column=col, sticky="w", padx=(0 if col == 0 else 18, 6), pady=3)
                ttk.Entry(
                    container,
                    textvariable=draft_vars[key],
                    justify="right",
                    width=14,
                ).grid(row=row, column=col + 1, sticky="ew", pady=3)

            ttk.Label(container, text="* Required", foreground="#555").grid(
                row=8,
                column=0,
                columnspan=4,
                sticky="sw",
                pady=(8, 0),
            )
            ttk.Label(container, textvariable=status_var, foreground="red").grid(
                row=9,
                column=0,
                columnspan=4,
                sticky="w",
                pady=(6, 0),
            )

            def apply_numeric_parameters():
                draft_values = {key: var.get().strip() for key, var in draft_vars.items()}
                try:
                    siop_config.validate_scalar_values(draft_values)
                except Exception as exc:
                    status_var.set(str(exc))
                    return
                for key, value in draft_values.items():
                    scalar_vars[key].set(value)
                refresh_scalar_summary()
                numeric_popup.destroy()

            numeric_actions = ttk.Frame(container)
            numeric_actions.grid(row=10, column=0, columnspan=4, sticky="ew", pady=(12, 0))
            numeric_actions.columnconfigure(0, weight=1)
            ttk.Button(numeric_actions, text="Cancel", command=numeric_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(numeric_actions, text="Apply", command=apply_numeric_parameters).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(numeric_popup)
            numeric_popup.wait_window()

        scalar_frame = ttk.Frame(preview_frame)
        scalar_frame.grid(row=2, column=0, sticky="ew", padx=4, pady=(0, 4))
        scalar_frame.columnconfigure(1, weight=1)
        ttk.Button(
            scalar_frame,
            text="Advanced numeric parameters",
            command=open_numeric_parameters_popup,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            scalar_frame,
            textvariable=scalar_summary_var,
            justify="left",
            wraplength=430,
        ).grid(row=0, column=1, sticky="w", padx=(10, 0))

        popup_status_var = StringVar()
        hovered_spectrum_name = {"name": None}
        display_rows = []

        def get_local_selection():
            chosen = []
            seen = set()
            for index in spectra_listbox.curselection():
                if 0 <= index < len(display_rows):
                    row = display_rows[index]
                    if row.get("type") == "spectrum":
                        name = row["name"]
                        if name not in seen:
                            chosen.append(name)
                            seen.add(name)
            return chosen

        def _sanitize_selection(names):
            available_names = set(spectral_library["names"])
            chosen = []
            seen = set()
            for name in names:
                if name in available_names and name not in seen:
                    chosen.append(name)
                    seen.add(name)
            return chosen[:3]

        def _build_grouped_display_rows():
            grouped_names = {}
            for name in spectral_library["names"]:
                tag = siop_config.display_tag(spectral_library.get("tags", {}).get(name, ""))
                grouped_names.setdefault(tag, []).append(name)

            ordered_tags = sorted(
                grouped_names.keys(),
                key=lambda tag: (tag == siop_config.UNTAGGED_TAG, tag.lower()),
            )

            rows = []
            for tag in ordered_tags:
                rows.append({
                    "type": "header",
                    "label": f"[{tag}]",
                })
                for name in grouped_names[tag]:
                    rows.append({
                        "type": "spectrum",
                        "name": name,
                        "label": f"  {name}",
                    })
            return rows

        def _set_listbox_selection(names):
            wanted = set(names)
            spectra_listbox.selection_clear(0, tk.END)
            for index, row in enumerate(display_rows):
                if row.get("type") == "spectrum" and row.get("name") in wanted:
                    spectra_listbox.selection_set(index)

        def refresh_popup_summary():
            chosen = get_local_selection()
            if len(chosen) == 2:
                popup_status_var.set("2 targets selected. The third substrate will be fixed to zero.")
            elif len(chosen) == 3:
                popup_status_var.set("3 targets selected.")
            elif len(chosen) < 2:
                popup_status_var.set("Select at least two target spectra.")
            else:
                popup_status_var.set("Select at most three target spectra.")

        def redraw_preview():
            _draw_spectra_preview(
                preview_canvas,
                spectral_library,
                get_local_selection(),
                hovered_name=hovered_spectrum_name["name"],
            )

        def refresh_spectra_listbox(selected_names=None):
            chosen = list(selected_names) if selected_names is not None else get_local_selection()
            chosen = _sanitize_selection(chosen)
            local_selected_names[:] = list(chosen)
            if hovered_spectrum_name["name"] not in spectral_library["spectra"]:
                hovered_spectrum_name["name"] = None
            spectra_listbox.delete(0, tk.END)
            display_rows.clear()
            display_rows.extend(_build_grouped_display_rows())
            for row in display_rows:
                spectra_listbox.insert(tk.END, row["label"])
            _set_listbox_selection(chosen)
            refresh_popup_summary()
            redraw_preview()

        def _sync_outer_selection_after_library_change(chosen_names=None):
            chosen = _sanitize_selection(chosen_names if chosen_names is not None else selected_target_names)
            selected_target_names[:] = chosen
            update_substrate_ui()
            return chosen

        def _persist_library_update(updated_library, action_description, selected_names=None):
            nonlocal spectral_library

            backup_path = siop_config.spectral_library_backup_path(updated_library["path"])
            backup_created = not os.path.exists(backup_path)
            siop_config.write_spectral_library(updated_library)
            spectral_library = siop_config.load_spectral_library(updated_library["path"])

            chosen = _sanitize_selection(selected_names if selected_names is not None else local_selected_names)
            local_selected_names[:] = chosen
            chosen = _sync_outer_selection_after_library_change(chosen)
            refresh_spectra_listbox(chosen)

            if backup_created:
                messagebox.showinfo(
                    "Spectral library updated",
                    f"{action_description}\n\nA backup of the original library was created at:\n{backup_path}",
                    parent=popup,
                )
            else:
                messagebox.showinfo(
                    "Spectral library updated",
                    action_description,
                    parent=popup,
                )

        def on_selection_change(_event=None):
            selected_indices = list(spectra_listbox.curselection())
            invalid_indices = []
            valid_indices = []
            for index in selected_indices:
                if 0 <= index < len(display_rows) and display_rows[index].get("type") == "spectrum":
                    valid_indices.append(index)
                else:
                    invalid_indices.append(index)
            for index in invalid_indices:
                spectra_listbox.selection_clear(index)
            if len(valid_indices) > 3:
                for index in valid_indices[3:]:
                    spectra_listbox.selection_clear(index)
                messagebox.showwarning("Too many spectra", "Select at most three target spectra.", parent=popup)
            local_selected_names[:] = get_local_selection()
            refresh_popup_summary()
            redraw_preview()

        def on_hover(event):
            if spectra_listbox.size() == 0:
                return
            index = spectra_listbox.nearest(event.y)
            if 0 <= index < spectra_listbox.size():
                if 0 <= index < len(display_rows) and display_rows[index].get("type") == "spectrum":
                    hovered_spectrum_name["name"] = display_rows[index]["name"]
                else:
                    hovered_spectrum_name["name"] = None
                redraw_preview()

        def on_hover_leave(_event):
            hovered_spectrum_name["name"] = None
            redraw_preview()

        def open_add_spectrum_popup():
            add_popup = tk.Toplevel(popup)
            add_popup.title("Add spectrum to library")
            apply_window_size(
                add_popup,
                preferred_size=(860, 340),
                minsize=(720, 260),
                width_ratio=0.68,
                height_ratio=0.36,
                max_width_ratio=0.74,
                max_height_ratio=0.42,
            )
            add_popup.transient(popup)
            add_popup.grab_set()
            add_popup.columnconfigure(0, weight=1)
            add_popup.rowconfigure(0, weight=1)

            name_var = StringVar()
            csv_path_var = StringVar()
            tag_var = StringVar()

            container = ttk.Frame(add_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(1, weight=1)
            container.rowconfigure(4, weight=1)

            ttk.Label(
                container,
                text=(
                    "Choose a two-column CSV containing wavelength and reflectance. "
                    "The file may include a single header row."
                ),
                wraplength=680,
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            ttk.Label(container, text="Spectrum name").grid(row=1, column=0, sticky="w")
            ttk.Entry(container, textvariable=name_var).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0))

            ttk.Label(container, text="Tag").grid(row=2, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=tag_var).grid(row=2, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(10, 0))

            ttk.Label(container, text="CSV file").grid(row=3, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=csv_path_var).grid(row=3, column=1, sticky="ew", padx=(8, 6), pady=(10, 0))

            def choose_csv():
                path = askopenfilename(
                    parent=add_popup,
                    title="Choose spectrum CSV",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                )
                if path:
                    csv_path_var.set(path)

            ttk.Button(container, text="Browse", command=choose_csv).grid(row=3, column=2, sticky="e", pady=(10, 0))

            def apply_add():
                csv_path = csv_path_var.get().strip()
                if not csv_path:
                    messagebox.showerror("Missing CSV", "Choose a CSV file for the new spectrum.", parent=add_popup)
                    return
                try:
                    wavelengths, values = siop_config.load_two_column_spectrum_csv(csv_path)
                    updated_library = siop_config.add_spectrum_to_library(
                        spectral_library,
                        name_var.get(),
                        wavelengths,
                        values,
                    )
                    updated_library = siop_config.modify_spectrum_in_library(
                        updated_library,
                        updated_library["names"][-1],
                        updated_library["names"][-1],
                        tag_var.get(),
                    )
                except Exception as exc:
                    messagebox.showerror("Invalid spectrum", str(exc), parent=add_popup)
                    return

                _persist_library_update(
                    updated_library,
                    f"Added spectrum '{updated_library['names'][-1]}' to the spectral library.",
                )
                add_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(14, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=add_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="Add", command=apply_add).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(add_popup)
            add_popup.wait_window()

        def open_modify_spectrum_popup():
            modify_popup = tk.Toplevel(popup)
            modify_popup.title("Modify spectrum")
            apply_window_size(
                modify_popup,
                preferred_size=(860, 420),
                minsize=(720, 340),
                width_ratio=0.68,
                height_ratio=0.42,
                max_width_ratio=0.74,
                max_height_ratio=0.5,
            )
            modify_popup.transient(popup)
            modify_popup.grab_set()
            modify_popup.columnconfigure(0, weight=1)
            modify_popup.rowconfigure(0, weight=1)

            current_selection = get_local_selection()
            initial_name = current_selection[0] if current_selection else (spectral_library["names"][0] if spectral_library["names"] else "")
            selected_name_var = StringVar(value=initial_name)
            rename_var = StringVar(value=initial_name)
            tag_var = StringVar(value=spectral_library.get("tags", {}).get(initial_name, ""))

            container = ttk.Frame(modify_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(1, weight=1)
            container.rowconfigure(1, weight=1)

            ttk.Label(
                container,
                text="Choose one spectrum, then edit its name and tag.",
                wraplength=700,
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

            modify_listbox = tk.Listbox(
                container,
                selectmode=tk.SINGLE,
                exportselection=False,
                height=10,
            )
            modify_listbox.grid(row=1, column=0, columnspan=3, sticky="nsew")
            modify_scroll = ttk.Scrollbar(container, orient="vertical", command=modify_listbox.yview)
            modify_scroll.grid(row=1, column=3, sticky="ns")
            modify_listbox.configure(yscrollcommand=modify_scroll.set)

            for name in spectral_library["names"]:
                tag_text = siop_config.display_tag(spectral_library.get("tags", {}).get(name, ""))
                modify_listbox.insert(tk.END, f"{name}  [{tag_text}]")
            if initial_name in spectral_library["names"]:
                modify_listbox.selection_set(spectral_library["names"].index(initial_name))

            ttk.Label(container, text="Name").grid(row=2, column=0, sticky="w", pady=(12, 0))
            ttk.Entry(container, textvariable=rename_var).grid(row=2, column=1, columnspan=3, sticky="ew", padx=(8, 0), pady=(12, 0))

            ttk.Label(container, text="Tag").grid(row=3, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=tag_var).grid(row=3, column=1, columnspan=3, sticky="ew", padx=(8, 0), pady=(10, 0))

            def load_selected_fields(name):
                selected_name_var.set(name)
                rename_var.set(name)
                tag_var.set(spectral_library.get("tags", {}).get(name, ""))

            def on_modify_selection(_event=None):
                selection = modify_listbox.curselection()
                if not selection:
                    return
                selected_name = spectral_library["names"][selection[0]]
                load_selected_fields(selected_name)

            def apply_modify():
                current_name = selected_name_var.get()
                if not current_name:
                    messagebox.showerror("No spectrum selected", "Choose a spectrum to modify.", parent=modify_popup)
                    return
                try:
                    updated_library = siop_config.modify_spectrum_in_library(
                        spectral_library,
                        current_name,
                        rename_var.get(),
                        tag_var.get(),
                    )
                except Exception as exc:
                    messagebox.showerror("Invalid spectrum update", str(exc), parent=modify_popup)
                    return

                new_name = rename_var.get().strip()
                updated_selection = [
                    new_name if name == current_name else name
                    for name in local_selected_names
                ]
                _persist_library_update(
                    updated_library,
                    f"Updated spectrum '{current_name}'.",
                    selected_names=updated_selection,
                )
                modify_popup.destroy()

            modify_listbox.bind("<<ListboxSelect>>", on_modify_selection)

            actions = ttk.Frame(container)
            actions.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(14, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=modify_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="Apply", command=apply_modify).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(modify_popup)
            modify_popup.wait_window()

        def open_remove_spectra_popup():
            remove_popup = tk.Toplevel(popup)
            remove_popup.title("Remove spectra from library")
            apply_window_size(
                remove_popup,
                preferred_size=(640, 520),
                minsize=(500, 380),
                width_ratio=0.5,
                height_ratio=0.52,
                max_width_ratio=0.58,
                max_height_ratio=0.62,
            )
            remove_popup.transient(popup)
            remove_popup.grab_set()
            remove_popup.columnconfigure(0, weight=1)
            remove_popup.rowconfigure(0, weight=1)

            container = ttk.Frame(remove_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(0, weight=1)
            container.rowconfigure(1, weight=1)

            ttk.Label(
                container,
                text="Select one or several spectra to remove from the library.",
                wraplength=440,
                justify="left",
            ).grid(row=0, column=0, sticky="w", pady=(0, 8))

            remove_listbox = tk.Listbox(
                container,
                selectmode=tk.MULTIPLE,
                exportselection=False,
                height=16,
            )
            remove_listbox.grid(row=1, column=0, sticky="nsew")
            remove_scroll = ttk.Scrollbar(container, orient="vertical", command=remove_listbox.yview)
            remove_scroll.grid(row=1, column=1, sticky="ns")
            remove_listbox.configure(yscrollcommand=remove_scroll.set)

            for name in spectral_library["names"]:
                remove_listbox.insert(tk.END, name)

            def apply_remove():
                selected_names = [remove_listbox.get(index) for index in remove_listbox.curselection()]
                if not selected_names:
                    messagebox.showerror("No spectra selected", "Select at least one spectrum to remove.", parent=remove_popup)
                    return
                confirm_message = (
                    "You are about to permanently remove the following spectra from the spectral library:\n\n"
                    + "\n".join(f"- {name}" for name in selected_names)
                    + "\n\nDo you want to continue?"
                )
                if not messagebox.askyesno("Confirm deletion", confirm_message, parent=remove_popup):
                    return
                try:
                    updated_library = siop_config.remove_spectra_from_library(spectral_library, selected_names)
                except Exception as exc:
                    messagebox.showerror("Cannot remove spectra", str(exc), parent=remove_popup)
                    return

                _persist_library_update(
                    updated_library,
                    f"Removed {len(selected_names)} spectrum(s) from the spectral library.",
                )
                remove_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=remove_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="OK", command=apply_remove).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(remove_popup)
            remove_popup.wait_window()

        def apply_popup_changes():
            chosen = get_local_selection()
            override_paths = {
                "a_water": str(local_spectrum_paths.get("a_water", "")).strip(),
                "a_ph_star": str(local_spectrum_paths.get("a_ph_star", "")).strip(),
            }
            try:
                siop_config.build_siop_config(
                    template_config,
                    spectral_library,
                    chosen,
                    {key: var.get() for key, var in scalar_vars.items()},
                    spectrum_override_paths=override_paths,
                )
            except Exception as exc:
                messagebox.showerror("Invalid Water & Bottom Settings", str(exc), parent=popup)
                return

            scalar_values.clear()
            for key, var in scalar_vars.items():
                scalar_values[key] = var.get().strip()

            selected_target_names.clear()
            selected_target_names.extend(chosen)
            spectrum_override_paths["a_water"] = override_paths["a_water"]
            spectrum_override_paths["a_ph_star"] = override_paths["a_ph_star"]
            update_substrate_ui()
            popup.destroy()

        spectra_listbox.bind("<<ListboxSelect>>", on_selection_change)
        spectra_listbox.bind("<Motion>", on_hover)
        spectra_listbox.bind("<Leave>", on_hover_leave)
        preview_canvas.bind("<Configure>", lambda _event: redraw_preview())

        ttk.Button(library_actions, text="Add", command=open_add_spectrum_popup).grid(row=0, column=0, sticky="w")
        ttk.Button(library_actions, text="Modify", command=open_modify_spectrum_popup).grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Button(library_actions, text="Remove", command=open_remove_spectra_popup).grid(row=0, column=2, sticky="w", padx=(8, 0))

        popup_actions = ttk.Frame(popup_container)
        popup_actions.grid(row=1, column=0, columnspan=2, sticky="ew")
        popup_actions.columnconfigure(0, weight=1)

        ttk.Label(popup_actions, textvariable=popup_status_var).grid(row=0, column=0, sticky="w")
        ttk.Button(popup_actions, text="Cancel", command=popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Button(popup_actions, text="Apply", command=apply_popup_changes).grid(row=0, column=2, sticky="e", padx=(8, 0))

        refresh_spectra_listbox(local_selected_names)
        refresh_absorption_summary()
        refresh_scalar_summary()
        center_window(popup)
        popup.wait_window()

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    container = ttk.Frame(root, padding=12)
    container.grid(row=0, column=0, sticky="nsew")
    container.columnconfigure(0, weight=1)
    container.rowconfigure(0, weight=1)
    container.rowconfigure(1, weight=0)

    notebook = ttk.Notebook(container)
    notebook.grid(row=0, column=0, sticky="nsew")

    input_tab = ttk.Frame(notebook, padding=8)
    params_tab = ttk.Frame(notebook, padding=8)
    notebook.add(input_tab, text="Inputs & Options")
    notebook.add(params_tab, text="Parameters")

    for tab in (input_tab, params_tab):
        tab.columnconfigure(0, weight=1)

    input_tab.rowconfigure(0, weight=0)
    input_tab.rowconfigure(1, weight=1)
    input_tab.rowconfigure(2, weight=0)
    params_tab.rowconfigure(0, weight=0)
    params_tab.rowconfigure(1, weight=0)
    params_tab.rowconfigure(2, weight=0)

    files_frame = ttk.Labelframe(input_tab, text="Files")
    files_frame.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
    for i in range(3):
        files_frame.columnconfigure(i, weight=1)

    ttk.Label(files_frame, text="Input image(s) (.nc/.hdf)").grid(row=0, column=0, sticky="w")
    ttk.Entry(files_frame, textvariable=input_image_var).grid(row=0, column=1, sticky="ew", padx=(0, 6))
    input_buttons = ttk.Frame(files_frame)
    input_buttons.grid(row=0, column=2, sticky="e")
    ttk.Button(input_buttons, text="Browse", command=select_file_im).grid(row=0, column=0, sticky="e")
    crop_button = ttk.Button(input_buttons, text="Crop", command=open_crop_popup)
    crop_button.grid(row=0, column=1, sticky="e", padx=(6, 0))

    ttk.Label(files_frame, text="Crop area").grid(row=1, column=0, sticky="nw")
    ttk.Label(files_frame, textvariable=crop_summary_var, wraplength=560, justify="left").grid(row=1, column=1, sticky="w", padx=(0, 6))

    ttk.Label(files_frame, text="Output folder").grid(row=2, column=0, sticky="w")
    ttk.Entry(files_frame, textvariable=output_folder_var).grid(row=2, column=1, sticky="ew", padx=(0, 6))
    ttk.Button(files_frame, text="Choose", command=select_folder).grid(row=2, column=2, sticky="e")

    ttk.Label(files_frame, text="Water & Bottom settings").grid(row=3, column=0, sticky="nw")
    ttk.Label(files_frame, textvariable=siop_summary_var, wraplength=560, justify="left").grid(row=3, column=1, sticky="w", padx=(0, 6))
    ttk.Button(files_frame, text="Configure", command=open_siop_popup).grid(row=3, column=2, sticky="e")

    ttk.Label(files_frame, text="Sensor").grid(row=4, column=0, sticky="nw")
    ttk.Label(files_frame, textvariable=sensor_summary_var, wraplength=560, justify="left").grid(row=4, column=1, sticky="w", padx=(0, 6))
    ttk.Button(files_frame, text="Configure", command=open_sensor_popup).grid(row=4, column=2, sticky="e")

    above_rrs_flag = BooleanVar(value=True)
    reflectance_input_flag = BooleanVar(value=False)
    shallow_flag = BooleanVar(value=False)
    anomaly_search_flag = BooleanVar(value=False)
    optimize_initial_guesses_flag = BooleanVar(value=False)
    five_initial_guess_testing_flag = BooleanVar(value=False)
    initial_guess_debug_flag = BooleanVar(value=False)
    output_modeled_reflectance_flag = BooleanVar(value=False)
    relaxed = BooleanVar(value=False)
    fully_relaxed_flag = BooleanVar(value=False)
    pp = BooleanVar(value=False)
    allow_split = BooleanVar(value=False)
    chunk_rows = StringVar(value="512")
    bathy_mode = tk.StringVar(value="estimate")
    bathy_source = tk.StringVar(value="estimate")   # "estimate" | "emodnet" | "user"
    bathy_path_var = tk.StringVar(value="")
    bathy_info_var = tk.StringVar(value="")
    bathy_correction = tk.StringVar(value="0")
    bathy_tolerance = tk.StringVar(value="0")
    use_emodnet_var = BooleanVar(value=False)
    user_defined_var = BooleanVar(value=False)

    def add_option_row(parent, row_index, label, variable, popup_command, pady=(0, 2)):
        info_button = ttk.Button(parent, text="Info", width=6, command=popup_command)
        info_button.grid(row=row_index, column=0, sticky="w", padx=(0, 8), pady=pady)
        checkbutton = ttk.Checkbutton(parent, text=label, variable=variable)
        checkbutton.grid(row=row_index, column=1, sticky=W, pady=pady)
        return info_button, checkbutton

    # ---- Options section: 3 columns ----
    flags_frame = ttk.Labelframe(input_tab, text="Options")
    flags_frame.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
    flags_frame.columnconfigure(0, weight=1)
    flags_frame.columnconfigure(1, weight=1)
    flags_frame.columnconfigure(2, weight=1)

    # Column 0 — Pre-processing
    pre_frame = ttk.Labelframe(flags_frame, text="Pre-processing")
    pre_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=2)
    pre_frame.columnconfigure(1, weight=1)
    add_option_row(pre_frame, 0, "Above RRS", above_rrs_flag, open_above_rrs_popup)
    add_option_row(pre_frame, 1, "Reflectance (dimensionless)", reflectance_input_flag, open_reflectance_input_popup)

    # Column 1 — Processing Options
    proc_frame = ttk.Labelframe(flags_frame, text="Processing Options")
    proc_frame.grid(row=0, column=1, sticky="nsew", padx=4, pady=2)
    proc_frame.columnconfigure(1, weight=1)
    add_option_row(proc_frame, 0, "Optimise initial guesses", optimize_initial_guesses_flag, open_initial_guess_popup)
    add_option_row(proc_frame, 1, "Relaxed constraints", relaxed, open_relaxed_constraints_popup)
    add_option_row(proc_frame, 2, "Allow image splitting", allow_split, open_split_popup)

    # Column 2 — Post-processing
    post_frame = ttk.Labelframe(flags_frame, text="Post-processing")
    post_frame.grid(row=0, column=2, sticky="nsew", padx=(4, 0), pady=2)
    post_frame.columnconfigure(1, weight=1)
    add_option_row(post_frame, 0, "Shallow waters", shallow_flag, open_shallow_water_popup)
    anomaly_search_info_button, anomaly_search_checkbutton = add_option_row(
        post_frame, 1,
        "Correct steep false-deep bathymetry",
        anomaly_search_flag,
        open_anomaly_search_popup,
    )

    def update_initial_guess_controls(*_args):
        if not optimize_initial_guesses_flag.get():
            five_initial_guess_testing_flag.set(False)
            initial_guess_debug_flag.set(False)

    def update_split_controls(*_args):
        return

    def update_relaxed_controls(*_args):
        if not relaxed.get():
            fully_relaxed_flag.set(False)

    optimize_initial_guesses_flag.trace_add("write", update_initial_guess_controls)
    allow_split.trace_add("write", update_split_controls)
    relaxed.trace_add("write", update_relaxed_controls)
    update_initial_guess_controls()
    update_split_controls()
    update_relaxed_controls()

    # ---- Input Bathymetry section (params_tab row 2) ----
    bathy_frame = ttk.Labelframe(params_tab, text="Input Bathymetry")
    bathy_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))
    bathy_frame.columnconfigure(1, weight=1)

    # Row 0: Estimate (default)
    ttk.Radiobutton(
        bathy_frame, text="Estimate from image (inverse model)",
        value="estimate", variable=bathy_source,
    ).grid(row=0, column=0, columnspan=3, sticky=W, pady=(2, 0))

    # Row 1: EMODnet
    ttk.Radiobutton(
        bathy_frame, text="EMODnet (bundled 2024 mosaic)",
        value="emodnet", variable=bathy_source,
    ).grid(row=1, column=0, columnspan=3, sticky=W)

    # Row 2: User defined  [Browse]  [filename]
    ttk.Radiobutton(
        bathy_frame, text="User defined",
        value="user", variable=bathy_source,
    ).grid(row=2, column=0, sticky=W)
    bathy_browse_btn = ttk.Button(
        bathy_frame, text="Browse…", width=9,
        command=lambda: _on_bathy_browse(),
    )
    bathy_browse_btn.grid(row=2, column=1, sticky="w", padx=(8, 0))
    ttk.Label(bathy_frame, textvariable=bathy_info_var, foreground="gray").grid(
        row=2, column=2, sticky="w", padx=(8, 0),
    )

    # Rows 3-4: correction / tolerance — visible only when a reference file is used
    bathy_detail_frame = ttk.Frame(bathy_frame)
    bathy_detail_frame.grid(row=3, column=0, columnspan=3, sticky="ew", padx=(16, 0), pady=(6, 2))
    bathy_detail_frame.columnconfigure(1, weight=0)

    ttk.Label(bathy_detail_frame, text="Water level correction (m)").grid(row=0, column=0, sticky=W)
    ttk.Entry(bathy_detail_frame, textvariable=bathy_correction, width=10).grid(row=0, column=1, sticky="w", padx=(8, 0))
    ttk.Label(bathy_detail_frame, text="Depth bounds around bathy (\u00b1 m)").grid(row=1, column=0, sticky=W, pady=(4, 0))
    ttk.Entry(bathy_detail_frame, textvariable=bathy_tolerance, width=10).grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(4, 0))

    def _on_bathy_browse():
        path = askopenfilename(
            parent=root,
            title="Choose bathymetry GeoTIFF",
            filetypes=[("GeoTIFF", "*.tif *.tiff"), ("All files", "*.*")],
        )
        if path:
            bathy_path_var.set(path)
            bathy_info_var.set(os.path.basename(path))
            bathy_source.set("user")
        elif bathy_source.get() == "user":
            bathy_source.set("estimate")

    def _sync_bathy_source(*_args):
        src = bathy_source.get()
        if src == "estimate":
            bathy_mode.set("estimate")
            use_emodnet_var.set(False)
            user_defined_var.set(False)
            bathy_path_var.set("")
            bathy_info_var.set("")
            bathy_detail_frame.grid_remove()
            bathy_browse_btn.state(["disabled"])
        elif src == "emodnet":
            bathy_mode.set("input")
            use_emodnet_var.set(True)
            user_defined_var.set(False)
            emodnet_path = _resolve_bundled_resource(cwd, os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"))
            bathy_path_var.set(emodnet_path)
            bathy_info_var.set("EMODnet: E4_2024.tif")
            bathy_detail_frame.grid()
            bathy_browse_btn.state(["disabled"])
        else:  # "user"
            bathy_mode.set("input")
            use_emodnet_var.set(False)
            user_defined_var.set(True)
            bathy_detail_frame.grid()
            bathy_browse_btn.state(["!disabled"])
            if not bathy_path_var.get():
                _on_bathy_browse()

    bathy_source.trace_add("write", _sync_bathy_source)
    _sync_bathy_source()  # apply initial state

    params_frame = ttk.Labelframe(params_tab, text="Parameter Bounds (min / max)")
    params_frame.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
    for i in range(3):
        params_frame.columnconfigure(i, weight=1)

    ttk.Label(params_frame, text="").grid(row=0, column=0, sticky=W)
    ttk.Label(params_frame, text="Min").grid(row=0, column=1, sticky=W)
    ttk.Label(params_frame, text="Max").grid(row=0, column=2, sticky=W)

    chl_min_var = StringVar(value="0.01")
    chl_max_var = StringVar(value="0.16")
    cdom_min_var = StringVar(value="0.0005")
    cdom_max_var = StringVar(value="0.01")
    nap_min_var = StringVar(value="0.2")
    nap_max_var = StringVar(value="1.5")
    depth_min_var = StringVar(value="0.1")
    depth_max_var = StringVar(value="30")
    sub1_min_var = StringVar(value="0")
    sub1_max_var = StringVar(value="1")
    sub2_min_var = StringVar(value="0")
    sub2_max_var = StringVar(value="1")
    sub3_min_var = StringVar(value="0")
    sub3_max_var = StringVar(value="1")

    ttk.Label(params_frame, text="CHL").grid(row=1, column=0, sticky=W)
    chl_min_entry = ttk.Entry(params_frame, textvariable=chl_min_var, justify="right")
    chl_min_entry.grid(row=1, column=1, sticky="ew", padx=(0, 6))
    chl_max_entry = ttk.Entry(params_frame, textvariable=chl_max_var, justify="right")
    chl_max_entry.grid(row=1, column=2, sticky="ew")

    ttk.Label(params_frame, text="CDOM").grid(row=2, column=0, sticky=W)
    cdom_min_entry = ttk.Entry(params_frame, textvariable=cdom_min_var, justify="right")
    cdom_min_entry.grid(row=2, column=1, sticky="ew", padx=(0, 6))
    cdom_max_entry = ttk.Entry(params_frame, textvariable=cdom_max_var, justify="right")
    cdom_max_entry.grid(row=2, column=2, sticky="ew")

    ttk.Label(params_frame, text="NAP").grid(row=3, column=0, sticky=W)
    nap_min_entry = ttk.Entry(params_frame, textvariable=nap_min_var, justify="right")
    nap_min_entry.grid(row=3, column=1, sticky="ew", padx=(0, 6))
    nap_max_entry = ttk.Entry(params_frame, textvariable=nap_max_var, justify="right")
    nap_max_entry.grid(row=3, column=2, sticky="ew")

    ttk.Label(params_frame, text="Depth").grid(row=4, column=0, sticky=W)
    depth_min_entry = ttk.Entry(params_frame, textvariable=depth_min_var, justify="right")
    depth_min_entry.grid(row=4, column=1, sticky="ew", padx=(0, 6))
    depth_max_entry = ttk.Entry(params_frame, textvariable=depth_max_var, justify="right")
    depth_max_entry.grid(row=4, column=2, sticky="ew")

    label_sub1 = ttk.Label(params_frame, text="Substrate 1")
    label_sub1.grid(row=5, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=sub1_min_var, justify="right").grid(row=5, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=sub1_max_var, justify="right").grid(row=5, column=2, sticky="ew")

    label_sub2 = ttk.Label(params_frame, text="Substrate 2")
    label_sub2.grid(row=6, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=sub2_min_var, justify="right").grid(row=6, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=sub2_max_var, justify="right").grid(row=6, column=2, sticky="ew")

    label_sub3 = ttk.Label(params_frame, text="Substrate 3")
    label_sub3.grid(row=7, column=0, sticky=W)
    sub3_min_entry = ttk.Entry(params_frame, textvariable=sub3_min_var, justify="right")
    sub3_min_entry.grid(row=7, column=1, sticky="ew", padx=(0, 6))
    sub3_max_entry = ttk.Entry(params_frame, textvariable=sub3_max_var, justify="right")
    sub3_max_entry.grid(row=7, column=2, sticky="ew")

    deep_water_frame = ttk.LabelFrame(params_tab, text="Deep-water priors")
    deep_water_frame.grid(row=1, column=0, sticky="nsew", padx=4, pady=(0, 4))
    deep_water_frame.columnconfigure(0, weight=0)
    deep_water_frame.columnconfigure(1, weight=0)
    deep_water_frame.columnconfigure(2, weight=1)

    ttk.Button(deep_water_frame, text="Select deep-water polygons", command=open_deep_water_popup).grid(row=0, column=0, sticky="w")
    ttk.Button(deep_water_frame, text="Clear", command=clear_deep_water_selection).grid(row=0, column=1, sticky="w", padx=(6, 0))
    ttk.Checkbutton(
        deep_water_frame,
        text="Use mean ± sd as bounds instead of fixed values",
        variable=deep_water_use_sd_var,
    ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))
    ttk.Label(
        deep_water_frame,
        textvariable=deep_water_summary_var,
        wraplength=620,
        justify="left",
    ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))

    deep_water_bound_entries = (
        chl_min_entry,
        chl_max_entry,
        cdom_min_entry,
        cdom_max_entry,
        nap_min_entry,
        nap_max_entry,
    )

    def _update_deep_water_parameter_controls():
        deep_water_active = bool(deep_water_selection and (deep_water_selection.get("polygons") or []))
        for widget in deep_water_bound_entries:
            _set_widget_enabled(widget, not deep_water_active)

    for tracked_var in (
        input_image_var,
        output_folder_var,
        chl_min_var,
        chl_max_var,
        cdom_min_var,
        cdom_max_var,
        nap_min_var,
        nap_max_var,
        depth_min_var,
        depth_max_var,
        sub1_min_var,
        sub1_max_var,
        sub2_min_var,
        sub2_max_var,
        sub3_min_var,
        sub3_max_var,
        chunk_rows,
        bathy_path_var,
        bathy_correction,
        bathy_tolerance,
    ):
        tracked_var.trace_add("write", update_run_button_state)
    deep_water_use_sd_var.trace_add("write", _refresh_deep_water_summary)
    input_image_var.trace_add("write", _track_input_field_change)
    output_folder_var.trace_add("write", _track_output_field_change)
    input_image_var.trace_add("write", _sync_input_files_with_entry)
    input_image_var.trace_add("write", update_crop_button_state)
    allow_split.trace_add("write", update_run_button_state)
    bathy_mode.trace_add("write", update_run_button_state)

    def update_depth_state(*_args):
        if bathy_mode.get() == "input":
            try:
                depth_min_entry.state(["disabled"])
                depth_max_entry.state(["disabled"])
            except Exception:
                depth_min_entry.configure(state="disabled")
                depth_max_entry.configure(state="disabled")
        else:
            try:
                depth_min_entry.state(["!disabled"])
                depth_max_entry.state(["!disabled"])
            except Exception:
                depth_min_entry.configure(state="normal")
                depth_max_entry.configure(state="normal")

    bathy_mode.trace_add("write", update_depth_state)
    update_depth_state()
    _update_deep_water_parameter_controls()

    def load_previous_run_settings():
        nonlocal template_config, spectral_library, compiled_siop, compiled_sensor, input_files

        xml_path = askopenfilename(
            parent=root,
            title="Load previous run log XML",
            filetypes=[("Run log XML", "*.xml"), ("All files", "*.*")],
        )
        if not xml_path:
            return

        try:
            xml_root = ET.parse(xml_path).getroot()

            new_template_config = template_config
            new_spectral_library = spectral_library
            new_selected_target_names = list(selected_target_names)
            new_scalar_values = dict(scalar_values)
            new_spectrum_override_paths = dict(spectrum_override_paths)
            new_sensor_name = sensor_state["sensor_name"]
            new_sensor_indices = {
                sensor_name: list(indices)
                for sensor_name, indices in sensor_state["selected_indices"].items()
            }
            new_sensor_mapping_configs = {}

            siop_popup_node = xml_root.find("siop_popup")
            if siop_popup_node is not None:
                template_source = _resolve_bundled_resource(cwd, _xml_find_text(siop_popup_node, "template_source"))
                if template_source and os.path.exists(template_source):
                    new_template_config = siop_config.load_template_config(template_source)

                spectral_library_source = _xml_find_text(siop_popup_node, "spectral_library")
                if spectral_library_source and os.path.exists(spectral_library_source):
                    new_spectral_library = siop_config.load_spectral_library(spectral_library_source)

                raw_selected_targets = _xml_find_items(siop_popup_node, "selected_targets")
                if not raw_selected_targets:
                    raw_selected_targets = [
                        name for name in _xml_find_items(siop_popup_node, "xml_substrate_names")
                        if siop_config.clean_label(name) != siop_config.UNUSED_SUBSTRATE_NAME
                    ]
                matched_targets = []
                seen_targets = set()
                for raw_name in raw_selected_targets:
                    matched_name = _match_clean_name(raw_name, new_spectral_library["names"])
                    if matched_name and matched_name not in seen_targets:
                        matched_targets.append(matched_name)
                        seen_targets.add(matched_name)
                if matched_targets:
                    new_selected_target_names = matched_targets

                for key, _label, _required in siop_config.SIOP_SCALAR_FIELDS:
                    loaded_value = _xml_find_text(siop_popup_node, key)
                    if loaded_value is not None:
                        new_scalar_values[key] = loaded_value

                for spectrum_key in ("a_water", "a_ph_star"):
                    source_key = f"{spectrum_key}_source"
                    source_path = _resolve_bundled_resource(cwd, _xml_find_text(siop_popup_node, source_key, ""))
                    if source_path and os.path.exists(source_path) and not _paths_equivalent(source_path, new_template_config["template_path"]):
                        new_spectrum_override_paths[spectrum_key] = source_path
                    else:
                        new_spectrum_override_paths[spectrum_key] = ""

            compiled_siop_candidate = siop_config.build_siop_config(
                new_template_config,
                new_spectral_library,
                new_selected_target_names,
                new_scalar_values,
                spectrum_override_paths=new_spectrum_override_paths,
            )

            sensor_popup_node = xml_root.find("sensor_popup")
            if sensor_popup_node is not None:
                requested_sensor_name = _xml_find_text(sensor_popup_node, "sensor_name", new_sensor_name)
                template_source = _resolve_bundled_resource(cwd, _xml_find_text(sensor_popup_node, "sensor_template_source", ""))
                if template_source and os.path.exists(template_source):
                    sensor_templates[requested_sensor_name] = sensor_config.load_sensor_template(template_source, requested_sensor_name)
                if requested_sensor_name not in sensor_templates:
                    raise ValueError(f"No sensor template is available for '{requested_sensor_name}'.")
                selected_centers = _xml_find_items(sensor_popup_node, "selected_band_centers")
                matched_indices = _match_band_indices_by_center(sensor_templates[requested_sensor_name], selected_centers)
                if not matched_indices:
                    matched_indices = sensor_config.default_selected_band_indices(sensor_templates[requested_sensor_name])
                new_sensor_name = requested_sensor_name
                new_sensor_indices[new_sensor_name] = matched_indices
                mapping_enabled = _parse_bool_text(
                    _xml_find_text(sensor_popup_node, "sensor_band_mapping_enabled"),
                    False,
                )
                if mapping_enabled:
                    source_band_labels = _xml_find_items(sensor_popup_node, "sensor_band_mapping_source_band_labels")
                    source_band_wavelengths_raw = _xml_find_items(sensor_popup_node, "sensor_band_mapping_source_band_wavelengths")
                    sensor_band_indices_raw = _xml_find_items(sensor_popup_node, "sensor_band_mapping_sensor_band_indices")
                    sensor_band_centers_raw = _xml_find_items(sensor_popup_node, "sensor_band_mapping_sensor_band_centers")
                    image_band_indices_raw = _xml_find_items(sensor_popup_node, "sensor_band_mapping_image_band_indices")
                    image_band_labels = _xml_find_items(sensor_popup_node, "sensor_band_mapping_image_band_labels")
                    image_band_wavelengths_raw = _xml_find_items(sensor_popup_node, "sensor_band_mapping_image_band_wavelengths")
                    try:
                        new_sensor_mapping_configs[new_sensor_name] = {
                            "mode": _xml_find_text(sensor_popup_node, "sensor_band_mapping_mode", "manual") or "manual",
                            "tolerance_nm": float(_xml_find_text(sensor_popup_node, "sensor_band_mapping_tolerance_nm", "10") or "10"),
                            "source_kind": _xml_find_text(sensor_popup_node, "sensor_band_mapping_source_kind", "") or "",
                            "source_name": _xml_find_text(sensor_popup_node, "sensor_band_mapping_source_name", "") or "",
                            "source_band_labels": list(source_band_labels),
                            "source_band_wavelengths": [
                                None if str(value).strip().lower() in {"", "none", "nan"} else float(value)
                                for value in source_band_wavelengths_raw
                            ],
                            "sensor_band_indices": [int(float(value)) for value in sensor_band_indices_raw],
                            "sensor_band_centers": [float(value) for value in sensor_band_centers_raw],
                            "image_band_indices": [int(float(value)) for value in image_band_indices_raw],
                            "image_band_labels": list(image_band_labels),
                            "image_band_wavelengths": [
                                None if str(value).strip().lower() in {"", "none", "nan"} else float(value)
                                for value in image_band_wavelengths_raw
                            ],
                        }
                    except Exception:
                        new_sensor_mapping_configs.pop(new_sensor_name, None)
                else:
                    new_sensor_mapping_configs.pop(new_sensor_name, None)

            compiled_sensor_candidate = sensor_config.build_sensor_config(
                sensor_templates[new_sensor_name],
                new_sensor_indices.get(new_sensor_name, []),
                band_mapping=new_sensor_mapping_configs.get(new_sensor_name),
            )
        except Exception as exc:
            messagebox.showerror("Invalid run log", f"Unable to load settings from:\n{xml_path}\n\n{exc}")
            return

        template_config = new_template_config
        spectral_library = new_spectral_library
        selected_target_names[:] = list(compiled_siop_candidate["actual_selected_targets"])
        scalar_values.clear()
        scalar_values.update(new_scalar_values)
        spectrum_override_paths.clear()
        spectrum_override_paths.update(new_spectrum_override_paths)
        sensor_state["sensor_name"] = compiled_sensor_candidate["sensor_name"]
        sensor_state["selected_indices"].update(new_sensor_indices)
        sensor_state["selected_indices"][compiled_sensor_candidate["sensor_name"]] = list(compiled_sensor_candidate["selected_indices"])
        sensor_state["band_mapping_configs"] = {
            sensor_name: _clone_sensor_band_mapping_config(mapping)
            for sensor_name, mapping in new_sensor_mapping_configs.items()
            if mapping
        }
        compiled_siop = None
        compiled_sensor = None
        show_overwrite_warning = bool(io_change_state["modified_since_load"])

        io_change_state["suspend_tracking"] = True

        try:
            loaded_images = _xml_find_items(xml_root, "images")
            if not loaded_images:
                loaded_image = _xml_find_text(xml_root, "image", "")
                if loaded_image:
                    loaded_images = [loaded_image]
            if loaded_images:
                input_files = list(loaded_images)
                input_image_var.set(_display_input_selection(input_files))

            loaded_output_folder = _xml_find_text(xml_root, "output_folder", "")
            if not loaded_output_folder:
                loaded_output_folder = _infer_output_folder_from_output_file(_xml_find_text(xml_root, "output_file", ""))
            if loaded_output_folder:
                output_folder_var.set(loaded_output_folder)

            loaded_crop_enabled = _parse_bool_text(_xml_find_text(xml_root, "crop_enabled"), False)
            if loaded_crop_enabled:
                try:
                    bbox = None
                    min_lon_text = _xml_find_text(xml_root, "crop_min_lon")
                    max_lon_text = _xml_find_text(xml_root, "crop_max_lon")
                    min_lat_text = _xml_find_text(xml_root, "crop_min_lat")
                    max_lat_text = _xml_find_text(xml_root, "crop_max_lat")
                    if None not in (min_lon_text, max_lon_text, min_lat_text, max_lat_text):
                        bbox = {
                            "min_lon": float(min_lon_text),
                            "max_lon": float(max_lon_text),
                            "min_lat": float(min_lat_text),
                            "max_lat": float(max_lat_text),
                        }
                    loaded_mask_path = _resolve_bundled_resource(cwd, _xml_find_text(xml_root, "crop_mask_path", "") or "")
                    _set_crop_selection({
                        "bbox": bbox,
                        "mask_path": loaded_mask_path,
                        "source_path": loaded_images[0] if loaded_images else _xml_find_text(xml_root, "crop_source_image", ""),
                    })
                except Exception:
                    clear_crop_selection()
            else:
                clear_crop_selection()

            loaded_deep_water_enabled = _parse_bool_text(_xml_find_text(xml_root, "deep_water_enabled"), False)
            deep_water_use_sd_var.set(_parse_bool_text(_xml_find_text(xml_root, "deep_water_use_sd_bounds"), False))
            if loaded_deep_water_enabled:
                try:
                    deep_water_polygons_json = _xml_find_text(xml_root, "deep_water_polygons_json", "") or "[]"
                    loaded_polygons = json.loads(deep_water_polygons_json)
                    _set_deep_water_selection({
                        "polygons": loaded_polygons,
                        "source_path": loaded_images[0] if loaded_images else _xml_find_text(xml_root, "deep_water_source_image", ""),
                    })
                except Exception:
                    clear_deep_water_selection()
            else:
                clear_deep_water_selection()
        finally:
            io_change_state["suspend_tracking"] = False
            io_change_state["modified_since_load"] = False

        above_rrs_flag.set(_parse_bool_text(_xml_find_text(xml_root, "rrs_flag"), above_rrs_flag.get()))
        reflectance_input_flag.set(_parse_bool_text(_xml_find_text(xml_root, "reflectance_input"), reflectance_input_flag.get()))
        shallow_flag.set(_parse_bool_text(_xml_find_text(xml_root, "shallow"), shallow_flag.get()))
        optimize_initial_guesses_flag.set(_parse_bool_text(_xml_find_text(xml_root, "optimize_initial_guesses"), optimize_initial_guesses_flag.get()))
        five_initial_guess_testing_flag.set(_parse_bool_text(_xml_find_text(xml_root, "use_five_initial_guesses"), five_initial_guess_testing_flag.get()))
        initial_guess_debug_flag.set(_parse_bool_text(_xml_find_text(xml_root, "initial_guess_debug"), initial_guess_debug_flag.get()))
        pp.set(_parse_bool_text(_xml_find_text(xml_root, "post_processing", _xml_find_text(xml_root, "pproc")), pp.get()))
        fully_relaxed_flag.set(_parse_bool_text(_xml_find_text(xml_root, "fully_relaxed"), fully_relaxed_flag.get()))
        output_modeled_reflectance_flag.set(_parse_bool_text(_xml_find_text(xml_root, "output_modeled_reflectance"), output_modeled_reflectance_flag.get()))
        anomaly_search_flag.set(_parse_bool_text(_xml_find_text(xml_root, "anomaly_search_enabled"), anomaly_search_flag.get()))
        relaxed.set(_parse_bool_text(_xml_find_text(xml_root, "relaxed"), relaxed.get()))
        allow_split.set(_parse_bool_text(_xml_find_text(xml_root, "allow_split"), allow_split.get()))
        loaded_chunk_rows = _xml_find_text(xml_root, "split_chunk_rows", chunk_rows.get())
        chunk_rows.set("" if loaded_chunk_rows is None else str(loaded_chunk_rows))

        loaded_output_format = (_xml_find_text(xml_root, "output_format", output_format.get()) or output_format.get()).lower()
        if loaded_output_format in {"netcdf", "geotiff", "both"}:
            output_format.set(loaded_output_format)

        loaded_pmin = _xml_find_items(xml_root, "pmin")
        loaded_pmax = _xml_find_items(xml_root, "pmax")
        if len(loaded_pmin) >= 7 and len(loaded_pmax) >= 7:
            chl_min_var.set(loaded_pmin[0]); chl_max_var.set(loaded_pmax[0])
            cdom_min_var.set(loaded_pmin[1]); cdom_max_var.set(loaded_pmax[1])
            nap_min_var.set(loaded_pmin[2]); nap_max_var.set(loaded_pmax[2])
            depth_min_var.set(loaded_pmin[3]); depth_max_var.set(loaded_pmax[3])
            sub1_min_var.set(loaded_pmin[4]); sub1_max_var.set(loaded_pmax[4])
            sub2_min_var.set(loaded_pmin[5]); sub2_max_var.set(loaded_pmax[5])
            sub3_saved_bounds["min"] = loaded_pmin[6]
            sub3_saved_bounds["max"] = loaded_pmax[6]
            sub3_min_var.set(loaded_pmin[6]); sub3_max_var.set(loaded_pmax[6])

        anomaly_search_settings["export_local_moran_raster"] = _parse_bool_text(
            _xml_find_text(xml_root, "anomaly_search_export_local_moran_raster"),
            anomaly_search_settings["export_local_moran_raster"],
        )
        anomaly_search_settings["export_suspicious_binary_raster"] = _parse_bool_text(
            _xml_find_text(xml_root, "anomaly_search_export_suspicious_binary_raster"),
            anomaly_search_settings["export_suspicious_binary_raster"],
        )
        anomaly_search_settings["export_interpolated_rasters"] = _parse_bool_text(
            _xml_find_text(xml_root, "anomaly_search_export_interpolated_rasters"),
            anomaly_search_settings["export_interpolated_rasters"],
        )

        use_bathy = _parse_bool_text(_xml_find_text(xml_root, "use_bathy"), False)
        default_emodnet_path = _resolve_bundled_resource(cwd, os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"))
        if use_bathy:
            loaded_bathy_path = _resolve_bundled_resource(cwd, _xml_find_text(xml_root, "bathy_path", "") or "")
            loaded_bathy_reference = (_xml_find_text(xml_root, "bathy_reference", "depth") or "depth").strip().lower()
            bathy_mode.set("input")
            bathy_path_var.set(loaded_bathy_path)
            bathy_correction.set(str(_xml_find_text(xml_root, "bathy_correction_m", bathy_correction.get()) or "0"))
            bathy_tolerance.set(str(_xml_find_text(xml_root, "bathy_tolerance_m", bathy_tolerance.get()) or "0"))
            is_emodnet = _paths_equivalent(loaded_bathy_path, default_emodnet_path) and loaded_bathy_reference == "depth"
            use_emodnet_var.set(is_emodnet)
            user_defined_var.set(not is_emodnet and bool(loaded_bathy_path))
            if is_emodnet:
                bathy_info_var.set("EMODnet: E4_2024.tif")
                bathy_source.set("emodnet")
            else:
                bathy_info_var.set(os.path.basename(loaded_bathy_path) if loaded_bathy_path else "")
                bathy_source.set("user")
        else:
            bathy_mode.set("estimate")
            bathy_path_var.set("")
            bathy_info_var.set("")
            bathy_correction.set("0")
            bathy_tolerance.set("0")
            use_emodnet_var.set(False)
            user_defined_var.set(False)
            bathy_source.set("estimate")

        update_substrate_ui()
        update_sensor_ui()
        update_initial_guess_controls()

        if show_overwrite_warning:
            messagebox.showwarning(
                "Settings loaded",
                "Run settings were loaded from the selected log XML.\n\n"
                "The input image(s) and processing extent were modified to match the loaded XML. "
                "The output folder and processing options were also restored from that run.\n\n"
                "If you want to apply these settings to another scene, change the input image and crop or mask before starting a new run.",
                parent=root,
            )

    # ---- Output section: 2 columns ----
    output_section_frame = ttk.Labelframe(input_tab, text="Output")
    output_section_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=4)
    output_section_frame.columnconfigure(0, weight=1)
    output_section_frame.columnconfigure(1, weight=1)

    # Column 0 — Format
    fmt_frame = ttk.Labelframe(output_section_frame, text="Format")
    fmt_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=2)
    fmt_frame.columnconfigure(0, weight=1)
    output_format = tk.StringVar(value="both")
    ttk.Radiobutton(fmt_frame, text="NetCDF", value="netcdf", variable=output_format).grid(row=0, column=0, sticky=W)
    ttk.Radiobutton(fmt_frame, text="GeoTIFF", value="geotiff", variable=output_format).grid(row=1, column=0, sticky=W)
    ttk.Radiobutton(fmt_frame, text="Both", value="both", variable=output_format).grid(row=2, column=0, sticky=W)

    # Column 1 — Options
    out_opts_frame = ttk.Labelframe(output_section_frame, text="Options")
    out_opts_frame.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=2)
    out_opts_frame.columnconfigure(1, weight=1)
    add_option_row(out_opts_frame, 0, "Output modeled reflectance", output_modeled_reflectance_flag, open_modeled_reflectance_popup)
    add_option_row(out_opts_frame, 1, "Output spectral parameters", pp, open_post_processing_popup)

    def _current_parameter_bounds():
        return (
            [
                float(chl_min_var.get()),
                float(cdom_min_var.get()),
                float(nap_min_var.get()),
                float(depth_min_var.get()),
                float(sub1_min_var.get()),
                float(sub2_min_var.get()),
                float(sub3_min_var.get()),
            ],
            [
                float(chl_max_var.get()),
                float(cdom_max_var.get()),
                float(nap_max_var.get()),
                float(depth_max_var.get()),
                float(sub1_max_var.get()),
                float(sub2_max_var.get()),
                float(sub3_max_var.get()),
            ],
        )

    def _run_version_suffix(index):
        return f"_settings{int(index):02d}"

    def _run_version_summary(version):
        pmin_values = version.get("pmin") or ["", "", "", ""]
        pmax_values = version.get("pmax") or ["", "", "", ""]
        depth_text = f"Depth {pmin_values[3]:g}-{pmax_values[3]:g} m" if len(pmin_values) > 3 and len(pmax_values) > 3 else "Depth bounds unavailable"
        enabled_features = []
        if version.get("anomaly_search_settings", {}).get("enabled"):
            enabled_features.append("anomaly search")
        if version.get("optimize_initial_guesses"):
            enabled_features.append("initial guesses")
        if version.get("relaxed"):
            enabled_features.append("relaxed")
        if version.get("shallow"):
            enabled_features.append("shallow")
        feature_text = ", ".join(enabled_features) if enabled_features else "standard"
        return f"{version.get('label', 'Settings')}: {depth_text}; {feature_text}; {version.get('output_format', 'both')}"

    def _capture_current_run_version(label=None, suffix=None):
        validation_error = _get_form_validation_error()
        if validation_error is not None:
            raise ValueError(validation_error)
        compiled_siop_candidate = build_current_siop()
        compiled_sensor_candidate = build_current_sensor()
        pmin_values, pmax_values = _current_parameter_bounds()
        current_bathy_path = ""
        bathy_reference = "depth"
        bathy_correction_m = 0.0
        bathy_tolerance_m = 0.0
        if bathy_mode.get() == "input":
            current_bathy_path = bathy_path_var.get() or _resolve_bundled_resource(cwd, os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"))
            bathy_reference = "hydrographic_zero" if user_defined_var.get() else "depth"
            bathy_correction_m = float(bathy_correction.get() or 0.0)
            bathy_tolerance_m = float(bathy_tolerance.get() or 0.0)

        version_index = len(saved_run_versions) + 1
        return {
            "label": label or f"Settings {version_index:02d}",
            "suffix": _run_version_suffix(version_index) if suffix is None else suffix,
            "compiled_siop": compiled_siop_candidate,
            "compiled_sensor": compiled_sensor_candidate,
            "siop_log_payload": siop_config.build_log_payload(compiled_siop_candidate, template_config, spectral_library),
            "sensor_log_payload": sensor_config.build_log_payload(compiled_sensor_candidate),
            "pmin": pmin_values,
            "pmax": pmax_values,
            "rrs_flag": bool(above_rrs_flag.get()),
            "reflectance_input": bool(reflectance_input_flag.get()),
            "relaxed": bool(relaxed.get()),
            "shallow": bool(shallow_flag.get()),
            "optimize_initial_guesses": bool(optimize_initial_guesses_flag.get()),
            "use_five_initial_guesses": bool(five_initial_guess_testing_flag.get()),
            "initial_guess_debug": bool(initial_guess_debug_flag.get()),
            "fully_relaxed": bool(fully_relaxed_flag.get()),
            "output_modeled_reflectance": bool(output_modeled_reflectance_flag.get()),
            "anomaly_search_settings": {
                "enabled": bool(anomaly_search_flag.get()),
                **copy.deepcopy(anomaly_search_settings),
            },
            "post_processing": bool(pp.get()),
            "output_format": output_format.get(),
            "allow_split": bool(allow_split.get()),
            "split_chunk_rows": chunk_rows.get().strip(),
            "crop_selection": copy.deepcopy(crop_selection),
            "deep_water_selection": copy.deepcopy(deep_water_selection),
            "deep_water_use_sd_bounds": bool(deep_water_use_sd_var.get()),
            "use_bathy": bathy_mode.get() == "input",
            "bathy_path": current_bathy_path,
            "bathy_reference": bathy_reference,
            "bathy_correction_m": bathy_correction_m,
            "bathy_tolerance_m": bathy_tolerance_m,
        }

    def _active_run_version_count():
        return max(1, len(saved_run_versions))

    def update_run_version_controls():
        count = _active_run_version_count()
        if run_button is not None:
            noun = "setting" if count == 1 else "settings"
            run_button.configure(text=f"Run ({count} {noun})")
        if versions_button is not None:
            _set_widget_enabled(versions_button, bool(saved_run_versions))

    def save_current_run_settings():
        try:
            version = _capture_current_run_version()
        except Exception as exc:
            messagebox.showerror("Cannot save settings", str(exc), parent=root)
            return
        saved_run_versions.append(version)
        update_run_version_controls()
        messagebox.showinfo("Settings saved", f"Saved {version['label']}.", parent=root)

    def _run_version_signature(version):
        signature_keys = (
            "siop_log_payload",
            "sensor_log_payload",
            "pmin",
            "pmax",
            "rrs_flag",
            "reflectance_input",
            "relaxed",
            "shallow",
            "optimize_initial_guesses",
            "use_five_initial_guesses",
            "initial_guess_debug",
            "fully_relaxed",
            "output_modeled_reflectance",
            "anomaly_search_settings",
            "post_processing",
            "output_format",
            "allow_split",
            "split_chunk_rows",
            "crop_selection",
            "deep_water_selection",
            "deep_water_use_sd_bounds",
            "use_bathy",
            "bathy_path",
            "bathy_reference",
            "bathy_correction_m",
            "bathy_tolerance_m",
        )

        def normalise(value):
            if isinstance(value, dict):
                return {str(key): normalise(value[key]) for key in sorted(value.keys(), key=str)}
            if isinstance(value, (list, tuple)):
                return [normalise(item) for item in value]
            if isinstance(value, np.generic):
                return value.item()
            return value

        comparable = {
            key: normalise(version.get(key))
            for key in signature_keys
        }
        return json.dumps(comparable, sort_keys=True, separators=(",", ":"), default=str)

    def _run_version_already_saved(version):
        current_signature = _run_version_signature(version)
        return any(_run_version_signature(saved_version) == current_signature for saved_version in saved_run_versions)

    def open_run_versions_popup():
        if not saved_run_versions:
            messagebox.showinfo("Saved settings", "No saved setting versions yet.", parent=root)
            return

        versions_popup = tk.Toplevel(root)
        versions_popup.title("Saved Run Settings")
        apply_window_size(
            versions_popup,
            preferred_size=(1120, 620),
            minsize=(900, 460),
            width_ratio=0.78,
            height_ratio=0.62,
            max_width_ratio=0.90,
            max_height_ratio=0.78,
        )
        versions_popup.transient(root)
        versions_popup.grab_set()
        versions_popup.columnconfigure(0, weight=1)
        versions_popup.rowconfigure(0, weight=1)

        container = ttk.Frame(versions_popup, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.columnconfigure(2, weight=2)
        container.rowconfigure(1, weight=1)

        ttk.Label(
            container,
            text="These saved setting versions will be run for every selected input image. Select one version to inspect it, or select exactly two versions to compare only the settings that differ.",
            wraplength=700,
            justify="left",
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

        versions_listbox = tk.Listbox(container, selectmode=tk.EXTENDED, exportselection=False, height=12)
        versions_listbox.grid(row=1, column=0, sticky="nsew")
        versions_scroll = ttk.Scrollbar(container, orient="vertical", command=versions_listbox.yview)
        versions_scroll.grid(row=1, column=1, sticky="ns")
        versions_listbox.configure(yscrollcommand=versions_scroll.set)

        detail_frame = ttk.Labelframe(container, text="Summary / comparison")
        detail_frame.grid(row=1, column=2, sticky="nsew", padx=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        detail_text = tk.Text(detail_frame, wrap="word", height=12, width=58, state="disabled")
        detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        detail_text.configure(yscrollcommand=detail_scroll.set)

        def _format_setting_value(value):
            if isinstance(value, bool):
                return "yes" if value else "no"
            if value is None:
                return ""
            if isinstance(value, float):
                return f"{value:g}"
            if isinstance(value, (list, tuple)):
                if not value:
                    return "none"
                shown = [_format_setting_value(item) for item in list(value)[:10]]
                suffix = "" if len(value) <= 10 else f", ... ({len(value)} total)"
                return ", ".join(shown) + suffix
            if isinstance(value, dict):
                return json.dumps(value, sort_keys=True)
            return str(value)

        def _version_setting_rows(version):
            rows = []

            def add(section, label, value):
                rows.append((section, label, _format_setting_value(value)))

            siop_payload = version.get("siop_log_payload") or {}
            sensor_payload = version.get("sensor_log_payload") or {}
            anomaly_search = version.get("anomaly_search_settings") or {}
            pmin_values = list(version.get("pmin") or [])
            pmax_values = list(version.get("pmax") or [])

            add("Run", "Label", version.get("label", ""))
            add("Run", "Output format", version.get("output_format", ""))
            add("Run", "Post-processing", version.get("post_processing", False))
            add("Run", "Output modeled reflectance", version.get("output_modeled_reflectance", False))
            add("Run", "Allow image splitting", version.get("allow_split", False))
            add("Run", "Rows per split chunk", version.get("split_chunk_rows", ""))

            add("Input", "Crop enabled", bool(version.get("crop_selection")))
            crop = version.get("crop_selection") or {}
            if crop.get("bbox"):
                bbox = crop["bbox"]
                add("Input", "Crop longitude", f"{bbox.get('min_lon', '')} to {bbox.get('max_lon', '')}")
                add("Input", "Crop latitude", f"{bbox.get('min_lat', '')} to {bbox.get('max_lat', '')}")
            add("Input", "Deep-water priors", bool(version.get("deep_water_selection")))
            add("Input", "Deep-water mean +/- sd bounds", version.get("deep_water_use_sd_bounds", False))

            add("Water & bottom", "Selected targets", siop_payload.get("selected_targets", []))
            add("Water & bottom", "Substrate names", siop_payload.get("xml_substrate_names", []))
            add("Water & bottom", "Water absorption source", siop_payload.get("a_water_source", ""))
            add("Water & bottom", "Chlorophyll absorption source", siop_payload.get("a_ph_star_source", ""))
            for key, label, _required in siop_config.SIOP_SCALAR_FIELDS:
                add("Water & bottom", label, siop_payload.get(key, ""))

            bound_labels = ["CHL", "CDOM", "NAP", "Depth", "Substrate 1", "Substrate 2", "Substrate 3"]
            substrate_names = siop_payload.get("xml_substrate_names") or []
            for index, name in enumerate(substrate_names[:3], start=4):
                if index < len(bound_labels):
                    bound_labels[index] = name
            for index, label in enumerate(bound_labels):
                min_value = pmin_values[index] if index < len(pmin_values) else ""
                max_value = pmax_values[index] if index < len(pmax_values) else ""
                add("Parameter bounds", label, f"{_format_setting_value(min_value)} to {_format_setting_value(max_value)}")

            add("Sensor", "Sensor name", sensor_payload.get("sensor_name", ""))
            add("Sensor", "Selected band count", sensor_payload.get("selected_band_count", ""))
            add("Sensor", "Selected band centers", sensor_payload.get("selected_band_centers", []))
            add("Sensor", "Band mapping enabled", sensor_payload.get("sensor_band_mapping_enabled", False))
            add("Sensor", "Band mapping mode", sensor_payload.get("sensor_band_mapping_mode", ""))

            add("Processing", "Above-water Rrs input", version.get("rrs_flag", True))
            add("Processing", "Reflectance input", version.get("reflectance_input", False))
            add("Processing", "Relaxed constraints", version.get("relaxed", False))
            add("Processing", "Fully relaxed", version.get("fully_relaxed", False))
            add("Processing", "Shallow water adjustment", version.get("shallow", False))
            add("Processing", "Optimise initial guesses", version.get("optimize_initial_guesses", False))
            add("Processing", "Use 5 initial guesses", version.get("use_five_initial_guesses", False))
            add("Processing", "Initial guess debug", version.get("initial_guess_debug", False))

            add("Bathymetry", "Use input bathymetry", version.get("use_bathy", False))
            add("Bathymetry", "Bathymetry path", version.get("bathy_path", ""))
            add("Bathymetry", "Bathymetry reference", version.get("bathy_reference", ""))
            add("Bathymetry", "Water level correction (m)", version.get("bathy_correction_m", ""))
            add("Bathymetry", "Depth bounds around bathy (m)", version.get("bathy_tolerance_m", ""))

            for key, value in anomaly_search.items():
                label = key.replace("_", " ")
                add("Anomaly search", label, value)

            return rows

        def _set_detail_text(text):
            detail_text.configure(state="normal")
            detail_text.delete("1.0", tk.END)
            detail_text.insert(tk.END, text)
            detail_text.configure(state="disabled")

        def _render_single_version(version):
            lines = []
            current_section = None
            for section, label, value in _version_setting_rows(version):
                if section != current_section:
                    if lines:
                        lines.append("")
                    lines.append(section)
                    lines.append("-" * len(section))
                    current_section = section
                lines.append(f"{label}: {value}")
            return "\n".join(lines)

        def _render_version_comparison(left, right):
            left_rows = _version_setting_rows(left)
            right_map = {
                (section, label): value
                for section, label, value in _version_setting_rows(right)
            }
            lines = [
                f"Different settings: {left.get('label', 'Version A')} vs {right.get('label', 'Version B')}",
                "",
            ]
            current_section = None
            difference_count = 0
            for section, label, left_value in left_rows:
                if section == "Run" and label == "Label":
                    continue
                key = (section, label)
                right_value = right_map.get(key, "")
                if left_value == right_value:
                    continue
                if section != current_section:
                    if difference_count:
                        lines.append("")
                    lines.append(section)
                    lines.append("-" * len(section))
                    current_section = section
                lines.append(f"{label}:")
                lines.append(f"  {left.get('label', 'Version A')}: {left_value}")
                lines.append(f"  {right.get('label', 'Version B')}: {right_value}")
                difference_count += 1
            if difference_count == 0:
                lines.append("No differences found.")
            return "\n".join(lines)

        def update_versions_detail(_event=None):
            selected = list(versions_listbox.curselection())
            if len(selected) == 1:
                _set_detail_text(_render_single_version(saved_run_versions[selected[0]]))
            elif len(selected) == 2:
                _set_detail_text(_render_version_comparison(
                    saved_run_versions[selected[0]],
                    saved_run_versions[selected[1]],
                ))
            elif not selected:
                _set_detail_text("Select one version to see its settings, or select two versions to compare differences.")
            else:
                _set_detail_text("Select exactly one version for a summary, or exactly two versions for a differences-only comparison.")

        def refresh_versions_list():
            versions_listbox.delete(0, tk.END)
            for version in saved_run_versions:
                versions_listbox.insert(tk.END, _run_version_summary(version))
            update_versions_detail()

        def delete_selected_versions():
            selected = list(versions_listbox.curselection())
            if not selected:
                return
            for index in sorted(selected, reverse=True):
                del saved_run_versions[index]
            for index, version in enumerate(saved_run_versions, start=1):
                version["label"] = f"Settings {index:02d}"
                version["suffix"] = _run_version_suffix(index)
            refresh_versions_list()
            update_run_version_controls()
            if not saved_run_versions:
                versions_popup.destroy()
            else:
                update_versions_detail()

        actions = ttk.Frame(container)
        actions.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(12, 0))
        actions.columnconfigure(0, weight=1)
        ttk.Button(actions, text="Delete selected", command=delete_selected_versions).grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Button(actions, text="Close", command=versions_popup.destroy).grid(row=0, column=2, sticky="e", padx=(8, 0))

        versions_listbox.bind("<<ListboxSelect>>", update_versions_detail)
        refresh_versions_list()
        center_window(versions_popup)
        versions_popup.wait_window()

    def validate_and_close():
        nonlocal compiled_siop, compiled_sensor, active_run_versions

        validation_error = _get_form_validation_error()
        if validation_error is not None:
            messagebox.showerror("Incomplete run settings", validation_error)
            return
        try:
            compiled_siop = build_current_siop()
        except Exception as exc:
            messagebox.showerror("Invalid Water & Bottom Settings", str(exc))
            return

        try:
            compiled_sensor = build_current_sensor()
        except Exception as exc:
            messagebox.showerror("Invalid sensor setup", str(exc))
            return

        try:
            current_version = _capture_current_run_version("Current settings", "")
        except Exception as exc:
            messagebox.showerror("Incomplete run settings", str(exc))
            return

        if saved_run_versions:
            active_run_versions = copy.deepcopy(saved_run_versions)
            if not _run_version_already_saved(current_version):
                response = messagebox.askyesnocancel(
                    "Unsaved current settings",
                    "The current settings have not been saved as a run version.\n\n"
                    "Yes: add the current settings to this batch run.\n"
                    "No: discard the current settings and run only the saved versions.\n"
                    "Cancel: return to the setup window.",
                    parent=root,
                )
                if response is None:
                    return
                if response:
                    current_version["label"] = f"Settings {len(active_run_versions) + 1:02d}"
                    current_version["suffix"] = _run_version_suffix(len(active_run_versions) + 1)
                    active_run_versions.append(current_version)
        else:
            active_run_versions = [current_version]
        root.destroy()

    actions = ttk.Frame(container)
    actions.grid(row=1, column=0, sticky="ew", padx=4, pady=(8, 0))
    actions.columnconfigure(0, weight=1)
    actions.columnconfigure(1, weight=0)
    actions.columnconfigure(2, weight=0)
    actions.columnconfigure(3, weight=0)
    actions.columnconfigure(4, weight=0)
    ttk.Button(actions, text="Cancel", command=on_close).grid(row=0, column=0, sticky="w")
    ttk.Button(actions, text="Load settings", command=load_previous_run_settings).grid(row=0, column=1, sticky="e", padx=(0, 8))
    ttk.Button(actions, text="Save current settings", command=save_current_run_settings).grid(row=0, column=2, sticky="e", padx=(0, 8))
    versions_button = ttk.Button(actions, text="Versions", command=open_run_versions_popup)
    versions_button.grid(row=0, column=3, sticky="e", padx=(0, 8))
    run_button = tk.Button(
        actions,
        text="Run",
        command=validate_and_close,
        width=18,
        padx=18,
        pady=8,
        bg=run_button_enabled_bg,
        activebackground=run_button_active_bg,
        fg=run_button_fg,
        activeforeground=run_button_fg,
        disabledforeground="#6f7f73",
        font=("Segoe UI", 12, "bold"),
        relief="raised",
        bd=1,
        cursor="hand2",
    )
    run_button.grid(row=0, column=4, sticky="e")

    update_substrate_ui()
    update_sensor_ui()
    update_crop_button_state()
    update_run_button_state()
    update_run_version_controls()
    root.protocol("WM_DELETE_WINDOW", on_close)

    # ---- React when the window is dragged to a different monitor ----
    _last_monitor_rect = [None]
    _monitor_check_id = [None]

    def _clamp_to_current_monitor():
        """
        Shrink and/or reposition the window so it fits inside the work area of
        whichever monitor it currently occupies.  Only acts when the monitor
        has actually changed since the last check, so routine resizes inside
        the same monitor are ignored.
        """
        scr_left, scr_top, scr_w, scr_h = _get_screen_info(root)
        cur_rect = (scr_left, scr_top, scr_w, scr_h)
        if cur_rect == _last_monitor_rect[0]:
            return
        _last_monitor_rect[0] = cur_rect

        max_w = max(960, int(scr_w * 0.94))
        max_h = max(600, int(scr_h * 0.90))
        new_w = min(root.winfo_width(), max_w)
        new_h = min(root.winfo_height(), max_h)

        # Clamp position so the window stays fully inside the work area
        wx = max(scr_left, min(root.winfo_x(), scr_left + scr_w - new_w))
        wy = max(scr_top,  min(root.winfo_y(), scr_top  + scr_h - new_h))

        root.geometry(f"{new_w}x{new_h}+{wx}+{wy}")
        root.minsize(min(960, new_w), min(600, new_h))

    def _on_root_configure(event):
        if event.widget is not root:
            return
        if _monitor_check_id[0]:
            root.after_cancel(_monitor_check_id[0])
        _monitor_check_id[0] = root.after(300, _clamp_to_current_monitor)

    root.bind("<Configure>", _on_root_configure)

    center_window(root)
    root.mainloop()

    if cancelled:
        return None

    if compiled_siop is None:
        try:
            compiled_siop = build_current_siop()
        except Exception:
            return None

    if compiled_sensor is None:
        try:
            compiled_sensor = build_current_sensor()
        except Exception:
            return None

    if not active_run_versions:
        active_run_versions = [_capture_current_run_version("Current settings", "")]

    file_list = input_files if input_files else ([input_image_var.get()] if input_image_var.get() else [])
    out_folder = output_folder_var.get()

    filename_im = os.path.basename(file_list[0]) if file_list else ""
    run_dir = os.path.join(out_folder, f"swampy_run_{year}{month}{day}_{hour}{minute}{second}")
    if not os.path.isdir(run_dir):
        os.makedirs(run_dir)
    input_base = os.path.splitext(filename_im)[0] if filename_im else f"run_{year}{month}{day}_{hour}{minute}{second}"
    multi_settings = len(active_run_versions) > 1 or bool(saved_run_versions)

    def _build_input_dict_for_version(version, file_iop_path, file_sensor_path, initial_ofile, version_index):
        version_crop = version.get("crop_selection")
        version_deep_water = version.get("deep_water_selection")
        version_anomaly_search = version.get("anomaly_search_settings", {})
        sensor_log_payload = version.get("sensor_log_payload", {})
        payload = {
            "image": file_list[0] if file_list else "",
            "images": list(file_list),
            "run_version_index": int(version_index),
            "run_version_count": int(len(active_run_versions)),
            "run_version_label": version.get("label", f"Settings {version_index:02d}"),
            "run_version_suffix": version.get("suffix", ""),
            "run_version_output_folder": os.path.dirname(initial_ofile),
            "crop_enabled": bool(version_crop),
            "crop_min_lon": float(version_crop["bbox"]["min_lon"]) if version_crop and version_crop.get("bbox") else "",
            "crop_max_lon": float(version_crop["bbox"]["max_lon"]) if version_crop and version_crop.get("bbox") else "",
            "crop_min_lat": float(version_crop["bbox"]["min_lat"]) if version_crop and version_crop.get("bbox") else "",
            "crop_max_lat": float(version_crop["bbox"]["max_lat"]) if version_crop and version_crop.get("bbox") else "",
            "crop_mask_path": str(version_crop.get("mask_path", "")) if version_crop else "",
            "crop_source_image": str(version_crop.get("source_path", "")) if version_crop else "",
            "deep_water_enabled": bool(version_deep_water),
            "deep_water_use_sd_bounds": bool(version.get("deep_water_use_sd_bounds", False)),
            "deep_water_polygons_json": json.dumps((version_deep_water or {}).get("polygons") or []),
            "deep_water_source_image": str((version_deep_water or {}).get("source_path", "")) if version_deep_water else "",
            "SIOPS": file_iop_path,
            "sensor_filter": file_sensor_path,
            "nedr_mode": "fixed",
            "pmin": list(version.get("pmin") or []),
            "pmax": list(version.get("pmax") or []),
            "rrs_flag": bool(version.get("rrs_flag", True)),
            "reflectance_input": bool(version.get("reflectance_input", False)),
            "shallow": bool(version.get("shallow", False)),
            "optimize_initial_guesses": bool(version.get("optimize_initial_guesses", False)),
            "use_five_initial_guesses": bool(version.get("use_five_initial_guesses", False)),
            "initial_guess_debug": bool(version.get("initial_guess_debug", False)),
            "post_processing": bool(version.get("post_processing", False)),
            "fully_relaxed": bool(version.get("fully_relaxed", False)),
            "output_modeled_reflectance": bool(version.get("output_modeled_reflectance", False)),
            "anomaly_search_enabled": bool(version_anomaly_search.get("enabled", False)),
            "anomaly_search_export_local_moran_raster": bool(version_anomaly_search.get("export_local_moran_raster", False)),
            "anomaly_search_export_suspicious_binary_raster": bool(version_anomaly_search.get("export_suspicious_binary_raster", False)),
            "anomaly_search_export_interpolated_rasters": bool(version_anomaly_search.get("export_interpolated_rasters", False)),
            "relaxed": bool(version.get("relaxed", False)),
            "output_folder": out_folder,
            "output_file": initial_ofile,
            "output_format": version.get("output_format", output_format.get()),
            "allow_split": bool(version.get("allow_split", False)),
            "split_chunk_rows": version.get("split_chunk_rows", ""),
            "siop_popup": version.get("siop_log_payload", {}),
            "sensor_popup": sensor_log_payload,
            "use_bathy": bool(version.get("use_bathy", False)),
            "bathy_path": version.get("bathy_path", "") if version.get("use_bathy", False) else "",
            "bathy_reference": version.get("bathy_reference", "depth"),
            "bathy_correction_m": version.get("bathy_correction_m", 0.0),
            "bathy_tolerance_m": version.get("bathy_tolerance_m", 0.0),
        }
        for mapping_key, mapping_value in sensor_log_payload.items():
            if mapping_key.startswith("sensor_band_mapping_"):
                payload[mapping_key] = mapping_value
        return payload

    run_versions_payloads = []
    for version_index, version in enumerate(active_run_versions, start=1):
        suffix = version.get("suffix", "")
        if not multi_settings:
            suffix = ""
        version_output_dir = run_dir
        if multi_settings:
            version_folder = str(suffix or _run_version_suffix(version_index)).lstrip("_")
            if not version_folder:
                version_folder = f"settings{version_index:02d}"
            version_output_dir = os.path.join(run_dir, version_folder)
            os.makedirs(version_output_dir, exist_ok=True)
        file_iop = os.path.join(version_output_dir, f"generated_siop{suffix}.xml")
        siop_config.write_siop_xml(file_iop, version["compiled_siop"])
        file_sensor = os.path.join(version_output_dir, f"generated_sensor_filter{suffix}.xml")
        sensor_config.write_sensor_xml(file_sensor, version["compiled_sensor"])
        ofile = os.path.join(version_output_dir, f"swampy_{input_base}{suffix}.nc")
        xml_file = os.path.join(version_output_dir, f"log_{input_base}{suffix}.xml")
        version_payload = {
            "label": version.get("label", f"Settings {version_index:02d}"),
            "suffix": suffix,
            "index": int(version_index),
            "count": int(len(active_run_versions)),
            "output_dir": version_output_dir,
            "siop_xml_path": file_iop,
            "file_sensor": file_sensor,
            "ofile": ofile,
            "xml_file": xml_file,
            "pmin": list(version.get("pmin") or []),
            "pmax": list(version.get("pmax") or []),
            "above_rrs_flag": bool(version.get("rrs_flag", True)),
            "reflectance_input_flag": bool(version.get("reflectance_input", False)),
            "relaxed": bool(version.get("relaxed", False)),
            "shallow_flag": bool(version.get("shallow", False)),
            "optimize_initial_guesses": bool(version.get("optimize_initial_guesses", False)),
            "use_five_initial_guesses": bool(version.get("use_five_initial_guesses", False)),
            "initial_guess_debug": bool(version.get("initial_guess_debug", False)),
            "fully_relaxed": bool(version.get("fully_relaxed", False)),
            "output_modeled_reflectance": bool(version.get("output_modeled_reflectance", False)),
            "anomaly_search_settings": copy.deepcopy(version.get("anomaly_search_settings", {})),
            "xml_dict": _build_input_dict_for_version(version, file_iop, file_sensor, ofile, version_index),
            "output_format": version.get("output_format", output_format.get()),
            "bathy_path": version.get("bathy_path", "") if version.get("use_bathy", False) else "",
            "post_processing": bool(version.get("post_processing", False)),
            "allow_split": bool(version.get("allow_split", False)),
            "split_chunk_rows": version.get("split_chunk_rows", ""),
        }
        run_versions_payloads.append(version_payload)

    primary_version = run_versions_payloads[0]

    return (
        file_list,
        primary_version["ofile"],
        primary_version["siop_xml_path"],
        primary_version["file_sensor"],
        primary_version["above_rrs_flag"],
        primary_version["reflectance_input_flag"],
        primary_version["relaxed"],
        primary_version["shallow_flag"],
        primary_version["optimize_initial_guesses"],
        primary_version["use_five_initial_guesses"],
        primary_version["initial_guess_debug"],
        primary_version["fully_relaxed"],
        primary_version["output_modeled_reflectance"],
        primary_version["anomaly_search_settings"],
        primary_version["pmin"],
        primary_version["pmax"],
        primary_version["xml_file"],
        primary_version["xml_dict"],
        primary_version["output_format"],
        primary_version["bathy_path"],
        primary_version["post_processing"],
        primary_version["allow_split"],
        primary_version["split_chunk_rows"],
        run_versions_payloads,
    )
