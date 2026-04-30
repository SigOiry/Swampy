# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 11:27:48 2017

@author: Marco
"""
from future.backports.datetime import timedelta

""" This version os SWAMpy run in Python3.6 without the need of a snappy installation.
 If no arguments are provided from command line, all inputs will be chosen from GUI.
If the "-f" arguments will be provided an xml file, containing all the inputs, must be chosen. 
The input image file can be a NetCDF or HDF product containing lat, lon, and
reflectance bands. Lat and lon should be named clearly, while reflectance bands
can be stacked in one cube or stored as wavelength-named 2D layers such as
"band_1, band_2", "rrs410, rrs443", or Polymer "Rw443, Rw490" variables.
If "-p" is set to True then the post-proc will be performed
to calculate additional spectra and parameters. 


"""
import glob
import copy
import csv
import json
import math
import os, sys
import re
import shutil
import sqlite3
import tempfile

sys.path.insert(0, os.getcwd())


def _resolve_bundled_resource(path):
    if not path:
        return path
    if os.path.exists(path):
        return path

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
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

def _parse_proj_layout_version(proj_dir):
    """Return (major, minor) layout version for proj.db in proj_dir, if available."""
    proj_db = os.path.join(proj_dir, 'proj.db')
    if not os.path.isfile(proj_db):
        return None
    conn = None
    try:
        conn = sqlite3.connect(proj_db)
        cur = conn.cursor()
        cur.execute(
            "SELECT key, value FROM metadata "
            "WHERE key IN ('DATABASE.LAYOUT.VERSION.MAJOR', 'DATABASE.LAYOUT.VERSION.MINOR')"
        )
        meta = {key: int(value) for key, value in cur.fetchall()}
        major = meta.get('DATABASE.LAYOUT.VERSION.MAJOR')
        minor = meta.get('DATABASE.LAYOUT.VERSION.MINOR')
        if major is None or minor is None:
            return None
        return (major, minor)
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def _parse_proj_package_version(path):
    """Extract a numeric version tuple from a conda proj package path."""
    match = re.search(r'proj-(\d+)\.(\d+)\.(\d+)', path.replace('\\', '/'))
    if not match:
        return ()
    return tuple(int(part) for part in match.groups())


def _iter_proj_candidate_dirs():
    seen = set()

    def add(path):
        if not path:
            return
        norm = os.path.normpath(path)
        if norm in seen or not os.path.isdir(norm):
            return
        seen.add(norm)
        yield norm

    for key in ('PROJ_DATA', 'PROJ_LIB'):
        yield from add(os.environ.get(key))

    try:
        import pyproj
        yield from add(pyproj.datadir.get_data_dir())
    except Exception:
        pass

    prefixes = {
        sys.prefix,
        sys.base_prefix,
        os.path.dirname(sys.executable),
    }
    for prefix in tuple(prefixes):
        yield from add(os.path.join(prefix, 'Library', 'share', 'proj'))
        yield from add(os.path.join(prefix, 'share', 'proj'))

        root = os.path.dirname(os.path.dirname(prefix))
        if root and os.path.isdir(root):
            for candidate in sorted(
                glob.glob(os.path.join(root, 'pkgs', 'proj-*', 'Library', 'share', 'proj')),
                reverse=True,
            ):
                yield from add(candidate)


def _find_best_proj_data_dir(min_layout_minor=6):
    """Pick the newest available PROJ data dir with a compatible proj.db layout."""
    best_path = None
    best_score = None
    fallback_path = None
    fallback_score = None

    for candidate in _iter_proj_candidate_dirs():
        layout = _parse_proj_layout_version(candidate)
        if layout is None:
            continue
        score = (layout[0], layout[1], _parse_proj_package_version(candidate))
        if fallback_score is None or score > fallback_score:
            fallback_path = candidate
            fallback_score = score
        if layout[1] >= min_layout_minor and (best_score is None or score > best_score):
            best_path = candidate
            best_score = score

    return best_path or fallback_path


def _find_gdal_data_dir():
    """Return an existing GDAL data directory if one is easy to locate."""
    candidates = [
        os.environ.get('GDAL_DATA'),
        os.path.join(sys.prefix, 'Library', 'share', 'gdal'),
        os.path.join(sys.prefix, 'share', 'gdal'),
        os.path.join(sys.base_prefix, 'Library', 'share', 'gdal'),
        os.path.join(sys.base_prefix, 'share', 'gdal'),
        os.path.join(os.path.dirname(sys.executable), 'Library', 'share', 'gdal'),
    ]
    for candidate in candidates:
        if candidate and os.path.isdir(candidate):
            return os.path.normpath(candidate)
    return None


def _configure_geospatial_runtime():
    """Prefer a compatible PROJ/GDAL data directory before rasterio is imported."""
    proj_dir = _find_best_proj_data_dir()
    if proj_dir:
        os.environ['PROJ_DATA'] = proj_dir
        os.environ['PROJ_LIB'] = proj_dir

    gdal_dir = _find_gdal_data_dir()
    if gdal_dir:
        os.environ['GDAL_DATA'] = gdal_dir


_configure_geospatial_runtime()

import numpy as np
import numpy.ma as ma
from scipy.interpolate import griddata
from scipy.optimize import minimize as scipy_minimize
import main_sambuca_snap
import output_calculation
import define_outputs
import sambuca as sb
import dicttoxml
import argparse
import xmltodict
import sambuca_core as sbc
from netCDF4 import Dataset
import gui_swampy
import create_input
from datetime import datetime
from multiprocessing import cpu_count
import rasterio
from scipy import ndimage
from scipy.spatial import QhullError
from rasterio.transform import from_bounds, Affine
from rasterio.crs import CRS
from rasterio.windows import Window

try:
    import image_io
except ImportError:  # pragma: no cover - fallback when imported as a package
    from app import image_io

_SPLIT_TARGET_PIXELS = 4_000_000  # approx pixels per chunk when splitting
_SPLIT_MIN_ROWS = 128
OUTPUT_FILL_VALUE = np.float32(-999.0)  # default nodata for outputs
_SCENE_NEDR_TARGET_FRACTION = 0.02
_SCENE_NEDR_MIN_PIXELS = 256
_SCENE_NEDR_MAX_PIXELS = 4096
_SCENE_NEDR_SIGMA_MULTIPLIER = 2.0
_PRIOR_PIXEL_SAMPLE_LIMIT = 1000
_SHALLOW_SUBSTRATE_PRIOR_MIN_EXP_BOTTOM = 0.05
_DEEP_WATER_IOP_RELAXED_BOUNDS = (
    (0.0, 10),  # CHL
    (0.0, 1.0),   # CDOM
    (0.0, 8),   # NAP
)
DEFAULT_ANOMALY_SEARCH_SETTINGS = {
    'enabled': False,
    'export_local_moran_raster': False,
    'export_suspicious_binary_raster': False,
    'export_interpolated_rasters': False,
    'seed_slope_threshold_percent': 10.0,
}
_ANOMALY_DEEP_PROTECTION_LOCAL_SD_WINDOW = 5
_ANOMALY_DEEP_PROTECTION_MODAL_WINDOW = 11
_ANOMALY_DEEP_PROTECTION_LOCAL_SD_THRESHOLD = 0.5
_ANOMALY_DEEP_PROTECTION_SMALL_PATCH_MAX_PIXELS = 15
_ANOMALY_SLOPE_THRESHOLD_PERCENT = 10.0
_ANOMALY_SUSPICIOUS_MODAL_WINDOW = 3
CHUNK_RESULT_KEYS = (
    'closed_rrs',
    'chl',
    'cdom',
    'nap',
    'depth',
    'nit',
    'kd',
    'sdi',
    'sub1_frac',
    'sub2_frac',
    'sub3_frac',
    'error_f',
    'total_abun',
    'sub1_norm',
    'sub2_norm',
    'sub3_norm',
    'r_sub',
    'initial_guess_stack',
)
LAT_VAR_NAMES = ('lat', 'latitude', 'lat_grid', 'latitudes')
LON_VAR_NAMES = ('lon', 'longitude', 'lon_grid', 'longitudes')
_LAMBERT_93_PROJ4 = (
    '+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 '
    '+x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs'
)


def _coerce_bool(value, default=False):
    """Translate typical truthy string/int values into bool."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ('1', 'true', 'yes', 'y', 'on'):
            return True
        if lowered in ('0', 'false', 'no', 'n', 'off'):
            return False
    return bool(value)


def _coerce_float(value, default=0.0):
    """Translate optional numeric strings into float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value, default=0):
    """Translate optional numeric strings into int."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_optional_float(value, default=np.nan):
    """Translate optional numeric-like values into float, or default."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_valid_positive_spacing(value):
    """Return True only for finite positive numeric pixel-spacing values."""
    value = _coerce_optional_float(value, np.nan)
    return bool(np.isfinite(value) and value > 0.0)


def _parse_chunk_rows(value):
    """Return a positive int row count if provided, otherwise None."""
    if value in (None, '', []):
        return None
    try:
        rows = int(float(value))
        if rows > 0:
            return rows
    except (TypeError, ValueError):
        pass
    return None


def _parse_crop_selection(config_root):
    """Return a validated spatial-selection dict from XML/log settings, or None."""
    if not config_root or not _coerce_bool(config_root.get('crop_enabled', False), False):
        return None
    bbox = None
    lon_values = [config_root.get('crop_min_lon'), config_root.get('crop_max_lon')]
    lat_values = [config_root.get('crop_min_lat'), config_root.get('crop_max_lat')]
    if all(value not in (None, '') for value in lon_values + lat_values):
        min_lon = _coerce_float(config_root.get('crop_min_lon'), np.nan)
        max_lon = _coerce_float(config_root.get('crop_max_lon'), np.nan)
        min_lat = _coerce_float(config_root.get('crop_min_lat'), np.nan)
        max_lat = _coerce_float(config_root.get('crop_max_lat'), np.nan)
        if np.isfinite(min_lon) and np.isfinite(max_lon) and np.isfinite(min_lat) and np.isfinite(max_lat):
            min_lon, max_lon = sorted((min_lon, max_lon))
            min_lat, max_lat = sorted((min_lat, max_lat))
            if max_lon > min_lon and max_lat > min_lat:
                bbox = {
                    'min_lon': min_lon,
                    'max_lon': max_lon,
                    'min_lat': min_lat,
                    'max_lat': max_lat,
                }
    mask_path = _resolve_bundled_resource(config_root.get('crop_mask_path'))
    mask_buffer_m = _coerce_float(config_root.get('crop_mask_buffer_m'), np.nan)
    if not np.isfinite(mask_buffer_m) or mask_buffer_m <= 0.0:
        mask_buffer_m = None
    if not bbox and not mask_path:
        return None
    return {
        'bbox': bbox,
        'mask_path': mask_path,
        'mask_buffer_m': mask_buffer_m if mask_path else None,
    }


def _parse_deep_water_selection(config_root):
    """Return a validated deep-water polygon selection from XML/log settings, or None."""
    if not config_root or not _coerce_bool(config_root.get('deep_water_enabled', False), False):
        return None
    polygons_raw = config_root.get('deep_water_polygons_json', '[]')
    try:
        if isinstance(polygons_raw, (list, tuple)):
            polygons = list(polygons_raw)
        else:
            polygons = json.loads(str(polygons_raw))
    except Exception:
        return None
    valid_polygons = []
    for geometry in polygons:
        if not isinstance(geometry, dict):
            continue
        geom_type = str(geometry.get('type') or '')
        coordinates = geometry.get('coordinates')
        if geom_type in ('Polygon', 'MultiPolygon') and coordinates:
            valid_polygons.append(geometry)
    if not valid_polygons:
        return None
    return {
        'polygons': valid_polygons,
        'use_sd_bounds': _coerce_bool(config_root.get('deep_water_use_sd_bounds', False), False),
        'subsample_pixels': _coerce_bool(config_root.get('deep_water_subsample_pixels', True), True),
        'source_image': str(config_root.get('deep_water_source_image', '') or ''),
    }


def _parse_shallow_substrate_prior_selection(config_root):
    """Return a validated shallow-water substrate prior selection from XML/log settings, or None."""
    if not config_root or not _coerce_bool(config_root.get('shallow_substrate_prior_enabled', False), False):
        return None
    target_name = str(config_root.get('shallow_substrate_prior_target_name', '') or '').strip()
    if not target_name:
        return None
    polygons_raw = config_root.get('shallow_substrate_prior_polygons_json', '[]')
    try:
        if isinstance(polygons_raw, (list, tuple)):
            polygons = list(polygons_raw)
        else:
            polygons = json.loads(str(polygons_raw))
    except Exception:
        return None
    valid_polygons = []
    for geometry in polygons:
        if not isinstance(geometry, dict):
            continue
        geom_type = str(geometry.get('type') or '')
        coordinates = geometry.get('coordinates')
        if geom_type in ('Polygon', 'MultiPolygon') and coordinates:
            valid_polygons.append(geometry)
    if not valid_polygons:
        return None
    return {
        'target_name': target_name,
        'polygons': valid_polygons,
        'use_sd_bounds': _coerce_bool(config_root.get('shallow_substrate_prior_use_sd_bounds', False), False),
        'source_image': str(config_root.get('shallow_substrate_prior_source_image', '') or ''),
    }


def _dict_item_list(node, key):
    if not isinstance(node, dict):
        return []
    value = node.get(key)
    if value is None:
        return []
    if isinstance(value, dict) and 'item' in value:
        value = value.get('item')
    if isinstance(value, list):
        return value
    return [value]


def _parse_saved_sensor_band_mapping(config_root):
    if not isinstance(config_root, dict):
        return None
    if not _coerce_bool(config_root.get('sensor_band_mapping_enabled', False), False):
        return None

    source_band_labels = [str(value) for value in _dict_item_list(config_root, 'sensor_band_mapping_source_band_labels')]
    source_band_wavelengths_raw = _dict_item_list(config_root, 'sensor_band_mapping_source_band_wavelengths')
    sensor_band_indices_raw = _dict_item_list(config_root, 'sensor_band_mapping_sensor_band_indices')
    sensor_band_centers_raw = _dict_item_list(config_root, 'sensor_band_mapping_sensor_band_centers')
    image_band_indices_raw = _dict_item_list(config_root, 'sensor_band_mapping_image_band_indices')
    image_band_labels = [str(value) for value in _dict_item_list(config_root, 'sensor_band_mapping_image_band_labels')]
    image_band_wavelengths_raw = _dict_item_list(config_root, 'sensor_band_mapping_image_band_wavelengths')

    if not sensor_band_indices_raw or not image_band_indices_raw:
        return None

    def _parse_optional_float_list(values):
        parsed = []
        for value in values:
            text = str(value).strip().lower()
            if text in {'', 'none', 'nan'}:
                parsed.append(None)
            else:
                parsed.append(float(value))
        return parsed

    try:
        return {
            'mode': str(config_root.get('sensor_band_mapping_mode', 'manual') or 'manual'),
            'tolerance_nm': _coerce_float(config_root.get('sensor_band_mapping_tolerance_nm'), 10.0),
            'source_kind': str(config_root.get('sensor_band_mapping_source_kind', '') or ''),
            'source_name': str(config_root.get('sensor_band_mapping_source_name', '') or ''),
            'source_band_labels': source_band_labels,
            'source_band_wavelengths': _parse_optional_float_list(source_band_wavelengths_raw),
            'sensor_band_indices': [int(float(value)) for value in sensor_band_indices_raw],
            'sensor_band_centers': [float(value) for value in sensor_band_centers_raw],
            'image_band_indices': [int(float(value)) for value in image_band_indices_raw],
            'image_band_labels': image_band_labels,
            'image_band_wavelengths': _parse_optional_float_list(image_band_wavelengths_raw),
        }
    except (TypeError, ValueError):
        return None


def _build_runtime_band_labels(single_band_layers, band_count, wavelengths=None):
    labels = []
    if single_band_layers:
        for layer in single_band_layers:
            wave = layer.get('wavelength')
            if wave is None:
                labels.append(str(layer['name']))
            else:
                labels.append(f"{layer['name']} ({float(wave):.1f} nm)")
        return labels

    wavelength_values = list(wavelengths) if wavelengths is not None else []
    for band_index in range(int(band_count)):
        wave = wavelength_values[band_index] if band_index < len(wavelength_values) else None
        if wave is None:
            labels.append(f"Band {band_index + 1}")
        else:
            labels.append(
                f"Band {band_index + 1} ({int(round(float(wave)))} nm)"
                if float(wave).is_integer()
                else f"Band {band_index + 1} ({float(wave):.1f} nm)"
            )
    return labels


def _saved_mapping_matches_current_source(saved_mapping, current_band_labels, current_band_wavelengths):
    if not saved_mapping:
        return False
    saved_labels = list(saved_mapping.get('source_band_labels') or [])
    if saved_labels and list(current_band_labels or []) == saved_labels:
        return True

    saved_wavelengths = np.array([
        np.nan if value is None else float(value)
        for value in (saved_mapping.get('source_band_wavelengths') or [])
    ], dtype=float)
    current_wavelengths = np.array([
        np.nan if value is None else float(value)
        for value in (current_band_wavelengths or [])
    ], dtype=float)
    if saved_wavelengths.size and current_wavelengths.size and saved_wavelengths.size == current_wavelengths.size:
        valid = np.isfinite(saved_wavelengths) & np.isfinite(current_wavelengths)
        if np.any(valid) and np.array_equal(np.isnan(saved_wavelengths), np.isnan(current_wavelengths)):
            return np.allclose(saved_wavelengths[valid], current_wavelengths[valid], atol=0.5)
    return False


def _apply_saved_sensor_band_mapping(rrs, saved_mapping, current_band_labels, current_band_wavelengths, target_band_centers):
    if not saved_mapping:
        return None, None
    sensor_centers = np.asarray(saved_mapping.get('sensor_band_centers') or [], dtype='float32')
    image_band_indices = [int(value) for value in (saved_mapping.get('image_band_indices') or [])]
    if sensor_centers.size == 0 or not image_band_indices:
        return None, None
    target_centers = np.asarray(target_band_centers, dtype='float32')
    if target_centers.size != sensor_centers.size or not np.allclose(target_centers, sensor_centers, atol=0.5):
        print("[WARN]: Saved sensor band mapping does not match the selected sensor bands. Falling back to automatic alignment.")
        return None, None
    if not _saved_mapping_matches_current_source(saved_mapping, current_band_labels, current_band_wavelengths):
        print("[WARN]: Saved sensor band mapping does not match the current input image bands. Falling back to automatic alignment.")
        return None, None
    if len(set(image_band_indices)) != len(image_band_indices):
        print("[WARN]: Saved sensor band mapping contains duplicate input band indices. Falling back to automatic alignment.")
        return None, None
    if any(index < 0 or index >= rrs.shape[0] for index in image_band_indices):
        print("[WARN]: Saved sensor band mapping references input bands that are not present in the current image. Falling back to automatic alignment.")
        return None, None
    aligned = rrs[image_band_indices, :, :]
    return aligned, sensor_centers


def _apply_crop_selection(rrs, lat_array, lon_array, crop_selection, file_im, grid_metadata=None):
    """Apply geographic crop and optional shapefile mask to reflectance and coordinates."""
    if not crop_selection:
        return rrs, lat_array, lon_array

    if lat_array is None or lon_array is None:
        raise RuntimeError("Spatial cropping requires latitude and longitude coordinates in the input image.")

    lat_grid = np.asarray(lat_array, dtype='float32')
    lon_grid = np.asarray(lon_array, dtype='float32')
    if lat_grid.ndim == 1 and lon_grid.ndim == 1:
        lon_grid, lat_grid = np.meshgrid(lon_grid, lat_grid)
    if lat_grid.ndim != 2 or lon_grid.ndim != 2:
        raise RuntimeError("Spatial cropping requires 2D latitude/longitude grids.")

    cropped_rrs = np.array(rrs, dtype='float32', copy=True)
    cropped_lat = lat_grid
    cropped_lon = lon_grid
    selection_mask = np.isfinite(cropped_lat) & np.isfinite(cropped_lon)
    crop_window = {
        'row_start': 0,
        'row_end': int(cropped_rrs.shape[1]),
        'col_start': 0,
        'col_end': int(cropped_rrs.shape[2]),
    }

    bbox = crop_selection.get('bbox')
    if bbox:
        bbox_mask = (
            selection_mask
            & (cropped_lon >= float(bbox['min_lon']))
            & (cropped_lon <= float(bbox['max_lon']))
            & (cropped_lat >= float(bbox['min_lat']))
            & (cropped_lat <= float(bbox['max_lat']))
        )
        if not np.any(bbox_mask):
            raise RuntimeError(
                f"The saved geographic crop does not overlap '{os.path.basename(file_im)}'."
            )
        rows, cols = np.where(bbox_mask)
        row_start, row_end = int(rows.min()), int(rows.max()) + 1
        col_start, col_end = int(cols.min()), int(cols.max()) + 1
        crop_window = {
            'row_start': row_start,
            'row_end': row_end,
            'col_start': col_start,
            'col_end': col_end,
        }
        cropped_rrs = cropped_rrs[:, row_start:row_end, col_start:col_end]
        cropped_lat = cropped_lat[row_start:row_end, col_start:col_end]
        cropped_lon = cropped_lon[row_start:row_end, col_start:col_end]
        selection_mask = bbox_mask[row_start:row_end, col_start:col_end]

    mask_path = str(crop_selection.get('mask_path') or '').strip()
    if mask_path:
        if not os.path.isfile(mask_path):
            raise RuntimeError(f"Shapefile mask not found: {mask_path}")
        point_buffer_m = crop_selection.get('mask_buffer_m')
        try:
            point_buffer_m = float(point_buffer_m)
        except (TypeError, ValueError):
            point_buffer_m = 50.0
        if point_buffer_m <= 0.0:
            point_buffer_m = 50.0
        import fiona
        from rasterio.features import geometry_mask
        from rasterio.warp import transform_geom
        from shapely.geometry import mapping, shape
        from shapely.ops import transform as shapely_transform
        from pyproj import Transformer

        def _transform_with_optional_point_buffer(geometry, src_crs, dst_crs, point_buffer_m=50.0):
            geom_type = str(geometry.get('type') or '')
            if geom_type in {'Point', 'MultiPoint'}:
                source_geom = shape(geometry)
                to_metric = Transformer.from_crs(src_crs, 'EPSG:3857', always_xy=True)
                to_target = Transformer.from_crs('EPSG:3857', dst_crs, always_xy=True)
                buffered_geom = shapely_transform(to_metric.transform, source_geom).buffer(float(point_buffer_m))
                return mapping(shapely_transform(to_target.transform, buffered_geom))
            return transform_geom(src_crs, dst_crs, geometry, precision=8)

        local_grid_metadata = _subset_grid_metadata(
            grid_metadata,
            crop_window.get('row_start', 0),
            crop_window.get('row_end'),
            crop_window.get('col_start', 0),
            crop_window.get('col_end'),
        ) if grid_metadata else None
        transform, crs = _derive_transform_crs(
            int(cropped_rrs.shape[2]),
            int(cropped_rrs.shape[1]),
            cropped_lat,
            cropped_lon,
            2,
            local_grid_metadata,
        )
        geometries = []
        with fiona.open(mask_path, 'r') as src:
            src_crs = src.crs_wkt or src.crs
            if not src_crs:
                raise RuntimeError("The shapefile mask has no CRS information.")
            for feature in src:
                geometry = feature.get('geometry')
                if not geometry:
                    continue
                geometries.append(_transform_with_optional_point_buffer(
                    geometry,
                    src_crs,
                    crs.to_string(),
                    point_buffer_m=point_buffer_m,
                ))
        if not geometries:
            raise RuntimeError("The shapefile mask does not contain any valid geometry.")
        shape_mask = geometry_mask(
            geometries,
            out_shape=(int(cropped_rrs.shape[1]), int(cropped_rrs.shape[2])),
            transform=transform,
            invert=True,
        )
        selection_mask &= shape_mask
        if not np.any(selection_mask):
            raise RuntimeError(
                f"The shapefile mask does not overlap '{os.path.basename(file_im)}'."
            )

    cropped_rrs[:, ~selection_mask] = np.nan
    return cropped_rrs, cropped_lat, cropped_lon, crop_window


def _rasterize_epsg4326_geometries(geometries, lat_array, lon_array, grid_metadata=None):
    if not geometries:
        return None
    from rasterio.features import geometry_mask
    from rasterio.warp import transform_geom

    lat_grid = np.asarray(lat_array, dtype='float32')
    lon_grid = np.asarray(lon_array, dtype='float32')
    if lat_grid.ndim == 1 and lon_grid.ndim == 1:
        lon_grid, lat_grid = np.meshgrid(lon_grid, lat_grid)
    transform, crs = _derive_transform_crs(
        int(lon_grid.shape[1]),
        int(lat_grid.shape[0]),
        lat_grid,
        lon_grid,
        2,
        grid_metadata,
    )
    raster_geometries = geometries
    try:
        if crs is not None:
            crs_text = crs.to_string() if hasattr(crs, 'to_string') else str(crs)
            if crs_text and str(crs_text).strip().lower() not in {'epsg:4326', 'ogc:crs84'}:
                raster_geometries = [
                    transform_geom('EPSG:4326', crs_text, geometry, precision=8)
                    for geometry in geometries
                ]
    except Exception:
        raster_geometries = geometries
    mask = geometry_mask(
        raster_geometries,
        out_shape=(int(lat_grid.shape[0]), int(lon_grid.shape[1])),
        transform=transform,
        invert=True,
    )
    return np.asarray(mask, dtype=bool)


def _deep_water_modelled_rrs(objective, chl, cdom, nap):
    a = objective._a_water + (float(chl) * objective._a_ph_star) + (float(cdom) * objective._a_cdom_star) + (float(nap) * objective._a_nap_star)
    bb = objective._bb_water + (float(chl) * objective._bb_ph_star) + (float(nap) * objective._bb_nap_star)
    kappa = np.clip(a + bb, 1.0e-12, None)
    u = bb / kappa
    rrsdp = (0.084 + 0.17 * u) * u
    return objective._filter_spectrum(rrsdp)


def _deep_water_alpha_f(observed_rrs, modelled_rrs, weights):
    observed_rrs = np.asarray(observed_rrs, dtype=float)
    modelled_rrs = np.asarray(modelled_rrs, dtype=float)
    weights = np.asarray(weights, dtype=float)
    observed_weighted = weights * observed_rrs
    observed_sum_weighted = np.clip(np.sum(observed_weighted), 1.0e-12, None)
    observed_norm = np.sqrt(np.clip(np.sum(observed_weighted * observed_rrs), 1.0e-24, None))
    residual = modelled_rrs - observed_rrs
    residual_weighted = weights * residual
    residual_norm = np.sqrt(np.clip(np.sum(residual_weighted * residual), 1.0e-24, None))
    modelled_weighted = weights * modelled_rrs
    model_norm = np.sqrt(np.clip(np.sum(modelled_weighted * modelled_rrs), 1.0e-24, None))
    weighted_dot = np.sum(observed_weighted * modelled_rrs)
    cosine = weighted_dot / np.clip(model_norm * observed_norm, 1.0e-24, None)
    cosine = np.clip(cosine, 0.0, 1.0)
    alpha = np.arccos(cosine)
    lsq = residual_norm / observed_sum_weighted
    return float(alpha * lsq)


def _estimate_deep_water_pixel(objective, observed_rrs, chl_bounds, cdom_bounds, nap_bounds):
    observed_rrs = np.asarray(observed_rrs, dtype=float)
    if observed_rrs.ndim != 1 or np.isnan(observed_rrs).any() or np.allclose(observed_rrs, 0):
        return None

    lower = np.array([float(chl_bounds[0]), float(cdom_bounds[0]), float(nap_bounds[0])], dtype=float)
    upper = np.array([float(chl_bounds[1]), float(cdom_bounds[1]), float(nap_bounds[1])], dtype=float)
    midpoint = 0.5 * (lower + upper)
    lower_mid = 0.5 * (lower + midpoint)
    upper_mid = 0.5 * (midpoint + upper)
    starts = [midpoint, lower_mid, upper_mid]

    def objective_func(x):
        modeled = _deep_water_modelled_rrs(objective, x[0], x[1], x[2])
        return _deep_water_alpha_f(observed_rrs, modeled, objective._weights)

    best = None
    for start in starts:
        try:
            res = scipy_minimize(
                objective_func,
                x0=np.asarray(start, dtype=float),
                method='L-BFGS-B',
                bounds=[tuple(chl_bounds), tuple(cdom_bounds), tuple(nap_bounds)],
                options={'maxiter': 120},
            )
        except Exception:
            continue
        if best is None or float(res.fun) < float(best.fun):
            best = res

    if best is None or best.x is None:
        return None
    modeled = _deep_water_modelled_rrs(objective, best.x[0], best.x[1], best.x[2])
    return {
        'chl': float(best.x[0]),
        'cdom': float(best.x[1]),
        'nap': float(best.x[2]),
        'error_alpha_f': float(_deep_water_alpha_f(observed_rrs, modeled, objective._weights)),
        'success': bool(getattr(best, 'success', False)),
    }


def _apply_iop_priors(siop, estimates, use_sd_bounds, iop_bounds=None):
    if not estimates:
        return None
    chl_vals = np.array([item['chl'] for item in estimates], dtype=float)
    cdom_vals = np.array([item['cdom'] for item in estimates], dtype=float)
    nap_vals = np.array([item['nap'] for item in estimates], dtype=float)
    stats = {
        'chl_mean': float(np.nanmean(chl_vals)),
        'chl_sd': float(np.nanstd(chl_vals)),
        'cdom_mean': float(np.nanmean(cdom_vals)),
        'cdom_sd': float(np.nanstd(cdom_vals)),
        'nap_mean': float(np.nanmean(nap_vals)),
        'nap_sd': float(np.nanstd(nap_vals)),
    }

    pmin = list(np.asarray(siop['p_min'], dtype=float))
    pmax = list(np.asarray(siop['p_max'], dtype=float))
    original_pmin = np.asarray(siop['p_min'], dtype=float)
    original_pmax = np.asarray(siop['p_max'], dtype=float)
    if iop_bounds is None:
        clip_bounds = tuple(
            (float(original_pmin[index]), float(original_pmax[index]))
            for index in range(3)
        )
    else:
        clip_bounds = tuple(
            (float(bounds[0]), float(bounds[1]))
            for bounds in iop_bounds
        )
    means = [stats['chl_mean'], stats['cdom_mean'], stats['nap_mean']]
    sds = [stats['chl_sd'], stats['cdom_sd'], stats['nap_sd']]

    for index, (mean_value, sd_value) in enumerate(zip(means, sds)):
        lower_clip, upper_clip = clip_bounds[index]
        mean_value = float(np.clip(mean_value, lower_clip, upper_clip))
        if use_sd_bounds and np.isfinite(sd_value) and sd_value > 0.0:
            lower = max(lower_clip, mean_value - float(sd_value))
            upper = min(upper_clip, mean_value + float(sd_value))
            if lower > upper:
                lower = upper = mean_value
        else:
            lower = upper = mean_value
        pmin[index] = lower
        pmax[index] = upper

    siop['p_min'] = sb.FreeParameters(*pmin)
    siop['p_max'] = sb.FreeParameters(*pmax)
    siop['p_bounds'] = tuple(zip(siop['p_min'], siop['p_max']))
    stats['use_sd_bounds'] = bool(use_sd_bounds)
    stats['applied_pmin'] = pmin[:3]
    stats['applied_pmax'] = pmax[:3]
    return stats


def _apply_deep_water_priors(siop, estimates, use_sd_bounds):
    return _apply_iop_priors(
        siop,
        estimates,
        use_sd_bounds,
        iop_bounds=_DEEP_WATER_IOP_RELAXED_BOUNDS,
    )


def _normalise_label_key(value):
    text = str(value or '').replace('\\', '/').split('/')[-1]
    if ':' in text:
        text = text.split(':')[-1]
    return re.sub(r'\s+', ' ', text).strip().lower()


def _resolve_shallow_substrate_target_index(selection, substrate_names):
    target_key = _normalise_label_key((selection or {}).get('target_name'))
    if not target_key:
        return None, ''
    for index, name in enumerate(list(substrate_names or [])[:3]):
        if _normalise_label_key(name) == target_key:
            return index, str(name)
    return None, ''


def _run_forward_model_for_objective_parameters(objective, parameters):
    fixed = objective._fixed_parameters
    return sbc.forward_model(
        chl=float(parameters[0]),
        cdom=float(parameters[1]),
        nap=float(parameters[2]),
        depth=float(parameters[3]),
        sub1_frac=float(parameters[4]),
        sub2_frac=float(parameters[5]),
        sub3_frac=float(parameters[6]),
        substrate1=fixed.substrates[0],
        substrate2=fixed.substrates[1],
        substrate3=fixed.substrates[2],
        wavelengths=np.asarray(fixed.wavelengths, dtype=float),
        a_water=np.asarray(fixed.a_water, dtype=float),
        a_ph_star=np.asarray(fixed.a_ph_star, dtype=float),
        num_bands=int(fixed.num_bands),
        a_cdom_slope=float(fixed.a_cdom_slope),
        a_nap_slope=float(fixed.a_nap_slope),
        bb_ph_slope=float(fixed.bb_ph_slope),
        bb_nap_slope=float(fixed.bb_nap_slope) if fixed.bb_nap_slope is not None else None,
        lambda0cdom=float(fixed.lambda0cdom),
        lambda0nap=float(fixed.lambda0nap),
        lambda0x=float(fixed.lambda0x),
        x_ph_lambda0x=float(fixed.x_ph_lambda0x),
        x_nap_lambda0x=float(fixed.x_nap_lambda0x),
        a_cdom_lambda0cdom=float(fixed.a_cdom_lambda0cdom),
        a_nap_lambda0nap=float(fixed.a_nap_lambda0nap),
        bb_lambda_ref=float(fixed.bb_lambda_ref),
        water_refractive_index=float(fixed.water_refractive_index),
        theta_air=float(fixed.theta_air),
        off_nadir=float(fixed.off_nadir),
        q_factor=float(fixed.q_factor),
    )


def _estimate_shallow_substrate_pixel(
    objective,
    observed_rrs,
    chl_bounds,
    cdom_bounds,
    nap_bounds,
    depth_bounds,
    substrate_fractions,
):
    observed_rrs = np.asarray(observed_rrs, dtype=float)
    if observed_rrs.ndim != 1 or np.isnan(observed_rrs).any() or np.allclose(observed_rrs, 0):
        return None

    lower = np.array(
        [float(chl_bounds[0]), float(cdom_bounds[0]), float(nap_bounds[0]), float(depth_bounds[0])],
        dtype=float,
    )
    upper = np.array(
        [float(chl_bounds[1]), float(cdom_bounds[1]), float(nap_bounds[1]), float(depth_bounds[1])],
        dtype=float,
    )
    midpoint = 0.5 * (lower + upper)
    if lower[3] > 0.0 and upper[3] > 0.0:
        midpoint[3] = 10 ** ((math.log10(lower[3]) + math.log10(upper[3])) / 2.0)
    lower_mid = 0.5 * (lower + midpoint)
    upper_mid = 0.5 * (midpoint + upper)
    starts = [midpoint, lower_mid, upper_mid]
    fractions = np.asarray(substrate_fractions, dtype=float)

    previous_observed_rrs = objective.observed_rrs
    objective.observed_rrs = observed_rrs
    try:
        def objective_func(x):
            full_parameters = np.array(
                [float(x[0]), float(x[1]), float(x[2]), float(x[3]), fractions[0], fractions[1], fractions[2]],
                dtype=float,
            )
            error_value, jacobian = objective(full_parameters)
            return float(error_value), np.asarray(jacobian[:4], dtype=float)

        best = None
        for start in starts:
            try:
                res = scipy_minimize(
                    objective_func,
                    x0=np.asarray(start, dtype=float),
                    method='L-BFGS-B',
                    jac=True,
                    bounds=[
                        tuple(chl_bounds),
                        tuple(cdom_bounds),
                        tuple(nap_bounds),
                        tuple(depth_bounds),
                    ],
                    options={'maxiter': 160},
                )
            except Exception:
                continue
            if best is None or float(res.fun) < float(best.fun):
                best = res
    finally:
        objective.observed_rrs = previous_observed_rrs

    if best is None or best.x is None:
        return None

    full_parameters = np.array(
        [float(best.x[0]), float(best.x[1]), float(best.x[2]), float(best.x[3]), fractions[0], fractions[1], fractions[2]],
        dtype=float,
    )
    model_results = _run_forward_model_for_objective_parameters(objective, full_parameters)
    modeled_rrs = objective._filter_spectrum(model_results.rrs)
    return {
        'chl': float(best.x[0]),
        'cdom': float(best.x[1]),
        'nap': float(best.x[2]),
        'depth': float(best.x[3]),
        'exp_bottom': float(model_results.exp_bottom),
        'error_alpha_f': float(_deep_water_alpha_f(observed_rrs, modeled_rrs, objective._weights)),
        'success': bool(getattr(best, 'success', False)),
    }


def _write_deep_water_iop_raster(tif_path, pixel_rows, width, height, lat_data, lon_data, shape_geo_val,
                                 grid_metadata=None):
    if not pixel_rows:
        return None
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping deep-water IOP raster export: lat/lon not available to derive georeferencing.")
        return None

    band_defs = (
        ('chl', 'CHL'),
        ('cdom', 'CDOM'),
        ('nap', 'NAP'),
    )
    layers = {
        key: ma.masked_all((height, width), dtype='float32')
        for key, _label in band_defs
    }
    written_pixel_count = 0
    for pixel_row in pixel_rows:
        if not _coerce_bool(pixel_row.get('success', False), False):
            continue
        try:
            row_index = int(pixel_row.get('row'))
            col_index = int(pixel_row.get('col'))
        except (TypeError, ValueError):
            continue
        if row_index < 0 or row_index >= height or col_index < 0 or col_index >= width:
            continue

        values = {}
        for key, _label in band_defs:
            try:
                value = float(pixel_row.get(key))
            except (TypeError, ValueError):
                value = np.nan
            if not np.isfinite(value):
                values = {}
                break
            values[key] = value
        if not values:
            continue

        for key, value in values.items():
            layers[key][row_index, col_index] = np.float32(value)
        written_pixel_count += 1

    if written_pixel_count == 0:
        return None

    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    _write_geotiff(
        tif_path,
        [(label, layers[key]) for key, label in band_defs],
        transform,
        crs,
        height,
        width,
        nodata=OUTPUT_FILL_VALUE)
    return {
        'path': tif_path,
        'written_pixel_count': written_pixel_count,
    }


def _write_shallow_substrate_prior_pixel_csv(csv_path, pixel_rows):
    if not pixel_rows:
        return
    fieldnames = [
        'row',
        'col',
        'lat',
        'lon',
        'target_name',
        'chl',
        'cdom',
        'nap',
        'depth',
        'exp_bottom',
        'error_alpha_f',
        'success',
        'accepted_for_prior',
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in pixel_rows:
            writer.writerow(row)


def _format_batch_setting_value(value):
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    if value is None:
        return ''
    if isinstance(value, float):
        return format(value, '.15g')
    if isinstance(value, (int, str)):
        return str(value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)
    if isinstance(value, (list, tuple)):
        if not value:
            return ''
        if any(isinstance(item, (dict, list, tuple, np.ndarray, np.generic)) for item in value):
            return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)
        return ' | '.join(_format_batch_setting_value(item) for item in value)
    return str(value)


def _flatten_batch_run_settings_record(record):
    row = {}

    def add(key, value):
        row[key] = _format_batch_setting_value(value)

    add('run_version_index', record.get('run_version_index', ''))
    add('run_version_label', record.get('run_version_label', ''))
    add('run_version_suffix', record.get('run_version_suffix', ''))
    add('run_version_output_folder', record.get('run_version_output_folder', ''))

    add('output_format', record.get('output_format', ''))
    add('post_processing', record.get('post_processing', False))
    add('output_modeled_reflectance', record.get('output_modeled_reflectance', False))
    add('allow_split', record.get('allow_split', False))
    add('split_chunk_rows', record.get('split_chunk_rows', ''))
    add('nedr_mode', record.get('nedr_mode', 'fixed'))

    crop_selection = record.get('crop_selection') or {}
    crop_bbox = crop_selection.get('bbox') or {}
    add('crop_enabled', bool(crop_selection))
    add('crop_min_lon', crop_bbox.get('min_lon', ''))
    add('crop_max_lon', crop_bbox.get('max_lon', ''))
    add('crop_min_lat', crop_bbox.get('min_lat', ''))
    add('crop_max_lat', crop_bbox.get('max_lat', ''))
    add('crop_mask_path', crop_selection.get('mask_path', ''))
    add('crop_mask_buffer_m', crop_selection.get('mask_buffer_m', ''))

    deep_water_selection = record.get('deep_water_selection') or {}
    deep_water_polygons = deep_water_selection.get('polygons') or []
    add('deep_water_enabled', bool(deep_water_selection))
    add('deep_water_use_sd_bounds', record.get('deep_water_use_sd_bounds', deep_water_selection.get('use_sd_bounds', False)))
    add('deep_water_subsample_pixels', deep_water_selection.get('subsample_pixels', True))
    add('deep_water_polygon_count', len(deep_water_polygons))
    add('deep_water_polygons_json', deep_water_polygons)

    shallow_prior_selection = record.get('shallow_substrate_prior_selection') or {}
    shallow_prior_polygons = shallow_prior_selection.get('polygons') or []
    add('shallow_substrate_prior_enabled', bool(shallow_prior_selection))
    add('shallow_substrate_prior_target_name', shallow_prior_selection.get('target_name', ''))
    add(
        'shallow_substrate_prior_use_sd_bounds',
        record.get(
            'shallow_substrate_prior_use_sd_bounds',
            shallow_prior_selection.get('use_sd_bounds', False),
        ),
    )
    add('shallow_substrate_prior_polygon_count', len(shallow_prior_polygons))
    add('shallow_substrate_prior_polygons_json', shallow_prior_polygons)

    siop_payload = record.get('siop_popup') or {}
    for key, value in siop_payload.items():
        add(f'siop_{key}', value)

    pmin_values = list(record.get('pmin') or [])
    pmax_values = list(record.get('pmax') or [])
    bound_names = ('chl', 'cdom', 'nap', 'depth', 'substrate_1', 'substrate_2', 'substrate_3')
    for index, name in enumerate(bound_names):
        add(f'pmin_{name}', pmin_values[index] if index < len(pmin_values) else '')
        add(f'pmax_{name}', pmax_values[index] if index < len(pmax_values) else '')

    sensor_payload = record.get('sensor_popup') or {}
    for key, value in sensor_payload.items():
        add(f'sensor_{key}', value)

    add('rrs_flag', record.get('rrs_flag', True))
    add('reflectance_input', record.get('reflectance_input', False))
    add('relaxed', record.get('relaxed', False))
    add(
        'standardize_relaxed_substrate_outputs',
        record.get('standardize_relaxed_substrate_outputs', False),
    )
    add('shallow', record.get('shallow', False))
    add('optimize_initial_guesses', record.get('optimize_initial_guesses', False))
    add('use_five_initial_guesses', record.get('use_five_initial_guesses', False))
    add('initial_guess_debug', record.get('initial_guess_debug', False))

    add('use_bathy', record.get('use_bathy', False))
    add('bathy_path', record.get('bathy_path', ''))
    add('bathy_reference', record.get('bathy_reference', ''))
    add('bathy_correction_m', record.get('bathy_correction_m', ''))
    add('bathy_tolerance_m', record.get('bathy_tolerance_m', ''))

    anomaly_search = record.get('anomaly_search_settings') or {}
    for key, value in anomaly_search.items():
        add(f'anomaly_search_{key}', value)

    return row


def _build_batch_run_settings_csv(records):
    flattened_rows = [_flatten_batch_run_settings_record(record) for record in records]
    if not flattened_rows:
        return [], []

    id_columns = [
        'run_version_index',
        'run_version_label',
        'run_version_suffix',
        'run_version_output_folder',
    ]
    ordered_keys = []
    for row in flattened_rows:
        for key in row.keys():
            if key not in ordered_keys:
                ordered_keys.append(key)

    varying_columns = []
    for key in ordered_keys:
        if key in id_columns:
            continue
        values = [row.get(key, '') for row in flattened_rows]
        if any(value != values[0] for value in values[1:]):
            varying_columns.append(key)

    fieldnames = [key for key in id_columns if key in ordered_keys] + varying_columns
    csv_rows = [
        {key: row.get(key, '') for key in fieldnames}
        for row in flattened_rows
    ]
    return fieldnames, csv_rows


def _write_batch_run_settings_csv(csv_path, records):
    fieldnames, rows = _build_batch_run_settings_csv(records)
    if not fieldnames or not rows:
        return
    with open(csv_path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _resolve_batch_run_root_dir(version_output_dirs, fallback_dir=None):
    normalised_dirs = []
    for output_dir in version_output_dirs or []:
        if not output_dir:
            continue
        try:
            abs_dir = os.path.abspath(output_dir)
        except Exception:
            continue
        if abs_dir not in normalised_dirs:
            normalised_dirs.append(abs_dir)

    fallback_abs = None
    if fallback_dir:
        try:
            fallback_abs = os.path.abspath(fallback_dir)
        except Exception:
            fallback_abs = None

    if not normalised_dirs:
        return fallback_abs
    if len(normalised_dirs) == 1:
        return normalised_dirs[0]

    parent_dirs = []
    for output_dir in normalised_dirs:
        parent_dir = os.path.dirname(output_dir.rstrip("\\/")) or output_dir
        if parent_dir not in parent_dirs:
            parent_dirs.append(parent_dir)
    if len(parent_dirs) == 1:
        return parent_dirs[0]

    try:
        common_dir = os.path.commonpath(normalised_dirs)
    except ValueError:
        common_dir = ''
    if common_dir:
        return common_dir
    return fallback_abs or normalised_dirs[0]


def _resolve_execution_version_settings(
    run_version,
    *,
    default_siop_xml_path,
    default_file_sensor,
    default_pmin,
    default_pmax,
    default_above_rrs_flag,
    default_reflectance_input_flag,
    default_relaxed,
    default_shallow_flag,
    default_optimize_initial_guesses,
    default_use_five_initial_guesses,
    default_initial_guess_debug,
    default_standardize_relaxed_substrate_outputs,
    default_output_modeled_reflectance,
    default_anomaly_search_settings,
    default_xml_dict,
    default_output_format,
    default_bathy_path,
    default_post_processing,
    default_allow_split,
    default_split_chunk_rows,
    default_bathy_reference,
    default_bathy_correction_m,
    default_bathy_tolerance_m,
    default_nedr_mode,
    format_override=None,
    nedr_mode_override=None,
):
    version_label = str(run_version.get('label', 'Settings 01'))
    resolved = {
        'label': version_label,
        'suffix': str(run_version.get('suffix', '') or ''),
        'index': int(run_version.get('index', 1) or 1),
        'count': int(run_version.get('count', 1) or 1),
        'output_dir': run_version.get('output_dir'),
        'siop_xml_path': run_version.get('siop_xml_path', default_siop_xml_path),
        'file_sensor': run_version.get('file_sensor', default_file_sensor),
        'pmin': np.asarray(run_version.get('pmin', default_pmin), dtype=float),
        'pmax': np.asarray(run_version.get('pmax', default_pmax), dtype=float),
        'above_rrs_flag': _coerce_bool(run_version.get('above_rrs_flag', default_above_rrs_flag), default_above_rrs_flag),
        'reflectance_input_flag': _coerce_bool(run_version.get('reflectance_input_flag', default_reflectance_input_flag), default_reflectance_input_flag),
        'relaxed': _coerce_bool(run_version.get('relaxed', default_relaxed), default_relaxed),
        'shallow_flag': _coerce_bool(run_version.get('shallow_flag', default_shallow_flag), default_shallow_flag),
        'optimize_initial_guesses': _coerce_bool(run_version.get('optimize_initial_guesses', default_optimize_initial_guesses), default_optimize_initial_guesses),
        'use_five_initial_guesses': _coerce_bool(run_version.get('use_five_initial_guesses', default_use_five_initial_guesses), default_use_five_initial_guesses),
        'initial_guess_debug': _coerce_bool(run_version.get('initial_guess_debug', default_initial_guess_debug), default_initial_guess_debug),
        'standardize_relaxed_substrate_outputs': _coerce_bool(
            run_version.get(
                'standardize_relaxed_substrate_outputs',
                default_standardize_relaxed_substrate_outputs,
            ),
            default_standardize_relaxed_substrate_outputs,
        ),
        'output_modeled_reflectance': _coerce_bool(run_version.get('output_modeled_reflectance', default_output_modeled_reflectance), default_output_modeled_reflectance),
        'bathy_path': _resolve_bundled_resource(run_version.get('bathy_path', default_bathy_path)),
        'xml_dict': copy.deepcopy(run_version.get('xml_dict', default_xml_dict)),
        'output_format': str(run_version.get('output_format', default_output_format)).lower(),
        'post_processing': _coerce_bool(run_version.get('post_processing', default_post_processing), default_post_processing),
        'allow_split': _coerce_bool(run_version.get('allow_split', default_allow_split), default_allow_split),
        'split_chunk_rows': _parse_chunk_rows(run_version.get('split_chunk_rows', default_split_chunk_rows)),
    }
    resolved['anomaly_search_settings'] = _finalise_anomaly_search_settings(
        run_version.get('anomaly_search_settings', default_anomaly_search_settings),
        use_input_bathy=bool(resolved['bathy_path']),
    )
    resolved['bathy_reference'] = str(resolved['xml_dict'].get('bathy_reference', default_bathy_reference)).strip().lower()
    resolved['bathy_correction_m'] = _coerce_float(
        resolved['xml_dict'].get('bathy_correction_m', default_bathy_correction_m),
        default_bathy_correction_m,
    )
    resolved['bathy_tolerance_m'] = _coerce_float(
        resolved['xml_dict'].get('bathy_tolerance_m', default_bathy_tolerance_m),
        default_bathy_tolerance_m,
    )
    resolved['nedr_mode'] = str(resolved['xml_dict'].get('nedr_mode', default_nedr_mode)).strip().lower()
    resolved['crop_selection'] = _parse_crop_selection(resolved['xml_dict'])
    resolved['deep_water_selection'] = _parse_deep_water_selection(resolved['xml_dict'])
    resolved['shallow_substrate_prior_selection'] = _parse_shallow_substrate_prior_selection(resolved['xml_dict'])
    resolved['saved_sensor_band_mapping'] = _parse_saved_sensor_band_mapping(resolved['xml_dict'])

    warnings = []
    if resolved['deep_water_selection'] and resolved['shallow_substrate_prior_selection']:
        warnings.append(
            f"{version_label}: deep-water priors and shallow-water substrate priors were both provided. "
            "Shallow-water substrate priors take precedence; deep-water priors will be ignored."
        )
        resolved['deep_water_selection'] = None
    if resolved['standardize_relaxed_substrate_outputs'] and not resolved['relaxed']:
        resolved['standardize_relaxed_substrate_outputs'] = False

    if resolved['use_five_initial_guesses'] and not resolved['optimize_initial_guesses']:
        warnings.append(f"{version_label}: five-point initial guess testing requires initial guess optimisation. Disabling 5-point testing.")
        resolved['use_five_initial_guesses'] = False
    if resolved['initial_guess_debug'] and not resolved['optimize_initial_guesses']:
        warnings.append(f"{version_label}: initial guess debug export requires initial guess optimisation. Disabling debug export.")
        resolved['initial_guess_debug'] = False

    if resolved['allow_split'] and resolved['post_processing']:
        warnings.append(f"{version_label}: post-processing is not supported when image splitting is enabled. Skipping post-processing step.")
        resolved['post_processing'] = False

    if format_override:
        resolved['output_format'] = format_override
    if nedr_mode_override:
        resolved['nedr_mode'] = nedr_mode_override

    if resolved['nedr_mode'] not in ('scene', 'fixed'):
        warnings.append(f"{version_label}: unsupported NEDR mode '{resolved['nedr_mode']}'. Falling back to fixed.")
        resolved['nedr_mode'] = 'fixed'

    resolved['warnings'] = warnings
    return resolved


def _suggest_chunk_rows(height, width, target_pixels=_SPLIT_TARGET_PIXELS, min_rows=_SPLIT_MIN_ROWS):
    """Return a reasonable chunk height so that chunk_width*rows ~= target_pixels."""
    if height <= 0 or width <= 0:
        return 1
    rows = target_pixels // max(width, 1)
    rows = max(1, rows)
    rows = max(min_rows, rows)
    rows = min(rows, height)
    return rows


def _is_rrs_band_variable(var_name):
    """Return True if the variable name looks like a single-band RRS layer."""
    return image_io.is_rrs_band_variable(var_name)


def _is_auxiliary_scene_variable(var_name):
    """Return True for common 2D QA / mask / geometry layers that should be skipped quietly."""
    return image_io.is_auxiliary_scene_variable(var_name)


def _looks_like_wavelength_var(var_name):
    """Heuristic to detect 1D wavelength coordinate variables."""
    return image_io.looks_like_wavelength_var(var_name)


def _extract_wavelength(var_name, variable=None):
    """Return the numeric wavelength embedded in a variable name, if any."""
    return image_io.extract_wavelength(var_name, variable)


def _band_sort_key(var_name):
    """Sort spectral layers numerically when possible."""
    return image_io.band_sort_key(var_name)


def _align_rrs_to_filter(rrs, source_wavelengths, target_wavelengths):
    """Subset/reorder observed RRS bands to match the sensor filter wavelength list."""
    target = np.array(target_wavelengths, dtype='float32')
    n_target = len(target)
    if n_target == 0:
        return rrs, np.array(source_wavelengths if source_wavelengths else [])
    if rrs.shape[0] == n_target and (not source_wavelengths or len(source_wavelengths) == n_target):
        return rrs, np.array(source_wavelengths if source_wavelengths else target)

    if n_target > rrs.shape[0]:
        print(f"[WARN]: Sensor filter requests {n_target} bands but input RRS only has {rrs.shape[0]}. "
              "Using the minimum available.")
        target = target[:rrs.shape[0]]
        n_target = len(target)

    if not source_wavelengths:
        print("[WARN]: RRS wavelengths unavailable; truncating/ordering bands to match sensor filter count.")
        aligned = rrs[:n_target, :, :]
        return aligned, target

    numeric_sources = []
    for idx, w in enumerate(source_wavelengths):
        if w is None:
            numeric_sources.append(float(idx))
        else:
            numeric_sources.append(float(w))
    source = np.array(numeric_sources, dtype='float32')
    indices = []
    used = set()
    for tw in target:
        order = np.argsort(np.abs(source - tw))
        idx = None
        for candidate in order:
            if int(candidate) not in used:
                idx = int(candidate)
                break
        if idx is None:
            idx = int(order[0])
        used.add(idx)
        indices.append(idx)
    aligned = rrs[indices, :, :]
    return aligned, source[indices]


def _load_coordinate_variable(nc_vars, primary_names, std_names=('latitude',)):
    """Return (name, array) for the first matching coordinate variable."""
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


def _load_axis_coordinate_variable(nc_vars, primary_names, std_names=(), expected_length=None):
    """Return (name, 1D array) for a projected x/y coordinate variable."""
    for cand in primary_names:
        if cand in nc_vars:
            try:
                data = np.asarray(nc_vars[cand][:])
                if data.ndim == 1 and (expected_length is None or data.shape[0] == expected_length):
                    return cand, data
            except Exception:
                continue
    for var_name, var in nc_vars.items():
        if getattr(var, 'ndim', None) != 1:
            continue
        std_name = getattr(var, 'standard_name', '').lower() if hasattr(var, 'standard_name') else ''
        if std_name in std_names:
            try:
                data = np.asarray(var[:])
                if expected_length is None or data.shape[0] == expected_length:
                    return var_name, data
            except Exception:
                continue
    return None, None


def _median_regular_step(values):
    values = np.asarray(values, dtype='float64').ravel()
    if values.size < 2:
        return None
    diffs = np.diff(values)
    diffs = diffs[np.isfinite(diffs)]
    if diffs.size == 0:
        return None
    step = float(np.median(diffs))
    if not np.isfinite(step) or np.isclose(step, 0.0):
        return None
    atol = max(abs(step), 1.0) * 1e-6
    if not np.allclose(diffs, step, atol=atol, rtol=1e-6):
        return None
    return step


def _transform_from_center_coords(x_coords, y_coords):
    """Build an affine transform from 1D centre coordinates."""
    x_coords = np.asarray(x_coords, dtype='float64').ravel()
    y_coords = np.asarray(y_coords, dtype='float64').ravel()
    if x_coords.size < 2 or y_coords.size < 2:
        return None
    dx = _median_regular_step(x_coords)
    dy = _median_regular_step(y_coords)
    if dx is None or dy is None:
        return None
    return Affine(dx, 0.0, float(x_coords[0]) - dx / 2.0,
                  0.0, dy, float(y_coords[0]) - dy / 2.0)


def _extract_regular_lonlat_center_axes(lat, lon, shape_geo):
    """Return 1D lon/lat centre axes when a geographic grid is rectilinear."""
    lat = np.asarray(lat, dtype='float64')
    lon = np.asarray(lon, dtype='float64')
    if shape_geo == 1 and lat.ndim == 1 and lon.ndim == 1:
        return lon, lat
    if shape_geo != 2 or lat.ndim != 2 or lon.ndim != 2 or lat.shape != lon.shape:
        return None, None
    lon_axis = lon[0, :]
    lat_axis = lat[:, 0]
    lon_residual = np.nanmax(np.abs(lon - lon_axis[np.newaxis, :]))
    lat_residual = np.nanmax(np.abs(lat - lat_axis[:, np.newaxis]))
    if not np.isfinite(lon_residual) or not np.isfinite(lat_residual):
        return None, None
    if lon_residual > 1e-6 or lat_residual > 1e-6:
        return None, None
    return lon_axis, lat_axis


def _extract_input_grid_metadata(source_product, width, height, sample_var_name=None):
    """Extract exact grid metadata from the input image when available."""
    metadata = {
        'transform': None,
        'crs': None,
        'x_coords': None,
        'y_coords': None,
        'x_name': None,
        'y_name': None,
        'grid_mapping_name': None,
    }
    try:
        nc_vars = source_product.variables
    except Exception:
        return metadata

    x_name, x_coords = _load_axis_coordinate_variable(
        nc_vars,
        ('x', 'X'),
        ('projection_x_coordinate',),
        expected_length=width,
    )
    y_name, y_coords = _load_axis_coordinate_variable(
        nc_vars,
        ('y', 'Y'),
        ('projection_y_coordinate',),
        expected_length=height,
    )
    if x_coords is not None and y_coords is not None:
        metadata['x_name'] = x_name
        metadata['y_name'] = y_name
        metadata['x_coords'] = np.asarray(x_coords, dtype='float32')
        metadata['y_coords'] = np.asarray(y_coords, dtype='float32')

    grid_mapping_name = None
    if sample_var_name and sample_var_name in nc_vars:
        grid_mapping_name = getattr(nc_vars[sample_var_name], 'grid_mapping', None)
    if not grid_mapping_name:
        for candidate in nc_vars.values():
            grid_mapping_name = getattr(candidate, 'grid_mapping', None)
            if grid_mapping_name:
                break
    metadata['grid_mapping_name'] = grid_mapping_name

    crs = None
    if grid_mapping_name and grid_mapping_name in nc_vars:
        grid_var = nc_vars[grid_mapping_name]
        for attr_name in ('crs_wkt', 'spatial_ref'):
            value = getattr(grid_var, attr_name, None)
            if not value:
                continue
            try:
                crs = CRS.from_wkt(str(value))
                break
            except Exception:
                try:
                    crs = CRS.from_string(str(value))
                    break
                except Exception:
                    pass
        if crs is None:
            cf_dict = {}
            for attr_name in getattr(grid_var, 'ncattrs', lambda: [])():
                value = getattr(grid_var, attr_name, None)
                if isinstance(value, np.ndarray):
                    if value.ndim == 0:
                        value = value.item()
                    else:
                        continue
                if isinstance(value, (np.generic,)):
                    value = value.item()
                cf_dict[attr_name] = value
            try:
                crs = CRS.from_cf(cf_dict)
            except Exception:
                pass

    if crs is None:
        for attr_name in ('proj4_string', 'scene_proj4_string'):
            if attr_name in getattr(source_product, 'ncattrs', lambda: [])():
                value = getattr(source_product, attr_name, None)
                if value:
                    try:
                        crs = CRS.from_string(str(value))
                        break
                    except Exception:
                        pass

    metadata['crs'] = crs

    if metadata['x_coords'] is not None and metadata['y_coords'] is not None:
        metadata['transform'] = _transform_from_center_coords(
            metadata['x_coords'],
            metadata['y_coords'],
        )

    return metadata


def _subset_grid_metadata(grid_metadata, row_start=0, row_end=None, col_start=0, col_end=None):
    """Return source-grid metadata for a rectangular row/column subset."""
    if not grid_metadata:
        return grid_metadata

    subset = dict(grid_metadata)
    x_coords = subset.get('x_coords')
    y_coords = subset.get('y_coords')

    if x_coords is not None:
        x_coords = np.asarray(x_coords)
        subset['x_coords'] = x_coords[col_start:col_end].copy()
    if y_coords is not None:
        y_coords = np.asarray(y_coords)
        subset['y_coords'] = y_coords[row_start:row_end].copy()

    transform = subset.get('transform')
    if transform is not None:
        subset['transform'] = transform * Affine.translation(col_start, row_start)

    if subset.get('x_coords') is not None and subset.get('y_coords') is not None:
        exact_transform = _transform_from_center_coords(subset['x_coords'], subset['y_coords'])
        if exact_transform is not None:
            subset['transform'] = exact_transform

    return subset


def _identify_spectral_axis(dim_names, shape):
    """Return the index of the spectral axis based on dimension names/size."""
    spectral_tokens = ('band', 'wavelength', 'wave', 'lambda', 'wl', 'spec')
    for idx, name in enumerate(dim_names):
        lowered = name.lower()
        if any(token in lowered for token in spectral_tokens):
            return idx
    # Fallback: spectral axis is usually the smallest dimension (fewest bands)
    return int(np.argmin(shape))


def _normalize_rrs_axes(rrs_arr, dim_names):
    """Reorder the RRS cube to (bands, rows, cols) and return ordered dims."""
    if rrs_arr.ndim != 3:
        raise ValueError("Expected a 3D RRS array (rows, cols, bands).")
    if not dim_names or len(dim_names) != 3:
        dim_names = tuple(f"dim_{i}" for i in range(rrs_arr.ndim))
    else:
        dim_names = tuple(dim_names)
    spectral_axis = _identify_spectral_axis(dim_names, rrs_arr.shape)
    spatial_axes = [idx for idx in range(rrs_arr.ndim) if idx != spectral_axis]
    if len(spatial_axes) != 2:
        raise ValueError("Cannot determine spatial axes for RRS array.")
    transpose_order = [spectral_axis] + spatial_axes
    ordered = np.transpose(rrs_arr, axes=transpose_order)
    row_dim_name = image_io.stable_dimension_name(dim_names[spatial_axes[0]], 'row')
    col_dim_name = image_io.stable_dimension_name(dim_names[spatial_axes[1]], 'col')
    spectral_dim_name = image_io.stable_dimension_name(dim_names[spectral_axis], 'band')
    new_dim_list = (row_dim_name, col_dim_name, spectral_dim_name)
    return ordered, new_dim_list


def _serialize_array(arr, fill_value=OUTPUT_FILL_VALUE):
    """Return float32 array with masked values replaced by fill_value."""
    serialised = ma.array(arr).astype('float32', copy=False).filled(fill_value)
    serialised = np.asarray(serialised, dtype='float32')
    serialised[~np.isfinite(serialised)] = fill_value
    return serialised


def _chunk_tuple_to_dict(chunk_tuple):
    return dict(zip(CHUNK_RESULT_KEYS, chunk_tuple))


def _run_chunked_model(algo, rrs, width, height, image_info, siop, fixed_parameters,
                       shallow_flag, error_name, opt_met, relaxed, free_cpu,
                       bathy_arr, bathy_exposed_mask, bathy_tolerance, objective,
                       optimize_initial_guesses=False, use_five_initial_guesses=False, initial_guess_debug=False, chunk_rows_override=None,
                       chunk_dir=None):
    """Process the image chunk-by-chunk, writing intermediate results to disk."""
    if chunk_dir is None:
        chunk_dir = tempfile.mkdtemp(prefix="swampy_chunks_")
    os.makedirs(chunk_dir, exist_ok=True)

    chunk_rows = chunk_rows_override if chunk_rows_override else _suggest_chunk_rows(height, width)
    chunk_rows = max(1, min(int(chunk_rows), height))
    manifest = []

    chunk_indices = list(range(0, height, chunk_rows))
    total_chunks = len(chunk_indices)
    print(f"[INFO]: Image splitting enabled -> {total_chunks} chunk(s) of up to {chunk_rows} rows.")
    for chunk_idx, start in enumerate(chunk_indices, start=1):
        end = min(height, start + chunk_rows)
        chunk_rrs = rrs[:, start:end, :]
        chunk_bathy = bathy_arr[start:end, :] if bathy_arr is not None else None
        chunk_bathy_exposed = bathy_exposed_mask[start:end, :] if bathy_exposed_mask is not None else None
        chunk_result = algo.main_sambuca_func_simpl(
            chunk_rrs,
            objective,
            width,
            end - start,
            image_info['sensor_filter'],
            image_info['nedr'],
            siop,
            fixed_parameters,
            shallow_flag,
            error_name,
            opt_met,
            relaxed,
            free_cpu,
            bathy=chunk_bathy,
            bathy_tolerance=bathy_tolerance,
            bathy_exposed_mask=chunk_bathy_exposed,
            optimize_initial_guesses=optimize_initial_guesses,
            use_five_initial_guesses=use_five_initial_guesses,
            initial_guess_debug=initial_guess_debug)

        chunk_data = _chunk_tuple_to_dict(chunk_result)
        if chunk_data.get('initial_guess_stack') is None:
            chunk_data['initial_guess_stack'] = np.full((end - start, width, 7), OUTPUT_FILL_VALUE, dtype='float32')
        payload = {key: _serialize_array(value) for key, value in chunk_data.items()}
        payload['row_start'] = np.int32(start)
        payload['row_end'] = np.int32(end)
        payload['chunk_index'] = np.int32(chunk_idx)
        chunk_path = os.path.join(chunk_dir, f"chunk_{chunk_idx:04d}.npz")
        np.savez_compressed(chunk_path, **payload)
        manifest.append({'path': chunk_path, 'row_start': start, 'row_end': end, 'chunk_index': chunk_idx})
        print(f"[INFO]: Processed chunk {chunk_idx}/{total_chunks}")

    return manifest, chunk_dir


def _load_chunk_file(chunk_path, fill_value=OUTPUT_FILL_VALUE):
    """Load a stored chunk and return (row_start, row_end, data_dict_of_masked_arrays)."""
    with np.load(chunk_path) as npz:
        row_start = int(npz['row_start'])
        row_end = int(npz['row_end'])
        chunk_arrays = {}
        for key in CHUNK_RESULT_KEYS:
            arr = npz[key]
            chunk_arrays[key] = ma.masked_values(arr, fill_value)
    return row_start, row_end, chunk_arrays


def _compute_chunk_substrate_norms(chunk_arrays, relaxed, substrate_var_names,
                                   standardize_relaxed_substrate_outputs=False):
    sub1_frac = ma.array(chunk_arrays['sub1_frac'], copy=False)
    sub2_frac = ma.array(chunk_arrays['sub2_frac'], copy=False)
    sub3_frac = ma.array(chunk_arrays['sub3_frac'], copy=False)
    total_abun = chunk_arrays.get('total_abun')
    if total_abun is None:
        total_abun = sub1_frac + sub2_frac + sub3_frac
    else:
        total_abun = ma.array(total_abun, copy=False)

    should_standardize = (not relaxed) or bool(standardize_relaxed_substrate_outputs)
    if should_standardize:
        with np.errstate(divide='ignore', invalid='ignore'):
            n1 = ma.divide(sub1_frac, total_abun)
            n2 = ma.divide(sub2_frac, total_abun)
            n3 = ma.divide(sub3_frac, total_abun)
    else:
        n1 = sub1_frac
        n2 = sub2_frac
        n3 = sub3_frac

    if relaxed:
        sum_of_substrats = total_abun
    else:
        sum_of_substrats = ma.array(
            np.ones(total_abun.shape, dtype=np.float32),
            mask=ma.getmaskarray(total_abun),
        )
    return {
        substrate_var_names[0]: n1,
        substrate_var_names[1]: n2,
        substrate_var_names[2]: n3,
        'sum_of_substrats': sum_of_substrats,
    }


def _derive_transform_crs(width, height, lat, lon, shape_geo, grid_metadata=None):
    """Derive an affine transform and CRS for GeoTIFF from lat/lon.
    Assumes geographic WGS84 and a regular grid.
    """
    try:
        if grid_metadata:
            exact_transform = grid_metadata.get('transform')
            exact_crs = grid_metadata.get('crs')
            if exact_transform is not None and exact_crs is not None:
                return exact_transform, exact_crs
            if grid_metadata.get('x_coords') is not None and grid_metadata.get('y_coords') is not None:
                derived_transform = _transform_from_center_coords(
                    grid_metadata['x_coords'],
                    grid_metadata['y_coords'],
                )
                if derived_transform is not None:
                    if exact_crs is None:
                        raise RuntimeError("Exact input grid coordinates are available, but CRS information is missing.")
                    return derived_transform, exact_crs

        lon_axis, lat_axis = _extract_regular_lonlat_center_axes(lat, lon, shape_geo)
        if lon_axis is not None and lat_axis is not None:
            transform = _transform_from_center_coords(lon_axis, lat_axis)
            if transform is not None:
                try:
                    crs = CRS.from_epsg(4326)
                except Exception:
                    crs = CRS.from_proj4('+proj=longlat +datum=WGS84 +no_defs')
                return transform, crs

        if shape_geo == 2:
            lon_min = float(np.nanmin(lon))
            lon_max = float(np.nanmax(lon))
            lat_min = float(np.nanmin(lat))
            lat_max = float(np.nanmax(lat))
        else:
            lon_min = float(np.nanmin(lon))
            lon_max = float(np.nanmax(lon))
            lat_min = float(np.nanmin(lat))
            lat_max = float(np.nanmax(lat))
        transform = from_bounds(lon_min, lat_min, lon_max, lat_max, width, height)
        # Use a PROJ4 string for WGS84 so CRS creation does not require a
        # PROJ database lookup — avoids failures when proj.db is missing or
        # comes from an incompatible PROJ installation.
        try:
            crs = CRS.from_epsg(4326)
        except Exception:
            crs = CRS.from_proj4('+proj=longlat +datum=WGS84 +no_defs')
        return transform, crs
    except Exception as e:
        raise RuntimeError(f"Unable to compute GeoTIFF transform from lat/lon: {e}")


def _haversine_distance_m(lat1, lon1, lat2, lon2):
    """Return great-circle distance in metres between paired coordinates."""
    earth_radius_m = 6371000.0
    lat1 = np.radians(np.asarray(lat1, dtype=float))
    lon1 = np.radians(np.asarray(lon1, dtype=float))
    lat2 = np.radians(np.asarray(lat2, dtype=float))
    lon2 = np.radians(np.asarray(lon2, dtype=float))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(np.clip(1.0 - a, 0.0, None)))
    return earth_radius_m * c


def _median_valid_distance(values):
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values) & (values > 0.0)]
    if values.size == 0:
        return np.nan
    return float(np.median(values))


def _derive_pixel_spacing_meters(lat, lon, shape_geo):
    """Estimate horizontal and vertical pixel spacing in metres."""
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)

    if shape_geo == 2:
        dx = np.nan
        dy = np.nan
        if lon.shape[1] > 1:
            dx = _median_valid_distance(
                _haversine_distance_m(lat[:, :-1], lon[:, :-1], lat[:, 1:], lon[:, 1:])
            )
        if lat.shape[0] > 1:
            dy = _median_valid_distance(
                _haversine_distance_m(lat[:-1, :], lon[:-1, :], lat[1:, :], lon[1:, :])
            )
        return dx, dy

    dx = np.nan
    dy = np.nan
    if lon.ndim == 1 and lon.size > 1:
        mean_lat = float(np.nanmean(lat)) if lat.size else 0.0
        dx = _median_valid_distance(
            _haversine_distance_m(
                np.full(lon.size - 1, mean_lat, dtype=float),
                lon[:-1],
                np.full(lon.size - 1, mean_lat, dtype=float),
                lon[1:],
            )
        )
    if lat.ndim == 1 and lat.size > 1:
        ref_lon = float(np.nanmean(lon)) if lon.size else 0.0
        dy = _median_valid_distance(
            _haversine_distance_m(
                lat[:-1],
                np.full(lat.size - 1, ref_lon, dtype=float),
                lat[1:],
                np.full(lat.size - 1, ref_lon, dtype=float),
            )
        )
    return dx, dy


def _derive_projected_pixel_spacing_meters(grid_metadata=None):
    """Estimate pixel spacing in metres from projected grid metadata."""
    if not grid_metadata:
        return np.nan, np.nan

    crs = grid_metadata.get('crs')
    if crs is None or not getattr(crs, 'is_projected', False):
        return np.nan, np.nan

    transform = grid_metadata.get('transform')
    if transform is not None:
        dx = math.hypot(float(transform.a), float(transform.d))
        dy = math.hypot(float(transform.b), float(transform.e))
        if np.isfinite(dx) and dx > 0.0 and np.isfinite(dy) and dy > 0.0:
            return float(dx), float(dy)

    x_coords = grid_metadata.get('x_coords')
    y_coords = grid_metadata.get('y_coords')
    dx = np.nan
    dy = np.nan
    if x_coords is not None:
        dx = _median_valid_distance(np.abs(np.diff(np.asarray(x_coords, dtype=float))))
    if y_coords is not None:
        dy = _median_valid_distance(np.abs(np.diff(np.asarray(y_coords, dtype=float))))
    return dx, dy


def _compute_depth_slope_ratio(depth_data, lat, lon, shape_geo, dx_m=None, dy_m=None):
    """Compute rise/run slope ratio from a depth raster."""
    depth = ma.array(depth_data)
    depth_values = np.asarray(depth.filled(np.nan), dtype=float)
    depth_mask = ma.getmaskarray(depth)
    if depth_values.ndim != 2 or depth_values.shape[0] < 2 or depth_values.shape[1] < 2:
        raise RuntimeError("Slope calculation requires at least a 2x2 depth raster.")

    dx_m = _coerce_optional_float(dx_m, np.nan)
    dy_m = _coerce_optional_float(dy_m, np.nan)
    if not (_is_valid_positive_spacing(dx_m) and _is_valid_positive_spacing(dy_m)):
        dx_m, dy_m = _derive_pixel_spacing_meters(lat, lon, shape_geo)
    dx_m = _coerce_optional_float(dx_m, np.nan)
    dy_m = _coerce_optional_float(dy_m, np.nan)
    if not _is_valid_positive_spacing(dx_m) or not _is_valid_positive_spacing(dy_m):
        raise RuntimeError("Unable to derive valid pixel spacing for slope calculation.")

    dz_dy, dz_dx = np.gradient(depth_values, dy_m, dx_m)
    slope_ratio = np.sqrt(dz_dx ** 2 + dz_dy ** 2)
    invalid = depth_mask | ~np.isfinite(depth_values) | ~np.isfinite(slope_ratio)
    return ma.masked_array(slope_ratio.astype('float32', copy=False), mask=invalid)


def _compute_depth_slope_degrees(depth_data, lat, lon, shape_geo, dx_m=None, dy_m=None):
    """Compute slope in degrees from a depth raster."""
    slope_ratio = _compute_depth_slope_ratio(depth_data, lat, lon, shape_geo, dx_m=dx_m, dy_m=dy_m)
    slope_degrees = np.degrees(np.arctan(np.asarray(slope_ratio.filled(np.nan), dtype=float)))
    invalid = ma.getmaskarray(slope_ratio) | ~np.isfinite(slope_degrees)
    return ma.masked_array(slope_degrees.astype('float32', copy=False), mask=invalid)


def _compute_depth_slope_percent(depth_data, lat, lon, shape_geo, dx_m=None, dy_m=None):
    """Compute slope in percent from a depth raster."""
    slope_ratio = _compute_depth_slope_ratio(depth_data, lat, lon, shape_geo, dx_m=dx_m, dy_m=dy_m)
    slope_percent = 100.0 * np.asarray(slope_ratio.filled(np.nan), dtype=float)
    invalid = ma.getmaskarray(slope_ratio) | ~np.isfinite(slope_percent)
    return ma.masked_array(slope_percent.astype('float32', copy=False), mask=invalid)


def _normalise_anomaly_search_settings(raw_settings):
    settings = dict(DEFAULT_ANOMALY_SEARCH_SETTINGS)
    if isinstance(raw_settings, dict):
        for key in (
            'enabled',
            'export_local_moran_raster',
            'export_suspicious_binary_raster',
            'export_interpolated_rasters',
        ):
            if key in raw_settings:
                settings[key] = _coerce_bool(raw_settings.get(key), settings[key])
        if 'seed_slope_threshold_percent' in raw_settings and raw_settings.get('seed_slope_threshold_percent') not in (None, ''):
            settings['seed_slope_threshold_percent'] = _coerce_float(
                raw_settings.get('seed_slope_threshold_percent'),
                settings['seed_slope_threshold_percent'],
            )
        elif 'seed_slope_threshold_degrees' in raw_settings and raw_settings.get('seed_slope_threshold_degrees') not in (None, ''):
            legacy_threshold_degrees = _coerce_float(raw_settings.get('seed_slope_threshold_degrees'), np.nan)
            if np.isfinite(legacy_threshold_degrees):
                settings['seed_slope_threshold_percent'] = 100.0 * math.tan(math.radians(legacy_threshold_degrees))
    return settings


def _finalise_anomaly_search_settings(raw_settings, use_input_bathy=False):
    settings = _normalise_anomaly_search_settings(raw_settings)
    slope_threshold = _coerce_float(
        settings.get('seed_slope_threshold_percent'),
        DEFAULT_ANOMALY_SEARCH_SETTINGS['seed_slope_threshold_percent'],
    )
    if not np.isfinite(slope_threshold) or slope_threshold <= 0.0:
        slope_threshold = DEFAULT_ANOMALY_SEARCH_SETTINGS['seed_slope_threshold_percent']
    settings['seed_slope_threshold_percent'] = float(slope_threshold)
    if use_input_bathy:
        settings['enabled'] = False
    return settings


def _compute_row_standardized_neighbour_mean(values, valid_mask, structure=None):
    """Return the 8-neighbour mean for valid pixels, ignoring masked/invalid neighbours."""
    values = np.asarray(values, dtype='float32')
    valid_mask = np.asarray(valid_mask, dtype=bool)
    if structure is None:
        structure = np.ones((3, 3), dtype='float32')
        structure[1, 1] = 0.0
    else:
        structure = np.asarray(structure, dtype='float32')

    filled = np.where(valid_mask, values, 0.0).astype('float32', copy=False)
    neighbour_sum = ndimage.convolve(filled, structure, mode='constant', cval=0.0)
    neighbour_count = ndimage.convolve(valid_mask.astype('float32'), structure, mode='constant', cval=0.0)
    neighbour_mean = np.full(values.shape, np.nan, dtype='float32')
    np.divide(neighbour_sum, neighbour_count, out=neighbour_mean, where=neighbour_count > 0.0)
    return neighbour_mean


def _compute_local_sd_ignore_nan(values, valid_mask, window_size):
    """Return focal standard deviation while ignoring invalid pixels."""
    values = np.asarray(values, dtype='float32')
    valid_mask = np.asarray(valid_mask, dtype=bool)
    window_size = int(max(1, window_size))
    window_area = float(window_size * window_size)

    valid_float = valid_mask.astype('float32', copy=False)
    count = ndimage.uniform_filter(valid_float, size=window_size, mode='constant', cval=0.0) * window_area

    filled = np.where(valid_mask, values, 0.0).astype('float32', copy=False)
    value_sum = ndimage.uniform_filter(filled, size=window_size, mode='constant', cval=0.0) * window_area
    value_sq_sum = ndimage.uniform_filter(filled * filled, size=window_size, mode='constant', cval=0.0) * window_area

    mean = np.full(values.shape, np.nan, dtype='float32')
    variance = np.full(values.shape, np.nan, dtype='float32')
    np.divide(value_sum, count, out=mean, where=count > 0.0)
    np.divide(value_sq_sum, count, out=variance, where=count > 0.0)
    variance = variance - (mean * mean)
    variance = np.where(count >= 2.0, np.maximum(variance, 0.0), np.nan)
    local_sd = np.sqrt(np.maximum(variance, 0.0)).astype('float32', copy=False)
    local_sd[~np.isfinite(variance)] = np.nan
    return local_sd


def _majority_filter(mask, window_size):
    """Return a majority-filtered binary mask."""
    mask = np.asarray(mask, dtype=bool)
    window_size = int(max(1, window_size))
    neighbourhood_fraction = ndimage.uniform_filter(
        mask.astype('float32', copy=False),
        size=window_size,
        mode='constant',
        cval=0.0,
    )
    return np.asarray(neighbourhood_fraction >= 0.5, dtype=bool)


def _clear_small_enclosed_true_components(mask, max_pixels=15, structure=None):
    """Clear small internal True components while keeping border-touching ones."""
    mask = np.asarray(mask, dtype=bool)
    max_pixels = int(max(0, max_pixels))
    if structure is None:
        structure = np.ones((3, 3), dtype=bool)
    else:
        structure = np.asarray(structure, dtype=bool)

    labels, component_count = ndimage.label(mask, structure=structure)
    filtered = np.array(mask, copy=True)
    if max_pixels <= 0 or component_count <= 0:
        return filtered

    for label_idx in range(1, component_count + 1):
        component = labels == label_idx
        component_size = int(np.count_nonzero(component))
        if component_size <= 0 or component_size >= max_pixels:
            continue
        touches_border = (
            np.any(component[0, :]) or np.any(component[-1, :])
            or np.any(component[:, 0]) or np.any(component[:, -1])
        )
        if touches_border:
            continue
        filtered[component] = False
    return filtered


def _fill_small_enclosed_false_components(mask, max_pixels=15, structure=None):
    """Fill small internal False holes while keeping border-connected ones."""
    mask = np.asarray(mask, dtype=bool)
    filled_complement = _clear_small_enclosed_true_components(
        ~mask,
        max_pixels=max_pixels,
        structure=structure,
    )
    return np.asarray(~filled_complement, dtype=bool)


def _collapse_enclosed_binary_regions(mask, structure=None, max_iterations=4):
    """Remove enclosed islands and holes regardless of component size."""
    mask = np.asarray(mask, dtype=bool)
    if structure is None:
        structure = np.ones((3, 3), dtype=bool)
    else:
        structure = np.asarray(structure, dtype=bool)

    collapsed = np.array(mask, copy=True)
    for _ in range(max(1, int(max_iterations))):
        updated = np.asarray(
            ndimage.binary_fill_holes(collapsed, structure=structure),
            dtype=bool,
        )
        updated = np.asarray(
            ~ndimage.binary_fill_holes(~updated, structure=structure),
            dtype=bool,
        )
        if np.array_equal(updated, collapsed):
            break
        collapsed = updated
    return collapsed


def _remove_border_touching_components(mask, structure=None):
    """Remove connected True components that touch the image border."""
    mask = np.asarray(mask, dtype=bool)
    if structure is None:
        structure = np.ones((3, 3), dtype=bool)
    else:
        structure = np.asarray(structure, dtype=bool)

    labels, component_count = ndimage.label(mask, structure=structure)
    if component_count <= 0:
        return np.array(mask, copy=True)

    filtered = np.array(mask, copy=True)
    for label_idx in range(1, component_count + 1):
        component = labels == label_idx
        if (
            np.any(component[0, :]) or np.any(component[-1, :])
            or np.any(component[:, 0]) or np.any(component[:, -1])
        ):
            filtered[component] = False
    return filtered


def _build_anomaly_search_deep_protection_mask(
    depth_data,
    depth_min=0.1,
    local_sd_window=_ANOMALY_DEEP_PROTECTION_LOCAL_SD_WINDOW,
    modal_window=_ANOMALY_DEEP_PROTECTION_MODAL_WINDOW,
    local_sd_threshold=_ANOMALY_DEEP_PROTECTION_LOCAL_SD_THRESHOLD,
    small_patch_max_pixels=_ANOMALY_DEEP_PROTECTION_SMALL_PATCH_MAX_PIXELS,
):
    """Build a stable-deep protection mask from the depth raster.

    This mirrors the user's R workflow:
    - ignore pixels shallower than the minimum depth bound,
    - compute a 5x5 focal SD on depth,
    - threshold SD > 0.5,
    - smooth with an 11x11 modal/majority filter,
    - collapse enclosed holes/islands so the true-deep mask stays spatially coherent.
    """
    depth_ma = ma.array(depth_data, copy=False)
    depth_arr = np.asarray(depth_ma.filled(np.nan), dtype='float32')
    depth_mask = ma.getmaskarray(depth_ma)
    valid_mask = (
        (~depth_mask)
        & np.isfinite(depth_arr)
        & (depth_arr >= float(depth_min))
    )
    if np.count_nonzero(valid_mask) == 0:
        return np.zeros(depth_arr.shape, dtype=bool)

    local_sd = _compute_local_sd_ignore_nan(depth_arr, valid_mask, window_size=local_sd_window)
    deep_pixel_seed = valid_mask & np.isfinite(local_sd) & (local_sd > float(local_sd_threshold))
    if not np.any(deep_pixel_seed):
        return np.zeros(depth_arr.shape, dtype=bool)

    protected_mask = _majority_filter(deep_pixel_seed, window_size=modal_window) & valid_mask
    # Only remove small isolated noise patches — do NOT fill enclosed holes.
    # _collapse_enclosed_binary_regions uses binary_fill_holes which would flood the interior
    # of any ring-shaped boundary mask (e.g. a false-deep vegetated patch surrounded by
    # high-SD boundary pixels), incorrectly marking the false-deep centre as "stable deep".
    protected_mask = _clear_small_enclosed_true_components(
        protected_mask, max_pixels=int(small_patch_max_pixels)
    ) & valid_mask
    return np.asarray(protected_mask, dtype=bool)


def _interpolate_suspicious_parameter_maps(parameter_maps, source_mask, target_mask):
    """Linearly interpolate parameter values from non-suspicious pixels onto suspicious pixels."""
    source_mask = np.asarray(source_mask, dtype=bool)
    target_mask = np.asarray(target_mask, dtype=bool)
    interpolated = {}
    if not np.any(target_mask):
        for key, value in parameter_maps.items():
            shape = np.asarray(ma.array(value).filled(np.nan)).shape
            interpolated[key] = ma.masked_all(shape, dtype='float32')
        return interpolated

    target_points = np.column_stack(np.where(target_mask))
    for key, value in parameter_maps.items():
        value_ma = ma.array(value, copy=False)
        value_arr = np.asarray(value_ma.filled(np.nan), dtype='float32')
        usable_source_mask = source_mask & ~ma.getmaskarray(value_ma) & np.isfinite(value_arr)
        layer = np.full(value_arr.shape, np.nan, dtype='float32')
        if np.any(usable_source_mask):
            source_points = np.column_stack(np.where(usable_source_mask))
            source_values = value_arr[usable_source_mask].astype('float64', copy=False)
            interpolated_values = None
            if source_points.shape[0] >= 3:
                try:
                    interpolated_values = griddata(source_points, source_values, target_points, method='linear')
                except (QhullError, ValueError):
                    interpolated_values = None
            if interpolated_values is None:
                interpolated_values = np.full(target_points.shape[0], np.nan, dtype='float64')
            interpolated_values = np.asarray(interpolated_values, dtype='float64')
            missing_mask = ~np.isfinite(interpolated_values)
            if np.any(missing_mask):
                try:
                    nearest_values = griddata(source_points, source_values, target_points[missing_mask], method='nearest')
                except (QhullError, ValueError):
                    nearest_values = None
                if nearest_values is not None:
                    interpolated_values[missing_mask] = np.asarray(nearest_values, dtype='float64')
            if np.any(np.isfinite(interpolated_values)):
                valid_targets = np.isfinite(interpolated_values)
                layer[target_points[valid_targets, 0], target_points[valid_targets, 1]] = interpolated_values[valid_targets].astype('float32', copy=False)
        interpolated[key] = ma.masked_array(layer, mask=~target_mask | ~np.isfinite(layer))
    return interpolated


def _build_substrate_only_rerun_items(result_recorder, suspicious_mask, interpolated_maps, parameter_bounds):
    """Create rerun items that fix water/depth parameters and re-optimise substrates only."""
    suspicious_mask = np.asarray(suspicious_mask, dtype=bool)
    bounds_template = [tuple(map(float, bound_pair)) for bound_pair in parameter_bounds]
    rerun_items = []
    suspicious_rows, suspicious_cols = np.where(suspicious_mask)
    for row, col in zip(suspicious_rows.tolist(), suspicious_cols.tolist()):
        try:
            chl_val = float(interpolated_maps['chl'][row, col])
            cdom_val = float(interpolated_maps['cdom'][row, col])
            nap_val = float(interpolated_maps['nap'][row, col])
            depth_val = float(interpolated_maps['depth'][row, col])
        except Exception:
            continue
        if not all(np.isfinite([chl_val, cdom_val, nap_val, depth_val])):
            continue

        local_bounds = list(bounds_template)
        fixed_values = (chl_val, cdom_val, nap_val, depth_val)
        initial_guess = [chl_val, cdom_val, nap_val, depth_val]
        for index, fixed_value in enumerate(fixed_values):
            lo, hi = bounds_template[index]
            clipped_value = float(np.clip(fixed_value, lo, hi))
            local_bounds[index] = (clipped_value, clipped_value)
            initial_guess[index] = clipped_value

        for index, pixel_value in enumerate((
            result_recorder.sub1_frac[row, col],
            result_recorder.sub2_frac[row, col],
            result_recorder.sub3_frac[row, col],
        ), start=4):
            lo, hi = bounds_template[index]
            if np.isfinite(pixel_value):
                guess_value = float(np.clip(pixel_value, lo, hi))
            else:
                guess_value = 0.5 * (lo + hi)
            initial_guess.append(guess_value)

        rerun_items.append({
            'x': int(row),
            'y': int(col),
            'target_depth': initial_guess[3],
            'depth_tolerance': 0.0,
            'initial_guess': tuple(initial_guess),
            'bounds': tuple(local_bounds),
        })
    return rerun_items


def _detect_local_moran_anomaly_pixels(
    depth_data,
    sdi_data=None,
    depth_min=0.1,
    exposed_mask=None,
    protected_mask=None,
    lat_data=None,
    lon_data=None,
    shape_geo=2,
    grid_metadata=None,
    dx_m=None,
    dy_m=None,
    slope_threshold_percent=_ANOMALY_SLOPE_THRESHOLD_PERCENT,
    suspicious_modal_window=_ANOMALY_SUSPICIOUS_MODAL_WINDOW,
):
    """Detect suspicious regions from steep bathymetry jumps.

    Detection is based on:
    - a true-deep protection mask supplied through ``protected_mask``,
    - all depth-only slopes above the configured threshold, and
    - enclosed lower-slope patches that are deeper than the direct outside of
      the steep belt that surrounds them.
    """
    _ = sdi_data, depth_min  # kept for compatibility with saved workflows and tests
    depth_ma = ma.array(depth_data, copy=False)
    depth_arr = np.asarray(depth_ma.filled(np.nan), dtype='float32')
    depth_mask = ma.getmaskarray(depth_ma)
    valid_mask = (~depth_mask) & np.isfinite(depth_arr)
    if exposed_mask is not None:
        valid_mask &= ~np.asarray(exposed_mask, dtype=bool)

    result = {
        'depth_jump': ma.masked_all(depth_arr.shape, dtype='float32'),
        'sdi_drop': ma.masked_all(depth_arr.shape, dtype='float32'),
        'slope_percent': ma.masked_all(depth_arr.shape, dtype='float32'),
        'suspicious_mask': np.zeros(depth_arr.shape, dtype=bool),
        'seed_mask': np.zeros(depth_arr.shape, dtype=bool),
        'depth_only_seed_mask': np.zeros(depth_arr.shape, dtype=bool),
        'confident_mask': np.zeros(depth_arr.shape, dtype=bool),
        'valid_mask': valid_mask,
        'protected_mask': np.zeros(depth_arr.shape, dtype=bool),
        'component_count': 0,
        'suspicious_pixel_count': 0,
        'depth_jump_threshold_m': np.nan,
        'sdi_drop_threshold': np.nan,
        'slope_threshold_percent': float(slope_threshold_percent),
    }
    if np.count_nonzero(valid_mask) < 4:
        return result

    protected_mask = np.zeros(depth_arr.shape, dtype=bool) if protected_mask is None else np.asarray(protected_mask, dtype=bool)
    result['protected_mask'] = protected_mask
    analysis_mask = valid_mask & ~protected_mask
    if np.count_nonzero(analysis_mask) < 4:
        return result

    dx_m = _coerce_optional_float(dx_m, np.nan)
    dy_m = _coerce_optional_float(dy_m, np.nan)
    if not (_is_valid_positive_spacing(dx_m) and _is_valid_positive_spacing(dy_m)):
        dx_m, dy_m = _derive_projected_pixel_spacing_meters(grid_metadata)
    dx_m = _coerce_optional_float(dx_m, np.nan)
    dy_m = _coerce_optional_float(dy_m, np.nan)
    if not (_is_valid_positive_spacing(dx_m) and _is_valid_positive_spacing(dy_m)):
        if lat_data is None or lon_data is None:
            return result
        dx_m, dy_m = _derive_pixel_spacing_meters(lat_data, lon_data, shape_geo)
    dx_m = _coerce_optional_float(dx_m, np.nan)
    dy_m = _coerce_optional_float(dy_m, np.nan)
    if not (_is_valid_positive_spacing(dx_m) and _is_valid_positive_spacing(dy_m)):
        return result

    slope_percent = _compute_depth_slope_percent(
        depth_ma,
        lat_data,
        lon_data,
        shape_geo,
        dx_m=dx_m,
        dy_m=dy_m,
    )
    slope_arr = np.asarray(slope_percent.filled(np.nan), dtype='float32')
    slope_mask = ma.getmaskarray(slope_percent)
    result['slope_percent'] = ma.masked_array(
        slope_arr,
        mask=~valid_mask | slope_mask | ~np.isfinite(slope_arr),
    )

    structure = np.ones((3, 3), dtype=bool)
    protected_buffer_mask = (
        ndimage.binary_dilation(protected_mask, structure=structure)
        if np.any(protected_mask)
        else np.zeros(depth_arr.shape, dtype=bool)
    )
    search_mask = analysis_mask & ~protected_buffer_mask

    steep_mask = search_mask & np.isfinite(slope_arr) & (slope_arr >= float(slope_threshold_percent))
    if not np.any(steep_mask):
        return result

    seed_mask = np.asarray(steep_mask, dtype=bool)
    result['seed_mask'] = np.asarray(seed_mask, dtype=bool)
    result['depth_only_seed_mask'] = np.asarray(seed_mask, dtype=bool)
    if not np.any(seed_mask):
        return result

    suspicious_mask = np.asarray(seed_mask, dtype=bool)

    low_slope_mask = search_mask & ~steep_mask
    low_slope_labels, low_slope_count = ndimage.label(low_slope_mask, structure=structure)
    for label_idx in range(1, low_slope_count + 1):
        component_mask = low_slope_labels == label_idx
        if not np.any(component_mask):
            continue
        if (
            np.any(component_mask[0, :]) or np.any(component_mask[-1, :])
            or np.any(component_mask[:, 0]) or np.any(component_mask[:, -1])
        ):
            continue

        belt_mask = (
            ndimage.binary_dilation(component_mask, structure=structure)
            & search_mask
            & ~component_mask
        )
        if np.count_nonzero(belt_mask) == 0:
            continue
        if not np.all(steep_mask[belt_mask]):
            continue

        outer_ring = (
            ndimage.binary_dilation(belt_mask, structure=structure)
            & search_mask
            & ~belt_mask
            & ~component_mask
        )
        if np.count_nonzero(outer_ring) == 0:
            continue

        component_depth_median = float(np.nanmedian(depth_arr[component_mask]))
        outer_depth_median = float(np.nanmedian(depth_arr[outer_ring]))
        if not np.isfinite(component_depth_median) or not np.isfinite(outer_depth_median):
            continue
        if component_depth_median <= outer_depth_median:
            continue

        suspicious_mask |= component_mask

    if not np.any(suspicious_mask):
        return result

    if suspicious_modal_window and suspicious_modal_window > 1:
        suspicious_mask = (
            suspicious_mask
            | _majority_filter(suspicious_mask, window_size=int(suspicious_modal_window))
        ) & search_mask
    suspicious_mask = np.asarray(ndimage.binary_fill_holes(suspicious_mask), dtype=bool) & search_mask
    suspicious_mask = _remove_border_touching_components(suspicious_mask, structure=structure) & search_mask

    _component_labels, component_count = ndimage.label(suspicious_mask, structure=structure)
    result['suspicious_mask'] = suspicious_mask
    result['component_count'] = int(component_count)
    result['suspicious_pixel_count'] = int(np.count_nonzero(suspicious_mask))
    return result


def _normalize_bathy_source_crs(src_crs):
    """Map common degraded GeoTIFF CRS definitions to a usable CRS."""
    if src_crs is None:
        return None, None

    parts = []
    for attr in ('to_string', 'to_wkt'):
        try:
            value = getattr(src_crs, attr)()
            if value:
                parts.append(str(value))
        except Exception:
            pass
    parts.append(str(src_crs))
    crs_text = ' '.join(parts).lower()

    is_degraded = (
        'local_cs' in crs_text
        or 'engineeringcrs' in crs_text
        or 'unknown engineering datum' in crs_text
    )
    if is_degraded and ('lambert-93' in crs_text or 'lambert 93' in crs_text):
        return CRS.from_string(_LAMBERT_93_PROJ4), 'Lambert-93'

    return src_crs, None


def _convert_hydrographic_zero_bathy_to_depth(bathy_elevation, water_level_correction=0.0):
    """Convert a hydrographic-zero-referenced elevation raster into depth.

    The user-defined raster is interpreted as elevation relative to hydrographic
    zero: values above ZH are positive and values below ZH are negative.
    The workflow depth convention is the opposite: zero at the water surface and
    positive downward.

    Conversion:
    1. subtract the user water level correction from the ZH elevation,
    2. mark values still above the water surface as exposed,
    3. multiply by -1 to obtain depth.
    """
    bathy_elevation = np.asarray(bathy_elevation, dtype='float32')
    relative_to_surface = bathy_elevation - float(water_level_correction)
    exposed_mask = np.isfinite(relative_to_surface) & (relative_to_surface > 0.0)
    depth = -relative_to_surface
    depth = np.where(exposed_mask, np.nan, depth)
    return depth.astype('float32', copy=False), exposed_mask


def _mean_abs_neighbor_difference(image):
    """Return a simple local texture metric from 4-neighbour differences."""
    image = np.asarray(image, dtype='float32')
    valid = np.isfinite(image)
    accum = np.zeros(image.shape, dtype='float64')
    count = np.zeros(image.shape, dtype='float32')

    for axis, shift in ((0, 1), (0, -1), (1, 1), (1, -1)):
        shifted = np.roll(image, shift=shift, axis=axis)
        shifted_valid = np.roll(valid, shift=shift, axis=axis)
        edge = [slice(None)] * image.ndim
        if shift > 0:
            edge[axis] = slice(0, shift)
        else:
            edge[axis] = slice(shift, None)
        shifted_valid[tuple(edge)] = False
        pair_valid = valid & shifted_valid
        accum[pair_valid] += np.abs(image[pair_valid] - shifted[pair_valid])
        count[pair_valid] += 1.0

    texture = np.full(image.shape, np.nan, dtype='float32')
    ok = count > 0
    texture[ok] = (accum[ok] / count[ok]).astype('float32')
    return texture


def _normalised_rank(values, valid_mask, prefer_low=True):
    """Rank valid values onto [0, 1), where lower is more desirable."""
    values = np.asarray(values, dtype='float64')
    valid_mask = np.asarray(valid_mask, dtype=bool)
    ranks = np.full(values.shape, np.nan, dtype='float32')

    flat_idx = np.flatnonzero(valid_mask.ravel())
    if flat_idx.size == 0:
        return ranks

    flat_values = values.ravel()[flat_idx]
    finite = np.isfinite(flat_values)
    if not np.any(finite):
        return ranks

    flat_idx = flat_idx[finite]
    flat_values = flat_values[finite]
    sort_values = flat_values if prefer_low else -flat_values
    order = np.argsort(sort_values, kind='mergesort')

    ordered_ranks = np.empty(order.size, dtype='float32')
    if order.size == 1:
        ordered_ranks[order] = 0.0
    else:
        ordered_ranks[order] = np.linspace(0.0, 1.0, order.size, endpoint=False, dtype='float32')

    ranks_flat = ranks.ravel()
    ranks_flat[flat_idx] = ordered_ranks
    return ranks


def _build_scene_nedr_candidate_mask(observed_rrs, bathy_arr=None, bathy_exposed_mask=None):
    """Pick dark, homogeneous pixels as a scene-noise proxy."""
    cube = np.asarray(observed_rrs, dtype='float32')
    if cube.ndim != 3:
        return None, {'reason': 'observed_rrs must be a 3D (band, row, col) array'}

    valid = np.all(np.isfinite(cube), axis=0)
    valid &= np.any(np.abs(cube) > 0.0, axis=0)
    if bathy_exposed_mask is not None:
        valid &= ~np.asarray(bathy_exposed_mask, dtype=bool)

    valid_count = int(np.count_nonzero(valid))
    effective_min = min(_SCENE_NEDR_MIN_PIXELS, max(32, valid_count // 10))
    if valid_count < effective_min:
        return None, {
            'reason': f'not enough valid pixels ({valid_count})',
            'valid_pixel_count': valid_count,
            'effective_min_pixels': effective_min,
        }

    brightness = np.nanmean(cube, axis=0)
    tail_count = min(2, cube.shape[0])
    deep_proxy = np.nanmean(cube[-tail_count:, :, :], axis=0)
    texture = _mean_abs_neighbor_difference(brightness)

    brightness_rank = _normalised_rank(brightness, valid, prefer_low=True)
    deep_rank = _normalised_rank(deep_proxy, valid, prefer_low=True)
    texture_rank = _normalised_rank(texture, valid, prefer_low=True)

    score = (0.50 * deep_rank) + (0.30 * brightness_rank) + (0.20 * texture_rank)
    selection_note = 'image-only dark homogeneous pixels'
    working_valid = valid

    if bathy_arr is not None:
        bathy_arr = np.asarray(bathy_arr, dtype='float32')
        depth_valid = valid & np.isfinite(bathy_arr)
        depth_count = int(np.count_nonzero(depth_valid))
        if depth_count >= effective_min:
            depth_rank = _normalised_rank(bathy_arr, depth_valid, prefer_low=False)
            score = np.full(score.shape, np.nan, dtype='float32')
            score[depth_valid] = (
                0.35 * deep_rank[depth_valid] +
                0.15 * brightness_rank[depth_valid] +
                0.15 * texture_rank[depth_valid] +
                0.35 * depth_rank[depth_valid]
            )
            working_valid = depth_valid
            selection_note = 'bathymetry-assisted dark homogeneous deep-water pixels'

    working_count = int(np.count_nonzero(working_valid))
    if working_count < effective_min:
        return None, {
            'reason': f'not enough candidate pixels after screening ({working_count})',
            'valid_pixel_count': valid_count,
            'effective_min_pixels': effective_min,
            'selection_note': selection_note,
        }

    target_count = int(np.ceil(working_count * _SCENE_NEDR_TARGET_FRACTION))
    target_count = max(effective_min, target_count)
    target_count = min(_SCENE_NEDR_MAX_PIXELS, target_count, working_count)

    valid_flat = np.flatnonzero(working_valid.ravel())
    score_flat = score.ravel()[valid_flat]
    finite_scores = np.isfinite(score_flat)
    valid_flat = valid_flat[finite_scores]
    score_flat = score_flat[finite_scores]
    if valid_flat.size < effective_min:
        return None, {
            'reason': f'not enough finite candidate scores ({valid_flat.size})',
            'valid_pixel_count': valid_count,
            'effective_min_pixels': effective_min,
            'selection_note': selection_note,
        }

    order = np.argsort(score_flat, kind='mergesort')
    chosen_flat = valid_flat[order[:target_count]]
    candidate_mask = np.zeros(working_valid.size, dtype=bool)
    candidate_mask[chosen_flat] = True
    candidate_mask = candidate_mask.reshape(working_valid.shape)

    return candidate_mask, {
        'selection_note': selection_note,
        'valid_pixel_count': valid_count,
        'screened_pixel_count': working_count,
        'candidate_pixel_count': int(np.count_nonzero(candidate_mask)),
        'effective_min_pixels': effective_min,
    }


def _estimate_scene_nedr(observed_rrs, default_nedr, bathy_arr=None, bathy_exposed_mask=None):
    """Estimate a conservative scene-aware NEDR lower bound.

    The XML NEDR values remain the floor. We only increase them when the scene
    itself shows larger dark-water variability than the nominal sensor noise.
    """
    if isinstance(default_nedr, tuple) and len(default_nedr) == 2:
        nedr_wavelengths = np.asarray(default_nedr[0], dtype='float32')
        default_values = np.asarray(default_nedr[1], dtype='float32')
        wrap_tuple = True
    else:
        nedr_wavelengths = None
        default_values = np.asarray(default_nedr, dtype='float32')
        wrap_tuple = False

    cube = np.asarray(observed_rrs, dtype='float32')
    info = {
        'mode': 'scene',
        'applied': False,
        'selection_note': 'fixed XML NEDR',
        'candidate_pixel_count': 0,
    }

    if cube.ndim != 3 or cube.shape[0] != default_values.size:
        info['reason'] = 'observed RRS and NEDR band counts do not align'
        return default_nedr, info

    candidate_mask, candidate_info = _build_scene_nedr_candidate_mask(
        cube,
        bathy_arr=bathy_arr,
        bathy_exposed_mask=bathy_exposed_mask)
    info.update(candidate_info or {})
    if candidate_mask is None:
        return default_nedr, info

    estimated_values = np.array(default_values, dtype='float32', copy=True)
    for band_index in range(cube.shape[0]):
        band_values = cube[band_index][candidate_mask]
        band_values = band_values[np.isfinite(band_values)]
        if band_values.size < max(16, info.get('effective_min_pixels', 32) // 4):
            continue
        median = np.median(band_values)
        mad = np.median(np.abs(band_values - median))
        robust_sigma = 1.4826 * mad
        scene_nedr = _SCENE_NEDR_SIGMA_MULTIPLIER * robust_sigma
        if np.isfinite(scene_nedr):
            estimated_values[band_index] = max(float(default_values[band_index]), float(scene_nedr))

    info['applied'] = bool(np.any(estimated_values > (default_values * 1.000001)))
    info['nedr_values'] = estimated_values.tolist()
    if nedr_wavelengths is not None:
        info['nedr_wavelengths'] = nedr_wavelengths.tolist()

    if wrap_tuple:
        return (nedr_wavelengths.astype('float32'), estimated_values.astype('float32')), info
    return estimated_values.astype('float32'), info

def _write_geotiff(path, bands, transform, crs, height, width, nodata=-999.0):
    """Write a multiband GeoTIFF.
    bands: list of (name, 2D array) in order. Arrays will be cast to float32 and masked values filled with nodata.
    """
    count = len(bands)
    # choose tiling with block sizes that are multiples of 16 when possible
    bx = (min(width, 256) // 16) * 16
    by = (min(height, 256) // 16) * 16
    use_tiled = bx >= 16 and by >= 16
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': count,
        'dtype': 'float32',
        'transform': transform,
        'crs': crs,
        'nodata': nodata,
        'compress': 'deflate',
        'predictor': 3,
        'tiled': use_tiled,
    }
    if use_tiled:
        profile['blockxsize'] = bx
        profile['blockysize'] = by
    with rasterio.open(path, 'w', **profile) as dst:
        for idx, (name, arr) in enumerate(bands, start=1):
            data = ma.array(arr)
            filled = data.filled(nodata).astype('float32', copy=False)
            filled[~np.isfinite(filled)] = nodata
            dst.write(filled, idx)
            dst.set_band_description(idx, name)


def _write_netcdf_from_chunks(chunk_manifest, ofile, dim_list, height, width, nbands,
                              lat, lon, wls, name_lat, name_lon, name_w, shape_geo, primary_var_defs,
                              substrate_var_names, relaxed,
                              standardize_relaxed_substrate_outputs=False):
    nc_o = Dataset(ofile, 'w')
    nc_o.createDimension(dim_list[0], height)
    nc_o.createDimension(dim_list[1], width)
    nc_o.createDimension(dim_list[2], nbands)
    if lat is not None:
        if shape_geo == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = dim_list[0]
        var_nc = nc_o.createVariable(name_lat, 'f4', dim_geo)
        var_nc[:, :] = lat
    if lon is not None:
        if shape_geo == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = dim_list[1]
        var_nc = nc_o.createVariable(name_lon, 'f4', dim_geo)
        var_nc[:] = lon
    var_nc = nc_o.createVariable(name_w, 'f4', (dim_list[2],))
    var_nc[:] = wls

    nc_vars = {}
    for var_name, display_name in primary_var_defs:
        var_sw = nc_o.createVariable(var_name, 'f4', (dim_list[0], dim_list[1],), fill_value=OUTPUT_FILL_VALUE)
        if display_name and display_name != var_name:
            var_sw.long_name = display_name
        nc_vars[var_name] = var_sw

    for chunk in chunk_manifest:
        row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
        row_slice = slice(row_start, row_end)
        substrate_norm_map = _compute_chunk_substrate_norms(
            chunk_arrays,
            relaxed,
            substrate_var_names,
            standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
        )
        for var_name, _ in primary_var_defs:
            data = chunk_arrays.get(var_name, substrate_norm_map.get(var_name))
            if data is None:
                continue
            nc_vars[var_name][row_slice, :] = ma.array(data).filled(OUTPUT_FILL_VALUE)

    nc_o.close()


def _write_geotiff_from_chunks(chunk_manifest, tif_path, width, height, lat, lon, shape_geo,
                               primary_var_defs, substrate_var_names, relaxed,
                               standardize_relaxed_substrate_outputs=False,
                               grid_metadata=None):
    transform, crs = _derive_transform_crs(width, height, lat, lon, shape_geo, grid_metadata)
    count = len(primary_var_defs)
    bx = (min(width, 256) // 16) * 16
    by = (min(height, 256) // 16) * 16
    use_tiled = bx >= 16 and by >= 16
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': count,
        'dtype': 'float32',
        'transform': transform,
        'crs': crs,
        'nodata': OUTPUT_FILL_VALUE,
        'compress': 'deflate',
        'predictor': 3,
        'tiled': use_tiled,
    }
    if use_tiled:
        profile['blockxsize'] = bx
        profile['blockysize'] = by
    with rasterio.open(tif_path, 'w', **profile) as dst:
        for idx, (name, display_name) in enumerate(primary_var_defs, start=1):
            dst.set_band_description(idx, display_name or name)
        for chunk in chunk_manifest:
            row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
            chunk_rows = row_end - row_start
            window = Window(0, row_start, width, chunk_rows)
            substrate_norm_map = _compute_chunk_substrate_norms(
                chunk_arrays,
                relaxed,
                substrate_var_names,
                standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
            )
            for band_idx, (var_name, _) in enumerate(primary_var_defs, start=1):
                data = chunk_arrays.get(var_name, substrate_norm_map.get(var_name))
                if data is None:
                    continue
                dst.write(ma.array(data).filled(OUTPUT_FILL_VALUE), band_idx, window=window)


def _build_primary_outputs_from_chunk(chunk_arrays, primary_var_defs, substrate_var_names, relaxed,
                                      standardize_relaxed_substrate_outputs=False):
    """Return list of (var_name, array, display_name) for a single chunk."""
    required_sub_keys = ('sub1_frac', 'sub2_frac', 'sub3_frac')
    if any(chunk_arrays.get(key) is None for key in required_sub_keys):
        return []
    chunk_like = {key: chunk_arrays.get(key) for key in required_sub_keys}
    chunk_like['total_abun'] = chunk_arrays.get('total_abun')
    substrate_norm_map = _compute_chunk_substrate_norms(
        chunk_like,
        relaxed,
        substrate_var_names,
        standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
    )
    metric_arrays = {
        'chl': chunk_arrays.get('chl'),
        'cdom': chunk_arrays.get('cdom'),
        'nap': chunk_arrays.get('nap'),
        'depth': chunk_arrays.get('depth'),
        'kd': chunk_arrays.get('kd'),
        'sdi': chunk_arrays.get('sdi'),
        'sum_of_substrats': substrate_norm_map.get('sum_of_substrats'),
        'error_f': chunk_arrays.get('error_f'),
        'r_sub': chunk_arrays.get('r_sub'),
    }
    outputs = []
    for var_name, display_name in primary_var_defs:
        data = metric_arrays.get(var_name)
        if data is None:
            data = substrate_norm_map.get(var_name)
        if data is None:
            continue
        outputs.append((var_name, data, display_name))
    return outputs


def _write_single_chunk_netcdf(path, primary_outputs, dim_list, row_count, width, nbands,
                               lat_slice, lon_slice, wls, name_lat, name_lon, name_w, shape_geo):
    nc_o = Dataset(path, 'w')
    nc_o.createDimension(dim_list[0], row_count)
    nc_o.createDimension(dim_list[1], width)
    nc_o.createDimension(dim_list[2], nbands)
    if lat_slice is not None:
        if shape_geo == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = (dim_list[0],)
        var_nc = nc_o.createVariable(name_lat, 'f4', dim_geo)
        var_nc[:] = lat_slice
    if lon_slice is not None:
        if shape_geo == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = (dim_list[1],)
        var_nc = nc_o.createVariable(name_lon, 'f4', dim_geo)
        var_nc[:] = lon_slice
    var_nc = nc_o.createVariable(name_w, 'f4', (dim_list[2],))
    var_nc[:] = wls
    for var_name, data, display_name in primary_outputs:
        var_sw = nc_o.createVariable(var_name, 'f4', (dim_list[0], dim_list[1],), fill_value=OUTPUT_FILL_VALUE)
        var_sw[:] = ma.array(data)
        if display_name and display_name != var_name:
            var_sw.long_name = display_name
    nc_o.close()


def _write_single_chunk_geotiff(path, primary_outputs, lat_slice, lon_slice, shape_geo, width, row_count,
                                grid_metadata=None):
    transform, crs = _derive_transform_crs(width, row_count, lat_slice, lon_slice, shape_geo, grid_metadata)
    bands = []
    for var_name, data, display_name in primary_outputs:
        label = display_name or var_name
        bands.append((label, ma.array(data)))
    _write_geotiff(path, bands, transform, crs, row_count, width, nodata=OUTPUT_FILL_VALUE)


def _write_chunk_outputs(chunk_manifest, ofile, output_format, dim_list, width, nbands,
                         lat, lon, wls, name_lat, name_lon, name_w, shape_geo,
                         primary_var_defs, substrate_var_names, relaxed,
                         standardize_relaxed_substrate_outputs=False, cleanup_paths=None,
                         grid_metadata=None):
    """Write per-chunk outputs matching the user-selected format."""
    if not chunk_manifest or not ofile:
        return
    base, _ = os.path.splitext(ofile)
    warn_geotiff_latlon = False
    for chunk in chunk_manifest:
        row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
        chunk_idx = chunk.get('chunk_index')
        chunk_suffix = f"_chunk{chunk_idx:04d}" if isinstance(chunk_idx, int) else f"_chunk_{row_start}_{row_end}"
        chunk_base = base + chunk_suffix
        row_count = row_end - row_start
        primary_outputs = _build_primary_outputs_from_chunk(
            chunk_arrays,
            primary_var_defs,
            substrate_var_names,
            relaxed,
            standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
        )
        if not primary_outputs:
            continue
        lat_slice = None
        lon_slice = None
        if lat is not None:
            if shape_geo == 2:
                lat_slice = lat[row_start:row_end, :]
            else:
                lat_slice = lat[row_start:row_end]
        if lon is not None:
            if shape_geo == 2:
                lon_slice = lon[row_start:row_end, :]
            else:
                lon_slice = lon
        if output_format in ("netcdf", "both"):
            nc_path = chunk_base + '.nc'
            _write_single_chunk_netcdf(
                nc_path,
                primary_outputs,
                dim_list,
                row_count,
                width,
                nbands,
                lat_slice,
                lon_slice,
                wls,
                name_lat,
                name_lon,
                name_w,
                shape_geo)
            if cleanup_paths is not None:
                cleanup_paths.append(nc_path)
        if output_format in ("geotiff", "both"):
            if lat_slice is not None and lon_slice is not None:
                tif_path = chunk_base + '.tif'
                chunk_grid_metadata = _subset_grid_metadata(grid_metadata, row_start, row_end, 0, width) if grid_metadata else None
                _write_single_chunk_geotiff(
                    tif_path,
                    primary_outputs,
                    lat_slice,
                    lon_slice,
                    shape_geo,
                    width,
                    row_count,
                    chunk_grid_metadata)
                if cleanup_paths is not None:
                    cleanup_paths.append(tif_path)
            elif not warn_geotiff_latlon:
                print("[WARN]: Skipping chunk GeoTIFF export because lat/lon data is unavailable.")
                warn_geotiff_latlon = True


def _write_direct_netcdf(ofile, dim_list, height, width, nbands,
                         lat_data, lon_data, wls, lat_name, lon_name, w_name,
                         primary_outputs):
    if not primary_outputs:
        print("[WARN]: No primary outputs available for NetCDF export.")
        return
    nc_o = Dataset(ofile, 'w')
    nc_o.createDimension(dim_list[0], height)
    nc_o.createDimension(dim_list[1], width)
    nc_o.createDimension(dim_list[2], nbands)
    if lat_data is not None:
        if lat_data.ndim == 2:
            dim_geo = (dim_list[0], dim_list[1],)
            var_nc = nc_o.createVariable(lat_name, 'f4', dim_geo)
            var_nc[:, :] = lat_data
        else:
            var_nc = nc_o.createVariable(lat_name, 'f4', (dim_list[0],))
            var_nc[:] = lat_data
    if lon_data is not None:
        if lon_data.ndim == 2:
            dim_geo = (dim_list[0], dim_list[1],)
            var_nc = nc_o.createVariable(lon_name, 'f4', dim_geo)
            var_nc[:, :] = lon_data
        else:
            var_nc = nc_o.createVariable(lon_name, 'f4', (dim_list[1],))
            var_nc[:] = lon_data
    var_nc = nc_o.createVariable(w_name, 'f4', (dim_list[2],))
    var_nc[:] = wls
    for var_name, data, display_name in primary_outputs:
        var_sw = nc_o.createVariable(var_name, 'f4', (dim_list[0], dim_list[1],), fill_value=OUTPUT_FILL_VALUE)
        var_sw[:] = ma.array(data)
        if display_name and display_name != var_name:
            var_sw.long_name = display_name
    nc_o.close()


def _write_direct_geotiff(ofile, width, height, lat_data, lon_data, shape_geo_val,
                          primary_outputs, grid_metadata=None):
    if not primary_outputs:
        print("[WARN]: No primary outputs available for GeoTIFF export.")
        return
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping GeoTIFF export: lat/lon not available to derive georeferencing.")
        return
    base, _ = os.path.splitext(ofile)
    tif_path = base + '.tif'
    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    bands = [(display_name, ma.array(data)) for _, data, display_name in primary_outputs]
    _write_geotiff(tif_path, bands, transform, crs, height, width, nodata=OUTPUT_FILL_VALUE)


def _export_outputs_legacy(ofile, output_format, dim_list, height, width, nbands,
                           lat_data, lon_data, wls, lat_name, lon_name, w_name,
                           shape_geo_val, primary_outputs, grid_metadata=None):
    if output_format in ("netcdf", "both"):
        _write_direct_netcdf(
            ofile,
            dim_list,
            height,
            width,
            nbands,
            lat_data,
            lon_data,
            wls,
            lat_name,
            lon_name,
            w_name,
            primary_outputs)
    if output_format in ("geotiff", "both"):
        _write_direct_geotiff(
            ofile,
            width,
            height,
            lat_data,
            lon_data,
            shape_geo_val,
            primary_outputs,
            grid_metadata)


def _export_outputs_modern(ofile, output_format, chunk_manifest, dim_list, height, width, nbands,
                           lat_data, lon_data, wls, lat_name, lon_name, w_name,
                           shape_geo_val, primary_outputs, primary_var_defs,
                           substrate_var_names, relaxed,
                           standardize_relaxed_substrate_outputs=False, grid_metadata=None):
    if output_format in ("netcdf", "both"):
        if chunk_manifest:
            _write_netcdf_from_chunks(
                chunk_manifest,
                ofile,
                dim_list,
                height,
                width,
                nbands,
                lat_data,
                lon_data,
                wls,
                lat_name,
                lon_name,
                w_name,
                shape_geo_val,
                primary_var_defs,
                substrate_var_names,
                relaxed,
                standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs)
        else:
            _write_direct_netcdf(
                ofile,
                dim_list,
                height,
                width,
                nbands,
                lat_data,
                lon_data,
                wls,
                lat_name,
                lon_name,
                w_name,
                primary_outputs)

    if output_format in ("geotiff", "both"):
        if chunk_manifest:
            if lat_data is not None and lon_data is not None:
                try:
                    _write_geotiff_from_chunks(
                        chunk_manifest,
                        os.path.splitext(ofile)[0] + '.tif',
                        width,
                        height,
                        lat_data,
                        lon_data,
                        shape_geo_val,
                        primary_var_defs,
                        substrate_var_names,
                        relaxed,
                        standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
                        grid_metadata=grid_metadata)
                except Exception as e:
                    print(f"[ERROR]: Failed to write chunked GeoTIFF '{ofile}': {e}")
            else:
                print("[WARN]: Skipping chunk GeoTIFF main output: lat/lon not available.")
        else:
            try:
                _write_direct_geotiff(
                    ofile,
                    width,
                    height,
                    lat_data,
                    lon_data,
                    shape_geo_val,
                    primary_outputs,
                    grid_metadata)
            except Exception as e:
                print(f"[ERROR]: Failed to write GeoTIFF '{ofile}': {e}")
def _extract_substrate_labels(siop_dict, expected=3):
    """Return a cleaned list of substrate labels from the SIOP dictionary."""
    labels = []
    raw_names = siop_dict.get('substrate_names', []) if isinstance(siop_dict, dict) else []
    if not isinstance(raw_names, (list, tuple)):
        raw_names = [raw_names] if raw_names else []
    for idx, raw in enumerate(raw_names):
        label = str(raw) if raw is not None else ''
        # take last portion after colon if present
        if ':' in label:
            label = label.split(':')[-1]
        label = label.strip()
        if not label:
            label = f'Substrate {idx + 1}'
        labels.append(label)
        if len(labels) == expected:
            break
    while len(labels) < expected:
        labels.append(f'Substrate {len(labels) + 1}')
    return labels[:expected]

def _make_safe_var_names(labels):
    """Generate NetCDF-safe variable names from human-readable labels."""
    safe_names = []
    used = set()
    for idx, label in enumerate(labels):
        candidate = re.sub(r'\s+', '_', label.strip())
        candidate = re.sub(r'[^0-9A-Za-z_]', '_', candidate)
        if not candidate:
            candidate = f'{idx + 1}'
        if candidate[0].isdigit():
            candidate = f'{candidate}'
        candidate = candidate.lower()
        unique = candidate
        suffix = 2
        while unique in used:
            unique = f"{candidate}_{suffix}"
            suffix += 1
        used.add(unique)
        safe_names.append(unique)
    return safe_names


def _build_initial_guess_band_names(substrate_labels):
    safe_substrate_names = _make_safe_var_names(substrate_labels)
    return [
        'initial_guess_chl',
        'initial_guess_cdom',
        'initial_guess_nap',
        'initial_guess_depth',
        f'initial_guess_{safe_substrate_names[0]}_fraction',
        f'initial_guess_{safe_substrate_names[1]}_fraction',
        f'initial_guess_{safe_substrate_names[2]}_fraction',
    ]


def _build_spectral_band_labels(wavelengths, prefix, count=None):
    labels = []
    values = np.asarray(wavelengths if wavelengths is not None else [], dtype='float32').flatten()
    for index, value in enumerate(values):
        if np.isfinite(value):
            rounded = round(float(value), 3)
            if abs(rounded - round(rounded)) < 1e-6:
                wavelength_text = f"{int(round(rounded))} nm"
            else:
                wavelength_text = f"{rounded:g} nm"
            labels.append(f"{prefix} {wavelength_text}")
        else:
            labels.append(f"{prefix} band {index + 1}")
    if count is None:
        count = len(labels)
    while len(labels) < int(max(0, count)):
        labels.append(f"{prefix} band {len(labels) + 1}")
    if not labels:
        return [f"{prefix} band 1"]
    return labels


def _convert_modeled_reflectance_for_export(data, above_rrs_flag):
    if not above_rrs_flag:
        return ma.array(data, copy=False)
    arr = ma.array(data, copy=False)
    with np.errstate(divide='ignore', invalid='ignore'):
        converted = arr / (2.0 - 3.0 * arr)
    return ma.array(converted, copy=False)


def _cube_to_band_first(data, nbands):
    cube = ma.array(data, copy=False)
    if cube.ndim != 3:
        raise ValueError(f"Expected a 3D spectral cube, got shape {cube.shape}.")
    if cube.shape[0] == nbands:
        return cube
    if cube.shape[2] == nbands:
        return ma.transpose(cube, (2, 0, 1))
    raise ValueError(f"Unexpected spectral cube shape {cube.shape}; expected {nbands} bands.")


def _write_modeled_reflectance_netcdf(path, cube, dim_list, height, width, nbands,
                                      lat_data, lon_data, wavelengths, lat_name, lon_name, w_name,
                                      shape_geo_val, above_rrs_flag):
    cube_band_first = _cube_to_band_first(cube, nbands)
    cube_export = _convert_modeled_reflectance_for_export(cube_band_first, above_rrs_flag)
    cube_hw_bands = ma.transpose(cube_export, (1, 2, 0))

    nc_o = Dataset(path, 'w')
    nc_o.createDimension(dim_list[0], height)
    nc_o.createDimension(dim_list[1], width)
    nc_o.createDimension(dim_list[2], nbands)
    if lat_data is not None:
        if shape_geo_val == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = (dim_list[0],)
        var_nc = nc_o.createVariable(lat_name, 'f4', dim_geo)
        var_nc[:] = lat_data
    if lon_data is not None:
        if shape_geo_val == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = (dim_list[1],)
        var_nc = nc_o.createVariable(lon_name, 'f4', dim_geo)
        var_nc[:] = lon_data
    var_w = nc_o.createVariable(w_name, 'f4', (dim_list[2],))
    var_w[:] = wavelengths
    var_w.units = 'nm'

    var_ref = nc_o.createVariable('modeled_reflectance', 'f4', dim_list, fill_value=OUTPUT_FILL_VALUE)
    var_ref[:] = cube_hw_bands.filled(OUTPUT_FILL_VALUE)
    var_ref.long_name = 'Modeled reflectance'
    var_ref.reflectance_convention = 'above_water_rrs' if above_rrs_flag else 'below_surface_rrs'
    var_ref.band_descriptions = ' | '.join(_build_spectral_band_labels(wavelengths, 'Modeled reflectance', nbands))
    if lat_data is not None and lon_data is not None:
        var_ref.coordinates = f"{lat_name} {lon_name}"

    nc_o.title = 'Modeled reflectance'
    nc_o.close()


def _write_modeled_reflectance_netcdf_from_chunks(chunk_manifest, path, dim_list, height, width, nbands,
                                                  lat_data, lon_data, wavelengths, lat_name, lon_name, w_name,
                                                  shape_geo_val, above_rrs_flag):
    if not chunk_manifest:
        return

    nc_o = Dataset(path, 'w')
    nc_o.createDimension(dim_list[0], height)
    nc_o.createDimension(dim_list[1], width)
    nc_o.createDimension(dim_list[2], nbands)
    if lat_data is not None:
        if shape_geo_val == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = (dim_list[0],)
        var_nc = nc_o.createVariable(lat_name, 'f4', dim_geo)
        var_nc[:] = lat_data
    if lon_data is not None:
        if shape_geo_val == 2:
            dim_geo = (dim_list[0], dim_list[1],)
        else:
            dim_geo = (dim_list[1],)
        var_nc = nc_o.createVariable(lon_name, 'f4', dim_geo)
        var_nc[:] = lon_data
    var_w = nc_o.createVariable(w_name, 'f4', (dim_list[2],))
    var_w[:] = wavelengths
    var_w.units = 'nm'

    var_ref = nc_o.createVariable('modeled_reflectance', 'f4', dim_list, fill_value=OUTPUT_FILL_VALUE)
    var_ref.long_name = 'Modeled reflectance'
    var_ref.reflectance_convention = 'above_water_rrs' if above_rrs_flag else 'below_surface_rrs'
    var_ref.band_descriptions = ' | '.join(_build_spectral_band_labels(wavelengths, 'Modeled reflectance', nbands))
    if lat_data is not None and lon_data is not None:
        var_ref.coordinates = f"{lat_name} {lon_name}"

    for chunk in chunk_manifest:
        row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
        closed_rrs = chunk_arrays.get('closed_rrs')
        if closed_rrs is None:
            continue
        cube_band_first = _cube_to_band_first(closed_rrs, nbands)
        cube_export = _convert_modeled_reflectance_for_export(cube_band_first, above_rrs_flag)
        var_ref[row_start:row_end, :, :] = ma.transpose(cube_export, (1, 2, 0)).filled(OUTPUT_FILL_VALUE)

    nc_o.title = 'Modeled reflectance'
    nc_o.close()


def _write_modeled_reflectance_geotiff(path, cube, width, height, lat_data, lon_data,
                                       shape_geo_val, wavelengths, nbands, above_rrs_flag,
                                       grid_metadata=None):
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping modeled reflectance GeoTIFF export: lat/lon not available to derive georeferencing.")
        return
    cube_band_first = _cube_to_band_first(cube, nbands)
    cube_export = _convert_modeled_reflectance_for_export(cube_band_first, above_rrs_flag)
    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    band_labels = _build_spectral_band_labels(wavelengths, 'Modeled reflectance', nbands)
    bands = [(band_labels[index], cube_export[index, :, :]) for index in range(nbands)]
    _write_geotiff(path, bands, transform, crs, height, width, nodata=OUTPUT_FILL_VALUE)


def _write_modeled_reflectance_geotiff_from_chunks(chunk_manifest, path, width, height, lat_data, lon_data,
                                                   shape_geo_val, wavelengths, nbands, above_rrs_flag,
                                                   grid_metadata=None):
    if not chunk_manifest:
        return
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping modeled reflectance GeoTIFF export: lat/lon not available to derive georeferencing.")
        return

    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    band_labels = _build_spectral_band_labels(wavelengths, 'Modeled reflectance', nbands)
    bx = (min(width, 256) // 16) * 16
    by = (min(height, 256) // 16) * 16
    use_tiled = bx >= 16 and by >= 16
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': nbands,
        'dtype': 'float32',
        'transform': transform,
        'crs': crs,
        'nodata': OUTPUT_FILL_VALUE,
        'compress': 'deflate',
        'predictor': 3,
        'tiled': use_tiled,
    }
    if use_tiled:
        profile['blockxsize'] = bx
        profile['blockysize'] = by

    with rasterio.open(path, 'w', **profile) as dst:
        for index, band_label in enumerate(band_labels, start=1):
            dst.set_band_description(index, band_label)
        for chunk in chunk_manifest:
            row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
            closed_rrs = chunk_arrays.get('closed_rrs')
            if closed_rrs is None:
                continue
            cube_band_first = _cube_to_band_first(closed_rrs, nbands)
            cube_export = _convert_modeled_reflectance_for_export(cube_band_first, above_rrs_flag)
            window = Window(0, row_start, width, row_end - row_start)
            for band_index in range(nbands):
                band_data = ma.array(cube_export[band_index, :, :]).filled(OUTPUT_FILL_VALUE).astype('float32', copy=False)
                band_data[~np.isfinite(band_data)] = OUTPUT_FILL_VALUE
                dst.write(band_data, band_index + 1, window=window)


def _write_modeled_reflectance_outputs(ofile, cube, dim_list, height, width, nbands,
                                       lat_data, lon_data, wavelengths, lat_name, lon_name, w_name,
                                       shape_geo_val, above_rrs_flag, grid_metadata=None):
    if cube is None:
        return
    base, _ = os.path.splitext(ofile)
    nc_path = base + '_modeled_reflectance.nc'
    tif_path = base + '_modeled_reflectance.tif'
    _write_modeled_reflectance_netcdf(
        nc_path,
        cube,
        dim_list,
        height,
        width,
        nbands,
        lat_data,
        lon_data,
        wavelengths,
        lat_name,
        lon_name,
        w_name,
        shape_geo_val,
        above_rrs_flag)
    _write_modeled_reflectance_geotiff(
        tif_path,
        cube,
        width,
        height,
        lat_data,
        lon_data,
        shape_geo_val,
        wavelengths,
        nbands,
        above_rrs_flag,
        grid_metadata)


def _write_modeled_reflectance_outputs_from_chunks(chunk_manifest, ofile, dim_list, height, width, nbands,
                                                   lat_data, lon_data, wavelengths, lat_name, lon_name, w_name,
                                                   shape_geo_val, above_rrs_flag, grid_metadata=None):
    if not chunk_manifest:
        return
    base, _ = os.path.splitext(ofile)
    nc_path = base + '_modeled_reflectance.nc'
    tif_path = base + '_modeled_reflectance.tif'
    _write_modeled_reflectance_netcdf_from_chunks(
        chunk_manifest,
        nc_path,
        dim_list,
        height,
        width,
        nbands,
        lat_data,
        lon_data,
        wavelengths,
        lat_name,
        lon_name,
        w_name,
        shape_geo_val,
        above_rrs_flag)
    _write_modeled_reflectance_geotiff_from_chunks(
        chunk_manifest,
        tif_path,
        width,
        height,
        lat_data,
        lon_data,
        shape_geo_val,
        wavelengths,
        nbands,
        above_rrs_flag,
        grid_metadata)


def _write_initial_guess_geotiff(ofile, initial_guess_stack, width, height,
                                 lat_data, lon_data, shape_geo_val, band_names, grid_metadata=None):
    if initial_guess_stack is None:
        return
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping initial guess debug GeoTIFF: lat/lon not available to derive georeferencing.")
        return
    stack = np.asarray(initial_guess_stack, dtype=np.float32)
    if stack.ndim != 3 or stack.shape[2] != len(band_names):
        print("[WARN]: Skipping initial guess debug GeoTIFF: unexpected stack shape.")
        return
    base, _ = os.path.splitext(ofile)
    tif_path = base + '_initial_guesses.tif'
    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    bands = [(band_name, stack[:, :, index]) for index, band_name in enumerate(band_names)]
    _write_geotiff(tif_path, bands, transform, crs, height, width, nodata=OUTPUT_FILL_VALUE)


def _write_initial_guess_geotiff_from_chunks(chunk_manifest, ofile, width, height,
                                             lat_data, lon_data, shape_geo_val, band_names, grid_metadata=None):
    if not chunk_manifest:
        return
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping initial guess debug GeoTIFF: lat/lon not available to derive georeferencing.")
        return

    base, _ = os.path.splitext(ofile)
    tif_path = base + '_initial_guesses.tif'
    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    count = len(band_names)
    bx = (min(width, 256) // 16) * 16
    by = (min(height, 256) // 16) * 16
    use_tiled = bx >= 16 and by >= 16
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': count,
        'dtype': 'float32',
        'transform': transform,
        'crs': crs,
        'nodata': OUTPUT_FILL_VALUE,
        'compress': 'deflate',
        'predictor': 3,
        'tiled': use_tiled,
    }
    if use_tiled:
        profile['blockxsize'] = bx
        profile['blockysize'] = by

    with rasterio.open(tif_path, 'w', **profile) as dst:
        for index, band_name in enumerate(band_names, start=1):
            dst.set_band_description(index, band_name)
        for chunk in chunk_manifest:
            row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
            stack = chunk_arrays.get('initial_guess_stack')
            if stack is None:
                continue
            stack = np.asarray(stack, dtype=np.float32)
            if stack.ndim != 3 or stack.shape[2] != count:
                continue
            window = Window(0, row_start, width, row_end - row_start)
            for band_index in range(count):
                band_data = ma.array(stack[:, :, band_index]).filled(OUTPUT_FILL_VALUE).astype('float32', copy=False)
                band_data[~np.isfinite(band_data)] = OUTPUT_FILL_VALUE
                dst.write(band_data, band_index + 1, window=window)


def _assemble_chunk_array(chunk_manifest, var_name, height, width):
    """Rebuild a full-sized masked array from chunk payloads for one variable."""
    if not chunk_manifest:
        return None
    sample_shape = None
    sample_dtype = 'float32'
    for chunk in chunk_manifest:
        _row_start, _row_end, chunk_arrays = _load_chunk_file(chunk['path'])
        data = chunk_arrays.get(var_name)
        if data is None:
            continue
        arr = ma.array(data, copy=False)
        sample_shape = arr.shape
        sample_dtype = arr.dtype
        break
    if sample_shape is None:
        return None
    if len(sample_shape) == 2:
        full = ma.masked_all((height, width), dtype=sample_dtype)
    else:
        full = ma.masked_all((height, width) + tuple(sample_shape[2:]), dtype=sample_dtype)
    for chunk in chunk_manifest:
        row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
        data = chunk_arrays.get(var_name)
        if data is None:
            continue
        full[row_start:row_end, :] = ma.array(data, copy=False)
    return full


def _assemble_chunk_variable(chunk_manifest, var_name, height, width):
    return _assemble_chunk_array(chunk_manifest, var_name, height, width)


def _build_result_recorder_from_outputs(height, width, sensor_filter, nedr, fixed_parameters,
                                        outputs=None, chunk_manifest=None, initial_guess_stack=None):
    result_recorder = sb.ArrayResultWriter(
        height,
        width,
        sensor_filter,
        nedr,
        fixed_parameters,
        store_initial_guesses=initial_guess_stack is not None)

    if chunk_manifest:
        closed_rrs = ma.masked_all((height, width, result_recorder.closed_rrs.shape[2]), dtype='float32')
        for chunk in chunk_manifest:
            row_start, row_end, chunk_arrays = _load_chunk_file(chunk['path'])
            chunk_closed = chunk_arrays.get('closed_rrs')
            if chunk_closed is None:
                continue
            chunk_closed = ma.array(chunk_closed, copy=False)
            if chunk_closed.ndim == 3 and chunk_closed.shape[0] == result_recorder.closed_rrs.shape[2]:
                closed_rrs[row_start:row_end, :, :] = np.transpose(chunk_closed, (1, 2, 0))
            elif chunk_closed.ndim == 3 and chunk_closed.shape[2] == result_recorder.closed_rrs.shape[2]:
                closed_rrs[row_start:row_end, :, :] = chunk_closed
        chl = _assemble_chunk_array(chunk_manifest, 'chl', height, width)
        cdom = _assemble_chunk_array(chunk_manifest, 'cdom', height, width)
        nap = _assemble_chunk_array(chunk_manifest, 'nap', height, width)
        depth = _assemble_chunk_array(chunk_manifest, 'depth', height, width)
        nit = _assemble_chunk_array(chunk_manifest, 'nit', height, width)
        kd = _assemble_chunk_array(chunk_manifest, 'kd', height, width)
        sdi = _assemble_chunk_array(chunk_manifest, 'sdi', height, width)
        sub1_frac = _assemble_chunk_array(chunk_manifest, 'sub1_frac', height, width)
        sub2_frac = _assemble_chunk_array(chunk_manifest, 'sub2_frac', height, width)
        sub3_frac = _assemble_chunk_array(chunk_manifest, 'sub3_frac', height, width)
        error_f = _assemble_chunk_array(chunk_manifest, 'error_f', height, width)
        r_sub = _assemble_chunk_array(chunk_manifest, 'r_sub', height, width)
        if initial_guess_stack is None:
            initial_guess_stack = _assemble_chunk_array(chunk_manifest, 'initial_guess_stack', height, width)
    else:
        if outputs is None:
            return result_recorder
        (closed_rrs, chl, cdom, nap, depth, nit, kd,
         sdi, sub1_frac, sub2_frac, sub3_frac, error_f,
         _total_abun, _sub1_norm, _sub2_norm, _sub3_norm, r_sub, _initial_guess_unused) = outputs

    def _filled(arr, fill_value=0.0, dtype='float32'):
        return np.asarray(ma.array(arr).filled(fill_value), dtype=dtype)

    skip_mask = ma.getmaskarray(ma.array(depth))
    result_recorder.success[:, :] = np.where(skip_mask, 0, 1)
    result_recorder.nit[:, :] = _filled(nit, fill_value=-1, dtype=np.int64)
    result_recorder.error_alpha_f[:, :] = _filled(error_f)
    result_recorder.error_f[:, :] = _filled(error_f)
    result_recorder.chl[:, :] = _filled(chl)
    result_recorder.cdom[:, :] = _filled(cdom)
    result_recorder.nap[:, :] = _filled(nap)
    result_recorder.depth[:, :] = _filled(depth)
    result_recorder.kd[:, :] = _filled(kd)
    result_recorder.sdi[:, :] = _filled(sdi)
    result_recorder.r_sub[:, :] = _filled(r_sub)
    result_recorder.sub1_frac[:, :] = np.asarray(ma.array(sub1_frac).filled(np.nan), dtype='float32')
    result_recorder.sub2_frac[:, :] = np.asarray(ma.array(sub2_frac).filled(np.nan), dtype='float32')
    result_recorder.sub3_frac[:, :] = np.asarray(ma.array(sub3_frac).filled(np.nan), dtype='float32')
    closed_rrs_arr = ma.array(closed_rrs)
    if closed_rrs_arr.ndim == 3 and closed_rrs_arr.shape[0] == result_recorder.closed_rrs.shape[2]:
        result_recorder.closed_rrs[:, :, :] = np.transpose(
            np.asarray(closed_rrs_arr.filled(0.0), dtype='float32'),
            (1, 2, 0),
        )
    elif closed_rrs_arr.ndim == 3 and closed_rrs_arr.shape[2] == result_recorder.closed_rrs.shape[2]:
        result_recorder.closed_rrs[:, :, :] = np.asarray(closed_rrs_arr.filled(0.0), dtype='float32')
    if initial_guess_stack is not None and getattr(result_recorder, 'initial_guess_stack', None) is not None:
        stack_arr = ma.array(initial_guess_stack)
        if stack_arr.ndim == 3:
            result_recorder.initial_guess_stack[:, :, :] = np.asarray(stack_arr.filled(np.nan), dtype='float32')
    return result_recorder


def _write_slope_geotiff(ofile, depth_data, width, height, lat_data, lon_data, shape_geo_val,
                         grid_metadata=None):
    """Write a single-band slope GeoTIFF computed from retrieved depth."""
    if depth_data is None:
        return
    if lat_data is None or lon_data is None:
        print("[WARN]: Skipping slope GeoTIFF export: lat/lon not available to derive georeferencing.")
        return

    slope = _compute_depth_slope_percent(depth_data, lat_data, lon_data, shape_geo_val)
    base, _ = os.path.splitext(ofile)
    tif_path = base + '_slope.tif'
    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    _write_geotiff(tif_path, [('slope_percent', slope)], transform, crs, height, width, nodata=OUTPUT_FILL_VALUE)


def _write_anomaly_search_debug_geotiffs(ofile, width, height, lat_data, lon_data, shape_geo_val, debug_layers,
                                         grid_metadata=None):
    if not debug_layers:
        return
    if (lat_data is None or lon_data is None) and not grid_metadata:
        return
    transform, crs = _derive_transform_crs(width, height, lat_data, lon_data, shape_geo_val, grid_metadata)
    base, _ = os.path.splitext(ofile)
    for suffix, layer_name, layer_data in debug_layers:
        tif_path = base + suffix
        _write_geotiff(
            tif_path,
            [(layer_name, ma.array(layer_data))],
            transform,
            crs,
            height,
            width,
            nodata=OUTPUT_FILL_VALUE)

if __name__ == "__main__":
    #try:
        start_time = datetime.now()
        print(f"[DEBUG]: starting at {start_time.isoformat()}")
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--path", action="store", help="xml file", required=False)  # read in the header
        parser.add_argument("-p", "--pproc", action="store", type=bool, default=False, help="post processing", required=False)
        parser.add_argument("--format", choices=["netcdf", "geotiff", "both"], default=None, help="override output format")
        parser.add_argument(
            "-c",
            "--free-cpu",
            action="store",
            type=int,
            default=1,
            help="CPUs to keep idle. Default 1 reserves one CPU for the OS.",
            required=False)  # read in the header
        parser.add_argument("--bathy", action="store", help="optional path to bathymetry GeoTIFF to use (will be cropped/resampled)", required=False)
        parser.add_argument("--nedr-mode", choices=["scene", "fixed"], default=None,
                            help="use scene-adaptive or fixed XML NEDR values")
        args = parser.parse_args()
        output_format = 'netcdf'
        allow_split = False
        split_chunk_rows = None
        bathy_reference = 'depth'
        bathy_correction_m = 0.0
        bathy_tolerance_m = 0.0
        nedr_mode = 'fixed'
        optimize_initial_guesses = False
        use_five_initial_guesses = False
        initial_guess_debug = False
        output_modeled_reflectance = False
        crop_selection = None
        deep_water_selection = None
        shallow_substrate_prior_selection = None
        saved_sensor_band_mapping = None
        gui_run_versions = []
        anomaly_search_settings = dict(DEFAULT_ANOMALY_SEARCH_SETTINGS)
        if args.path:
            # the xml file has been provided, so let's read the inputs from the xml file
            xml = open(args.path, 'r')
            my_dict = xmltodict.parse(xml.read())
            root = my_dict['root']
            pp = _coerce_bool(root.get('post_processing', root.get('pproc', args.pproc)), args.pproc)
            file_im = root['image']  # input image
            siop_xml_path = _resolve_bundled_resource(root['SIOPS'])  # the xml containing the SIOPS and substrate reflectance
            pmin = np.array(root['pmin']['item']).astype('float')
            pmax = np.array(root['pmax']['item']).astype('float')  # bounds of the parameters
            file_sensor = _resolve_bundled_resource(root['sensor_filter'])  # the file containing the sensor filter
            """the three flags"""
            above_rrs_flag = _coerce_bool(root.get('rrs_flag', True))  # if above or below rrs
            reflectance_input_flag = _coerce_bool(root.get('reflectance_input', False))  # divide by pi
            relaxed = _coerce_bool(root.get('relaxed', False))  # relaxed constraints for substrates
            shallow_flag = _coerce_bool(root.get('shallow', False))  # if this shallow water
            optimize_initial_guesses = _coerce_bool(root.get('optimize_initial_guesses', False))
            use_five_initial_guesses = _coerce_bool(root.get('use_five_initial_guesses', False))
            initial_guess_debug = _coerce_bool(root.get('initial_guess_debug', False))
            if _coerce_bool(root.get('fully_relaxed', False)) and not relaxed:
                relaxed = True
            standardize_relaxed_substrate_outputs = _coerce_bool(
                root.get('standardize_relaxed_substrate_outputs', False)
            )
            output_modeled_reflectance = _coerce_bool(root.get('output_modeled_reflectance', False))
            crop_selection = _parse_crop_selection(root)
            deep_water_selection = _parse_deep_water_selection(root)
            shallow_substrate_prior_selection = _parse_shallow_substrate_prior_selection(root)
            saved_sensor_band_mapping = _parse_saved_sensor_band_mapping(root)
            raw_anomaly_search_settings = {
                'enabled': root.get('anomaly_search_enabled', False),
                'export_local_moran_raster': root.get('anomaly_search_export_local_moran_raster', False),
                'export_suspicious_binary_raster': root.get('anomaly_search_export_suspicious_binary_raster', False),
                'export_interpolated_rasters': root.get('anomaly_search_export_interpolated_rasters', False),
                'seed_slope_threshold_percent': root.get(
                    'anomaly_search_seed_slope_threshold_percent',
                    None,
                ),
                'seed_slope_threshold_degrees': root.get(
                    'anomaly_search_seed_slope_threshold_degrees',
                    None,
                ),
            }



            ofile = root['output_file']  # path of the old ouput file
            # output format from XML if available
            if 'output_format' in root:
                output_format = str(root['output_format']).lower()
            if args.free_cpu and cpu_count() <= args.free_cpu:
                args.free_cpu = 0
            bathy_path = _resolve_bundled_resource(args.bathy or root.get('bathy_path'))
            bathy_reference = str(root.get('bathy_reference', 'depth')).strip().lower()
            bathy_correction_m = _coerce_float(root.get('bathy_correction_m'), 0.0)
            bathy_tolerance_m = _coerce_float(root.get('bathy_tolerance_m'), 0.0)
            anomaly_search_settings = _finalise_anomaly_search_settings(
                raw_anomaly_search_settings,
                use_input_bathy=bool(bathy_path),
            )
            nedr_mode = str(root.get('nedr_mode', 'fixed')).strip().lower()
            allow_split = _coerce_bool(root.get('allow_split', False))
            split_chunk_rows = _parse_chunk_rows(root.get('split_chunk_rows'))
            xml_dict = copy.deepcopy(root)

        else:
            # call the GUI
            gui_result = gui_swampy.gui()
            if gui_result is None:
                print("[INFO]: GUI closed without running. Exiting.")
                sys.exit(0)
            file_list, ofile, siop_xml_path, file_sensor, above_rrs_flag, reflectance_input_flag, relaxed, shallow_flag, \
                optimize_initial_guesses, use_five_initial_guesses, initial_guess_debug, standardize_relaxed_substrate_outputs, output_modeled_reflectance, anomaly_search_settings, pmin, pmax, xml_file, xml_dict, output_format, bathy_path, pp, allow_split, split_chunk_rows_str, *extra_gui_result = gui_result
            gui_run_versions = extra_gui_result[0] if extra_gui_result else []
            split_chunk_rows = _parse_chunk_rows(split_chunk_rows_str)
            bathy_path = _resolve_bundled_resource(bathy_path)
            anomaly_search_settings = _finalise_anomaly_search_settings(
                anomaly_search_settings,
                use_input_bathy=bool(bathy_path),
            )
            bathy_reference = str(xml_dict.get('bathy_reference', 'depth')).strip().lower()
            bathy_correction_m = _coerce_float(xml_dict.get('bathy_correction_m'), 0.0)
            bathy_tolerance_m = _coerce_float(xml_dict.get('bathy_tolerance_m'), 0.0)
            nedr_mode = str(xml_dict.get('nedr_mode', 'fixed')).strip().lower()
            crop_selection = _parse_crop_selection(xml_dict)
            deep_water_selection = _parse_deep_water_selection(xml_dict)
            shallow_substrate_prior_selection = _parse_shallow_substrate_prior_selection(xml_dict)
            saved_sensor_band_mapping = _parse_saved_sensor_band_mapping(xml_dict)

        requested_workers = max(1, cpu_count() - max(0, args.free_cpu))
        if args.free_cpu > 0:
            print(f"[INFO]: Reserving {args.free_cpu} CPU(s); SWAMpy will use {requested_workers} CPU(s).")
        else:
            print(f"[INFO]: Using all available CPUs ({requested_workers}).")

        # Build list of files to process
        if args.path:
            files_to_process = [file_im]
        else:
            files_to_process = file_list if isinstance(file_list, (list, tuple)) else [file_list]
        execution_versions = list(gui_run_versions or [])
        if not execution_versions:
            execution_versions = [{
                'label': 'Settings 01',
                'suffix': '',
                'index': 1,
                'count': 1,
                'siop_xml_path': siop_xml_path,
                'file_sensor': file_sensor,
                'ofile': ofile,
                'xml_file': xml_file if 'xml_file' in locals() else '',
                'pmin': pmin,
                'pmax': pmax,
                'above_rrs_flag': above_rrs_flag,
                'reflectance_input_flag': reflectance_input_flag,
                'relaxed': relaxed,
                'shallow_flag': shallow_flag,
                'optimize_initial_guesses': optimize_initial_guesses,
                'use_five_initial_guesses': use_five_initial_guesses,
                'initial_guess_debug': initial_guess_debug,
                'standardize_relaxed_substrate_outputs': standardize_relaxed_substrate_outputs,
                'output_modeled_reflectance': output_modeled_reflectance,
                'anomaly_search_settings': copy.deepcopy(anomaly_search_settings),
                'xml_dict': copy.deepcopy(xml_dict) if 'xml_dict' in locals() else {},
                'output_format': output_format,
                'bathy_path': bathy_path,
                'post_processing': pp,
                'allow_split': allow_split,
                'split_chunk_rows': split_chunk_rows_str if 'split_chunk_rows_str' in locals() else split_chunk_rows,
            }]
        total_task_count = len(files_to_process) * len(execution_versions)

        # Batch setup
        batch_mode = len(files_to_process) > 1 or len(execution_versions) > 1
        gui_run_dir = None
        gui_default_ofile = ofile
        gui_default_ext = os.path.splitext(gui_default_ofile)[1] if gui_default_ofile else '.nc'
        if not gui_default_ext:
            gui_default_ext = '.nc'
        if not args.path:
            try:
                gui_run_dir = os.path.dirname(ofile) if ofile else None
            except Exception:
                gui_run_dir = None
            if gui_run_dir:
                os.makedirs(gui_run_dir, exist_ok=True)
        # GUI mode previously wrote a single XML; we'll now write one per output alongside the file
        resolved_execution_versions = []
        for run_version in execution_versions:
            resolved_version = _resolve_execution_version_settings(
                run_version,
                default_siop_xml_path=siop_xml_path,
                default_file_sensor=file_sensor,
                default_pmin=pmin,
                default_pmax=pmax,
                default_above_rrs_flag=above_rrs_flag,
                default_reflectance_input_flag=reflectance_input_flag,
                default_relaxed=relaxed,
                default_shallow_flag=shallow_flag,
                default_optimize_initial_guesses=optimize_initial_guesses,
                default_use_five_initial_guesses=use_five_initial_guesses,
                default_initial_guess_debug=initial_guess_debug,
                default_standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
                default_output_modeled_reflectance=output_modeled_reflectance,
                default_anomaly_search_settings=anomaly_search_settings,
                default_xml_dict=xml_dict if 'xml_dict' in locals() else {},
                default_output_format=output_format,
                default_bathy_path=bathy_path,
                default_post_processing=pp,
                default_allow_split=allow_split,
                default_split_chunk_rows=split_chunk_rows,
                default_bathy_reference=bathy_reference,
                default_bathy_correction_m=bathy_correction_m,
                default_bathy_tolerance_m=bathy_tolerance_m,
                default_nedr_mode=nedr_mode,
                format_override=args.format,
                nedr_mode_override=args.nedr_mode,
            )
            for warning_message in resolved_version.pop('warnings', []):
                print(f"[WARN]: {warning_message}")
            resolved_execution_versions.append(resolved_version)

        batch_run_root_dir = None
        if not args.path:
            batch_run_root_dir = _resolve_batch_run_root_dir(
                [resolved_version.get('output_dir') for resolved_version in resolved_execution_versions],
                fallback_dir=gui_run_dir,
            )
            if batch_run_root_dir:
                os.makedirs(batch_run_root_dir, exist_ok=True)
        batch_settings_csv_path = (
            os.path.join(batch_run_root_dir, 'batch_run_settings.csv')
            if (len(resolved_execution_versions) > 1 and batch_run_root_dir)
            else None
        )
        if batch_settings_csv_path:
            batch_settings_records = []
            for resolved_version in resolved_execution_versions:
                batch_settings_records.append({
                    'run_version_index': resolved_version['index'],
                    'run_version_label': resolved_version['label'],
                    'run_version_suffix': resolved_version['suffix'],
                    'run_version_output_folder': resolved_version.get('output_dir') or gui_run_dir,
                    'output_format': resolved_version['output_format'],
                    'post_processing': resolved_version['post_processing'],
                    'output_modeled_reflectance': resolved_version['output_modeled_reflectance'],
                    'allow_split': resolved_version['allow_split'],
                    'split_chunk_rows': resolved_version['split_chunk_rows'],
                    'nedr_mode': resolved_version['nedr_mode'],
                    'crop_selection': copy.deepcopy(resolved_version['crop_selection']),
                    'deep_water_selection': copy.deepcopy(resolved_version['deep_water_selection']),
                    'deep_water_use_sd_bounds': bool((resolved_version['deep_water_selection'] or {}).get('use_sd_bounds', False)),
                    'shallow_substrate_prior_selection': copy.deepcopy(
                        resolved_version['shallow_substrate_prior_selection']
                    ),
                    'shallow_substrate_prior_use_sd_bounds': bool(
                        (resolved_version['shallow_substrate_prior_selection'] or {}).get('use_sd_bounds', False)
                    ),
                    'siop_popup': copy.deepcopy((resolved_version.get('xml_dict') or {}).get('siop_popup', {})),
                    'sensor_popup': copy.deepcopy((resolved_version.get('xml_dict') or {}).get('sensor_popup', {})),
                    'pmin': list(np.asarray(resolved_version['pmin']).tolist()),
                    'pmax': list(np.asarray(resolved_version['pmax']).tolist()),
                    'rrs_flag': resolved_version['above_rrs_flag'],
                    'reflectance_input': resolved_version['reflectance_input_flag'],
                    'relaxed': resolved_version['relaxed'],
                    'standardize_relaxed_substrate_outputs': resolved_version['standardize_relaxed_substrate_outputs'],
                    'shallow': resolved_version['shallow_flag'],
                    'optimize_initial_guesses': resolved_version['optimize_initial_guesses'],
                    'use_five_initial_guesses': resolved_version['use_five_initial_guesses'],
                    'initial_guess_debug': resolved_version['initial_guess_debug'],
                    'use_bathy': bool(resolved_version['bathy_path']),
                    'bathy_path': resolved_version['bathy_path'] if resolved_version['bathy_path'] else '',
                    'bathy_reference': resolved_version['bathy_reference'],
                    'bathy_correction_m': resolved_version['bathy_correction_m'],
                    'bathy_tolerance_m': resolved_version['bathy_tolerance_m'],
                    'anomaly_search_settings': copy.deepcopy(resolved_version['anomaly_search_settings']),
                })
            try:
                _write_batch_run_settings_csv(batch_settings_csv_path, batch_settings_records)
                print(f"[INFO]: Wrote batch settings summary CSV: {batch_settings_csv_path}")
            except Exception as e:
                print(f"[WARN]: Failed to write batch settings summary CSV '{batch_settings_csv_path}': {e}")

        execution_tasks = [
            (run_version, file_im)
            for run_version in resolved_execution_versions
            for file_im in files_to_process
        ]
        for task_index, (run_version, file_im) in enumerate(execution_tasks, start=1):
            version_label = str(run_version.get('label', 'Settings 01'))
            run_version_suffix = str(run_version.get('suffix', '') or '')
            version_index = int(run_version.get('index', 1) or 1)
            version_count = int(run_version.get('count', len(resolved_execution_versions)) or len(resolved_execution_versions))
            version_output_dir = run_version.get('output_dir') or gui_run_dir
            if version_output_dir:
                os.makedirs(version_output_dir, exist_ok=True)
            siop_xml_path = run_version.get('siop_xml_path', siop_xml_path)
            file_sensor = run_version.get('file_sensor', file_sensor)
            pmin = np.asarray(run_version.get('pmin', pmin), dtype=float)
            pmax = np.asarray(run_version.get('pmax', pmax), dtype=float)
            above_rrs_flag = _coerce_bool(run_version.get('above_rrs_flag', above_rrs_flag), above_rrs_flag)
            reflectance_input_flag = _coerce_bool(run_version.get('reflectance_input_flag', reflectance_input_flag), reflectance_input_flag)
            relaxed = _coerce_bool(run_version.get('relaxed', relaxed), relaxed)
            shallow_flag = _coerce_bool(run_version.get('shallow_flag', shallow_flag), shallow_flag)
            optimize_initial_guesses = _coerce_bool(run_version.get('optimize_initial_guesses', optimize_initial_guesses), optimize_initial_guesses)
            use_five_initial_guesses = _coerce_bool(run_version.get('use_five_initial_guesses', use_five_initial_guesses), use_five_initial_guesses)
            initial_guess_debug = _coerce_bool(run_version.get('initial_guess_debug', initial_guess_debug), initial_guess_debug)
            standardize_relaxed_substrate_outputs = _coerce_bool(
                run_version.get(
                    'standardize_relaxed_substrate_outputs',
                    standardize_relaxed_substrate_outputs,
                ),
                standardize_relaxed_substrate_outputs,
            )
            output_modeled_reflectance = _coerce_bool(run_version.get('output_modeled_reflectance', output_modeled_reflectance), output_modeled_reflectance)
            bathy_path = run_version.get('bathy_path', bathy_path)
            anomaly_search_settings = copy.deepcopy(run_version.get('anomaly_search_settings', anomaly_search_settings))
            xml_dict = copy.deepcopy(run_version.get('xml_dict', xml_dict if 'xml_dict' in locals() else {}))
            output_format = str(run_version.get('output_format', output_format)).lower()
            pp = _coerce_bool(run_version.get('post_processing', pp), pp)
            allow_split = _coerce_bool(run_version.get('allow_split', allow_split), allow_split)
            split_chunk_rows = _parse_chunk_rows(run_version.get('split_chunk_rows', split_chunk_rows))
            bathy_reference = str(run_version.get('bathy_reference', bathy_reference)).strip().lower()
            bathy_correction_m = _coerce_float(run_version.get('bathy_correction_m'), bathy_correction_m)
            bathy_tolerance_m = _coerce_float(run_version.get('bathy_tolerance_m'), bathy_tolerance_m)
            nedr_mode = str(run_version.get('nedr_mode', nedr_mode)).strip().lower()
            crop_selection = copy.deepcopy(run_version.get('crop_selection'))
            deep_water_selection = copy.deepcopy(run_version.get('deep_water_selection'))
            shallow_substrate_prior_selection = copy.deepcopy(run_version.get('shallow_substrate_prior_selection'))
            saved_sensor_band_mapping = copy.deepcopy(run_version.get('saved_sensor_band_mapping'))

            print(
                f"[INFO]: Processing task {task_index}/{total_task_count}: "
                f"{file_im} with {version_label}"
            )
            input_name = os.path.basename(file_im)
            input_base, _ = os.path.splitext(input_name)
            if not input_base:
                input_base = input_name
            if version_output_dir:
                if batch_mode:
                    ofile = os.path.join(version_output_dir, f'swampy_{input_base}{run_version_suffix}{gui_default_ext}')
                else:
                    ofile = gui_default_ofile
            xml_dict['image'] = file_im
            xml_dict['output_file'] = ofile
            xml_dict['run_version_index'] = version_index
            xml_dict['run_version_count'] = version_count
            xml_dict['run_version_label'] = version_label
            xml_dict['run_version_suffix'] = run_version_suffix
            xml_dict['run_version_output_folder'] = os.path.dirname(ofile)
            source_product = Dataset(file_im, 'r')  # read the product

            rrs = None
            dim_list = None
            nbands = None
            height = None
            width = None
            name_rrs = None
            wls = None
            name_w = None
            rrs_band_wavelengths = []
            rrs_band_labels = []
            name_lat = None
            name_lon = None
            sensor_xml_path = file_sensor  # the sensor filter file
            band_names = []
            single_band_layers = []
            single_band_dims = None
            if os.path.exists(sensor_xml_path):
                filter_xml = open(sensor_xml_path, 'r')
                filter_dict = xmltodict.parse(filter_xml.read())
                if 'root' in filter_dict and 'nedr' in filter_dict['root'] and 'item' in filter_dict['root']['nedr']:
                    items = filter_dict["root"]["nedr"]["item"]
                    band_names = [float(i) for i in items[0]["item"]]

            # get band names from the selected image
            var_band = source_product.variables
            lat_name = None
            lon_name = None
            lat_array = None
            lon_array = None
            shape_geo = None
            lat_name, lat_array = _load_coordinate_variable(var_band, LAT_VAR_NAMES, ('latitude',))
            lon_name, lon_array = _load_coordinate_variable(var_band, LON_VAR_NAMES, ('longitude',))
            if lat_array is not None:
                lat_array = np.array(lat_array, dtype='float32', copy=True)
                name_lat = lat_name or name_lat
            if lon_array is not None:
                lon_array = np.array(lon_array, dtype='float32', copy=True)
                name_lon = lon_name or name_lon
            if lat_array is not None and lon_array is not None:
                shape_geo = 2 if np.asarray(lat_array).ndim == 2 else 1
            for var_band_name in var_band:
                print('current var_band_name: ', var_band_name)
                if var_band_name == lat_name or var_band_name == lon_name:
                    continue
                variable = source_product.variables[var_band_name]
                var_temp = variable[:]
                upper_name = var_band_name[:3].upper()
                if upper_name != 'LAT' and upper_name != 'LON':
                    if var_temp.ndim == 1:
                        if _looks_like_wavelength_var(var_band_name):
                            wls = np.asarray(var_temp)
                            name_w = var_band_name
                        continue
                    elif var_temp.ndim == 3:
                        raw_rrs = np.asarray(var_temp)
                        name_rrs = var_band_name
                        raw_dims = variable.dimensions
                        rrs, ordered_dims = _normalize_rrs_axes(raw_rrs, raw_dims)
                        nbands = rrs.shape[0]
                        height = rrs.shape[1]
                        width = rrs.shape[2]
                        dim_list = ordered_dims
                        if wls is not None and len(wls) == nbands:
                            rrs_band_wavelengths = list(map(float, np.array(wls).flatten()))
                        continue
                    elif var_temp.ndim == 2 and _is_rrs_band_variable(var_band_name):
                        band_arr = np.asarray(var_temp)
                        layer_dims = variable.dimensions
                        wave = _extract_wavelength(var_band_name, variable)
                        single_band_layers.append({
                            'name': var_band_name,
                            'data': band_arr,
                            'dims': layer_dims,
                            'wavelength': wave,
                        })
                        if single_band_dims is None:
                            single_band_dims = layer_dims
                        elif layer_dims[:2] != single_band_dims[:2] and band_arr.shape != single_band_layers[0]['data'].shape:
                            print(f"[WARN]: RRS band '{var_band_name}' dims {layer_dims} differ from expected {single_band_dims}.")
                        continue
                    elif var_temp.ndim == 2 and _is_auxiliary_scene_variable(var_band_name):
                        continue
                    else:
                        print(f"[WARN]: Skipping variable '{var_band_name}' with unsupported shape {var_temp.shape}. Expected 1D (wavelength) or 2D/3D (rrs).")

            if rrs is None and single_band_layers:
                single_band_layers.sort(key=lambda entry: (
                    entry['wavelength'] if entry['wavelength'] is not None else _band_sort_key(entry['name']),
                    entry['name']))
                first_shape = single_band_layers[0]['data'].shape
                if any(layer['data'].shape != first_shape for layer in single_band_layers):
                    raise ValueError("Single-band RRS layers have mismatched shapes.")
                stacked = np.stack([layer['data'] for layer in single_band_layers], axis=0)
                rrs = stacked
                nbands = rrs.shape[0]
                height = rrs.shape[1]
                width = rrs.shape[2]
                row_dim = image_io.stable_dimension_name(
                    single_band_layers[0]['dims'][0] if single_band_layers[0]['dims'] else None,
                    'row',
                )
                col_dim = image_io.stable_dimension_name(
                    single_band_layers[0]['dims'][1] if single_band_layers[0]['dims'] and len(single_band_layers[0]['dims']) > 1 else None,
                    'col',
                )
                dim_list = (row_dim, col_dim, 'band')
                name_rrs = 'stacked_rrs'
                rrs_band_wavelengths = [layer['wavelength'] for layer in single_band_layers]
                if name_w is None:
                    name_w = 'wavelength'
                if not isinstance(wls, np.ndarray) or len(wls) != nbands:
                    wls = np.array([
                        wave if wave is not None else idx for idx, wave in enumerate(rrs_band_wavelengths)
                    ], dtype='float32')

            if lat_array is None:
                for cand in ('lat', 'latitude', 'Lat', 'Latitude', 'LAT', 'LATITUDE'):
                    if cand in source_product.variables:
                        try:
                            lat_array = np.asarray(source_product.variables[cand][:])
                            name_lat = cand
                            break
                        except Exception:
                            continue

            if lon_array is None:
                for cand in ('lon', 'longitude', 'Lon', 'Longitude', 'LON', 'LONGITUDE'):
                    if cand in source_product.variables:
                        try:
                            lon_array = np.asarray(source_product.variables[cand][:])
                            name_lon = cand
                            break
                        except Exception:
                            continue

            if lat_array is not None and lon_array is not None:
                if lat_array.ndim == 1 and lon_array.ndim == 1:
                    lon_grid, lat_grid = np.meshgrid(lon_array, lat_array)
                    lon_array = lon_grid
                    lat_array = lat_grid
                    shape_geo = 2
                elif shape_geo is None:
                    shape_geo = len(np.asarray(lon_array).shape)

            if rrs is None:
                raise RuntimeError("Unable to locate any valid RRS data in the input image.")

            rotated_input_mode = bool(name_rrs and not single_band_layers)
            if rotated_input_mode:
                print("[INFO]: Rotated input detected -> falling back to legacy output behavior.")

            if lat_array is not None and lon_array is not None and height is not None and width is not None:
                expected_shape = (height, width)
                if lat_array.shape != expected_shape or lon_array.shape != expected_shape:
                    if lat_array.shape[::-1] == expected_shape and lon_array.shape[::-1] == expected_shape:
                        lat_array = np.transpose(lat_array)
                        lon_array = np.transpose(lon_array)
                        shape_geo = 2
                    else:
                        print(f"[WARN]: Lat/Lon shapes {lat_array.shape}, {lon_array.shape} do not match expected {(height, width)}.")
            if rrs_band_wavelengths and (wls is None or len(wls) != nbands):
                try:
                    wls = np.array(rrs_band_wavelengths, dtype='float32')
                except Exception:
                    wls = None

            rrs_band_labels = _build_runtime_band_labels(single_band_layers, nbands, rrs_band_wavelengths)

            if band_names and saved_sensor_band_mapping is not None:
                mapped_rrs, mapped_wls = _apply_saved_sensor_band_mapping(
                    rrs,
                    saved_sensor_band_mapping,
                    rrs_band_labels,
                    rrs_band_wavelengths,
                    band_names,
                )
                if mapped_rrs is not None:
                    print(f"[INFO]: Applying explicit input-to-sensor band mapping for {mapped_rrs.shape[0]} band(s).")
                    rrs = mapped_rrs
                    nbands = rrs.shape[0]
                    rrs_band_wavelengths = mapped_wls.tolist()
                    wls = mapped_wls
                    name_w = name_w or 'wavelength'
                    rrs_band_labels = list(saved_sensor_band_mapping.get('image_band_labels') or rrs_band_labels)

            if band_names and nbands is not None and len(band_names) != nbands:
                print(f"[INFO]: Aligning observed RRS ({nbands} bands) to sensor filter ({len(band_names)} bands).")
                rrs, selected_wls = _align_rrs_to_filter(rrs, rrs_band_wavelengths, band_names)
                nbands = rrs.shape[0]
                rrs_band_wavelengths = selected_wls.tolist()
                wls = selected_wls
                if name_w is None:
                    name_w = 'wavelength'

            if wls is None and nbands is not None:
                wls = np.arange(nbands, dtype='float32')
                if name_w is None:
                    name_w = 'wavelength'

            # Final fallback: reload lat/lon if still missing
            if (lat_array is None or lon_array is None):
                try:
                    with Dataset(file_im, 'r') as coord_src:
                        if lat_array is None:
                            for cand in ('lat', 'latitude', 'Lat', 'Latitude', 'LAT', 'LATITUDE'):
                                if cand in coord_src.variables:
                                    try:
                                        lat_array = np.asarray(coord_src.variables[cand][:])
                                        name_lat = cand
                                        break
                                    except Exception:
                                        continue
                        if lon_array is None:
                            for cand in ('lon', 'longitude', 'Lon', 'Longitude', 'LON', 'LONGITUDE'):
                                if cand in coord_src.variables:
                                    try:
                                        lon_array = np.asarray(coord_src.variables[cand][:])
                                        name_lon = cand
                                        break
                                    except Exception:
                                        continue
                except Exception:
                    pass

            if lat_array is not None:
                lat_array = np.array(lat_array, dtype='float32', copy=True)
            if lon_array is not None:
                lon_array = np.array(lon_array, dtype='float32', copy=True)

            lat_grid = None
            lon_grid = None
            if lat_array is not None and lon_array is not None:
                lat_grid = lat_array
                lon_grid = lon_array
                if shape_geo is None:
                    shape_geo = 2 if lat_grid.ndim == 2 else 1
            elif height is not None and width is not None:
                row_coords = np.arange(height, dtype='float32')
                col_coords = np.arange(width, dtype='float32')
                lon_grid, lat_grid = np.meshgrid(col_coords, row_coords)
                shape_geo = 2
                if name_lat is None:
                    name_lat = 'row_index'
                if name_lon is None:
                    name_lon = 'col_index'
            lat_array = lat_grid
            lon_array = lon_grid

            grid_reference_var_name = None
            if name_rrs and name_rrs in source_product.variables:
                grid_reference_var_name = name_rrs
            elif single_band_layers:
                first_layer_name = single_band_layers[0].get('name')
                if first_layer_name in source_product.variables:
                    grid_reference_var_name = first_layer_name
            source_grid_metadata = _extract_input_grid_metadata(
                source_product,
                width,
                height,
                sample_var_name=grid_reference_var_name,
            )

            deep_water_rrs_full = None
            deep_water_lat_full = None
            deep_water_lon_full = None
            deep_water_grid_metadata = None
            shallow_prior_rrs_full = None
            shallow_prior_lat_full = None
            shallow_prior_lon_full = None
            shallow_prior_grid_metadata = None
            if deep_water_selection and lat_array is not None and lon_array is not None:
                deep_water_rrs_full = np.array(rrs, dtype='float32', copy=True)
                deep_water_lat_full = np.array(lat_array, dtype='float32', copy=True)
                deep_water_lon_full = np.array(lon_array, dtype='float32', copy=True)
                deep_water_grid_metadata = source_grid_metadata
            if shallow_substrate_prior_selection and lat_array is not None and lon_array is not None:
                shallow_prior_rrs_full = np.array(rrs, dtype='float32', copy=True)
                shallow_prior_lat_full = np.array(lat_array, dtype='float32', copy=True)
                shallow_prior_lon_full = np.array(lon_array, dtype='float32', copy=True)
                shallow_prior_grid_metadata = source_grid_metadata

            # Sensor filter
            if args.path:
                [sensor_filter, nedr] = create_input.read_sensor_filter(sensor_xml_path)
            else:
                [sensor_filter, nedr] = create_input.read_sensor_filter_gui(sensor_xml_path, nbands)

            # Read SIOP and prepare inputs
            [siop, envmeta] = create_input.read_siop(siop_xml_path, pmin, pmax)
            error_name = 'alpha_f'
            opt_met = 'SLSQP'
            image_info = {'sensor_filter': sensor_filter, 'nedr': nedr}
            [wavelengths, siop, image_info, fixed_parameters, objective] = create_input.prepare_input(siop, envmeta, image_info, error_name)
            algo = main_sambuca_snap.main_sambuca()

            # initialize the rrs array
            if reflectance_input_flag:
                rrs = rrs / np.pi
                if deep_water_rrs_full is not None:
                    deep_water_rrs_full = deep_water_rrs_full / np.pi
                if shallow_prior_rrs_full is not None:
                    shallow_prior_rrs_full = shallow_prior_rrs_full / np.pi

            if above_rrs_flag == True:
                rrs = (2 * rrs) / ((3 * rrs) + 1)
                if deep_water_rrs_full is not None:
                    deep_water_rrs_full = (2 * deep_water_rrs_full) / ((3 * deep_water_rrs_full) + 1)
                if shallow_prior_rrs_full is not None:
                    shallow_prior_rrs_full = (2 * shallow_prior_rrs_full) / ((3 * shallow_prior_rrs_full) + 1)

            shallow_prior_stats = None
            shallow_prior_csv_path = None
            shallow_prior_mask = None
            shallow_prior_successful_estimates = []
            shallow_prior_accepted_estimates = []
            if (
                shallow_substrate_prior_selection
                and shallow_prior_rrs_full is not None
                and shallow_prior_lat_full is not None
                and shallow_prior_lon_full is not None
            ):
                try:
                    shallow_target_index, shallow_target_name = _resolve_shallow_substrate_target_index(
                        shallow_substrate_prior_selection,
                        siop.get('substrate_names', []),
                    )
                    if shallow_target_index is None:
                        print(
                            "[WARN]: Shallow-water substrate priors were provided, but the selected target "
                            f"'{shallow_substrate_prior_selection.get('target_name', '')}' is not active in this run. "
                            "Ignoring shallow-water priors."
                        )
                    else:
                        shallow_prior_mask = _rasterize_epsg4326_geometries(
                            shallow_substrate_prior_selection.get('polygons') or [],
                            shallow_prior_lat_full,
                            shallow_prior_lon_full,
                            shallow_prior_grid_metadata,
                        )
                        if shallow_prior_mask is not None and np.any(shallow_prior_mask):
                            shallow_rows, shallow_cols = np.where(shallow_prior_mask)
                            selected_pixel_count = int(shallow_rows.size)
                            if selected_pixel_count > _PRIOR_PIXEL_SAMPLE_LIMIT:
                                rng = np.random.default_rng(42)
                                chosen_indices = np.sort(
                                    rng.choice(
                                        selected_pixel_count,
                                        size=_PRIOR_PIXEL_SAMPLE_LIMIT,
                                        replace=False,
                                    )
                                )
                                shallow_rows = shallow_rows[chosen_indices]
                                shallow_cols = shallow_cols[chosen_indices]
                                print(
                                    f"[INFO]: Shallow-water prior polygons selected {selected_pixel_count} pixel(s); "
                                    f"randomly subsampling {_PRIOR_PIXEL_SAMPLE_LIMIT} pixels for prior analysis."
                                )
                            chl_bounds = tuple(float(v) for v in siop['p_bounds'][0])
                            cdom_bounds = tuple(float(v) for v in siop['p_bounds'][1])
                            nap_bounds = tuple(float(v) for v in siop['p_bounds'][2])
                            depth_bounds = tuple(float(v) for v in siop['p_bounds'][3])
                            substrate_fractions = [0.0, 0.0, 0.0]
                            substrate_fractions[int(shallow_target_index)] = 1.0
                            shallow_prior_pixel_rows = []
                            for row, col in zip(shallow_rows.tolist(), shallow_cols.tolist()):
                                observed_rrs = shallow_prior_rrs_full[:, int(row), int(col)]
                                estimate = _estimate_shallow_substrate_pixel(
                                    objective,
                                    observed_rrs,
                                    chl_bounds,
                                    cdom_bounds,
                                    nap_bounds,
                                    depth_bounds,
                                    substrate_fractions,
                                )
                                if estimate is None:
                                    continue
                                accepted_for_prior = bool(
                                    estimate.get('success', False)
                                    and float(estimate.get('exp_bottom', 0.0)) >= _SHALLOW_SUBSTRATE_PRIOR_MIN_EXP_BOTTOM
                                )
                                pixel_row = {
                                    'row': int(row),
                                    'col': int(col),
                                    'lat': float(shallow_prior_lat_full[int(row), int(col)]),
                                    'lon': float(shallow_prior_lon_full[int(row), int(col)]),
                                    'target_name': shallow_target_name,
                                    'chl': estimate['chl'],
                                    'cdom': estimate['cdom'],
                                    'nap': estimate['nap'],
                                    'depth': estimate['depth'],
                                    'exp_bottom': estimate['exp_bottom'],
                                    'error_alpha_f': estimate['error_alpha_f'],
                                    'success': int(bool(estimate.get('success', False))),
                                    'accepted_for_prior': int(accepted_for_prior),
                                }
                                shallow_prior_pixel_rows.append(pixel_row)
                                if estimate.get('success', False):
                                    shallow_prior_successful_estimates.append(estimate)
                                if accepted_for_prior:
                                    shallow_prior_accepted_estimates.append(estimate)
                            if shallow_prior_accepted_estimates:
                                shallow_prior_stats = _apply_iop_priors(
                                    siop,
                                    shallow_prior_accepted_estimates,
                                    shallow_substrate_prior_selection.get('use_sd_bounds', False),
                                )
                                shallow_prior_csv_path = os.path.splitext(ofile)[0] + '_shallow_substrate_prior_pixels.csv'
                                _write_shallow_substrate_prior_pixel_csv(
                                    shallow_prior_csv_path,
                                    shallow_prior_pixel_rows,
                                )
                                rejected_count = max(
                                    0,
                                    len(shallow_prior_successful_estimates) - len(shallow_prior_accepted_estimates),
                                )
                                print(
                                    "[INFO]: Shallow-water substrate priors estimated from "
                                    f"{len(shallow_prior_accepted_estimates)} accepted pixel(s) for {shallow_target_name} "
                                    f"(out of {len(shallow_prior_successful_estimates)} converged, {rejected_count} rejected because "
                                    f"exp_bottom < {_SHALLOW_SUBSTRATE_PRIOR_MIN_EXP_BOTTOM:g}). "
                                    f"CHL={shallow_prior_stats['chl_mean']:.6f}, "
                                    f"CDOM={shallow_prior_stats['cdom_mean']:.6f}, "
                                    f"NAP={shallow_prior_stats['nap_mean']:.6f}."
                                )
                                if shallow_substrate_prior_selection.get('use_sd_bounds', False):
                                    print(
                                        "[INFO]: Applying shallow-water substrate priors as mean ± sd bounds for CHL, CDOM and NAP."
                                    )
                                    retained_lines = (
                                        (
                                            'CHL',
                                            shallow_prior_stats['chl_mean'],
                                            shallow_prior_stats['chl_sd'],
                                            shallow_prior_stats['applied_pmin'][0],
                                            shallow_prior_stats['applied_pmax'][0],
                                        ),
                                        (
                                            'CDOM',
                                            shallow_prior_stats['cdom_mean'],
                                            shallow_prior_stats['cdom_sd'],
                                            shallow_prior_stats['applied_pmin'][1],
                                            shallow_prior_stats['applied_pmax'][1],
                                        ),
                                        (
                                            'NAP',
                                            shallow_prior_stats['nap_mean'],
                                            shallow_prior_stats['nap_sd'],
                                            shallow_prior_stats['applied_pmin'][2],
                                            shallow_prior_stats['applied_pmax'][2],
                                        ),
                                    )
                                    for name, mean_value, sd_value, lower_value, upper_value in retained_lines:
                                        print(
                                            f"[INFO]: Retained shallow-water {name}: "
                                            f"mean={mean_value:.6f}, sd={sd_value:.6f}, "
                                            f"bounds=[{lower_value:.6f}, {upper_value:.6f}]"
                                        )
                                else:
                                    print(
                                        "[INFO]: Applying shallow-water substrate priors as fixed CHL, CDOM and NAP values."
                                    )
                                    retained_lines = (
                                        ('CHL', shallow_prior_stats['applied_pmin'][0]),
                                        ('CDOM', shallow_prior_stats['applied_pmin'][1]),
                                        ('NAP', shallow_prior_stats['applied_pmin'][2]),
                                    )
                                    for name, retained_value in retained_lines:
                                        print(f"[INFO]: Retained shallow-water {name}: value={retained_value:.6f}")
                            elif shallow_prior_successful_estimates:
                                print(
                                    "[WARN]: Shallow-water prior polygons converged, but every solution behaved like optically deep water "
                                    f"(exp_bottom < {_SHALLOW_SUBSTRATE_PRIOR_MIN_EXP_BOTTOM:g}). Ignoring shallow-water priors."
                                )
                            else:
                                print(
                                    "[WARN]: Shallow-water prior polygons were provided, but no valid shallow-water parameter estimates converged. "
                                    "Ignoring shallow-water priors."
                                )
                        else:
                            print("[WARN]: Shallow-water prior polygons do not overlap the current scene grid. Ignoring shallow-water priors.")
                except Exception as e:
                    print(f"[WARN]: Failed to estimate shallow-water substrate priors: {e}")

            deep_water_prior_stats = None
            deep_water_iop_raster_path = None
            deep_water_mask = None
            deep_water_successful_estimates = []
            if deep_water_selection and deep_water_rrs_full is not None and deep_water_lat_full is not None and deep_water_lon_full is not None:
                try:
                    deep_water_mask = _rasterize_epsg4326_geometries(
                        deep_water_selection.get('polygons') or [],
                        deep_water_lat_full,
                        deep_water_lon_full,
                        deep_water_grid_metadata,
                    )
                    if deep_water_mask is not None and np.any(deep_water_mask):
                        deep_rows, deep_cols = np.where(deep_water_mask)
                        selected_pixel_count = int(deep_rows.size)
                        subsample_deep_water_pixels = _coerce_bool(
                            deep_water_selection.get('subsample_pixels', True),
                            True,
                        )
                        if selected_pixel_count > _PRIOR_PIXEL_SAMPLE_LIMIT and subsample_deep_water_pixels:
                            rng = np.random.default_rng(42)
                            chosen_indices = np.sort(
                                rng.choice(
                                    selected_pixel_count,
                                    size=_PRIOR_PIXEL_SAMPLE_LIMIT,
                                    replace=False,
                                )
                            )
                            deep_rows = deep_rows[chosen_indices]
                            deep_cols = deep_cols[chosen_indices]
                            print(
                                f"[INFO]: Deep-water polygons selected {selected_pixel_count} pixel(s); "
                                f"randomly subsampling {_PRIOR_PIXEL_SAMPLE_LIMIT} pixels for deep-water analysis."
                            )
                        elif selected_pixel_count > _PRIOR_PIXEL_SAMPLE_LIMIT:
                            print(
                                f"[INFO]: Deep-water polygons selected {selected_pixel_count} pixel(s); "
                                "analyzing all selected pixels because deep-water subsampling is disabled."
                            )
                        chl_bounds, cdom_bounds, nap_bounds = _DEEP_WATER_IOP_RELAXED_BOUNDS
                        print(
                            "[INFO]: Deep-water mode is using relaxed IOP bounds "
                            f"CHL=[{chl_bounds[0]:g}, {chl_bounds[1]:g}], "
                            f"CDOM=[{cdom_bounds[0]:g}, {cdom_bounds[1]:g}], "
                            f"NAP=[{nap_bounds[0]:g}, {nap_bounds[1]:g}]."
                        )
                        deep_water_pixel_rows = []
                        for row, col in zip(deep_rows.tolist(), deep_cols.tolist()):
                            observed_rrs = deep_water_rrs_full[:, int(row), int(col)]
                            estimate = _estimate_deep_water_pixel(
                                objective,
                                observed_rrs,
                                chl_bounds,
                                cdom_bounds,
                                nap_bounds,
                            )
                            if estimate is None:
                                continue
                            pixel_row = {
                                'row': int(row),
                                'col': int(col),
                                'lat': float(deep_water_lat_full[int(row), int(col)]),
                                'lon': float(deep_water_lon_full[int(row), int(col)]),
                                'chl': estimate['chl'],
                                'cdom': estimate['cdom'],
                                'nap': estimate['nap'],
                                'error_alpha_f': estimate['error_alpha_f'],
                                'success': int(bool(estimate.get('success', False))),
                            }
                            deep_water_pixel_rows.append(pixel_row)
                            if estimate.get('success', False):
                                deep_water_successful_estimates.append(estimate)
                        if deep_water_successful_estimates:
                            deep_water_prior_stats = _apply_deep_water_priors(
                                siop,
                                deep_water_successful_estimates,
                                deep_water_selection.get('use_sd_bounds', False),
                            )
                            deep_water_iop_raster_path = os.path.splitext(ofile)[0] + '_deep_water_iop_estimates.tif'
                            try:
                                raster_info = _write_deep_water_iop_raster(
                                    deep_water_iop_raster_path,
                                    deep_water_pixel_rows,
                                    int(deep_water_rrs_full.shape[2]),
                                    int(deep_water_rrs_full.shape[1]),
                                    deep_water_lat_full,
                                    deep_water_lon_full,
                                    2 if np.asarray(deep_water_lat_full).ndim == 2 else shape_geo,
                                    deep_water_grid_metadata,
                                )
                                if raster_info is None:
                                    deep_water_iop_raster_path = None
                                else:
                                    print(
                                        "[INFO]: Wrote deep-water IOP raster with "
                                        f"{raster_info['written_pixel_count']} successful pixel estimate(s): "
                                        f"{deep_water_iop_raster_path}"
                                    )
                            except Exception as raster_exc:
                                deep_water_iop_raster_path = None
                                print(f"[WARN]: Failed to write deep-water IOP raster: {raster_exc}")
                            print(
                                "[INFO]: Deep-water priors estimated from "
                                f"{len(deep_water_successful_estimates)} selected pixel(s). "
                                f"CHL={deep_water_prior_stats['chl_mean']:.6f}, "
                                f"CDOM={deep_water_prior_stats['cdom_mean']:.6f}, "
                                f"NAP={deep_water_prior_stats['nap_mean']:.6f}."
                            )
                            if deep_water_selection.get('use_sd_bounds', False):
                                print("[INFO]: Applying deep-water priors as mean ± sd bounds for CHL, CDOM and NAP.")
                                retained_lines = (
                                    (
                                        'CHL',
                                        deep_water_prior_stats['chl_mean'],
                                        deep_water_prior_stats['chl_sd'],
                                        deep_water_prior_stats['applied_pmin'][0],
                                        deep_water_prior_stats['applied_pmax'][0],
                                    ),
                                    (
                                        'CDOM',
                                        deep_water_prior_stats['cdom_mean'],
                                        deep_water_prior_stats['cdom_sd'],
                                        deep_water_prior_stats['applied_pmin'][1],
                                        deep_water_prior_stats['applied_pmax'][1],
                                    ),
                                    (
                                        'NAP',
                                        deep_water_prior_stats['nap_mean'],
                                        deep_water_prior_stats['nap_sd'],
                                        deep_water_prior_stats['applied_pmin'][2],
                                        deep_water_prior_stats['applied_pmax'][2],
                                    ),
                                )
                                for name, mean_value, sd_value, lower_value, upper_value in retained_lines:
                                    print(
                                        f"[INFO]: Retained deep-water {name}: "
                                        f"mean={mean_value:.6f}, sd={sd_value:.6f}, "
                                        f"bounds=[{lower_value:.6f}, {upper_value:.6f}]"
                                    )
                            else:
                                print("[INFO]: Applying deep-water priors as fixed CHL, CDOM and NAP values.")
                                retained_lines = (
                                    ('CHL', deep_water_prior_stats['applied_pmin'][0]),
                                    ('CDOM', deep_water_prior_stats['applied_pmin'][1]),
                                    ('NAP', deep_water_prior_stats['applied_pmin'][2]),
                                )
                                for name, retained_value in retained_lines:
                                    print(f"[INFO]: Retained deep-water {name}: value={retained_value:.6f}")
                        else:
                            print("[WARN]: Deep-water polygons were provided, but no valid deep-water parameter estimates converged. Ignoring deep-water priors.")
                    else:
                        print("[WARN]: Deep-water polygons do not overlap the current scene grid. Ignoring deep-water priors.")
                except Exception as e:
                    print(f"[WARN]: Failed to estimate deep-water priors: {e}")

            if not args.path and shallow_prior_stats is not None:
                xml_dict['shallow_substrate_prior_enabled'] = True
                xml_dict['shallow_substrate_prior_target_name'] = str(
                    shallow_substrate_prior_selection.get('target_name', '')
                )
                xml_dict['shallow_substrate_prior_use_sd_bounds'] = bool(
                    shallow_substrate_prior_selection.get('use_sd_bounds', False)
                )
                xml_dict['shallow_substrate_prior_selected_pixel_count'] = (
                    int(np.count_nonzero(shallow_prior_mask))
                    if shallow_prior_mask is not None
                    else 0
                )
                xml_dict['shallow_substrate_prior_success_pixel_count'] = int(len(shallow_prior_successful_estimates))
                xml_dict['shallow_substrate_prior_accepted_pixel_count'] = int(len(shallow_prior_accepted_estimates))
                xml_dict['shallow_substrate_prior_csv_path'] = shallow_prior_csv_path or ''
                xml_dict['shallow_substrate_prior_min_exp_bottom'] = _SHALLOW_SUBSTRATE_PRIOR_MIN_EXP_BOTTOM
                xml_dict['shallow_substrate_prior_chl_mean'] = shallow_prior_stats['chl_mean']
                xml_dict['shallow_substrate_prior_chl_sd'] = shallow_prior_stats['chl_sd']
                xml_dict['shallow_substrate_prior_cdom_mean'] = shallow_prior_stats['cdom_mean']
                xml_dict['shallow_substrate_prior_cdom_sd'] = shallow_prior_stats['cdom_sd']
                xml_dict['shallow_substrate_prior_nap_mean'] = shallow_prior_stats['nap_mean']
                xml_dict['shallow_substrate_prior_nap_sd'] = shallow_prior_stats['nap_sd']
                xml_dict['shallow_substrate_prior_applied_pmin'] = shallow_prior_stats['applied_pmin']
                xml_dict['shallow_substrate_prior_applied_pmax'] = shallow_prior_stats['applied_pmax']

            if not args.path and deep_water_prior_stats is not None:
                xml_dict['deep_water_enabled'] = True
                xml_dict['deep_water_use_sd_bounds'] = bool(deep_water_selection.get('use_sd_bounds', False))
                xml_dict['deep_water_subsample_pixels'] = bool(deep_water_selection.get('subsample_pixels', True))
                xml_dict['deep_water_selected_pixel_count'] = (
                    int(np.count_nonzero(deep_water_mask))
                    if deep_water_mask is not None
                    else 0
                )
                xml_dict['deep_water_success_pixel_count'] = int(len(deep_water_successful_estimates))
                xml_dict['deep_water_iop_raster_path'] = deep_water_iop_raster_path or ''
                xml_dict['deep_water_chl_mean'] = deep_water_prior_stats['chl_mean']
                xml_dict['deep_water_chl_sd'] = deep_water_prior_stats['chl_sd']
                xml_dict['deep_water_cdom_mean'] = deep_water_prior_stats['cdom_mean']
                xml_dict['deep_water_cdom_sd'] = deep_water_prior_stats['cdom_sd']
                xml_dict['deep_water_nap_mean'] = deep_water_prior_stats['nap_mean']
                xml_dict['deep_water_nap_sd'] = deep_water_prior_stats['nap_sd']
                xml_dict['deep_water_applied_pmin'] = deep_water_prior_stats['applied_pmin']
                xml_dict['deep_water_applied_pmax'] = deep_water_prior_stats['applied_pmax']

            if crop_selection:
                rrs, lat_array, lon_array, crop_window = _apply_crop_selection(
                    rrs,
                    lat_array,
                    lon_array,
                    crop_selection,
                    file_im,
                    source_grid_metadata,
                )
                height = int(rrs.shape[1])
                width = int(rrs.shape[2])
                lat_grid = lat_array
                lon_grid = lon_array
                source_grid_metadata = _subset_grid_metadata(
                    source_grid_metadata,
                    crop_window.get('row_start', 0),
                    crop_window.get('row_end'),
                    crop_window.get('col_start', 0),
                    crop_window.get('col_end'),
                )
                message_parts = []
                bbox = crop_selection.get('bbox')
                if bbox:
                    message_parts.append(
                        f"lon {bbox['min_lon']:.5f}:{bbox['max_lon']:.5f}, "
                        f"lat {bbox['min_lat']:.5f}:{bbox['max_lat']:.5f}"
                    )
                mask_path = str(crop_selection.get('mask_path') or '').strip()
                if mask_path:
                    mask_buffer_m = crop_selection.get('mask_buffer_m')
                    if mask_buffer_m not in (None, ''):
                        message_parts.append(f"mask {os.path.basename(mask_path)} (buffer {float(mask_buffer_m):g} m)")
                    else:
                        message_parts.append(f"mask {os.path.basename(mask_path)}")
                print(f"[INFO]: Applying spatial selection ({'; '.join(message_parts)}).")

            legacy_lat = lat_grid.copy() if rotated_input_mode and lat_grid is not None else None
            legacy_lon = lon_grid.copy() if rotated_input_mode and lon_grid is not None else None

            geo_metadata = {
                'lat_grid': lat_grid,
                'lon_grid': lon_grid,
                'lat_name': name_lat or 'lat',
                'lon_name': name_lon or 'lon',
                'shape_geo': shape_geo if shape_geo is not None else 2,
                'legacy_mode': rotated_input_mode,
                'legacy_lat': legacy_lat,
                'legacy_lon': legacy_lon,
                'grid_metadata': source_grid_metadata,
            }

            image_info['lat_grid'] = geo_metadata.get('lat_grid')
            image_info['lon_grid'] = geo_metadata.get('lon_grid')
            image_info['lat_name'] = geo_metadata.get('lat_name', 'lat')
            image_info['lon_name'] = geo_metadata.get('lon_name', 'lon')
            image_info['shape_geo'] = geo_metadata.get('shape_geo', 2)
            image_info['legacy_output_mode'] = geo_metadata.get('legacy_mode', False)
            image_info['legacy_lat'] = geo_metadata.get('legacy_lat')
            image_info['legacy_lon'] = geo_metadata.get('legacy_lon')
            image_info['grid_metadata'] = geo_metadata.get('grid_metadata')

            print('rrs shape: ', rrs.shape)

            # Load and resample bathymetry for the final study area only.
            bathy_arr = None
            bathy_exposed_mask = None
            bathy_tol = 0.0
            if bathy_path:
                try:
                    from rasterio.warp import reproject, Resampling
                    import rasterio

                    transform, crs = _derive_transform_crs(
                        width,
                        height,
                        lat_array,
                        lon_array,
                        shape_geo if shape_geo is not None else 2,
                        geo_metadata.get('grid_metadata'))

                    with rasterio.open(bathy_path) as src:
                        dest = np.empty((height, width), dtype='float32')
                        src_crs, crs_note = _normalize_bathy_source_crs(src.crs)
                        if src_crs is None:
                            raise RuntimeError(
                                "Bathymetry raster has no CRS. Please provide a georeferenced GeoTIFF."
                            )
                        if crs_note is not None:
                            print(f"[INFO]: Interpreting bathymetry CRS '{src.crs}' as {crs_note}.")
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=dest,
                            src_transform=src.transform,
                            src_crs=src_crs,
                            dst_transform=transform,
                            dst_crs=crs,
                            resampling=Resampling.bilinear,
                            num_threads=2,
                        )
                        nodata = src.nodata if src.nodata is not None else np.nan
                        dest = np.where(np.isfinite(nodata) & (dest == nodata), np.nan, dest)

                    normalized_reference = bathy_reference.replace('-', '_').replace(' ', '_')
                    if normalized_reference in ('hydrographic_zero', 'zh'):
                        dest, bathy_exposed_mask = _convert_hydrographic_zero_bathy_to_depth(
                            dest,
                            water_level_correction=bathy_correction_m)
                        exposed_count = int(np.count_nonzero(bathy_exposed_mask))
                        if exposed_count > 0:
                            print(
                                f"[INFO]: Masked {exposed_count} exposed bathymetry pixel(s) "
                                f"after ZH->depth conversion.")
                    elif bathy_correction_m != 0.0:
                        dest = dest + bathy_correction_m

                    bathy_tol = bathy_tolerance_m
                    bathy_arr = dest
                except Exception as e:
                    print(f"[WARN]: Failed to load/resample bathy '{bathy_path}': {e}")

            nedr_info = {
                'mode': nedr_mode,
                'applied': False,
                'selection_note': 'fixed XML NEDR',
            }
            if nedr_mode == 'scene':
                print("[INFO]: Scene-adaptive NEDR is enabled. Use this only when the scene contains homogeneous optically deep water.")
                estimated_nedr, nedr_info = _estimate_scene_nedr(
                    rrs,
                    image_info['nedr'],
                    bathy_arr=bathy_arr,
                    bathy_exposed_mask=bathy_exposed_mask)
                image_info['nedr'] = estimated_nedr
                if isinstance(estimated_nedr, tuple) and len(estimated_nedr) == 2:
                    objective._nedr = np.asarray(estimated_nedr[1], dtype='float32')
                else:
                    objective._nedr = np.asarray(estimated_nedr, dtype='float32')

                nedr_summary = ', '.join(f"{value:.6f}" for value in objective._nedr.tolist())
                if nedr_info.get('applied'):
                    print(
                        f"[INFO]: Scene-adaptive NEDR updated using "
                        f"{nedr_info.get('candidate_pixel_count', 0)} candidate pixel(s) "
                        f"from {nedr_info.get('selection_note', 'scene screening')}: {nedr_summary}"
                    )
                else:
                    print(
                        f"[INFO]: Scene-adaptive NEDR retained XML defaults "
                        f"({nedr_info.get('selection_note', nedr_info.get('reason', 'scene screening'))}): "
                        f"{nedr_summary}"
                    )
            else:
                print("[INFO]: Using fixed XML NEDR values.")

            if not args.path:
                xml_dict['nedr_mode'] = nedr_mode
                if 'nedr_wavelengths' in nedr_info:
                    xml_dict['nedr_wavelengths'] = nedr_info['nedr_wavelengths']
                if 'nedr_values' in nedr_info:
                    xml_dict['nedr_values'] = nedr_info['nedr_values']
                xml_dict['nedr_selection_note'] = nedr_info.get('selection_note', 'fixed XML NEDR')
                xml_dict['nedr_candidate_pixel_count'] = nedr_info.get('candidate_pixel_count', 0)

            chunk_manifest = None
            chunk_dir = None
            chunk_export_paths = []
            run_allow_split = allow_split and not rotated_input_mode
            if allow_split and not run_allow_split:
                print("[INFO]: Disabling chunked processing for rotated input to preserve legacy output behavior.")
            if run_allow_split:
                base_dir = os.path.dirname(ofile) if ofile else None
                if base_dir and not os.path.isdir(base_dir):
                    os.makedirs(base_dir, exist_ok=True)
                chunk_dir = tempfile.mkdtemp(prefix=f"swampy_chunks_{input_base}_", dir=base_dir)
                try:
                    chunk_manifest, _ = _run_chunked_model(
                        algo,
                        rrs,
                        width,
                        height,
                        image_info,
                        siop,
                        fixed_parameters,
                        shallow_flag,
                        error_name,
                        opt_met,
                        relaxed,
                        args.free_cpu,
                        bathy_arr,
                        bathy_exposed_mask,
                        bathy_tol,
                        objective,
                        optimize_initial_guesses=optimize_initial_guesses,
                        use_five_initial_guesses=use_five_initial_guesses,
                        initial_guess_debug=initial_guess_debug,
                        chunk_rows_override=split_chunk_rows,
                        chunk_dir=chunk_dir)
                except Exception:
                    if chunk_dir and os.path.isdir(chunk_dir):
                        shutil.rmtree(chunk_dir, ignore_errors=True)
                    raise
                model_outputs = None
            else:
                model_outputs = algo.main_sambuca_func_simpl(
                    rrs,
                    objective,
                    width,
                    height,
                    image_info['sensor_filter'],
                    image_info['nedr'],
                    siop,
                    fixed_parameters,
                    shallow_flag,
                    error_name,
                    opt_met,
                    relaxed,
                    args.free_cpu,
                    bathy=bathy_arr,
                    bathy_tolerance=bathy_tol,
                    bathy_exposed_mask=bathy_exposed_mask,
                    optimize_initial_guesses=optimize_initial_guesses,
                    use_five_initial_guesses=use_five_initial_guesses,
                    initial_guess_debug=initial_guess_debug)

            if model_outputs is not None:
                (closed_rrs, chl, cdom, nap, depth, nit, kd,
                 sdi, sub1_frac, sub2_frac, sub3_frac, error_f,
                 total_abun, sub1_norm, sub2_norm, sub3_norm, r_sub, initial_guess_stack) = model_outputs
            else:
                initial_guess_stack = None
            anomaly_search_result = None
            anomaly_debug_layers = []
            anomaly_debug_multiband_exports = []
            anomaly_interpolated_maps = {}
            anomaly_corrected_pixel_count = 0
            if anomaly_search_settings.get('enabled'):
                depth_for_anomaly = depth if model_outputs is not None else _assemble_chunk_array(chunk_manifest, 'depth', height, width)
                protected_mask = None
                stable_deep_mask = _build_anomaly_search_deep_protection_mask(
                    depth_for_anomaly,
                    depth_min=float(pmin[3]),
                )
                if np.any(stable_deep_mask):
                    protected_mask = np.asarray(stable_deep_mask, dtype=bool)
                    print(
                        "[INFO]: Slope/plateau anomaly search excluded "
                        f"{int(np.count_nonzero(stable_deep_mask))} stable deep pixel(s) "
                        "from suspicious-pixel correction."
                    )
                lat_for_anomaly = lat_array if lat_array is not None else image_info.get('lat_grid')
                lon_for_anomaly = lon_array if lon_array is not None else image_info.get('lon_grid')
                anomaly_grid_metadata = image_info.get('grid_metadata')
                if deep_water_selection and lat_for_anomaly is not None and lon_for_anomaly is not None:
                    try:
                        polygon_protected_mask = _rasterize_epsg4326_geometries(
                            deep_water_selection.get('polygons') or [],
                            lat_for_anomaly,
                            lon_for_anomaly,
                            anomaly_grid_metadata,
                        )
                        if polygon_protected_mask is not None:
                            polygon_protected_mask = np.asarray(polygon_protected_mask, dtype=bool)
                            protected_mask = (
                                polygon_protected_mask
                                if protected_mask is None
                                else (protected_mask | polygon_protected_mask)
                            )
                    except Exception as protection_exc:
                        print(f"[WARN]: Failed to build deep-water protection mask for anomaly search: {protection_exc}")
                elif deep_water_selection:
                    print("[WARN]: Deep-water polygons were provided, but anomaly-search protection could not be applied because scene coordinates are unavailable.")

                anomaly_search_result = _detect_local_moran_anomaly_pixels(
                    depth_for_anomaly,
                    depth_min=float(pmin[3]),
                    exposed_mask=bathy_exposed_mask,
                    protected_mask=protected_mask,
                    lat_data=lat_for_anomaly,
                    lon_data=lon_for_anomaly,
                    shape_geo=shape_geo if shape_geo is not None else image_info.get('shape_geo', 2),
                    grid_metadata=anomaly_grid_metadata,
                    slope_threshold_percent=float(
                        anomaly_search_settings.get(
                            'seed_slope_threshold_percent',
                            _ANOMALY_SLOPE_THRESHOLD_PERCENT,
                        )
                    ),
                )
                anomaly_search_result['true_deep_mask'] = np.asarray(stable_deep_mask, dtype=bool)
                suspicious_pixel_count = int(anomaly_search_result.get('suspicious_pixel_count', 0))
                suspicious_component_count = int(anomaly_search_result.get('component_count', 0))
                if suspicious_pixel_count > 0:
                    print(
                        "[INFO]: Slope/plateau anomaly search flagged "
                        f"{suspicious_pixel_count} suspicious pixel(s) across "
                        f"{suspicious_component_count} patch(es)."
                    )
                else:
                    print("[INFO]: Slope/plateau anomaly search found no suspicious false-deep patches.")
                suspicious_mask = np.asarray(anomaly_search_result.get('suspicious_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                valid_mask = np.asarray(anomaly_search_result.get('valid_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                exclusion_mask = np.asarray(anomaly_search_result.get('protected_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                anomaly_search_result['confident_mask'] = np.asarray(valid_mask & ~exclusion_mask & ~suspicious_mask, dtype=bool)
                if np.any(suspicious_mask):
                    correction_recorder = _build_result_recorder_from_outputs(
                        height,
                        width,
                        image_info['sensor_filter'],
                        image_info['nedr'],
                        fixed_parameters,
                        outputs=model_outputs,
                        chunk_manifest=chunk_manifest,
                        initial_guess_stack=initial_guess_stack,
                    )
                    _suspicious_buffered = ndimage.binary_dilation(suspicious_mask, structure=np.ones((3, 3), dtype=bool))
                    anchor_mask = valid_mask & ~_suspicious_buffered & ~exclusion_mask
                    anomaly_search_result['confident_mask'] = np.asarray(anchor_mask, dtype=bool)
                    anomaly_interpolated_maps = _interpolate_suspicious_parameter_maps(
                        {
                            'depth': correction_recorder.depth,
                            'chl': correction_recorder.chl,
                            'cdom': correction_recorder.cdom,
                            'nap': correction_recorder.nap,
                        },
                        anchor_mask,
                        suspicious_mask,
                    )
                    rerun_items = _build_substrate_only_rerun_items(
                        correction_recorder,
                        suspicious_mask,
                        anomaly_interpolated_maps,
                        siop['p_bounds'],
                    )
                    if rerun_items:
                        rerun_executor = None
                        try:
                            rerun_executor = output_calculation.create_rerun_worker_pool(
                                objective,
                                siop,
                                opt_met,
                                relaxed,
                                free_cpu=args.free_cpu,
                                bathy_tolerance=0.0,
                                optimize_initial_guesses=False,
                                use_five_initial_guesses=False,
                                apply_shallow_adjustment=False,
                                allow_target_sum_over_one=False,
                                max_tasks=len(rerun_items),
                            )
                            output_calculation.rerun_selected_pixels(
                                rrs,
                                objective,
                                siop,
                                correction_recorder,
                                rerun_items,
                                opt_met,
                                relaxed,
                                free_cpu=args.free_cpu,
                                bathy_tolerance=0.0,
                                optimize_initial_guesses=False,
                                use_five_initial_guesses=False,
                                apply_shallow_adjustment=False,
                                allow_target_sum_over_one=False,
                                normalise_target_fractions=False,
                                executor=rerun_executor,
                            )
                        finally:
                            if rerun_executor is not None:
                                rerun_executor.shutdown()

                        anomaly_corrected_pixel_count = int(len(rerun_items))
                        print(
                            "[INFO]: Substrate-only re-optimisation attempted "
                            f"{anomaly_corrected_pixel_count} suspicious pixel(s) "
                            "using interpolated depth, CHL, CDOM, and NAP."
                        )
                        correction_image_info = dict(image_info)
                        correction_image_info.setdefault('observed_rrs_height', height)
                        correction_image_info.setdefault('observed_rrs_width', width)
                        suite_outputs = define_outputs.output_suite(correction_recorder, correction_image_info)
                        (closed_rrs, chl, cdom, nap, depth, nit, kd, sdi, sub1_frac, sub2_frac, sub3_frac,
                         error_f, total_abun, sub1_norm, sub2_norm, sub3_norm, _rgbimg_unused, r_sub) = suite_outputs
                        initial_guess_stack = correction_recorder.initial_guess_stack
                        model_outputs = (
                            closed_rrs, chl, cdom, nap, depth, nit, kd, sdi,
                            sub1_frac, sub2_frac, sub3_frac, error_f, total_abun,
                            sub1_norm, sub2_norm, sub3_norm, r_sub, initial_guess_stack
                        )
                        chunk_manifest = None

            substrate_labels = _extract_substrate_labels(siop)
            substrate_var_names = _make_safe_var_names(substrate_labels)
            initial_guess_band_names = _build_initial_guess_band_names(substrate_labels)
            substrate_defs = list(zip(substrate_var_names, substrate_labels))
            base_metric_defs = [
                ('chl', 'chl'),
                ('cdom', 'cdom'),
                ('nap', 'nap'),
                ('depth', 'depth'),
                ('kd', 'kd'),
                ('sdi', 'sdi'),
            ]
            tail_metric_defs = [
                ('sum_of_substrats', 'sum_of substrats'),
                ('error_f', 'error_f'),
                ('r_sub', 'r_sub'),
            ]
            primary_var_defs = base_metric_defs + substrate_defs + tail_metric_defs

            primary_outputs = None
            if model_outputs is not None:
                chunk_like = {
                    'sub1_frac': sub1_frac,
                    'sub2_frac': sub2_frac,
                    'sub3_frac': sub3_frac,
                    'total_abun': total_abun,
                }
                substrate_norm_map = _compute_chunk_substrate_norms(
                    chunk_like,
                    relaxed,
                    substrate_var_names,
                    standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
                )
                metric_arrays = {
                    'chl': chl,
                    'cdom': cdom,
                    'nap': nap,
                    'depth': depth,
                    'kd': kd,
                    'sdi': sdi,
                    'sum_of_substrats': substrate_norm_map.get('sum_of_substrats'),
                    'error_f': error_f,
                    'r_sub': r_sub,
                }
                primary_outputs = []
                for var_name, display_name in base_metric_defs:
                    primary_outputs.append((var_name, metric_arrays[var_name], display_name))
                for var_name, display_name in substrate_defs:
                    primary_outputs.append((var_name, substrate_norm_map[var_name], display_name))
                for var_name, display_name in tail_metric_defs:
                    primary_outputs.append((var_name, metric_arrays[var_name], display_name))

            # Write per-run XML log next to outputs (GUI mode only)
            if not args.path:
                try:
                    xml_dict['standardize_relaxed_substrate_outputs'] = bool(
                        standardize_relaxed_substrate_outputs
                    )
                    xml_dict['anomaly_search_enabled'] = anomaly_search_settings.get('enabled', False)
                    xml_dict['anomaly_search_export_local_moran_raster'] = anomaly_search_settings.get('export_local_moran_raster', False)
                    xml_dict['anomaly_search_export_suspicious_binary_raster'] = anomaly_search_settings.get('export_suspicious_binary_raster', False)
                    xml_dict['anomaly_search_export_interpolated_rasters'] = anomaly_search_settings.get('export_interpolated_rasters', False)
                    xml_dict['anomaly_search_seed_slope_threshold_percent'] = float(
                        anomaly_search_settings.get(
                            'seed_slope_threshold_percent',
                            DEFAULT_ANOMALY_SEARCH_SETTINGS['seed_slope_threshold_percent'],
                        )
                    )
                    if anomaly_search_result is not None:
                        xml_dict['anomaly_search_method'] = 'slope_plateau_depth_only'
                        xml_dict['anomaly_search_suspicious_pixel_count'] = int(anomaly_search_result.get('suspicious_pixel_count', 0))
                        xml_dict['anomaly_search_component_count'] = int(anomaly_search_result.get('component_count', 0))
                        xml_dict['anomaly_search_rerun_pixel_count'] = int(anomaly_corrected_pixel_count)
                    log_dir = os.path.dirname(ofile)
                    log_name = f'log_{input_base}{run_version_suffix}.xml' if input_base else f'log_output{run_version_suffix}.xml'
                    log_path = os.path.join(log_dir, log_name) if log_dir else log_name
                    xml_content = dicttoxml.dicttoxml(xml_dict, attr_type=False)
                    with open(log_path, 'wb') as log_f:
                        log_f.write(xml_content)
                except Exception as e:
                    print(f"[WARN]: Failed to write XML log '{log_path}': {e}")

            # NetCDF output
            legacy_mode = image_info.get('legacy_output_mode', False)
            legacy_lat = image_info.get('legacy_lat')
            legacy_lon = image_info.get('legacy_lon')
            lat_data_default = lat_array if lat_array is not None else image_info.get('lat_grid')
            lon_data_default = lon_array if lon_array is not None else image_info.get('lon_grid')
            lat_data = legacy_lat if legacy_mode and legacy_lat is not None else lat_data_default
            lon_data = legacy_lon if legacy_mode and legacy_lon is not None else lon_data_default
            lat_name = name_lat or image_info.get('lat_name', 'lat')
            lon_name = name_lon or image_info.get('lon_name', 'lon')
            w_name = name_w or 'wavelength'
            shape_geo_val = shape_geo if shape_geo is not None else image_info.get('shape_geo', 2)
            grid_metadata = image_info.get('grid_metadata')
            if anomaly_search_result is not None:
                valid_mask = np.asarray(anomaly_search_result.get('valid_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                slope_layer = anomaly_search_result.get('slope_percent')
                seed_mask = np.asarray(anomaly_search_result.get('seed_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                true_deep_mask = np.asarray(anomaly_search_result.get('true_deep_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                confident_mask = np.asarray(anomaly_search_result.get('confident_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                if anomaly_search_settings.get('export_local_moran_raster') and slope_layer is not None:
                    anomaly_debug_layers.append(
                        ('_anomaly_search_slope_percent.tif', 'slope_percent', slope_layer)
                    )
                if anomaly_search_settings.get('export_local_moran_raster'):
                    anomaly_debug_layers.append(
                        ('_truedeepmask.tif', 'true_deep_mask', ma.masked_array(true_deep_mask.astype('float32'), mask=~valid_mask))
                    )
                    anomaly_debug_layers.append(
                        ('_anomaly_search_seed_mask.tif', 'seed_mask', ma.masked_array(seed_mask.astype('float32'), mask=~valid_mask))
                    )
                if anomaly_search_settings.get('export_suspicious_binary_raster'):
                    suspicious_mask = np.asarray(anomaly_search_result.get('suspicious_mask', np.zeros((height, width), dtype=bool)), dtype=bool)
                    suspicious_layer = ma.masked_array(
                        suspicious_mask.astype('float32'),
                        mask=~valid_mask,
                    )
                    anomaly_debug_layers.append(
                        ('_anomaly_search_suspicious_mask.tif', 'suspicious_mask', suspicious_layer)
                    )
                    confident_layer = ma.masked_array(
                        confident_mask.astype('float32'),
                        mask=~valid_mask,
                    )
                    anomaly_debug_layers.append(
                        ('_anomaly_search_confident_mask.tif', 'confident_mask', confident_layer)
                    )
                if anomaly_search_settings.get('export_interpolated_rasters') and anomaly_interpolated_maps:
                    interpolated_bands = []
                    for key in ('depth', 'chl', 'cdom', 'nap'):
                        layer = anomaly_interpolated_maps.get(key)
                        if layer is not None:
                            interpolated_bands.append((f'interpolated_{key}', layer))
                    if interpolated_bands:
                        anomaly_debug_multiband_exports.append(
                            ('_anomaly_search_interpolated_values.tif', interpolated_bands)
                        )

            if chunk_manifest and not legacy_mode:
                    _write_chunk_outputs(
                        chunk_manifest,
                        ofile,
                        output_format,
                    dim_list,
                    width,
                    nbands,
                    lat_data,
                    lon_data,
                    wls,
                    lat_name,
                    lon_name,
                        w_name,
                        shape_geo_val,
                        primary_var_defs,
                        substrate_var_names,
                        relaxed,
                        standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
                        cleanup_paths=chunk_export_paths,
                        grid_metadata=grid_metadata)

            if legacy_mode:
                _export_outputs_legacy(
                    ofile,
                    output_format,
                    dim_list,
                    height,
                    width,
                    nbands,
                    lat_data,
                    lon_data,
                    wls,
                    lat_name,
                    lon_name,
                    w_name,
                    shape_geo_val,
                    primary_outputs,
                    grid_metadata=grid_metadata)
            else:
                _export_outputs_modern(
                    ofile,
                    output_format,
                    chunk_manifest,
                    dim_list,
                    height,
                    width,
                    nbands,
                    lat_data,
                    lon_data,
                    wls,
                    lat_name,
                    lon_name,
                    w_name,
                    shape_geo_val,
                    primary_outputs,
                    primary_var_defs,
                    substrate_var_names,
                    relaxed,
                    standardize_relaxed_substrate_outputs=standardize_relaxed_substrate_outputs,
                    grid_metadata=grid_metadata)

            if anomaly_debug_layers:
                try:
                    _write_anomaly_search_debug_geotiffs(
                        ofile,
                        width,
                        height,
                        lat_data,
                        lon_data,
                        shape_geo_val,
                        anomaly_debug_layers,
                        grid_metadata=grid_metadata,
                    )
                except Exception as e:
                    print(f"[WARN]: Failed to write anomaly-search debug GeoTIFFs '{ofile}': {e}")
            if anomaly_debug_multiband_exports:
                try:
                    transform, crs = _derive_transform_crs(
                        width,
                        height,
                        lat_data,
                        lon_data,
                        shape_geo_val,
                        grid_metadata,
                    )
                    base, _ = os.path.splitext(ofile)
                    for suffix, bands in anomaly_debug_multiband_exports:
                        _write_geotiff(
                            base + suffix,
                            bands,
                            transform,
                            crs,
                            height,
                            width,
                            nodata=OUTPUT_FILL_VALUE,
                        )
                except Exception as e:
                    print(f"[WARN]: Failed to write anomaly-search interpolated GeoTIFFs '{ofile}': {e}")

            if initial_guess_debug:
                if chunk_manifest:
                    try:
                        _write_initial_guess_geotiff_from_chunks(
                            chunk_manifest,
                            ofile,
                            width,
                            height,
                            lat_data,
                            lon_data,
                            shape_geo_val,
                            initial_guess_band_names,
                            grid_metadata=grid_metadata)
                    except Exception as e:
                        print(f"[ERROR]: Failed to write initial guess debug GeoTIFF '{ofile}': {e}")
                else:
                    try:
                        _write_initial_guess_geotiff(
                            ofile,
                            initial_guess_stack,
                            width,
                            height,
                            lat_data,
                            lon_data,
                            shape_geo_val,
                            initial_guess_band_names,
                            grid_metadata=grid_metadata)
                    except Exception as e:
                        print(f"[ERROR]: Failed to write initial guess debug GeoTIFF '{ofile}': {e}")

            if output_modeled_reflectance:
                try:
                    if chunk_manifest:
                        _write_modeled_reflectance_outputs_from_chunks(
                            chunk_manifest,
                            ofile,
                            dim_list,
                            height,
                            width,
                            nbands,
                            lat_data,
                            lon_data,
                            wls,
                            lat_name,
                            lon_name,
                            w_name,
                            shape_geo_val,
                            above_rrs_flag,
                            grid_metadata=grid_metadata)
                    else:
                        _write_modeled_reflectance_outputs(
                            ofile,
                            closed_rrs,
                            dim_list,
                            height,
                            width,
                            nbands,
                            lat_data,
                            lon_data,
                            wls,
                            lat_name,
                            lon_name,
                            w_name,
                            shape_geo_val,
                            above_rrs_flag,
                            grid_metadata=grid_metadata)
                except Exception as e:
                    print(f"[ERROR]: Failed to write modeled reflectance outputs '{ofile}': {e}")

            """the post-processing use the run input and output files to calculate spectra of rrs, deep_rrs,
            substrate_reflectance, absorption, and backscattering and store them in different files"""
            if pp == True:
                # intialize output array
                file_result = {'rrs': ma.zeros((chl.shape[0], chl.shape[1], nbands)),
                               'rrs_deep': ma.zeros((chl.shape[0], chl.shape[1], nbands)),
                               'kd': ma.zeros((chl.shape[0], chl.shape[1], nbands)),
                               'r_sub': ma.zeros((chl.shape[0], chl.shape[1], nbands)),
                               'a': ma.zeros((chl.shape[0], chl.shape[1], nbands)),
                               'bb': ma.zeros((chl.shape[0], chl.shape[1], nbands))}
                skip_mask = ma.getmask(chl)

                # call the forward model
                for i in range(chl.shape[0]):
                    for j in range(chl.shape[1]):
                        result = sbc.forward_model(
                            chl=chl[i, j], cdom=cdom[i, j], nap=nap[i, j], depth=depth[i, j],
                            sub1_frac=sub1_frac[i, j], sub2_frac=sub2_frac[i, j], sub3_frac=sub3_frac[i, j],
                            substrate1=fixed_parameters.substrates[0], substrate2=fixed_parameters.substrates[1], substrate3=fixed_parameters.substrates[2],
                            wavelengths=fixed_parameters.wavelengths, a_water=fixed_parameters.a_water, a_ph_star=fixed_parameters.a_ph_star,
                            num_bands=fixed_parameters.num_bands, a_cdom_slope=fixed_parameters.a_cdom_slope, a_nap_slope=fixed_parameters.a_nap_slope,
                            bb_ph_slope=fixed_parameters.bb_ph_slope, bb_nap_slope=fixed_parameters.bb_nap_slope,
                            lambda0cdom=fixed_parameters.lambda0cdom, lambda0nap=fixed_parameters.lambda0nap,
                            lambda0x=fixed_parameters.lambda0x, x_ph_lambda0x=fixed_parameters.x_ph_lambda0x, x_nap_lambda0x=fixed_parameters.x_nap_lambda0x,
                            a_cdom_lambda0cdom=fixed_parameters.a_cdom_lambda0cdom, a_nap_lambda0nap=fixed_parameters.a_nap_lambda0nap,
                            bb_lambda_ref=fixed_parameters.bb_lambda_ref, water_refractive_index=fixed_parameters.water_refractive_index,
                            theta_air=fixed_parameters.theta_air, off_nadir=fixed_parameters.off_nadir, q_factor=fixed_parameters.q_factor)
                        file_name_dict = {'rrs': result.rrs, 'rrs_deep': result.rrsdp, 'kd': result.kd,
                                          'r_sub': result.r_substratum, 'a': result.a, 'bb': result.bb}
                    # store the results in a dictionary
                    for key in file_name_dict:
                        file_result[key][i, j, :] = sbc.apply_sensor_filter(file_name_dict[key], objective._sensor_filter)

                # loop over the different product, to create different files for each product
                file_name_dict_count = 0
                for key in file_name_dict:
                    file_name_dict_count += 1
                    var = ma.array(file_result[key])
                    if key in ['rrs', 'rrsd']:
                        if above_rrs_flag == True:
                            var = var / (2. - 3 * var)
                    if output_format in ("netcdf", "both"):
                        nc_o = Dataset(ofile[:-3] + '_' + key + '.nc', 'w')
                        nc_o.createDimension(dim_list[0], height)
                        nc_o.createDimension(dim_list[1], width)
                        nc_o.createDimension(dim_list[2], nbands)
                        if lat_data is not None:
                            if shape_geo_val == 2:
                                dim_geo = (dim_list[0], dim_list[1],)
                            else:
                                dim_geo = dim_list[0]
                            var_nc = nc_o.createVariable(lat_name, 'f4', dim_geo)
                            var_nc[:] = lat_data
                        if lon_data is not None:
                            if shape_geo_val == 2:
                                dim_geo = (dim_list[0], dim_list[1],)
                            else:
                                dim_geo = dim_list[1]
                            var_nc = nc_o.createVariable(lon_name, 'f4', dim_geo)
                            var_nc[:] = lon_data
                        var_nc = nc_o.createVariable(w_name, 'f4', (dim_list[2],))
                        var_nc[:] = wls
                        var_sw = nc_o.createVariable(key, 'f4', dim_list)
                        var_sw[:] = ma.array(var)
                        nc_o.close()
                    if output_format in ("geotiff", "both"):
                        # Write a per-product multiband GeoTIFF (spectral bands as raster bands)
                        tif_p = ofile[:-3] + '_' + key + '.tif'
                        if lat_data is not None and lon_data is not None:
                            try:
                                transform, crs = _derive_transform_crs(
                                    width,
                                    height,
                                    lat_data,
                                    lon_data,
                                    shape_geo_val,
                                    grid_metadata)
                                arr = ma.array(var)
                                if arr.ndim != 3:
                                    arr = arr.reshape((height, width, -1))
                                bands = []
                                for b in range(arr.shape[2]):
                                    bands.append((f"{key}_{b+1}", arr[:, :, b]))
                                _write_geotiff(tif_p, bands, transform, crs, height, width, nodata=-999.0)
                            except Exception as e:
                                print(f"[ERROR]: Failed to write GeoTIFF '{tif_p}': {e}")
                        else:
                            print(f"[WARN]: Skipping GeoTIFF for '{key}': lat/lon not available to derive georeferencing.")

            if chunk_export_paths:
                for temp_path in chunk_export_paths:
                    try:
                        if temp_path and os.path.exists(temp_path):
                            os.remove(temp_path)
                    except OSError as e:
                        print(f"[WARN]: Failed to remove chunk output '{temp_path}': {e}")

            if chunk_dir and os.path.isdir(chunk_dir):
                shutil.rmtree(chunk_dir, ignore_errors=True)

        end_time = datetime.now()
        print(f"[DEBUG]: ending at {end_time.isoformat()}. Total time: {end_time - start_time}")

    #except Exception as err:
    #    print(f"generic exception in main: {err}")
    #    # print(taceback.format_exec())
