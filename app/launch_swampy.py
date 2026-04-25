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
import csv
import json
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
from scipy.spatial import cKDTree
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
DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS = {
    'enabled': False,
    'anchor_min_sdi': 1.5,
    'anchor_max_depth_m': 8.0,
    'anchor_max_slope_percent': 10.0,
    'anchor_max_error_f': 0.003,
    'anchor_min_depth_margin_m': 0.5,
    'suspect_max_sdi': 1.0,
    'suspect_min_slope_percent': 10.0,
    'suspect_min_depth_jump_m': 2.0,
    'search_radius_px': 12,
    'min_anchor_count': 4,
    'correction_tolerance_m': 1.5,
    'max_patch_size_px': 64,
    'treat_min_depth_as_barrier': True,
    'barrier_depth_margin_m': 0.25,
    'barrier_min_sdi': 3.0,
    'fixed_depth_max_slope_percent': 10.0,
    'seed_min_adjacent_depth_jump_m': 5.0,
    'max_depth_tolerance_m': 3.0,
    'local_param_relative_tolerance': 0.15,
    'local_param_global_fraction_floor': 0.02,
    'local_param_global_fraction_cap': 0.20,
    'max_rerun_attempts': 2,
    'continuity_depth_floor_m': 1.0,
    'continuity_min_depth_improvement_m': 0.25,
    'continuity_max_error_growth_factor': 1.35,
    'continuity_max_error_growth_abs': 0.0005,
    'debug_export': False,
}
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
    if not bbox and not mask_path:
        return None
    return {
        'bbox': bbox,
        'mask_path': mask_path,
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
        'source_image': str(config_root.get('deep_water_source_image', '') or ''),
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
                geometries.append(_transform_with_optional_point_buffer(geometry, src_crs, crs.to_string()))
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


def _apply_deep_water_priors(siop, estimates, use_sd_bounds):
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
    means = [stats['chl_mean'], stats['cdom_mean'], stats['nap_mean']]
    sds = [stats['chl_sd'], stats['cdom_sd'], stats['nap_sd']]

    for index, (mean_value, sd_value) in enumerate(zip(means, sds)):
        mean_value = float(np.clip(mean_value, original_pmin[index], original_pmax[index]))
        if use_sd_bounds and np.isfinite(sd_value) and sd_value > 0.0:
            lower = max(float(original_pmin[index]), mean_value - float(sd_value))
            upper = min(float(original_pmax[index]), mean_value + float(sd_value))
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


def _write_deep_water_pixel_csv(csv_path, pixel_rows):
    if not pixel_rows:
        return
    fieldnames = ['row', 'col', 'lat', 'lon', 'chl', 'cdom', 'nap', 'error_alpha_f', 'success']
    with open(csv_path, 'w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in pixel_rows:
            writer.writerow(row)


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
                       optimize_initial_guesses=False, use_five_initial_guesses=False, initial_guess_debug=False, fully_relaxed=False, chunk_rows_override=None,
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
            initial_guess_debug=initial_guess_debug,
            fully_relaxed=fully_relaxed)

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


def _compute_chunk_substrate_norms(chunk_arrays, relaxed, substrate_var_names, fully_relaxed=False):
    if fully_relaxed:
        n1 = ma.array(chunk_arrays['sub1_frac'], copy=False)
        n2 = ma.array(chunk_arrays['sub2_frac'], copy=False)
        n3 = ma.array(chunk_arrays['sub3_frac'], copy=False)
    else:
        denom = chunk_arrays['sub1_frac'] + chunk_arrays['sub2_frac'] + chunk_arrays['sub3_frac']
        with np.errstate(divide='ignore', invalid='ignore'):
            n1 = ma.divide(chunk_arrays['sub1_frac'], denom)
            n2 = ma.divide(chunk_arrays['sub2_frac'], denom)
            n3 = ma.divide(chunk_arrays['sub3_frac'], denom)
    return {
        substrate_var_names[0]: n1,
        substrate_var_names[1]: n2,
        substrate_var_names[2]: n3,
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


def _compute_depth_slope_ratio(depth_data, lat, lon, shape_geo, dx_m=None, dy_m=None):
    """Compute rise/run slope ratio from a depth raster."""
    depth = ma.array(depth_data)
    depth_values = np.asarray(depth.filled(np.nan), dtype=float)
    depth_mask = ma.getmaskarray(depth)
    if depth_values.ndim != 2 or depth_values.shape[0] < 2 or depth_values.shape[1] < 2:
        raise RuntimeError("Slope calculation requires at least a 2x2 depth raster.")

    if not (np.isfinite(dx_m) and dx_m > 0.0 and np.isfinite(dy_m) and dy_m > 0.0):
        dx_m, dy_m = _derive_pixel_spacing_meters(lat, lon, shape_geo)
    if not np.isfinite(dx_m) or dx_m <= 0.0 or not np.isfinite(dy_m) or dy_m <= 0.0:
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


def _normalise_false_deep_correction_settings(raw_settings):
    settings = dict(DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS)
    if isinstance(raw_settings, dict):
        settings.update(raw_settings)
        legacy_slope_pairs = (
            ('anchor_max_slope_percent', 'anchor_max_slope_deg'),
            ('suspect_min_slope_percent', 'suspect_min_slope_deg'),
            ('fixed_depth_max_slope_percent', 'fixed_depth_max_slope_deg'),
        )
        for percent_key, degree_key in legacy_slope_pairs:
            percent_value = raw_settings.get(percent_key)
            if percent_value not in (None, '', []):
                settings[percent_key] = percent_value
                continue
            degree_value = raw_settings.get(degree_key)
            if degree_value in (None, '', []):
                continue
            try:
                settings[percent_key] = 100.0 * np.tan(np.radians(float(degree_value)))
            except (TypeError, ValueError):
                settings[percent_key] = DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS[percent_key]

    float_keys = (
        'anchor_min_sdi',
        'anchor_max_depth_m',
        'anchor_max_slope_percent',
        'anchor_max_error_f',
        'anchor_min_depth_margin_m',
        'suspect_max_sdi',
        'suspect_min_slope_percent',
        'suspect_min_depth_jump_m',
        'correction_tolerance_m',
        'barrier_depth_margin_m',
        'barrier_min_sdi',
        'fixed_depth_max_slope_percent',
        'seed_min_adjacent_depth_jump_m',
        'max_depth_tolerance_m',
        'local_param_relative_tolerance',
        'local_param_global_fraction_floor',
        'local_param_global_fraction_cap',
        'continuity_depth_floor_m',
        'continuity_min_depth_improvement_m',
        'continuity_max_error_growth_factor',
        'continuity_max_error_growth_abs',
    )
    int_keys = ('search_radius_px', 'min_anchor_count', 'max_patch_size_px', 'max_rerun_attempts')
    bool_keys = ('enabled', 'treat_min_depth_as_barrier', 'debug_export')

    for key in float_keys:
        settings[key] = _coerce_float(settings.get(key), DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS[key])
    for key in int_keys:
        try:
            settings[key] = max(1, int(float(settings.get(key, DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS[key]))))
        except (TypeError, ValueError):
            settings[key] = DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS[key]
    for key in bool_keys:
        settings[key] = _coerce_bool(settings.get(key), DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS[key])

    # Older saved XML runs can carry very conservative false-deep settings from the
    # experimental UI. Keep those files runnable, but enforce saner internal floors
    # now that the feature is effectively automatic.
    settings['search_radius_px'] = max(settings['search_radius_px'], 20)
    settings['max_patch_size_px'] = max(settings['max_patch_size_px'], 256)
    settings['max_rerun_attempts'] = max(settings['max_rerun_attempts'], 3)
    return settings


def _false_deep_float_values(data):
    return np.asarray(ma.array(data, copy=False).filled(np.nan), dtype=float)


def _false_deep_base_masks(depth_data, sdi_data, error_f_data, slope_data,
                           settings, depth_min, exposed_mask=None, required_data=()):
    depth_values = _false_deep_float_values(depth_data)
    sdi_values = _false_deep_float_values(sdi_data)
    error_values = _false_deep_float_values(error_f_data)
    slope_values = _false_deep_float_values(slope_data)

    valid_water = (
        np.isfinite(depth_values)
        & np.isfinite(sdi_values)
        & np.isfinite(error_values)
        & np.isfinite(slope_values)
    )
    for required in required_data or ():
        valid_water &= np.isfinite(_false_deep_float_values(required))

    barrier_mask = np.zeros(depth_values.shape, dtype=bool)
    if exposed_mask is not None:
        barrier_mask |= np.asarray(exposed_mask, dtype=bool)
    if settings['treat_min_depth_as_barrier']:
        barrier_mask |= (
            valid_water
            & (depth_values <= (float(depth_min) + settings['barrier_depth_margin_m']))
            & (sdi_values >= settings['barrier_min_sdi'])
        )

    return depth_values, sdi_values, error_values, slope_values, valid_water & ~barrier_mask, barrier_mask


def _build_false_deep_confident_mask(depth_data, sdi_data, error_f_data, slope_data,
                                     settings, depth_min, exposed_mask=None,
                                     required_data=()):
    depth_values, sdi_values, error_values, slope_values, water_mask, _ = _false_deep_base_masks(
        depth_data,
        sdi_data,
        error_f_data,
        slope_data,
        settings,
        depth_min,
        exposed_mask=exposed_mask,
        required_data=required_data,
    )
    return (
        water_mask
        & (sdi_values >= settings['anchor_min_sdi'])
        & (depth_values <= settings['anchor_max_depth_m'])
        & (depth_values >= (float(depth_min) + settings['anchor_min_depth_margin_m']))
        & (slope_values <= settings['anchor_max_slope_percent'])
        & (error_values <= settings['anchor_max_error_f'])
    )


def _finite_percentile(values, percentile):
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return np.nan
    return float(np.nanpercentile(values, percentile))


def _derive_scene_adaptive_false_deep_settings(depth_data, sdi_data, error_f_data,
                                               slope_data, settings, depth_min,
                                               exposed_mask=None, required_data=()):
    """Relax false-deep anchor thresholds from first-pass scene statistics."""
    effective = dict(settings)
    depth_values, sdi_values, error_values, slope_values, water_mask, _ = _false_deep_base_masks(
        depth_data,
        sdi_data,
        error_f_data,
        slope_data,
        effective,
        depth_min,
        exposed_mask=exposed_mask,
        required_data=required_data,
    )
    if not np.any(water_mask):
        return effective

    shallow_low_slope_mask = (
        water_mask
        & (depth_values <= effective['anchor_max_depth_m'])
        & (depth_values >= (float(depth_min) + effective['anchor_min_depth_margin_m']))
        & (slope_values <= effective['anchor_max_slope_percent'])
    )
    if int(np.count_nonzero(shallow_low_slope_mask)) < int(effective['min_anchor_count']):
        shallow_low_slope_mask = water_mask

    configured_sdi = float(effective['anchor_min_sdi'])
    scene_sdi = _finite_percentile(sdi_values[shallow_low_slope_mask], 60.0)
    if np.isfinite(scene_sdi):
        sdi_floor = max(0.25, min(configured_sdi, float(effective['suspect_max_sdi']) * 0.75))
        effective['anchor_min_sdi'] = min(configured_sdi, max(sdi_floor, scene_sdi))

    configured_error = float(effective['anchor_max_error_f'])
    scene_error = _finite_percentile(error_values[shallow_low_slope_mask], 70.0)
    if np.isfinite(scene_error):
        error_cap = max(configured_error, min(0.02, max(configured_error * 4.0, configured_error + 0.001)))
        effective['anchor_max_error_f'] = min(max(configured_error, scene_error), error_cap)

    base_radius = int(effective['search_radius_px'])
    anchor_mask = _build_false_deep_confident_mask(
        depth_data,
        sdi_data,
        error_f_data,
        slope_data,
        effective,
        depth_min,
        exposed_mask=exposed_mask,
        required_data=required_data,
    )
    water_count = int(np.count_nonzero(water_mask))
    anchor_count = int(np.count_nonzero(anchor_mask))
    anchor_density = (anchor_count / water_count) if water_count else 0.0
    radius_multiplier = 1.0
    if anchor_count < int(effective['min_anchor_count']):
        radius_multiplier = 3.0
    elif anchor_density < 0.01:
        radius_multiplier = 3.0
    elif anchor_density < 0.03:
        radius_multiplier = 2.0
    elif anchor_density < 0.06:
        radius_multiplier = 1.5

    scene_diag = float(np.hypot(*water_mask.shape))
    scene_radius_cap = max(base_radius, min(96, int(np.ceil(0.10 * scene_diag))))
    effective['search_radius_px'] = min(scene_radius_cap, max(base_radius, int(np.ceil(base_radius * radius_multiplier))))
    return effective


def _weighted_median(values, weights):
    values = np.asarray(values, dtype=float)
    weights = np.asarray(weights, dtype=float)
    valid = np.isfinite(values) & np.isfinite(weights) & (weights > 0.0)
    if not np.any(valid):
        return np.nan
    values = values[valid]
    weights = weights[valid]
    order = np.argsort(values)
    values = values[order]
    weights = weights[order]
    cumulative = np.cumsum(weights)
    cutoff = 0.5 * cumulative[-1]
    index = int(np.searchsorted(cumulative, cutoff, side='left'))
    index = min(max(index, 0), len(values) - 1)
    return float(values[index])


def _robust_local_spread(values, reference_value):
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0 or not np.isfinite(reference_value):
        return 0.0
    abs_dev = np.abs(values - float(reference_value))
    mad = float(np.nanmedian(abs_dev)) if abs_dev.size else 0.0
    if values.size >= 2:
        q25, q75 = np.nanpercentile(values, [25.0, 75.0])
        iqr = float(q75 - q25)
    else:
        iqr = 0.0
    return float(max(1.4826 * mad, 0.5 * iqr, 0.0))


def _build_local_parameter_bounds(reference_value, local_values, global_bounds, settings):
    if global_bounds is None:
        return None
    lower_global, upper_global = global_bounds
    if lower_global is None or upper_global is None:
        return global_bounds
    lower_global = float(lower_global)
    upper_global = float(upper_global)
    if not np.isfinite(reference_value):
        return (lower_global, upper_global)

    span = max(upper_global - lower_global, 0.0)
    local_spread = _robust_local_spread(local_values, reference_value)
    relative_pad = float(settings['local_param_relative_tolerance']) * max(abs(float(reference_value)), 1.0e-6)
    global_floor = float(settings['local_param_global_fraction_floor']) * span
    global_cap = max(global_floor, float(settings['local_param_global_fraction_cap']) * span)

    half_width = max((2.0 * local_spread), relative_pad, global_floor)
    if global_cap > 0.0:
        half_width = min(half_width, global_cap)
    if half_width <= 0.0:
        half_width = max(global_floor, 1.0e-6)

    lower = max(lower_global, float(reference_value) - half_width)
    upper = min(upper_global, float(reference_value) + half_width)
    if not np.isfinite(lower) or not np.isfinite(upper) or lower > upper:
        return (lower_global, upper_global)
    if abs(upper - lower) <= 1.0e-12:
        epsilon = max(global_floor, 1.0e-6)
        lower = max(lower_global, lower - epsilon)
        upper = min(upper_global, upper + epsilon)
    return (lower, upper)


def _intersect_bounds(bounds_a, bounds_b):
    if bounds_a is None:
        return bounds_b
    if bounds_b is None:
        return bounds_a
    lower_a, upper_a = bounds_a
    lower_b, upper_b = bounds_b
    lower = max(float(lower_a), float(lower_b))
    upper = min(float(upper_a), float(upper_b))
    if lower > upper:
        midpoint = 0.5 * (lower + upper)
        return (midpoint, midpoint)
    return (lower, upper)


def _fixed_parameter_bounds(value, global_bounds):
    if global_bounds is None:
        if np.isfinite(value):
            return (float(value), float(value))
        return None
    lower_global, upper_global = global_bounds
    lower_global = float(lower_global)
    upper_global = float(upper_global)
    if not np.isfinite(value):
        fallback = min(max(0.5 * (lower_global + upper_global), lower_global), upper_global)
        return (fallback, fallback)
    fixed_value = min(max(float(value), lower_global), upper_global)
    return (fixed_value, fixed_value)


def _build_local_depth_constraint(expected_depth, anchor_depth_reference, local_depths,
                                  global_bounds, settings):
    if global_bounds is None:
        return None, float(settings['correction_tolerance_m'])
    lower_global, upper_global = global_bounds
    lower_global = float(lower_global)
    upper_global = float(upper_global)

    depth_spread = _robust_local_spread(local_depths, anchor_depth_reference)
    min_tolerance = float(settings['correction_tolerance_m'])
    max_tolerance = max(min_tolerance, float(settings['max_depth_tolerance_m']))
    depth_tolerance = min(max(min_tolerance, depth_spread), max_tolerance)
    lower = max(lower_global, float(expected_depth) - depth_tolerance)
    upper = min(upper_global, float(expected_depth) + depth_tolerance)
    if lower > upper:
        lower = upper = min(max(float(expected_depth), lower_global), upper_global)
    return (lower, upper), depth_tolerance


def _normalise_fraction_guess(fractions):
    fractions = np.asarray(fractions, dtype=float)
    fractions = np.clip(fractions, 0.0, None)
    if fractions.size != 3 or not np.all(np.isfinite(fractions)):
        return np.array([1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0], dtype=float)
    total = float(np.sum(fractions))
    if total <= 0.0:
        return np.array([1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0], dtype=float)
    if total > 1.0:
        fractions = fractions / total
    return fractions


def _adjacent_anchor_values(values, anchor_mask, row, col):
    row_min = max(0, row - 1)
    row_max = min(anchor_mask.shape[0], row + 2)
    col_min = max(0, col - 1)
    col_max = min(anchor_mask.shape[1], col + 2)
    local_mask = np.asarray(anchor_mask[row_min:row_max, col_min:col_max], dtype=bool).copy()
    local_mask[(row - row_min), (col - col_min)] = False
    if not np.any(local_mask):
        return np.array([], dtype=float)
    local_values = np.asarray(values[row_min:row_max, col_min:col_max], dtype=float)[local_mask]
    return local_values[np.isfinite(local_values)]


def _adjacent_confident_depth_jump(depth_values, confident_mask, row, col):
    local_depths = _adjacent_anchor_values(depth_values, confident_mask, row, col)
    if local_depths.size == 0 or not np.isfinite(depth_values[row, col]):
        return np.nan
    return float(depth_values[row, col] - np.nanmedian(local_depths))


def _continuity_component_score(value, neighbour_values, absolute_floor, relative_floor=0.0):
    neighbour_values = np.asarray(neighbour_values, dtype=float)
    neighbour_values = neighbour_values[np.isfinite(neighbour_values)]
    if neighbour_values.size == 0 or not np.isfinite(value):
        return np.nan, np.nan
    reference_value = float(np.nanmedian(neighbour_values))
    local_spread = _robust_local_spread(neighbour_values, reference_value)
    scale = max(
        float(absolute_floor),
        2.0 * float(local_spread),
        float(relative_floor) * max(abs(reference_value), 1.0e-6),
    )
    if not np.isfinite(scale) or scale <= 0.0:
        scale = max(float(absolute_floor), 1.0e-6)
    raw_jump = abs(float(value) - reference_value)
    return raw_jump / scale, raw_jump


def _pixel_continuity_metrics(depth_values, chl_values, cdom_values, nap_values, kd_values,
                              stable_mask, row, col, settings,
                              overrides=None):
    stable_mask = np.asarray(stable_mask, dtype=bool)
    overrides = overrides or {}

    def _resolve_override(name, default_value):
        override_value = overrides.get(name, default_value)
        if override_value is None:
            override_value = default_value
        return float(override_value)

    depth_value = _resolve_override('depth', depth_values[row, col])
    chl_value = _resolve_override('chl', chl_values[row, col])
    cdom_value = _resolve_override('cdom', cdom_values[row, col])
    nap_value = _resolve_override('nap', nap_values[row, col])
    kd_value = _resolve_override('kd', kd_values[row, col])

    depth_neighbours = _adjacent_anchor_values(depth_values, stable_mask, row, col)
    if depth_neighbours.size == 0:
        return None

    depth_score, depth_jump = _continuity_component_score(
        depth_value,
        depth_neighbours,
        settings['continuity_depth_floor_m'],
        relative_floor=0.0,
    )
    if not np.isfinite(depth_score) or not np.isfinite(depth_jump):
        return None

    chl_score, _ = _continuity_component_score(
        chl_value,
        _adjacent_anchor_values(chl_values, stable_mask, row, col),
        absolute_floor=0.05,
        relative_floor=settings['local_param_relative_tolerance'],
    )
    cdom_score, _ = _continuity_component_score(
        cdom_value,
        _adjacent_anchor_values(cdom_values, stable_mask, row, col),
        absolute_floor=0.02,
        relative_floor=settings['local_param_relative_tolerance'],
    )
    nap_score, _ = _continuity_component_score(
        nap_value,
        _adjacent_anchor_values(nap_values, stable_mask, row, col),
        absolute_floor=0.05,
        relative_floor=settings['local_param_relative_tolerance'],
    )
    kd_score, _ = _continuity_component_score(
        kd_value,
        _adjacent_anchor_values(kd_values, stable_mask, row, col),
        absolute_floor=0.05,
        relative_floor=settings['local_param_relative_tolerance'],
    )

    total_score = 4.0 * depth_score
    for component_score, weight in (
        (chl_score, 1.0),
        (cdom_score, 1.0),
        (nap_score, 1.0),
        (kd_score, 0.5),
    ):
        if np.isfinite(component_score):
            total_score += weight * component_score

    return {
        'score': float(total_score),
        'depth_jump': float(depth_jump),
    }


def _snapshot_result_recorder_pixels(result_recorder, pixel_coords):
    pixel_states = {}
    fields = (
        'error_alpha',
        'error_alpha_f',
        'error_f',
        'error_lsq',
        'chl',
        'cdom',
        'nap',
        'depth',
        'sub1_frac',
        'sub2_frac',
        'sub3_frac',
        'closed_rrs',
        'nit',
        'success',
        'kd',
        'sdi',
        'r_sub',
        'exp_bottom',
    )
    for row, col in pixel_coords:
        state = {}
        for field_name in fields:
            array = getattr(result_recorder, field_name, None)
            if array is None:
                continue
            value = array[row, col]
            state[field_name] = np.array(value, copy=True) if np.ndim(value) else value
        pixel_states[(int(row), int(col))] = state
    return pixel_states


def _restore_result_recorder_pixel(result_recorder, pixel_state, row, col):
    for field_name, value in pixel_state.items():
        array = getattr(result_recorder, field_name, None)
        if array is None:
            continue
        array[row, col] = value


def _accept_corrected_pixel(result_recorder, pixel_state, stable_mask, row, col, settings):
    success_value = getattr(result_recorder, 'success', None)
    if success_value is None or int(success_value[row, col]) < 1:
        return False

    before_metrics = _pixel_continuity_metrics(
        result_recorder.depth,
        result_recorder.chl,
        result_recorder.cdom,
        result_recorder.nap,
        result_recorder.kd,
        stable_mask,
        row,
        col,
        settings,
        overrides={
            'depth': pixel_state.get('depth'),
            'chl': pixel_state.get('chl'),
            'cdom': pixel_state.get('cdom'),
            'nap': pixel_state.get('nap'),
            'kd': pixel_state.get('kd'),
        },
    )
    after_metrics = _pixel_continuity_metrics(
        result_recorder.depth,
        result_recorder.chl,
        result_recorder.cdom,
        result_recorder.nap,
        result_recorder.kd,
        stable_mask,
        row,
        col,
        settings,
    )
    if before_metrics is None or after_metrics is None:
        return True

    old_error = float(pixel_state.get('error_alpha_f', np.nan))
    new_error = float(result_recorder.error_alpha_f[row, col])
    if np.isfinite(old_error) and np.isfinite(new_error):
        allowed_error = max(
            old_error + float(settings['continuity_max_error_growth_abs']),
            old_error * float(settings['continuity_max_error_growth_factor']),
        )
        if new_error > allowed_error:
            return False

    min_depth_improvement = float(settings['continuity_min_depth_improvement_m'])
    depth_improved = after_metrics['depth_jump'] <= (before_metrics['depth_jump'] - min_depth_improvement)
    score_improved = after_metrics['score'] <= (before_metrics['score'] - 0.1)
    if not depth_improved and not score_improved:
        return False
    if after_metrics['depth_jump'] > (before_metrics['depth_jump'] + 1.0e-6):
        return False
    return True


def _build_false_deep_correction_plan(depth_data, sdi_data, error_f_data, slope_data,
                                      chl_data, cdom_data, nap_data, kd_data,
                                      sub1_data, sub2_data, sub3_data,
                                      settings, depth_min, p_bounds,
                                      exposed_mask=None, dx_m=np.nan, dy_m=np.nan,
                                      extra_confident_mask=None,
                                      base_confident_mask=None,
                                      lock_water_parameters=False):
    depth = ma.array(depth_data, copy=False)
    sdi = ma.array(sdi_data, copy=False)
    error_f = ma.array(error_f_data, copy=False)
    slope = ma.array(slope_data, copy=False)
    chl = ma.array(chl_data, copy=False)
    cdom = ma.array(cdom_data, copy=False)
    nap = ma.array(nap_data, copy=False)
    kd = ma.array(kd_data, copy=False)
    sub1 = ma.array(sub1_data, copy=False)
    sub2 = ma.array(sub2_data, copy=False)
    sub3 = ma.array(sub3_data, copy=False)

    depth_values, sdi_values, error_values, slope_values, water_mask, barrier_mask = _false_deep_base_masks(
        depth,
        sdi,
        error_f,
        slope,
        settings,
        depth_min,
        exposed_mask=exposed_mask,
        required_data=(chl, cdom, nap, kd),
    )
    chl_values = chl.filled(np.nan).astype(float)
    cdom_values = cdom.filled(np.nan).astype(float)
    nap_values = nap.filled(np.nan).astype(float)
    kd_values = kd.filled(np.nan).astype(float)
    sub1_values = sub1.filled(np.nan).astype(float)
    sub2_values = sub2.filled(np.nan).astype(float)
    sub3_values = sub3.filled(np.nan).astype(float)
    empty_reference = ma.masked_all(depth.shape, dtype='float32')
    if not np.any(water_mask):
        return {
            'slope': ma.array(slope, copy=True),
            'confident_mask': np.zeros(depth.shape, dtype=bool),
            'anchor_mask': np.zeros(depth.shape, dtype=bool),
            'barrier_mask': barrier_mask,
            'seed_mask': np.zeros(depth.shape, dtype=bool),
            'candidate_mask': np.zeros(depth.shape, dtype=bool),
            'anomalous_mask': np.zeros(depth.shape, dtype=bool),
            'suspicious_mask': np.zeros(depth.shape, dtype=bool),
            'suspect_mask': np.zeros(depth.shape, dtype=bool),
            'reference_depth': empty_reference,
            'reference_tolerance': ma.masked_all(depth.shape, dtype='float32'),
            'reference_chl': ma.masked_all(depth.shape, dtype='float32'),
            'reference_cdom': ma.masked_all(depth.shape, dtype='float32'),
            'reference_nap': ma.masked_all(depth.shape, dtype='float32'),
            'reference_kd': ma.masked_all(depth.shape, dtype='float32'),
            'rerun_items': [],
        }

    if base_confident_mask is None:
        confident_mask = _build_false_deep_confident_mask(
            depth,
            sdi,
            error_f,
            slope,
            settings,
            depth_min,
            exposed_mask=exposed_mask,
            required_data=(chl, cdom, nap, kd),
        )
    else:
        confident_mask = np.asarray(base_confident_mask, dtype=bool) & water_mask
    if extra_confident_mask is not None:
        confident_mask |= (np.asarray(extra_confident_mask, dtype=bool) & water_mask)

    component_labels, component_count = ndimage.label(water_mask, structure=np.ones((3, 3), dtype=int))
    reference_depth = ma.masked_all(depth.shape, dtype='float32')
    reference_tolerance = ma.masked_all(depth.shape, dtype='float32')
    reference_chl = ma.masked_all(depth.shape, dtype='float32')
    reference_cdom = ma.masked_all(depth.shape, dtype='float32')
    reference_nap = ma.masked_all(depth.shape, dtype='float32')
    reference_kd = ma.masked_all(depth.shape, dtype='float32')
    candidate_mask = np.zeros(depth.shape, dtype=bool)
    seed_mask = np.zeros(depth.shape, dtype=bool)
    suspicious_mask = np.zeros(depth.shape, dtype=bool)
    pixel_plans = {}
    radius_px = int(settings['search_radius_px'])
    slope_limit_ratio = max(0.0, float(settings.get('fixed_depth_max_slope_percent', 10.0))) / 100.0
    candidate_sdi_threshold = float(settings['suspect_max_sdi'])

    for component_id in range(1, component_count + 1):
        component_mask = component_labels == component_id
        component_confident_mask = confident_mask & component_mask
        if not np.any(component_confident_mask):
            continue

        confident_rows, confident_cols = np.where(component_confident_mask)
        confident_depths = depth_values[confident_rows, confident_cols]
        if confident_depths.size < settings['min_anchor_count']:
            continue
        confident_chl = chl_values[confident_rows, confident_cols]
        confident_cdom = cdom_values[confident_rows, confident_cols]
        confident_nap = nap_values[confident_rows, confident_cols]
        confident_kd = kd_values[confident_rows, confident_cols]
        confident_coords = np.column_stack((confident_rows, confident_cols)).astype(float, copy=False)
        confident_tree = cKDTree(confident_coords)

        component_candidate_mask = (
            component_mask
            & ~component_confident_mask
            & (sdi_values <= candidate_sdi_threshold)
        )
        candidate_rows, candidate_cols = np.where(component_candidate_mask)
        if candidate_rows.size == 0:
            continue
        candidate_coords = np.column_stack((candidate_rows, candidate_cols)).astype(float, copy=False)
        candidate_neighbour_indices = confident_tree.query_ball_point(candidate_coords, r=radius_px, eps=0.0)
        for row, col, neighbour_indices in zip(candidate_rows, candidate_cols, candidate_neighbour_indices):
            if len(neighbour_indices) < settings['min_anchor_count']:
                continue
            local_indices = np.sort(np.asarray(neighbour_indices, dtype=int))
            local_depths = confident_depths[local_indices]
            local_chl = confident_chl[local_indices]
            local_cdom = confident_cdom[local_indices]
            local_nap = confident_nap[local_indices]
            local_kd = confident_kd[local_indices]
            adjacent_depths = _adjacent_anchor_values(depth_values, component_confident_mask, row, col)
            adjacent_chl = _adjacent_anchor_values(chl_values, component_confident_mask, row, col)
            adjacent_cdom = _adjacent_anchor_values(cdom_values, component_confident_mask, row, col)
            adjacent_nap = _adjacent_anchor_values(nap_values, component_confident_mask, row, col)
            adjacent_kd = _adjacent_anchor_values(kd_values, component_confident_mask, row, col)
            local_row_dist = (confident_rows[local_indices] - row).astype(float, copy=False)
            local_col_dist = (confident_cols[local_indices] - col).astype(float, copy=False)
            if np.isfinite(dx_m) and dx_m > 0.0 and np.isfinite(dy_m) and dy_m > 0.0:
                local_distances = np.sqrt((local_row_dist * dy_m) ** 2 + (local_col_dist * dx_m) ** 2)
            else:
                local_distances = np.sqrt(local_row_dist ** 2 + local_col_dist ** 2)
            local_weights = 1.0 / np.maximum(local_distances, 1.0)
            if adjacent_depths.size > 0:
                anchor_depth_reference = float(np.nanmedian(adjacent_depths))
            else:
                anchor_depth_reference = _weighted_median(local_depths, local_weights)
            if not np.isfinite(anchor_depth_reference):
                continue
            if adjacent_depths.size > 0:
                expected_depth = float(np.nanmedian(adjacent_depths))
            else:
                slope_limited_depths = local_depths + (slope_limit_ratio * local_distances)
                expected_depth = _weighted_median(slope_limited_depths, local_weights)
            if not np.isfinite(expected_depth):
                continue
            if (depth_values[row, col] - expected_depth) < settings['suspect_min_depth_jump_m']:
                continue
            if lock_water_parameters:
                reference_chl_value = float(chl_values[row, col])
                reference_cdom_value = float(cdom_values[row, col])
                reference_nap_value = float(nap_values[row, col])
            else:
                if adjacent_chl.size > 0:
                    reference_chl_value = float(np.nanmedian(adjacent_chl))
                else:
                    reference_chl_value = _weighted_median(local_chl, local_weights)
                if adjacent_cdom.size > 0:
                    reference_cdom_value = float(np.nanmedian(adjacent_cdom))
                else:
                    reference_cdom_value = _weighted_median(local_cdom, local_weights)
                if adjacent_nap.size > 0:
                    reference_nap_value = float(np.nanmedian(adjacent_nap))
                else:
                    reference_nap_value = _weighted_median(local_nap, local_weights)
            if adjacent_kd.size > 0:
                reference_kd_value = float(np.nanmedian(adjacent_kd))
            else:
                reference_kd_value = _weighted_median(local_kd, local_weights)
            if not (
                np.isfinite(reference_chl_value)
                and np.isfinite(reference_cdom_value)
                and np.isfinite(reference_nap_value)
                and np.isfinite(reference_kd_value)
            ):
                continue
            if lock_water_parameters:
                initial_chl_value = reference_chl_value
                initial_cdom_value = reference_cdom_value
                initial_nap_value = reference_nap_value
            else:
                representative_score = (
                    np.abs(local_kd - reference_kd_value)
                    + 0.1 * np.abs(local_depths - anchor_depth_reference)
                )
                representative_score[~np.isfinite(representative_score)] = np.inf
                representative_index = int(np.argmin(representative_score))
                initial_chl_value = float(local_chl[representative_index])
                initial_cdom_value = float(local_cdom[representative_index])
                initial_nap_value = float(local_nap[representative_index])
                if not (
                    np.isfinite(initial_chl_value)
                    and np.isfinite(initial_cdom_value)
                    and np.isfinite(initial_nap_value)
                ):
                    initial_chl_value = reference_chl_value
                    initial_cdom_value = reference_cdom_value
                    initial_nap_value = reference_nap_value

            if p_bounds is None or len(p_bounds) < 7:
                continue
            local_bounds = [tuple(bound) if bound is not None else None for bound in p_bounds]
            if lock_water_parameters:
                local_bounds[0] = _fixed_parameter_bounds(reference_chl_value, p_bounds[0])
                local_bounds[1] = _fixed_parameter_bounds(reference_cdom_value, p_bounds[1])
                local_bounds[2] = _fixed_parameter_bounds(reference_nap_value, p_bounds[2])
            else:
                local_bounds[0] = _build_local_parameter_bounds(reference_chl_value, local_chl, p_bounds[0], settings)
                local_bounds[1] = _build_local_parameter_bounds(reference_cdom_value, local_cdom, p_bounds[1], settings)
                local_bounds[2] = _build_local_parameter_bounds(reference_nap_value, local_nap, p_bounds[2], settings)
            depth_bounds, depth_tolerance = _build_local_depth_constraint(
                expected_depth,
                anchor_depth_reference,
                local_depths,
                p_bounds[3],
                settings,
            )
            local_bounds[3] = depth_bounds
            if not lock_water_parameters:
                if adjacent_chl.size > 0:
                    adjacent_chl_ref = float(np.nanmedian(adjacent_chl))
                    adjacent_chl_bounds = _build_local_parameter_bounds(adjacent_chl_ref, adjacent_chl, p_bounds[0], settings)
                    local_bounds[0] = _intersect_bounds(local_bounds[0], adjacent_chl_bounds)
                if adjacent_cdom.size > 0:
                    adjacent_cdom_ref = float(np.nanmedian(adjacent_cdom))
                    adjacent_cdom_bounds = _build_local_parameter_bounds(adjacent_cdom_ref, adjacent_cdom, p_bounds[1], settings)
                    local_bounds[1] = _intersect_bounds(local_bounds[1], adjacent_cdom_bounds)
                if adjacent_nap.size > 0:
                    adjacent_nap_ref = float(np.nanmedian(adjacent_nap))
                    adjacent_nap_bounds = _build_local_parameter_bounds(adjacent_nap_ref, adjacent_nap, p_bounds[2], settings)
                    local_bounds[2] = _intersect_bounds(local_bounds[2], adjacent_nap_bounds)
            if adjacent_depths.size > 0:
                adjacent_depth_ref = float(np.nanmedian(adjacent_depths))
                adjacent_depth_bounds, adjacent_depth_tolerance = _build_local_depth_constraint(
                    expected_depth,
                    adjacent_depth_ref,
                    adjacent_depths,
                    p_bounds[3],
                    settings,
                )
                local_bounds[3] = _intersect_bounds(local_bounds[3], adjacent_depth_bounds)
                if local_bounds[3] is not None:
                    depth_tolerance = min(
                        depth_tolerance,
                        max(0.0, float(local_bounds[3][1]) - float(expected_depth)),
                        max(0.0, float(expected_depth) - float(local_bounds[3][0])),
                        adjacent_depth_tolerance,
                    )

            substrate_guess = _normalise_fraction_guess(
                [sub1_values[row, col], sub2_values[row, col], sub3_values[row, col]]
            )
            initial_guess = np.array(
                [
                    initial_chl_value,
                    initial_cdom_value,
                    initial_nap_value,
                    expected_depth,
                    substrate_guess[0],
                    substrate_guess[1],
                    substrate_guess[2],
                ],
                dtype=float,
            )

            reference_depth[row, col] = expected_depth
            reference_tolerance[row, col] = depth_tolerance
            reference_chl[row, col] = reference_chl_value
            reference_cdom[row, col] = reference_cdom_value
            reference_nap[row, col] = reference_nap_value
            reference_kd[row, col] = reference_kd_value
            candidate_mask[row, col] = True
            adjacent_jump = _adjacent_confident_depth_jump(depth_values, component_confident_mask, row, col)
            if (
                slope_values[row, col] > settings['suspect_min_slope_percent']
                or (
                    np.isfinite(adjacent_jump)
                    and adjacent_jump >= settings['seed_min_adjacent_depth_jump_m']
                )
            ):
                seed_mask[row, col] = True
            pixel_plans[(int(row), int(col))] = {
                'x': int(row),
                'y': int(col),
                'target_depth': float(expected_depth),
                'depth_tolerance': float(depth_tolerance),
                'initial_guess': initial_guess.tolist(),
                'bounds': [tuple(bound) if bound is not None else None for bound in local_bounds],
            }

    if np.any(candidate_mask):
        patch_labels, patch_count = ndimage.label(candidate_mask, structure=np.ones((3, 3), dtype=int))
        for patch_id in range(1, patch_count + 1):
            patch_mask = patch_labels == patch_id
            if int(np.count_nonzero(patch_mask)) > settings['max_patch_size_px']:
                reference_depth[patch_mask] = ma.masked
                reference_tolerance[patch_mask] = ma.masked
                reference_chl[patch_mask] = ma.masked
                reference_cdom[patch_mask] = ma.masked
                reference_nap[patch_mask] = ma.masked
                reference_kd[patch_mask] = ma.masked
                continue
            if not np.any(seed_mask & patch_mask):
                reference_depth[patch_mask] = ma.masked
                reference_tolerance[patch_mask] = ma.masked
                reference_chl[patch_mask] = ma.masked
                reference_cdom[patch_mask] = ma.masked
                reference_nap[patch_mask] = ma.masked
                reference_kd[patch_mask] = ma.masked
                continue
            suspicious_mask[patch_mask] = True

    rerun_items = []
    for row, col in np.argwhere(suspicious_mask):
        pixel_plan = pixel_plans.get((int(row), int(col)))
        if pixel_plan is not None:
            rerun_items.append(pixel_plan)

    return {
        'slope': ma.array(slope, copy=True),
        'confident_mask': confident_mask,
        'anchor_mask': confident_mask,
        'barrier_mask': barrier_mask,
        'seed_mask': seed_mask,
        'candidate_mask': candidate_mask,
        'anomalous_mask': candidate_mask,
        'suspicious_mask': suspicious_mask,
        'suspect_mask': suspicious_mask,
        'reference_depth': ma.masked_array(reference_depth, mask=~suspicious_mask | ~np.isfinite(reference_depth)),
        'reference_tolerance': ma.masked_array(reference_tolerance, mask=~suspicious_mask | ~np.isfinite(reference_tolerance)),
        'reference_chl': ma.masked_array(reference_chl, mask=~suspicious_mask | ~np.isfinite(reference_chl)),
        'reference_cdom': ma.masked_array(reference_cdom, mask=~suspicious_mask | ~np.isfinite(reference_cdom)),
        'reference_nap': ma.masked_array(reference_nap, mask=~suspicious_mask | ~np.isfinite(reference_nap)),
        'reference_kd': ma.masked_array(reference_kd, mask=~suspicious_mask | ~np.isfinite(reference_kd)),
        'rerun_items': rerun_items,
    }


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
                              substrate_var_names, relaxed, fully_relaxed=False):
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
        substrate_norm_map = _compute_chunk_substrate_norms(chunk_arrays, relaxed, substrate_var_names, fully_relaxed=fully_relaxed)
        for var_name, _ in primary_var_defs:
            data = chunk_arrays.get(var_name, substrate_norm_map.get(var_name))
            if data is None:
                continue
            nc_vars[var_name][row_slice, :] = ma.array(data).filled(OUTPUT_FILL_VALUE)

    nc_o.close()


def _write_geotiff_from_chunks(chunk_manifest, tif_path, width, height, lat, lon, shape_geo,
                               primary_var_defs, substrate_var_names, relaxed, fully_relaxed=False,
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
            substrate_norm_map = _compute_chunk_substrate_norms(chunk_arrays, relaxed, substrate_var_names, fully_relaxed=fully_relaxed)
            for band_idx, (var_name, _) in enumerate(primary_var_defs, start=1):
                data = chunk_arrays.get(var_name, substrate_norm_map.get(var_name))
                if data is None:
                    continue
                dst.write(ma.array(data).filled(OUTPUT_FILL_VALUE), band_idx, window=window)


def _build_primary_outputs_from_chunk(chunk_arrays, primary_var_defs, substrate_var_names, relaxed, fully_relaxed=False):
    """Return list of (var_name, array, display_name) for a single chunk."""
    required_sub_keys = ('sub1_frac', 'sub2_frac', 'sub3_frac')
    if any(chunk_arrays.get(key) is None for key in required_sub_keys):
        return []
    chunk_like = {key: chunk_arrays.get(key) for key in required_sub_keys}
    substrate_norm_map = _compute_chunk_substrate_norms(chunk_like, relaxed, substrate_var_names, fully_relaxed=fully_relaxed)
    metric_arrays = {
        'chl': chunk_arrays.get('chl'),
        'cdom': chunk_arrays.get('cdom'),
        'nap': chunk_arrays.get('nap'),
        'depth': chunk_arrays.get('depth'),
        'kd': chunk_arrays.get('kd'),
        'sdi': chunk_arrays.get('sdi'),
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
                         primary_var_defs, substrate_var_names, relaxed, fully_relaxed=False, cleanup_paths=None,
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
        primary_outputs = _build_primary_outputs_from_chunk(chunk_arrays, primary_var_defs, substrate_var_names, relaxed, fully_relaxed=fully_relaxed)
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
                           substrate_var_names, relaxed, fully_relaxed=False, grid_metadata=None):
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
                fully_relaxed=fully_relaxed)
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
                        fully_relaxed=fully_relaxed,
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


def _write_false_deep_debug_geotiffs(ofile, width, height, lat_data, lon_data, shape_geo_val, debug_layers,
                                     grid_metadata=None):
    if lat_data is None or lon_data is None or not debug_layers:
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


def _select_suspicious_frontier(suspicious_mask, confident_mask, already_done_mask):
    suspicious_mask = np.asarray(suspicious_mask, dtype=bool)
    confident_mask = np.asarray(confident_mask, dtype=bool)
    already_done_mask = np.asarray(already_done_mask, dtype=bool)
    pending_mask = suspicious_mask & ~already_done_mask
    if not np.any(pending_mask):
        return pending_mask
    structure = np.ones((3, 3), dtype=bool)
    neighbour_mask = ndimage.binary_dilation(confident_mask | already_done_mask, structure=structure)
    frontier_mask = pending_mask & neighbour_mask
    return frontier_mask

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
        fully_relaxed = False
        output_modeled_reflectance = False
        crop_selection = None
        deep_water_selection = None
        saved_sensor_band_mapping = None
        false_deep_correction_settings = dict(DEFAULT_FALSE_DEEP_CORRECTION_SETTINGS)
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
            fully_relaxed = _coerce_bool(root.get('fully_relaxed', False))
            output_modeled_reflectance = _coerce_bool(root.get('output_modeled_reflectance', False))
            crop_selection = _parse_crop_selection(root)
            deep_water_selection = _parse_deep_water_selection(root)
            saved_sensor_band_mapping = _parse_saved_sensor_band_mapping(root)
            false_deep_correction_settings = _normalise_false_deep_correction_settings({
                'enabled': root.get('false_deep_correction_enabled', False),
                'anchor_min_sdi': root.get('false_deep_anchor_min_sdi'),
                'anchor_max_depth_m': root.get('false_deep_anchor_max_depth_m'),
                'anchor_max_slope_percent': root.get('false_deep_anchor_max_slope_percent'),
                'anchor_max_slope_deg': root.get('false_deep_anchor_max_slope_deg'),
                'anchor_max_error_f': root.get('false_deep_anchor_max_error_f'),
                'anchor_min_depth_margin_m': root.get('false_deep_anchor_min_depth_margin_m'),
                'suspect_max_sdi': root.get('false_deep_suspect_max_sdi'),
                'suspect_min_slope_percent': root.get('false_deep_suspect_min_slope_percent'),
                'suspect_min_slope_deg': root.get('false_deep_suspect_min_slope_deg'),
                'suspect_min_depth_jump_m': root.get('false_deep_suspect_min_depth_jump_m'),
                'search_radius_px': root.get('false_deep_search_radius_px'),
                'min_anchor_count': root.get('false_deep_min_anchor_count'),
                'correction_tolerance_m': root.get('false_deep_correction_tolerance_m'),
                'max_patch_size_px': root.get('false_deep_max_patch_size_px'),
                'treat_min_depth_as_barrier': root.get('false_deep_treat_min_depth_as_barrier'),
                'barrier_depth_margin_m': root.get('false_deep_barrier_depth_margin_m'),
                'barrier_min_sdi': root.get('false_deep_barrier_min_sdi'),
                'fixed_depth_max_slope_percent': root.get('false_deep_fixed_depth_max_slope_percent'),
                'fixed_depth_max_slope_deg': root.get('false_deep_fixed_depth_max_slope_deg'),
                'debug_export': root.get('false_deep_debug_export', False),
            })



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
            nedr_mode = str(root.get('nedr_mode', 'fixed')).strip().lower()
            allow_split = _coerce_bool(root.get('allow_split', False))
            split_chunk_rows = _parse_chunk_rows(root.get('split_chunk_rows'))

        else:
            # call the GUI
            gui_result = gui_swampy.gui()
            if gui_result is None:
                print("[INFO]: GUI closed without running. Exiting.")
                sys.exit(0)
            file_list, ofile, siop_xml_path, file_sensor, above_rrs_flag, reflectance_input_flag, relaxed, shallow_flag, \
                optimize_initial_guesses, use_five_initial_guesses, initial_guess_debug, fully_relaxed, output_modeled_reflectance, false_deep_correction_settings, pmin, pmax, xml_file, xml_dict, output_format, bathy_path, pp, allow_split, split_chunk_rows_str = gui_result
            split_chunk_rows = _parse_chunk_rows(split_chunk_rows_str)
            bathy_path = _resolve_bundled_resource(bathy_path)
            bathy_reference = str(xml_dict.get('bathy_reference', 'depth')).strip().lower()
            bathy_correction_m = _coerce_float(xml_dict.get('bathy_correction_m'), 0.0)
            bathy_tolerance_m = _coerce_float(xml_dict.get('bathy_tolerance_m'), 0.0)
            nedr_mode = str(xml_dict.get('nedr_mode', 'fixed')).strip().lower()
            crop_selection = _parse_crop_selection(xml_dict)
            deep_water_selection = _parse_deep_water_selection(xml_dict)
            saved_sensor_band_mapping = _parse_saved_sensor_band_mapping(xml_dict)

        if fully_relaxed and not relaxed:
            print("[WARN]: Fully relaxed substrate mode requires relaxed constraints. Disabling fully relaxed mode.")
            fully_relaxed = False

        if use_five_initial_guesses and not optimize_initial_guesses:
            print("[WARN]: Five-point initial guess testing requires initial guess optimisation. Disabling 5-point testing.")
            use_five_initial_guesses = False
        if initial_guess_debug and not optimize_initial_guesses:
            print("[WARN]: Initial guess debug export requires initial guess optimisation. Disabling debug export.")
            initial_guess_debug = False
        false_deep_correction_settings = _normalise_false_deep_correction_settings(false_deep_correction_settings)
        if false_deep_correction_settings.get('enabled') and bathy_path:
            print("[WARN]: False-deep bathymetry correction only applies when bathymetry is estimated. Disabling correction because input bathymetry is in use.")
            false_deep_correction_settings['enabled'] = False

        if allow_split and pp:
            print("[WARN]: Post-processing is not supported when image splitting is enabled. Skipping post-processing step.")
            pp = False

        # CLI override for output format, if provided
        if args.format:
            output_format = args.format
        if args.nedr_mode:
            nedr_mode = args.nedr_mode

        if nedr_mode not in ('scene', 'fixed'):
            print(f"[WARN]: Unsupported NEDR mode '{nedr_mode}'. Falling back to fixed.")
            nedr_mode = 'fixed'

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

        # Batch setup
        batch_mode = len(files_to_process) > 1
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

        for idx, file_im in enumerate(files_to_process, start=1):
            print(f"[INFO]: Processing file {idx}/{len(files_to_process)}: {file_im}")
            input_name = os.path.basename(file_im)
            input_base, _ = os.path.splitext(input_name)
            if not input_base:
                input_base = input_name
            if gui_run_dir:
                if batch_mode:
                    ofile = os.path.join(gui_run_dir, f'swampy_{input_base}{gui_default_ext}')
                else:
                    ofile = gui_default_ofile
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
            if deep_water_selection and lat_array is not None and lon_array is not None:
                deep_water_rrs_full = np.array(rrs, dtype='float32', copy=True)
                deep_water_lat_full = np.array(lat_array, dtype='float32', copy=True)
                deep_water_lon_full = np.array(lon_array, dtype='float32', copy=True)
                deep_water_grid_metadata = source_grid_metadata

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

            if above_rrs_flag == True:
                rrs = (2 * rrs) / ((3 * rrs) + 1)
                if deep_water_rrs_full is not None:
                    deep_water_rrs_full = (2 * deep_water_rrs_full) / ((3 * deep_water_rrs_full) + 1)

            deep_water_prior_stats = None
            deep_water_csv_path = None
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
                        if selected_pixel_count > 1000:
                            rng = np.random.default_rng(42)
                            chosen_indices = np.sort(rng.choice(selected_pixel_count, size=1000, replace=False))
                            deep_rows = deep_rows[chosen_indices]
                            deep_cols = deep_cols[chosen_indices]
                            print(
                                f"[INFO]: Deep-water polygons selected {selected_pixel_count} pixel(s); "
                                "randomly subsampling 1000 pixels for deep-water analysis."
                            )
                        chl_bounds = tuple(float(v) for v in siop['p_bounds'][0])
                        cdom_bounds = tuple(float(v) for v in siop['p_bounds'][1])
                        nap_bounds = tuple(float(v) for v in siop['p_bounds'][2])
                        deep_water_pixel_rows = []
                        successful_estimates = []
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
                                successful_estimates.append(estimate)
                        if successful_estimates:
                            deep_water_prior_stats = _apply_deep_water_priors(
                                siop,
                                successful_estimates,
                                deep_water_selection.get('use_sd_bounds', False),
                            )
                            deep_water_csv_path = os.path.splitext(ofile)[0] + '_deep_water_pixels.csv'
                            _write_deep_water_pixel_csv(deep_water_csv_path, deep_water_pixel_rows)
                            print(
                                "[INFO]: Deep-water priors estimated from "
                                f"{len(successful_estimates)} selected pixel(s). "
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

            if not args.path and deep_water_prior_stats is not None:
                xml_dict['deep_water_enabled'] = True
                xml_dict['deep_water_use_sd_bounds'] = bool(deep_water_selection.get('use_sd_bounds', False))
                xml_dict['deep_water_selected_pixel_count'] = int(np.count_nonzero(deep_water_mask)) if 'deep_water_mask' in locals() and deep_water_mask is not None else 0
                xml_dict['deep_water_success_pixel_count'] = int(len(successful_estimates)) if 'successful_estimates' in locals() else 0
                xml_dict['deep_water_csv_path'] = deep_water_csv_path or ''
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
                        fully_relaxed=fully_relaxed,
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
                    initial_guess_debug=initial_guess_debug,
                    fully_relaxed=fully_relaxed)

            if model_outputs is not None:
                (closed_rrs, chl, cdom, nap, depth, nit, kd,
                 sdi, sub1_frac, sub2_frac, sub3_frac, error_f,
                 total_abun, sub1_norm, sub2_norm, sub3_norm, r_sub, initial_guess_stack) = model_outputs
            else:
                initial_guess_stack = None

            correction_debug_layers = []
            false_deep_corrected_pixel_count = 0
            false_deep_reoptimised_mask = np.zeros((height, width), dtype=bool)
            false_deep_attempt_count = np.zeros((height, width), dtype=np.uint8)
            false_deep_effective_settings = None
            if false_deep_correction_settings.get('enabled'):
                lat_for_correction = lat_array if lat_array is not None else image_info.get('lat_grid')
                lon_for_correction = lon_array if lon_array is not None else image_info.get('lon_grid')
                shape_geo_for_correction = shape_geo if shape_geo is not None else image_info.get('shape_geo', 2)
                try:
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
                    depth_for_plan = ma.masked_array(correction_recorder.depth, mask=(correction_recorder.success < 1))
                    sdi_for_plan = ma.masked_array(correction_recorder.sdi, mask=(correction_recorder.success < 1))
                    error_for_plan = ma.masked_array(correction_recorder.error_alpha_f, mask=(correction_recorder.success < 1))
                    try:
                        dx_m, dy_m = _derive_pixel_spacing_meters(
                            lat_for_correction,
                            lon_for_correction,
                            shape_geo_for_correction,
                        )
                    except Exception:
                        dx_m, dy_m = np.nan, np.nan
                    slope_before = _compute_depth_slope_percent(
                        depth_for_plan,
                        lat_for_correction,
                        lon_for_correction,
                        shape_geo_for_correction,
                        dx_m=dx_m,
                        dy_m=dy_m,
                    )
                    false_deep_effective_settings = _derive_scene_adaptive_false_deep_settings(
                        depth_for_plan,
                        sdi_for_plan,
                        error_for_plan,
                        slope_before,
                        false_deep_correction_settings,
                        float(pmin[3]),
                        exposed_mask=bathy_exposed_mask,
                        required_data=(
                            correction_recorder.chl,
                            correction_recorder.cdom,
                            correction_recorder.nap,
                            correction_recorder.kd,
                        ),
                    )
                    print(
                        "[INFO]: False-deep adaptive thresholds: "
                        f"anchor_min_sdi={false_deep_effective_settings['anchor_min_sdi']:.4f}, "
                        f"anchor_max_error_f={false_deep_effective_settings['anchor_max_error_f']:.6f}, "
                        f"search_radius_px={int(false_deep_effective_settings['search_radius_px'])}."
                    )
                    locked_confident_mask = _build_false_deep_confident_mask(
                        depth_for_plan,
                        sdi_for_plan,
                        error_for_plan,
                        slope_before,
                        false_deep_effective_settings,
                        float(pmin[3]),
                        exposed_mask=bathy_exposed_mask,
                        required_data=(
                            correction_recorder.chl,
                            correction_recorder.cdom,
                            correction_recorder.nap,
                            correction_recorder.kd,
                        ),
                    )
                    correction_plan = None
                    accepted_confident_mask = np.zeros((height, width), dtype=bool)
                    suspect_mask = np.zeros((height, width), dtype=bool)
                    rerun_wave = 0
                    rerun_executor = None
                    try:
                        while True:
                            stable_confident_mask = locked_confident_mask | accepted_confident_mask
                            depth_for_plan = ma.masked_array(correction_recorder.depth, mask=(correction_recorder.success < 1))
                            sdi_for_plan = ma.masked_array(correction_recorder.sdi, mask=(correction_recorder.success < 1))
                            error_for_plan = ma.masked_array(correction_recorder.error_alpha_f, mask=(correction_recorder.success < 1))
                            slope_before = _compute_depth_slope_percent(
                                depth_for_plan,
                                lat_for_correction,
                                lon_for_correction,
                                shape_geo_for_correction,
                                dx_m=dx_m,
                                dy_m=dy_m,
                            )
                            correction_plan = _build_false_deep_correction_plan(
                                depth_for_plan,
                                sdi_for_plan,
                                error_for_plan,
                                slope_before,
                                correction_recorder.chl,
                                correction_recorder.cdom,
                                correction_recorder.nap,
                                correction_recorder.kd,
                                correction_recorder.sub1_frac,
                                correction_recorder.sub2_frac,
                                correction_recorder.sub3_frac,
                                false_deep_effective_settings,
                                float(pmin[3]),
                                siop['p_bounds'],
                                exposed_mask=bathy_exposed_mask,
                                dx_m=dx_m,
                                dy_m=dy_m,
                                extra_confident_mask=accepted_confident_mask,
                                base_confident_mask=locked_confident_mask,
                                lock_water_parameters=(deep_water_prior_stats is not None),
                            )
                            suspect_mask = np.asarray(correction_plan['suspicious_mask'], dtype=bool)
                            eligible_mask = suspect_mask & (
                                ~stable_confident_mask
                                & (false_deep_attempt_count < int(false_deep_effective_settings['max_rerun_attempts']))
                            )
                            pending_count = int(np.count_nonzero(eligible_mask))
                            if pending_count <= 0:
                                break
                            frontier_mask = _select_suspicious_frontier(
                                eligible_mask,
                                stable_confident_mask,
                                np.zeros_like(eligible_mask, dtype=bool),
                            )
                            pixel_constraints = [
                                item for item in (correction_plan.get('rerun_items') or [])
                                if (
                                    frontier_mask[int(item['x']), int(item['y'])]
                                    and not stable_confident_mask[int(item['x']), int(item['y'])]
                                )
                            ]
                            if not pixel_constraints:
                                break
                            if rerun_executor is None:
                                rerun_executor = output_calculation.create_rerun_worker_pool(
                                    objective,
                                    siop,
                                    opt_met,
                                    relaxed,
                                    fully_relaxed=fully_relaxed,
                                    free_cpu=args.free_cpu,
                                    bathy_tolerance=0.0,
                                    optimize_initial_guesses=False,
                                    use_five_initial_guesses=False,
                                    apply_shallow_adjustment=False,
                                    allow_target_sum_over_one=False,
                                    max_tasks=pending_count,
                                )
                            rerun_wave += 1
                            print(
                                f"[INFO]: Re-optimising suspicious pixel wave {rerun_wave} "
                                f"({len(pixel_constraints)} pixel(s)) using nearby neighbour constraints."
                            )
                            pixel_coords = [(int(item['x']), int(item['y'])) for item in pixel_constraints]
                            pixel_snapshot = _snapshot_result_recorder_pixels(correction_recorder, pixel_coords)
                            for item in pixel_constraints:
                                row = int(item['x'])
                                col = int(item['y'])
                                false_deep_reoptimised_mask[row, col] = True
                                false_deep_attempt_count[row, col] = min(
                                    np.iinfo(false_deep_attempt_count.dtype).max,
                                    false_deep_attempt_count[row, col] + 1,
                                )
                            output_calculation.rerun_selected_pixels(
                                rrs,
                                objective,
                                siop,
                                correction_recorder,
                                pixel_constraints,
                                opt_met,
                                relaxed,
                                free_cpu=args.free_cpu,
                                bathy_tolerance=0.0,
                                optimize_initial_guesses=False,
                                use_five_initial_guesses=False,
                                apply_shallow_adjustment=False,
                                allow_target_sum_over_one=False,
                                normalise_target_fractions=False,
                                fully_relaxed=fully_relaxed,
                                executor=rerun_executor,
                            )
                            accepted_this_wave = 0
                            stable_mask = stable_confident_mask.copy()
                            for item in pixel_constraints:
                                row = int(item['x'])
                                col = int(item['y'])
                                pixel_state = pixel_snapshot.get((row, col), {})
                                if _accept_corrected_pixel(
                                    correction_recorder,
                                    pixel_state,
                                    stable_mask,
                                    row,
                                    col,
                                    false_deep_effective_settings,
                                ):
                                    accepted_confident_mask[row, col] = True
                                    accepted_this_wave += 1
                                else:
                                    _restore_result_recorder_pixel(correction_recorder, pixel_state, row, col)
                            false_deep_corrected_pixel_count += accepted_this_wave
                            print(
                                f"[INFO]: Accepted {accepted_this_wave}/{len(pixel_constraints)} "
                                f"pixel update(s) after local continuity checks."
                            )
                            if accepted_this_wave <= 0:
                                print("[INFO]: False-deep correction wave produced no accepted continuity improvements; stopping reruns.")
                                break
                    finally:
                        if rerun_executor is not None:
                            rerun_executor.shutdown()

                    if correction_plan is None or not np.any(false_deep_reoptimised_mask):
                        print("[INFO]: False-deep bathymetry correction found no suspect pixels.")

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
                    if chunk_manifest:
                        print("[INFO]: Writing corrected outputs from the full corrected raster instead of per-chunk first-pass files.")
                    chunk_manifest = None

                    if false_deep_correction_settings.get('debug_export'):
                        reoptimised_mask = ma.masked_array(
                            false_deep_reoptimised_mask.astype('float32'),
                            mask=np.zeros(false_deep_reoptimised_mask.shape, dtype=bool),
                        )
                        corrected_mask = ma.masked_array(
                            suspect_mask.astype('float32'),
                            mask=np.zeros(suspect_mask.shape, dtype=bool),
                        )
                        correction_debug_layers = [
                            ('_depth_reoptimised_pixels.tif', 'depth_reoptimised_pixels', reoptimised_mask),
                            ('_depth_correction_mask.tif', 'depth_correction_mask', corrected_mask),
                            ('_depth_correction_locked_confident_mask.tif', 'depth_correction_locked_confident_mask', ma.masked_array(locked_confident_mask.astype('float32'))),
                            ('_depth_correction_accepted_confident_mask.tif', 'depth_correction_accepted_confident_mask', ma.masked_array(accepted_confident_mask.astype('float32'))),
                            ('_depth_correction_confident_mask.tif', 'depth_correction_confident_mask', ma.masked_array(correction_plan['confident_mask'].astype('float32'))),
                            ('_depth_correction_reference_depth.tif', 'depth_correction_reference_depth', correction_plan['reference_depth']),
                            ('_depth_correction_reference_tolerance.tif', 'depth_correction_reference_tolerance', correction_plan['reference_tolerance']),
                            ('_depth_correction_reference_chl.tif', 'depth_correction_reference_chl', correction_plan['reference_chl']),
                            ('_depth_correction_reference_cdom.tif', 'depth_correction_reference_cdom', correction_plan['reference_cdom']),
                            ('_depth_correction_reference_nap.tif', 'depth_correction_reference_nap', correction_plan['reference_nap']),
                            ('_depth_correction_reference_kd.tif', 'depth_correction_reference_kd', correction_plan['reference_kd']),
                            ('_depth_correction_pre_slope_percent.tif', 'depth_correction_pre_slope_percent', correction_plan['slope']),
                            ('_depth_correction_barrier_mask.tif', 'depth_correction_barrier_mask', ma.masked_array(correction_plan['barrier_mask'].astype('float32'))),
                            ('_depth_correction_seed_mask.tif', 'depth_correction_seed_mask', ma.masked_array(correction_plan['seed_mask'].astype('float32'))),
                            ('_depth_correction_candidate_mask.tif', 'depth_correction_candidate_mask', ma.masked_array(correction_plan['candidate_mask'].astype('float32'))),
                        ]
                except Exception as correction_exc:
                    print(f"[WARN]: False-deep bathymetry correction skipped: {correction_exc}")

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
            tail_metric_defs = [('error_f', 'error_f'), ('r_sub', 'r_sub')]
            primary_var_defs = base_metric_defs + substrate_defs + tail_metric_defs

            primary_outputs = None
            if model_outputs is not None:
                chunk_like = {
                    'sub1_frac': sub1_frac,
                    'sub2_frac': sub2_frac,
                    'sub3_frac': sub3_frac,
                }
                substrate_norm_map = _compute_chunk_substrate_norms(chunk_like, relaxed, substrate_var_names, fully_relaxed=fully_relaxed)
                metric_arrays = {
                    'chl': chl,
                    'cdom': cdom,
                    'nap': nap,
                    'depth': depth,
                    'kd': kd,
                    'sdi': sdi,
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
                    xml_dict['false_deep_correction_enabled'] = false_deep_correction_settings.get('enabled', False)
                    xml_dict['false_deep_corrected_pixel_count'] = false_deep_corrected_pixel_count
                    if false_deep_effective_settings is not None:
                        xml_dict['false_deep_effective_anchor_min_sdi'] = false_deep_effective_settings['anchor_min_sdi']
                        xml_dict['false_deep_effective_anchor_max_error_f'] = false_deep_effective_settings['anchor_max_error_f']
                        xml_dict['false_deep_effective_search_radius_px'] = false_deep_effective_settings['search_radius_px']
                    log_dir = os.path.dirname(ofile)
                    log_name = f'log_{input_base}.xml' if input_base else 'log_output.xml'
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
                        fully_relaxed=fully_relaxed,
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
                    fully_relaxed=fully_relaxed,
                    grid_metadata=grid_metadata)

            if correction_debug_layers:
                try:
                    _write_false_deep_debug_geotiffs(
                        ofile,
                        width,
                        height,
                        lat_data,
                        lon_data,
                        shape_geo_val,
                        correction_debug_layers,
                        grid_metadata=grid_metadata,
                    )
                except Exception as e:
                    print(f"[ERROR]: Failed to write false-deep correction debug GeoTIFFs '{ofile}': {e}")

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
