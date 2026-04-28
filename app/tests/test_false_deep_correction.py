from pathlib import Path
import sys
import xml.etree.ElementTree as ET

import numpy as np
import numpy.ma as ma
import pytest


pytest.importorskip("future")
pytest.importorskip("scipy")

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))

import launch_swampy as swampy


def test_normalise_anomaly_search_settings_uses_defaults():
    settings = swampy._normalise_anomaly_search_settings(None)

    assert settings == swampy.DEFAULT_ANOMALY_SEARCH_SETTINGS
    assert settings is not swampy.DEFAULT_ANOMALY_SEARCH_SETTINGS


def test_normalise_anomaly_search_settings_coerces_bool_values():
    settings = swampy._normalise_anomaly_search_settings({
        "enabled": "true",
        "export_local_moran_raster": 1,
        "export_suspicious_binary_raster": "0",
    })

    assert settings == {
        "enabled": True,
        "export_local_moran_raster": True,
        "export_suspicious_binary_raster": False,
        "export_interpolated_rasters": False,
    }


def test_normalise_anomaly_search_settings_ignores_legacy_false_deep_keys():
    settings = swampy._normalise_anomaly_search_settings({
        "false_deep_correction_enabled": True,
        "false_deep_debug_export": True,
    })

    assert settings == swampy.DEFAULT_ANOMALY_SEARCH_SETTINGS


def test_finalise_anomaly_search_settings_disables_with_input_bathymetry():
    settings = swampy._finalise_anomaly_search_settings(
        {
            "enabled": True,
            "export_local_moran_raster": True,
            "export_suspicious_binary_raster": True,
        },
        use_input_bathy=True,
    )

    assert settings == {
        "enabled": False,
        "export_local_moran_raster": True,
        "export_suspicious_binary_raster": True,
        "export_interpolated_rasters": False,
    }


def test_build_batch_run_settings_csv_only_keeps_varying_columns():
    records = [
        {
            "run_version_index": 1,
            "run_version_label": "Settings 01",
            "run_version_suffix": "_settings01",
            "run_version_output_folder": "out/settings01",
            "output_format": "both",
            "post_processing": False,
            "output_modeled_reflectance": False,
            "allow_split": False,
            "split_chunk_rows": "",
            "nedr_mode": "fixed",
            "crop_selection": None,
            "deep_water_selection": None,
            "siop_popup": {
                "template_source": "template_a.xml",
                "selected_targets": ["Sand"],
            },
            "sensor_popup": {
                "sensor_name": "Sentinel-2",
                "selected_band_count": 5,
            },
            "pmin": [0.01, 0.0005, 0.2, 0.1, 0.0, 0.0, 0.0],
            "pmax": [0.16, 0.01, 1.5, 20.0, 1.0, 1.0, 1.0],
            "rrs_flag": True,
            "reflectance_input": False,
            "relaxed": False,
            "fully_relaxed": False,
            "shallow": False,
            "optimize_initial_guesses": False,
            "use_five_initial_guesses": False,
            "initial_guess_debug": False,
            "use_bathy": False,
            "bathy_path": "",
            "bathy_reference": "depth",
            "bathy_correction_m": 0.0,
            "bathy_tolerance_m": 0.0,
            "anomaly_search_settings": {
                "enabled": False,
                "export_local_moran_raster": False,
            },
        },
        {
            "run_version_index": 2,
            "run_version_label": "Settings 02",
            "run_version_suffix": "_settings02",
            "run_version_output_folder": "out/settings02",
            "output_format": "netcdf",
            "post_processing": False,
            "output_modeled_reflectance": False,
            "allow_split": True,
            "split_chunk_rows": "256",
            "nedr_mode": "fixed",
            "crop_selection": None,
            "deep_water_selection": None,
            "siop_popup": {
                "template_source": "template_a.xml",
                "selected_targets": ["Sand"],
            },
            "sensor_popup": {
                "sensor_name": "Sentinel-2",
                "selected_band_count": 5,
            },
            "pmin": [0.01, 0.0005, 0.2, 0.1, 0.0, 0.0, 0.0],
            "pmax": [0.16, 0.01, 1.5, 30.0, 1.0, 1.0, 1.0],
            "rrs_flag": True,
            "reflectance_input": False,
            "relaxed": False,
            "fully_relaxed": False,
            "shallow": False,
            "optimize_initial_guesses": False,
            "use_five_initial_guesses": False,
            "initial_guess_debug": False,
            "use_bathy": False,
            "bathy_path": "",
            "bathy_reference": "depth",
            "bathy_correction_m": 0.0,
            "bathy_tolerance_m": 0.0,
            "anomaly_search_settings": {
                "enabled": False,
                "export_local_moran_raster": False,
            },
        },
    ]

    fieldnames, rows = swampy._build_batch_run_settings_csv(records)

    assert fieldnames[:4] == [
        "run_version_index",
        "run_version_label",
        "run_version_suffix",
        "run_version_output_folder",
    ]
    assert "output_format" in fieldnames
    assert "allow_split" in fieldnames
    assert "split_chunk_rows" in fieldnames
    assert "pmax_depth" in fieldnames
    assert "post_processing" not in fieldnames
    assert "sensor_sensor_name" not in fieldnames
    assert rows[0]["run_version_label"] == "Settings 01"
    assert rows[1]["output_format"] == "netcdf"
    assert rows[0]["allow_split"] == "no"
    assert rows[1]["allow_split"] == "yes"


def test_resolve_batch_run_root_dir_uses_common_parent_for_version_folders(tmp_path):
    root_dir = tmp_path / "batch_output"
    settings01_dir = root_dir / "settings01"
    settings02_dir = root_dir / "settings02"

    result = swampy._resolve_batch_run_root_dir(
        [str(settings01_dir), str(settings02_dir)],
        fallback_dir=str(settings01_dir),
    )

    assert result == str(root_dir.resolve())


def test_bundled_paper_siop_defaults_match_reference_parameterization():
    template_path = APP_DIR.parent / "Data" / "Templates" / "new_input_sub.xml"
    root = ET.parse(template_path).getroot()

    assert root.findtext("lambda0cdom") == "440.0"
    assert root.findtext("a_cdom_slope") == "0.0157"
    assert root.findtext("a_cdom_lambda0cdom") == "1.0"
    assert root.findtext("lambda0nap") == "440.0"
    assert root.findtext("a_nap_slope") == "0.0106"
    assert root.findtext("a_nap_lambda0nap") == "0.0048"
    assert root.findtext("bb_lambda_ref") == "500"
    assert root.findtext("lambda0x") == "542.0"
    assert root.findtext("bb_ph_slope") == "0.681"
    assert root.findtext("bb_nap_slope") == "0.681"
    assert root.findtext("x_ph_lambda0x") == "0.00038"
    assert root.findtext("x_nap_lambda0x") == "0.0054"


def test_gui_default_water_column_bounds_match_paper_defaults():
    gui_source = (APP_DIR / "gui_swampy.py").read_text(encoding="utf-8")

    assert 'chl_min_var = StringVar(value="0.4")' in gui_source
    assert 'chl_max_var = StringVar(value="1.0")' in gui_source
    assert 'cdom_min_var = StringVar(value="0.04")' in gui_source
    assert 'cdom_max_var = StringVar(value="0.11")' in gui_source
    assert 'nap_min_var = StringVar(value="1.0")' in gui_source
    assert 'nap_max_var = StringVar(value="3.3")' in gui_source


def test_parse_crop_selection_preserves_point_buffer():
    selection = swampy._parse_crop_selection({
        "crop_enabled": True,
        "crop_min_lon": "1.0",
        "crop_max_lon": "2.0",
        "crop_min_lat": "3.0",
        "crop_max_lat": "4.0",
        "crop_mask_path": "points.shp",
        "crop_mask_buffer_m": "75",
    })

    assert selection == {
        "bbox": {
            "min_lon": 1.0,
            "max_lon": 2.0,
            "min_lat": 3.0,
            "max_lat": 4.0,
        },
        "mask_path": "points.shp",
        "mask_buffer_m": 75.0,
    }


def test_anomaly_search_persistence_keys_replace_false_deep_keys():
    launch_source = (APP_DIR / "launch_swampy.py").read_text(encoding="utf-8")
    gui_source = (APP_DIR / "gui_swampy.py").read_text(encoding="utf-8")

    for source in (launch_source, gui_source):
        assert "anomaly_search_enabled" in source
        assert "anomaly_search_export_local_moran_raster" in source
        assert "anomaly_search_export_suspicious_binary_raster" in source
        assert "anomaly_search_export_interpolated_rasters" in source
        assert "false_deep_" not in source


def test_interpolate_suspicious_parameter_maps_uses_non_suspicious_values():
    source_mask = np.ones((3, 3), dtype=bool)
    source_mask[1, 1] = False
    target_mask = np.zeros((3, 3), dtype=bool)
    target_mask[1, 1] = True
    interpolated = swampy._interpolate_suspicious_parameter_maps(
        {
            "depth": ma.array([[2.0, 2.0, 2.0], [2.0, 9.0, 2.0], [2.0, 2.0, 2.0]], dtype="float32"),
            "chl": ma.array([[1.0, 1.0, 1.0], [1.0, 5.0, 1.0], [1.0, 1.0, 1.0]], dtype="float32"),
            "cdom": ma.array([[0.2, 0.2, 0.2], [0.2, 0.8, 0.2], [0.2, 0.2, 0.2]], dtype="float32"),
            "nap": ma.array([[0.1, 0.1, 0.1], [0.1, 0.6, 0.1], [0.1, 0.1, 0.1]], dtype="float32"),
        },
        source_mask,
        target_mask,
    )

    assert np.isclose(float(interpolated["depth"][1, 1]), 2.0, atol=1.0e-6)
    assert np.isclose(float(interpolated["chl"][1, 1]), 1.0, atol=1.0e-6)
    assert np.isclose(float(interpolated["cdom"][1, 1]), 0.2, atol=1.0e-6)
    assert np.isclose(float(interpolated["nap"][1, 1]), 0.1, atol=1.0e-6)


def test_local_moran_detection_flags_enclosed_deep_patch_and_fills_hole():
    depth = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    depth[2:5, 2:5] = 8.0
    depth[3, 3] = 2.5
    sdi = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    sdi[2:5, 2:5] = 0.4
    sdi[3, 3] = 0.7

    result = swampy._detect_local_moran_anomaly_pixels(depth, sdi, depth_min=0.1)

    assert np.any(result["seed_mask"])
    assert result["component_count"] == 1
    assert result["suspicious_pixel_count"] == 9
    assert result["suspicious_mask"][3, 3]
    assert np.all(result["suspicious_mask"][2:5, 2:5])
    assert np.isfinite(float(result["depth_jump"][2, 2]))
    assert np.isfinite(float(result["sdi_drop"][2, 2]))


def test_local_moran_detection_rejects_border_connected_open_deep_water():
    depth = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    depth[:, :3] = 8.0
    sdi = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    sdi[:, :3] = 0.4

    result = swampy._detect_local_moran_anomaly_pixels(depth, sdi, depth_min=0.1)

    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"])


def test_local_moran_detection_requires_low_sdi_as_well_as_high_depth():
    depth = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    depth[2:5, 2:5] = 8.0
    sdi = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    sdi[2:5, 2:5] = 2.1

    result = swampy._detect_local_moran_anomaly_pixels(depth, sdi, depth_min=0.1)

    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"])


def test_local_moran_detection_excludes_pixels_with_sdi_above_20():
    depth = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    depth[2:5, 2:5] = 8.0
    sdi = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    sdi[2:5, 2:5] = 25.0

    result = swampy._detect_local_moran_anomaly_pixels(depth, sdi, depth_min=0.1)

    assert not np.any(result["valid_mask"][2:5, 2:5])
    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"])


def test_local_moran_detection_respects_protected_mask():
    depth = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    depth[2:5, 2:5] = 8.0
    sdi = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    sdi[2:5, 2:5] = 0.4
    protected_mask = np.zeros((7, 7), dtype=bool)
    protected_mask[2:5, 2:5] = True

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        sdi,
        depth_min=0.1,
        protected_mask=protected_mask,
    )

    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"])
