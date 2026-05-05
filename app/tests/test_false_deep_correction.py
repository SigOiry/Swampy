import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET
import types

import numpy as np
import numpy.ma as ma
import pytest


pytest.importorskip("future")
pytest.importorskip("scipy")

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))

import launch_swampy as swampy
import leaflet_crop_window


def test_normalise_anomaly_search_settings_uses_defaults():
    settings = swampy._normalise_anomaly_search_settings(None)

    assert settings == swampy.DEFAULT_ANOMALY_SEARCH_SETTINGS
    assert settings is not swampy.DEFAULT_ANOMALY_SEARCH_SETTINGS


def test_normalise_anomaly_search_settings_coerces_bool_values():
    settings = swampy._normalise_anomaly_search_settings({
        "enabled": "true",
        "export_local_moran_raster": 1,
        "export_suspicious_binary_raster": "0",
        "seed_slope_threshold_percent": "12.5",
    })

    assert settings == {
        "enabled": True,
        "export_local_moran_raster": True,
        "export_suspicious_binary_raster": False,
        "export_interpolated_rasters": False,
        "seed_slope_threshold_percent": 12.5,
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
        "seed_slope_threshold_percent": 10.0,
    }


def test_finalise_anomaly_search_settings_restores_default_threshold_when_invalid():
    settings = swampy._finalise_anomaly_search_settings({
        "enabled": True,
        "seed_slope_threshold_percent": "0",
    })

    assert settings["enabled"] is True
    assert settings["seed_slope_threshold_percent"] == pytest.approx(10.0)


def test_normalise_anomaly_search_settings_converts_legacy_degree_threshold_to_percent():
    settings = swampy._normalise_anomaly_search_settings({
        "seed_slope_threshold_degrees": "45",
    })

    assert settings["seed_slope_threshold_percent"] == pytest.approx(100.0)


def test_compute_chunk_substrate_norms_exports_raw_relaxed_substrates_by_default():
    chunk_arrays = {
        "sub1_frac": ma.array([[0.8]], dtype="float32"),
        "sub2_frac": ma.array([[0.7]], dtype="float32"),
        "sub3_frac": ma.array([[0.0]], dtype="float32"),
        "total_abun": ma.array([[1.5]], dtype="float32"),
    }

    outputs = swampy._compute_chunk_substrate_norms(
        chunk_arrays,
        relaxed=True,
        substrate_var_names=("sand", "mud", "unused"),
    )

    assert outputs["sand"][0, 0] == pytest.approx(0.8)
    assert outputs["mud"][0, 0] == pytest.approx(0.7)
    assert outputs["unused"][0, 0] == pytest.approx(0.0)
    assert outputs["sum_of_substrats"][0, 0] == pytest.approx(1.5)


def test_compute_chunk_substrate_norms_standardizes_relaxed_substrates_only_when_requested():
    chunk_arrays = {
        "sub1_frac": ma.array([[0.8]], dtype="float32"),
        "sub2_frac": ma.array([[0.7]], dtype="float32"),
        "sub3_frac": ma.array([[0.0]], dtype="float32"),
        "total_abun": ma.array([[1.5]], dtype="float32"),
    }

    outputs = swampy._compute_chunk_substrate_norms(
        chunk_arrays,
        relaxed=True,
        substrate_var_names=("sand", "mud", "unused"),
        standardize_relaxed_substrate_outputs=True,
    )

    assert outputs["sand"][0, 0] == pytest.approx(0.8 / 1.5)
    assert outputs["mud"][0, 0] == pytest.approx(0.7 / 1.5)
    assert outputs["unused"][0, 0] == pytest.approx(0.0)
    assert outputs["sum_of_substrats"][0, 0] == pytest.approx(1.5)


def test_compute_chunk_substrate_norms_keeps_sum_of_substrats_fixed_to_one_in_strict_mode():
    mask = np.array([[False, True]])
    chunk_arrays = {
        "sub1_frac": ma.masked_array([[0.25, 0.0]], mask=mask, dtype="float32"),
        "sub2_frac": ma.masked_array([[0.75, 0.0]], mask=mask, dtype="float32"),
        "sub3_frac": ma.masked_array([[0.0, 0.0]], mask=mask, dtype="float32"),
        "total_abun": ma.masked_array([[1.0, 0.0]], mask=mask, dtype="float32"),
    }

    outputs = swampy._compute_chunk_substrate_norms(
        chunk_arrays,
        relaxed=False,
        substrate_var_names=("sand", "mud", "unused"),
    )

    assert outputs["sum_of_substrats"][0, 0] == pytest.approx(1.0)
    assert outputs["sum_of_substrats"].mask[0, 1]


def test_build_primary_outputs_from_chunk_includes_sum_of_substrats_band():
    chunk_arrays = {
        "chl": ma.array([[0.5]], dtype="float32"),
        "sub1_frac": ma.array([[0.8]], dtype="float32"),
        "sub2_frac": ma.array([[0.7]], dtype="float32"),
        "sub3_frac": ma.array([[0.0]], dtype="float32"),
        "total_abun": ma.array([[1.5]], dtype="float32"),
    }

    outputs = swampy._build_primary_outputs_from_chunk(
        chunk_arrays,
        primary_var_defs=[("sum_of_substrats", "sum_of substrats")],
        substrate_var_names=("sand", "mud", "unused"),
        relaxed=True,
    )

    assert len(outputs) == 1
    assert outputs[0][0] == "sum_of_substrats"
    assert outputs[0][2] == "sum_of substrats"
    assert outputs[0][1][0, 0] == pytest.approx(1.5)


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


def test_bundled_default_phyto_absorption_matches_ciotti_spectrum():
    template_path = APP_DIR.parent / "Data" / "Templates" / "new_input_sub.xml"
    root = ET.parse(template_path).getroot()

    a_ph_star = root.find("a_ph_star")
    assert a_ph_star is not None

    wavelength_items = a_ph_star.findall("./item[1]/item")
    value_items = a_ph_star.findall("./item[2]/item")
    wavelengths = [float(item.text) for item in wavelength_items]
    values = [float(item.text) for item in value_items]

    assert len(wavelengths) == 301
    assert len(values) == 301
    assert wavelengths[0] == 400.0
    assert wavelengths[-1] == 700.0
    assert values[0] == pytest.approx(0.024396194)
    assert values[50] == pytest.approx(0.032789242)
    assert values[150] == pytest.approx(0.006364135)
    assert values[-1] == pytest.approx(0.001328027)


def test_gui_default_water_column_bounds_match_paper_defaults():
    gui_source = (APP_DIR / "gui_swampy.py").read_text(encoding="utf-8")

    assert 'chl_min_var = StringVar(value="0.4")' in gui_source
    assert 'chl_max_var = StringVar(value="1.0")' in gui_source
    assert 'cdom_min_var = StringVar(value="0.04")' in gui_source
    assert 'cdom_max_var = StringVar(value="0.11")' in gui_source
    assert 'nap_min_var = StringVar(value="1.0")' in gui_source
    assert 'nap_max_var = StringVar(value="3.3")' in gui_source


def test_deep_water_popup_html_contains_subsampling_checkbox_in_toolbar():
    html = leaflet_crop_window._build_html({
        "mode": "polygons",
        "title": "Deep-water polygons",
        "subtitle": "Preview",
        "lat_min": 0.0,
        "lat_max": 1.0,
        "lon_min": 0.0,
        "lon_max": 1.0,
        "image_data_url": "data:,",
        "allow_polygon": True,
        "selection": {
            "polygons": [],
            "subsample_pixels": True,
        },
        "option_checkboxes": [
            {
                "id": "subsample_pixels",
                "label": "Subsample selected pixels when many polygons overlap large deep-water areas",
                "hint": "Keeps deep-water prior estimation faster on large selections.",
                "value": True,
                "summary_when_true": "subsampling enabled",
                "summary_when_false": "all selected pixels retained",
            }
        ],
    })

    assert '<div id="toolbar-options"></div>' in html
    assert 'Subsample selected pixels when many polygons overlap large deep-water areas' in html
    assert "document.getElementById('toolbar-options')" in html


def test_deep_water_mode_uses_relaxed_iop_bounds():
    assert swampy._DEEP_WATER_IOP_RELAXED_BOUNDS == (
        (0.0, 10.0),
        (0.0, 1.0),
        (0.0, 8.0),
    )


def test_apply_deep_water_priors_uses_relaxed_bounds_instead_of_scene_bounds():
    siop = {
        "p_min": swampy.sb.FreeParameters(0.4, 0.04, 1.0, 0.1, 0.0, 0.0, 0.0),
        "p_max": swampy.sb.FreeParameters(1.0, 0.11, 3.3, 30.0, 1.0, 1.0, 1.0),
    }
    siop["p_bounds"] = tuple(zip(siop["p_min"], siop["p_max"]))

    stats = swampy._apply_deep_water_priors(
        siop,
        [
            {"chl": 5.5, "cdom": 0.60, "nap": 6.5},
            {"chl": 5.7, "cdom": 0.70, "nap": 6.7},
        ],
        use_sd_bounds=False,
    )

    assert stats["applied_pmin"] == pytest.approx([5.6, 0.65, 6.6])
    assert stats["applied_pmax"] == pytest.approx([5.6, 0.65, 6.6])
    assert list(siop["p_min"][:3]) == pytest.approx([5.6, 0.65, 6.6])
    assert list(siop["p_max"][:3]) == pytest.approx([5.6, 0.65, 6.6])


def test_apply_deep_water_priors_clips_sd_bounds_to_relaxed_limits():
    siop = {
        "p_min": swampy.sb.FreeParameters(0.4, 0.04, 1.0, 0.1, 0.0, 0.0, 0.0),
        "p_max": swampy.sb.FreeParameters(1.0, 0.11, 3.3, 30.0, 1.0, 1.0, 1.0),
    }
    siop["p_bounds"] = tuple(zip(siop["p_min"], siop["p_max"]))

    stats = swampy._apply_deep_water_priors(
        siop,
        [
            {"chl": 9.0, "cdom": 0.80, "nap": 7.5},
            {"chl": 10.0, "cdom": 1.00, "nap": 8.0},
        ],
        use_sd_bounds=True,
    )

    assert stats["applied_pmin"] == pytest.approx([9.0, 0.8, 7.5])
    assert stats["applied_pmax"] == pytest.approx([10.0, 1.0, 8.0])
    assert list(siop["p_min"][:3]) == pytest.approx([9.0, 0.8, 7.5])
    assert list(siop["p_max"][:3]) == pytest.approx([10.0, 1.0, 8.0])


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


def test_parse_shallow_substrate_prior_selection_preserves_target_and_polygons():
    polygon = {
        "type": "Polygon",
        "coordinates": [[[1.0, 2.0], [2.0, 2.0], [2.0, 3.0], [1.0, 2.0]]],
    }

    selection = swampy._parse_shallow_substrate_prior_selection({
        "shallow_substrate_prior_enabled": True,
        "shallow_substrate_prior_target_name": "Sand patch",
        "shallow_substrate_prior_use_sd_bounds": True,
        "shallow_substrate_prior_polygons_json": json.dumps([polygon]),
        "shallow_substrate_prior_source_image": "scene.nc",
    })

    assert selection == {
        "target_name": "Sand patch",
        "polygons": [polygon],
        "use_sd_bounds": True,
        "source_image": "scene.nc",
    }


def test_parse_deep_water_selection_preserves_subsample_choice():
    polygon = {
        "type": "Polygon",
        "coordinates": [[[1.0, 2.0], [2.0, 2.0], [2.0, 3.0], [1.0, 2.0]]],
    }

    selection = swampy._parse_deep_water_selection({
        "deep_water_enabled": True,
        "deep_water_use_sd_bounds": True,
        "deep_water_subsample_pixels": False,
        "deep_water_polygons_json": json.dumps([polygon]),
        "deep_water_source_image": "scene.nc",
    })

    assert selection == {
        "polygons": [polygon],
        "use_sd_bounds": True,
        "subsample_pixels": False,
        "source_image": "scene.nc",
    }


def test_parse_deep_water_selection_defaults_subsample_to_true_for_legacy_logs():
    polygon = {
        "type": "Polygon",
        "coordinates": [[[1.0, 2.0], [2.0, 2.0], [2.0, 3.0], [1.0, 2.0]]],
    }

    selection = swampy._parse_deep_water_selection({
        "deep_water_enabled": True,
        "deep_water_polygons_json": json.dumps([polygon]),
    })

    assert selection["subsample_pixels"] is True


def test_prepare_scene_prior_runtime_state_clears_stale_prior_values_per_image():
    xml_dict = {
        "deep_water_enabled": True,
        "deep_water_source_image": "scene_01.nc",
        "deep_water_chl_mean": 9.5,
        "deep_water_applied_pmin": [9.0, 0.8, 7.5],
        "deep_water_prior_scene_image": "old_scene.nc",
        "shallow_substrate_prior_enabled": True,
        "shallow_substrate_prior_source_image": "scene_01.nc",
        "shallow_substrate_prior_chl_mean": 1.5,
        "shallow_substrate_prior_applied_pmax": [2.0, 0.2, 0.4],
        "shallow_substrate_prior_scene_image": "old_scene.nc",
    }

    result = swampy._prepare_scene_prior_runtime_state(
        xml_dict,
        current_image="scene_02.nc",
        deep_water_selection={"polygons": [{"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}]},
        shallow_substrate_prior_selection={"polygons": [{"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}]},
    )

    assert result is xml_dict
    assert result["deep_water_enabled"] is True
    assert result["shallow_substrate_prior_enabled"] is True
    assert "deep_water_chl_mean" not in result
    assert "deep_water_applied_pmin" not in result
    assert "shallow_substrate_prior_chl_mean" not in result
    assert "shallow_substrate_prior_applied_pmax" not in result
    assert result["deep_water_source_image"] == "scene_02.nc"
    assert result["shallow_substrate_prior_source_image"] == "scene_02.nc"
    assert result["deep_water_prior_scene_image"] == "scene_02.nc"
    assert result["shallow_substrate_prior_scene_image"] == "scene_02.nc"


def test_write_deep_water_iop_raster_writes_successful_estimates(tmp_path):
    rasterio = pytest.importorskip("rasterio")
    output_path = tmp_path / "deep_water_iops.tif"
    lat = np.array([[10.0, 10.0], [9.0, 9.0]], dtype="float32")
    lon = np.array([[1.0, 2.0], [1.0, 2.0]], dtype="float32")
    pixel_rows = [
        {
            "row": 0,
            "col": 1,
            "chl": 1.25,
            "cdom": 0.12,
            "nap": 0.34,
            "success": 1,
        },
        {
            "row": 1,
            "col": 0,
            "chl": 9.0,
            "cdom": 9.0,
            "nap": 9.0,
            "success": 0,
        },
    ]

    info = swampy._write_deep_water_iop_raster(str(output_path), pixel_rows, 2, 2, lat, lon, 2)

    assert info == {
        "path": str(output_path),
        "written_pixel_count": 1,
    }
    with rasterio.open(output_path) as dataset:
        assert dataset.count == 3
        assert dataset.descriptions == ("CHL", "CDOM", "NAP")
        assert dataset.nodata == pytest.approx(float(swampy.OUTPUT_FILL_VALUE))
        chl = dataset.read(1)
        cdom = dataset.read(2)
        nap = dataset.read(3)

    assert chl[0, 1] == pytest.approx(1.25)
    assert cdom[0, 1] == pytest.approx(0.12)
    assert nap[0, 1] == pytest.approx(0.34)
    assert chl[1, 0] == pytest.approx(float(swampy.OUTPUT_FILL_VALUE))


def test_resolve_execution_version_settings_prefers_shallow_priors_over_deep_water():
    polygon = {
        "type": "Polygon",
        "coordinates": [[[1.0, 2.0], [2.0, 2.0], [2.0, 3.0], [1.0, 2.0]]],
    }
    resolved = swampy._resolve_execution_version_settings(
        {
            "label": "Settings 01",
            "xml_dict": {
                "deep_water_enabled": True,
                "deep_water_polygons_json": json.dumps([polygon]),
                "shallow_substrate_prior_enabled": True,
                "shallow_substrate_prior_target_name": "Sand",
                "shallow_substrate_prior_polygons_json": json.dumps([polygon]),
            },
        },
        default_siop_xml_path="template.xml",
        default_file_sensor="sensor.xml",
        default_pmin=np.array([0.01, 0.02, 0.03, 0.1, 0.0, 0.0, 0.0]),
        default_pmax=np.array([1.0, 1.0, 1.0, 30.0, 1.0, 1.0, 1.0]),
        default_above_rrs_flag=True,
        default_reflectance_input_flag=False,
        default_relaxed=False,
        default_shallow_flag=False,
        default_optimize_initial_guesses=False,
        default_use_five_initial_guesses=False,
        default_initial_guess_debug=False,
        default_standardize_relaxed_substrate_outputs=False,
        default_output_modeled_reflectance=False,
        default_anomaly_search_settings=swampy.DEFAULT_ANOMALY_SEARCH_SETTINGS,
        default_xml_dict={},
        default_output_format="netcdf",
        default_bathy_path="",
        default_post_processing=False,
        default_allow_split=False,
        default_split_chunk_rows=None,
        default_bathy_reference="depth",
        default_bathy_correction_m=0.0,
        default_bathy_tolerance_m=0.0,
        default_nedr_mode="fixed",
    )

    assert resolved["deep_water_selection"] is None
    assert resolved["shallow_substrate_prior_selection"]["target_name"] == "Sand"
    assert resolved["warnings"]
    assert "Shallow-water substrate priors take precedence" in resolved["warnings"][0]


def test_anomaly_search_persistence_keys_replace_false_deep_keys():
    launch_source = (APP_DIR / "launch_swampy.py").read_text(encoding="utf-8")
    gui_source = (APP_DIR / "gui_swampy.py").read_text(encoding="utf-8")

    for source in (launch_source, gui_source):
        assert "anomaly_search_enabled" in source
        assert "anomaly_search_export_local_moran_raster" in source
        assert "anomaly_search_export_suspicious_binary_raster" in source
        assert "anomaly_search_export_interpolated_rasters" in source
        assert "anomaly_search_seed_slope_threshold_percent" in source
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


def test_local_moran_detection_flags_enclosed_deep_plateau():
    depth = ma.array(np.full((9, 9), 2.0, dtype="float32"))
    depth[3:6, 3:6] = 8.0

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert np.any(result["seed_mask"])
    assert np.isfinite(float(result["slope_percent"][3, 3]))
    assert result["component_count"] == 1
    assert result["suspicious_pixel_count"] >= 9
    assert np.all(result["suspicious_mask"][3:6, 3:6])


def test_local_moran_detection_rejects_border_touching_plateau():
    depth = ma.array(np.full((9, 9), 2.0, dtype="float32"))
    depth[:, :3] = 8.0

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert np.any(result["seed_mask"][:, :4])
    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"][:, :3])


def test_local_moran_detection_marks_steep_pixels_directly_as_suspicious():
    depth = ma.array(np.full((9, 9), 2.0, dtype="float32"))
    depth[3:6, 3:6] = 8.0
    depth[4, 4] = 12.0

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert result["component_count"] == 1
    assert result["suspicious_mask"][4, 4]


def test_local_moran_detection_marks_enclosed_low_slope_patch_when_deeper_than_outside():
    depth = ma.array(np.full((9, 9), 2.0, dtype="float32"))
    depth[3:6, 3:6] = 8.0
    depth[4, 4] = 7.5

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert result["component_count"] == 1
    assert result["suspicious_mask"][4, 4]


def test_local_moran_detection_ignores_low_slope_patch_without_closed_steep_belt():
    depth = ma.array(np.full((11, 11), 2.0, dtype="float32"))
    depth[3:8, 3:8] = 8.0
    depth[4:7, 4:7] = 7.5
    depth[5, 7] = 6.5
    depth[5, 8] = 5.0
    depth[5, 9] = 3.5

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert result["component_count"] == 0
    assert not result["suspicious_mask"][5, 5]
    assert not result["suspicious_mask"][5, 7]


def test_local_moran_detection_requires_steep_slope_seed():
    depth = ma.array(np.tile(np.linspace(2.0, 3.0, 9, dtype="float32"), (9, 1)))

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert not np.any(result["seed_mask"])
    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"])


def test_local_moran_detection_keeps_depth_equal_to_min_bound_in_analysis():
    depth = ma.array(np.full((7, 7), 0.1, dtype="float32"))
    depth[2:5, 2:5] = 1.0

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=1.0,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert np.any(result["seed_mask"])
    assert np.all(result["valid_mask"][2:5, 2:5])
    assert np.any(result["suspicious_mask"][2:5, 2:5])


def test_local_moran_detection_uses_projected_grid_spacing_when_dx_dy_missing():
    depth = ma.array(np.full((9, 9), 2.0, dtype="float32"))
    depth[3:6, 3:6] = 8.0

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        grid_metadata={
            "transform": swampy.Affine.translation(500000.0, 5400000.0) * swampy.Affine.scale(10.0, -10.0),
            "crs": types.SimpleNamespace(is_projected=True),
        },
    )

    assert result["component_count"] == 1
    assert result["suspicious_pixel_count"] >= 9
    assert np.any(result["seed_mask"])


def test_local_moran_detection_respects_protected_mask():
    depth = ma.array(np.full((7, 7), 2.0, dtype="float32"))
    depth[2:5, 2:5] = 8.0
    protected_mask = np.zeros((7, 7), dtype=bool)
    protected_mask[2:5, 2:5] = True

    result = swampy._detect_local_moran_anomaly_pixels(
        depth,
        depth_min=0.1,
        protected_mask=protected_mask,
        dx_m=10.0,
        dy_m=10.0,
    )

    assert result["component_count"] == 0
    assert result["suspicious_pixel_count"] == 0
    assert not np.any(result["suspicious_mask"])
    assert np.array_equal(result["protected_mask"], protected_mask)


def test_clear_small_enclosed_true_components_removes_internal_island():
    mask = np.zeros((7, 7), dtype=bool)
    mask[2:4, 2:5] = True

    filtered = swampy._clear_small_enclosed_true_components(mask, max_pixels=15)

    assert not np.any(filtered)


def test_clear_small_enclosed_true_components_keeps_border_touching_island():
    mask = np.zeros((7, 7), dtype=bool)
    mask[1:4, :2] = True

    filtered = swampy._clear_small_enclosed_true_components(mask, max_pixels=15)

    assert np.array_equal(filtered, mask)


def test_fill_small_enclosed_false_components_fills_internal_hole():
    mask = np.ones((7, 7), dtype=bool)
    mask[2:4, 2:5] = False

    filled = swampy._fill_small_enclosed_false_components(mask, max_pixels=15)

    assert np.all(filled)


def test_collapse_enclosed_binary_regions_removes_large_internal_island():
    mask = np.zeros((15, 15), dtype=bool)
    mask[4:11, 4:11] = True

    collapsed = swampy._collapse_enclosed_binary_regions(mask)

    assert not np.any(collapsed)


def test_collapse_enclosed_binary_regions_fills_large_internal_hole():
    mask = np.ones((15, 15), dtype=bool)
    mask[4:11, 4:11] = False

    collapsed = swampy._collapse_enclosed_binary_regions(mask)

    assert np.all(collapsed)


def test_build_anomaly_search_deep_protection_mask_is_empty_for_uniform_depth():
    depth = ma.array(np.full((21, 21), 5.0, dtype="float32"))

    protected = swampy._build_anomaly_search_deep_protection_mask(depth, depth_min=0.1)

    assert protected.shape == depth.shape
    assert protected.dtype == bool
    assert not np.any(protected)
