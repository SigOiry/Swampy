from pathlib import Path
import sys

import numpy as np
import numpy.ma as ma
import pytest


pytest.importorskip("future")
pytest.importorskip("scipy")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import launch_swampy as swampy


P_BOUNDS = (
    (0.0, 10.0),
    (0.0, 1.0),
    (0.0, 1.0),
    (0.1, 20.0),
    (0.0, 1.0),
    (0.0, 1.0),
    (0.0, 1.0),
)


def test_weighted_median_prefers_heavier_values():
    value = swampy._weighted_median(
        np.array([1.0, 3.0, 10.0], dtype="float32"),
        np.array([1.0, 5.0, 1.0], dtype="float32"),
    )

    assert value == 3.0


def test_false_deep_correction_plan_marks_confident_and_suspicious_pixels():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 10.0,
        "search_radius_px": 2,
        "min_anchor_count": 4,
        "max_patch_size_px": 4,
        "treat_min_depth_as_barrier": False,
        "correction_tolerance_m": 0.5,
        "max_depth_tolerance_m": 2.0,
    })

    depth = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 10.0, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 0.2, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    error_f = ma.array(np.full((3, 3), 0.001, dtype="float32"))
    slope = ma.array(
        [
            [0.0, 0.0, 0.0],
            [0.0, 25.0, 0.0],
            [0.0, 0.0, 0.0],
        ],
        dtype="float32",
    )
    chl = ma.array(np.full((3, 3), 1.0, dtype="float32"))
    cdom = ma.array(np.full((3, 3), 0.2, dtype="float32"))
    nap = ma.array(np.full((3, 3), 0.1, dtype="float32"))
    kd = ma.array(np.full((3, 3), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((3, 3), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((3, 3), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((3, 3), 0.2, dtype="float32"))

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert plan["confident_mask"][0, 0]
    assert plan["suspicious_mask"][1, 1]
    assert plan["seed_mask"][1, 1]
    assert np.isclose(float(plan["reference_depth"][1, 1]), 2.0, atol=1.0e-6)
    assert np.isclose(float(plan["reference_chl"][1, 1]), 1.0, atol=1.0e-6)
    assert np.isclose(float(plan["reference_kd"][1, 1]), 0.4, atol=1.0e-6)
    assert len(plan["rerun_items"]) == 1
    rerun_item = plan["rerun_items"][0]
    assert rerun_item["x"] == 1
    assert rerun_item["y"] == 1
    assert len(rerun_item["initial_guess"]) == 7
    assert len(rerun_item["bounds"]) == 7


def test_depth_and_sdi_drop_without_slope_does_not_seed_suspicious_pixel():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 50.0,
        "search_radius_px": 2,
        "min_anchor_count": 4,
        "max_patch_size_px": 4,
        "treat_min_depth_as_barrier": False,
        "correction_tolerance_m": 0.5,
        "max_depth_tolerance_m": 2.0,
    })

    depth = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 4.5, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 0.2, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    error_f = ma.array(np.full((3, 3), 0.001, dtype="float32"))
    slope = ma.array(np.zeros((3, 3), dtype="float32"))
    chl = ma.array(np.full((3, 3), 1.0, dtype="float32"))
    cdom = ma.array(np.full((3, 3), 0.2, dtype="float32"))
    nap = ma.array(np.full((3, 3), 0.1, dtype="float32"))
    kd = ma.array(np.full((3, 3), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((3, 3), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((3, 3), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((3, 3), 0.2, dtype="float32"))

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert not plan["seed_mask"][1, 1]
    assert not plan["suspicious_mask"][1, 1]


def test_false_deep_correction_plan_grows_flat_interior_from_seed():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 10.0,
        "search_radius_px": 3,
        "min_anchor_count": 4,
        "max_patch_size_px": 16,
        "treat_min_depth_as_barrier": False,
        "correction_tolerance_m": 0.5,
        "max_depth_tolerance_m": 2.0,
    })

    depth = ma.array(
        [
            [2.0, 2.0, 2.0, 2.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 2.0, 2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(np.where(np.asarray(depth) > 4.0, 0.2, 2.0), dtype="float32")
    error_f = ma.array(np.full((5, 5), 0.001, dtype="float32"))
    slope = ma.array(
        [
            [0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 20.0, 20.0, 20.0, 0.0],
            [0.0, 20.0, 2.0, 20.0, 0.0],
            [0.0, 20.0, 20.0, 20.0, 0.0],
            [0.0, 0.0, 0.0, 0.0, 0.0],
        ],
        dtype="float32",
    )
    chl = ma.array(np.full((5, 5), 1.0, dtype="float32"))
    cdom = ma.array(np.full((5, 5), 0.2, dtype="float32"))
    nap = ma.array(np.full((5, 5), 0.1, dtype="float32"))
    kd = ma.array(np.full((5, 5), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((5, 5), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((5, 5), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((5, 5), 0.2, dtype="float32"))

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert plan["seed_mask"][1, 1]
    assert not plan["seed_mask"][2, 2]
    assert plan["candidate_mask"][2, 2]
    assert plan["suspicious_mask"][2, 2]
    assert not np.any(plan["true_deep_protected_mask"])
    assert len(plan["rerun_items"]) == 9
    assert np.isfinite(float(plan["reference_depth"][2, 2]))
    assert 2.0 <= float(plan["reference_depth"][2, 2]) < 8.0


def test_false_deep_growth_stays_near_seed_depth_and_sdi():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.8,
        "suspect_min_slope_percent": 10.0,
        "suspect_growth_depth_fraction": 0.10,
        "suspect_growth_sdi_margin": 0.10,
        "search_radius_px": 3,
        "min_anchor_count": 4,
        "max_patch_size_px": 16,
        "treat_min_depth_as_barrier": False,
        "correction_tolerance_m": 0.5,
        "max_depth_tolerance_m": 2.0,
    })

    depth = ma.array(
        [
            [2.0, 2.0, 2.0, 2.0, 2.0],
            [2.0, 8.0, 8.2, 6.5, 2.0],
            [2.0, 8.0, 8.1, 8.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 2.0, 2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(
        [
            [2.0, 2.0, 2.0, 2.0, 2.0],
            [2.0, 0.2, 0.25, 0.2, 2.0],
            [2.0, 0.2, 0.25, 0.55, 2.0],
            [2.0, 0.2, 0.2, 0.2, 2.0],
            [2.0, 2.0, 2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    error_f = ma.array(np.full((5, 5), 0.001, dtype="float32"))
    slope = ma.array(np.zeros((5, 5), dtype="float32"))
    slope[1, 1] = 20.0
    chl = ma.array(np.full((5, 5), 1.0, dtype="float32"))
    cdom = ma.array(np.full((5, 5), 0.2, dtype="float32"))
    nap = ma.array(np.full((5, 5), 0.1, dtype="float32"))
    kd = ma.array(np.full((5, 5), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((5, 5), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((5, 5), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((5, 5), 0.2, dtype="float32"))

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert plan["seed_mask"][1, 1]
    assert plan["suspicious_mask"][2, 2]
    assert not plan["suspicious_mask"][1, 3]
    assert not plan["suspicious_mask"][2, 3]


def test_false_deep_interpolation_maps_use_confident_pixel_values():
    values = {
        "depth": np.array(
            [
                [2.0, 2.0, 2.0],
                [2.0, 9.0, 2.0],
                [2.0, 2.0, 2.0],
            ],
            dtype="float32",
        ),
        "chl": np.array(
            [
                [1.0, 1.0, 1.0],
                [1.0, 5.0, 1.0],
                [1.0, 1.0, 1.0],
            ],
            dtype="float32",
        ),
    }
    source_mask = np.ones((3, 3), dtype=bool)
    source_mask[1, 1] = False
    target_mask = np.zeros((3, 3), dtype=bool)
    target_mask[1, 1] = True

    interpolated = swampy._interpolate_false_deep_parameter_maps(
        values,
        source_mask,
        target_mask,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert np.isclose(float(interpolated["depth"][1, 1]), 2.0, atol=1.0e-6)
    assert np.isclose(float(interpolated["chl"][1, 1]), 1.0, atol=1.0e-6)


def test_false_deep_correction_protects_open_border_connected_deep_water():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 10.0,
        "min_anchor_count": 4,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(np.full((5, 5), 2.0, dtype="float32"))
    depth[:, 3:] = 10.0
    sdi = ma.array(np.where(np.asarray(depth) > 4.0, 0.2, 2.0), dtype="float32")
    error_f = ma.array(np.full((5, 5), 0.001, dtype="float32"))
    slope = ma.array(np.where(np.asarray(depth) > 4.0, 20.0, 0.0), dtype="float32")
    chl = ma.array(np.full((5, 5), 1.0, dtype="float32"))
    cdom = ma.array(np.full((5, 5), 0.2, dtype="float32"))
    nap = ma.array(np.full((5, 5), 0.1, dtype="float32"))
    kd = ma.array(np.full((5, 5), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((5, 5), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((5, 5), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((5, 5), 0.2, dtype="float32"))

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert np.any(plan["candidate_mask"][:, 3:])
    assert np.any(plan["true_deep_protected_mask"][:, 3:])
    assert not np.any(plan["suspicious_mask"][:, 3:])
    assert plan["correction_block_reason"][0, 4] == swampy.FALSE_DEEP_BLOCK_REASON_BORDER_OPEN
    assert not plan["rerun_items"]


def test_false_deep_correction_keeps_enclosed_vegetated_patch_correctable():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 10.0,
        "min_anchor_count": 4,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(
        [
            [2.0, 2.0, 2.0, 2.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 8.0, 8.0, 8.0, 2.0],
            [2.0, 2.0, 2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(np.where(np.asarray(depth) > 4.0, 0.2, 2.0), dtype="float32")
    error_f = ma.array(np.full((5, 5), 0.001, dtype="float32"))
    slope = ma.array(np.where(np.asarray(depth) > 4.0, 20.0, 0.0), dtype="float32")
    chl = ma.array(np.full((5, 5), 1.0, dtype="float32"))
    cdom = ma.array(np.full((5, 5), 0.2, dtype="float32"))
    nap = ma.array(np.full((5, 5), 0.1, dtype="float32"))
    kd = ma.array(np.full((5, 5), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((5, 5), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((5, 5), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((5, 5), 0.2, dtype="float32"))

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
    )

    assert plan["suspicious_mask"][2, 2]
    assert not plan["true_deep_protected_mask"][2, 2]
    assert len(plan["rerun_items"]) == 9


def test_false_deep_correction_protects_patch_overlapping_deep_water_mask():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 10.0,
        "min_anchor_count": 4,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 10.0, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(np.where(np.asarray(depth) > 4.0, 0.2, 2.0), dtype="float32")
    error_f = ma.array(np.full((3, 3), 0.001, dtype="float32"))
    slope = ma.array(np.where(np.asarray(depth) > 4.0, 20.0, 0.0), dtype="float32")
    chl = ma.array(np.full((3, 3), 1.0, dtype="float32"))
    cdom = ma.array(np.full((3, 3), 0.2, dtype="float32"))
    nap = ma.array(np.full((3, 3), 0.1, dtype="float32"))
    kd = ma.array(np.full((3, 3), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((3, 3), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((3, 3), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((3, 3), 0.2, dtype="float32"))
    true_deep_mask = np.zeros((3, 3), dtype=bool)
    true_deep_mask[1, 1] = True

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
        true_deep_mask=true_deep_mask,
    )

    assert plan["candidate_mask"][1, 1]
    assert plan["true_deep_protected_mask"][1, 1]
    assert not plan["suspicious_mask"][1, 1]
    assert plan["correction_block_reason"][1, 1] == swampy.FALSE_DEEP_BLOCK_REASON_DEEP_WATER_MASK
    assert not plan["rerun_items"]


def test_scene_adaptive_thresholds_relax_sdi_and_error_for_turbid_scene():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.5,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.003,
        "suspect_max_sdi": 1.0,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(np.full((20, 20), 2.0, dtype="float32"))
    sdi = ma.array(np.linspace(0.85, 1.25, 400, dtype="float32").reshape(20, 20))
    error_f = ma.array(np.linspace(0.004, 0.008, 400, dtype="float32").reshape(20, 20))
    slope = ma.array(np.zeros((20, 20), dtype="float32"))

    effective = swampy._derive_scene_adaptive_false_deep_settings(
        depth,
        sdi,
        error_f,
        slope,
        settings,
        depth_min=0.1,
    )

    assert effective["anchor_min_sdi"] < settings["anchor_min_sdi"]
    assert effective["anchor_max_error_f"] > settings["anchor_max_error_f"]
    assert effective["anchor_max_error_f"] <= 0.012


def test_scene_adaptive_search_radius_increases_when_anchor_density_is_sparse():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.5,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.003,
        "suspect_max_sdi": 1.0,
        "search_radius_px": 20,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(np.full((300, 300), 2.0, dtype="float32"))
    sdi_values = np.full((300, 300), 0.2, dtype="float32")
    sdi_values[0, 0] = 2.0
    sdi_values[299, 299] = 2.0
    sdi = ma.array(sdi_values)
    error_f = ma.array(np.full((300, 300), 0.001, dtype="float32"))
    slope = ma.array(np.zeros((300, 300), dtype="float32"))

    effective = swampy._derive_scene_adaptive_false_deep_settings(
        depth,
        sdi,
        error_f,
        slope,
        settings,
        depth_min=0.1,
    )

    assert effective["search_radius_px"] > settings["search_radius_px"]


def test_base_confident_pixels_are_excluded_from_rerun_items():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 10.0, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(
        [
            [2.0, 2.0, 2.0],
            [2.0, 0.2, 2.0],
            [2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    error_f = ma.array(np.full((3, 3), 0.001, dtype="float32"))
    slope = ma.array(np.zeros((3, 3), dtype="float32"))
    chl = ma.array(np.full((3, 3), 1.0, dtype="float32"))
    cdom = ma.array(np.full((3, 3), 0.2, dtype="float32"))
    nap = ma.array(np.full((3, 3), 0.1, dtype="float32"))
    kd = ma.array(np.full((3, 3), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((3, 3), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((3, 3), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((3, 3), 0.2, dtype="float32"))
    base_confident_mask = np.ones((3, 3), dtype=bool)

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
        base_confident_mask=base_confident_mask,
    )

    assert plan["confident_mask"][1, 1]
    assert (1, 1) not in {(item["x"], item["y"]) for item in plan["rerun_items"]}


def test_accepted_confident_pixels_are_excluded_from_later_rerun_items():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "treat_min_depth_as_barrier": False,
    })
    depth = ma.array(
        [
            [2.0, 2.0, 2.0, 2.0, 2.0],
            [2.0, 10.0, 10.0, 10.0, 2.0],
            [2.0, 10.0, 10.0, 10.0, 2.0],
            [2.0, 10.0, 10.0, 10.0, 2.0],
            [2.0, 2.0, 2.0, 2.0, 2.0],
        ],
        dtype="float32",
    )
    sdi = ma.array(np.where(np.asarray(depth) > 4.0, 0.2, 2.0), dtype="float32")
    error_f = ma.array(np.full((5, 5), 0.001, dtype="float32"))
    slope = ma.array(np.where(np.asarray(depth) > 4.0, 20.0, 0.0), dtype="float32")
    chl = ma.array(np.full((5, 5), 1.0, dtype="float32"))
    cdom = ma.array(np.full((5, 5), 0.2, dtype="float32"))
    nap = ma.array(np.full((5, 5), 0.1, dtype="float32"))
    kd = ma.array(np.full((5, 5), 0.4, dtype="float32"))
    sub1 = ma.array(np.full((5, 5), 0.5, dtype="float32"))
    sub2 = ma.array(np.full((5, 5), 0.3, dtype="float32"))
    sub3 = ma.array(np.full((5, 5), 0.2, dtype="float32"))
    base_confident_mask = np.zeros((5, 5), dtype=bool)
    base_confident_mask[0, :] = True
    base_confident_mask[-1, :] = True
    base_confident_mask[:, 0] = True
    base_confident_mask[:, -1] = True
    accepted_confident_mask = np.zeros((5, 5), dtype=bool)
    accepted_confident_mask[1, 1] = True

    plan = swampy._build_false_deep_correction_plan(
        depth,
        sdi,
        error_f,
        slope,
        chl,
        cdom,
        nap,
        kd,
        sub1,
        sub2,
        sub3,
        settings,
        depth_min=0.1,
        p_bounds=P_BOUNDS,
        exposed_mask=None,
        dx_m=1.0,
        dy_m=1.0,
        extra_confident_mask=accepted_confident_mask,
        base_confident_mask=base_confident_mask,
    )

    assert plan["confident_mask"][1, 1]
    assert (1, 1) not in {(item["x"], item["y"]) for item in plan["rerun_items"]}


def test_suspicious_frontier_does_not_fall_back_to_disconnected_interior():
    suspicious = np.zeros((5, 5), dtype=bool)
    suspicious[0, 0] = True
    confident = np.zeros((5, 5), dtype=bool)
    confident[4, 4] = True

    frontier = swampy._select_suspicious_frontier(
        suspicious,
        confident,
        np.zeros((5, 5), dtype=bool),
    )

    assert not np.any(frontier)


def test_suspicious_frontier_grows_one_layer_at_a_time():
    suspicious = np.zeros((5, 5), dtype=bool)
    suspicious[1:4, 1:4] = True
    stable = np.zeros((5, 5), dtype=bool)
    stable[0, :] = True
    stable[-1, :] = True
    stable[:, 0] = True
    stable[:, -1] = True

    first_frontier = swampy._select_suspicious_frontier(
        suspicious,
        stable,
        np.zeros((5, 5), dtype=bool),
    )
    assert first_frontier[1, 1]
    assert first_frontier[1, 2]
    assert not first_frontier[2, 2]

    second_frontier = swampy._select_suspicious_frontier(
        suspicious & ~first_frontier,
        stable | first_frontier,
        np.zeros((5, 5), dtype=bool),
    )
    assert second_frontier[2, 2]
