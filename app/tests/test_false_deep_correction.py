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
        "suspect_min_depth_jump_m": 2.0,
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
    assert np.isclose(float(plan["reference_depth"][1, 1]), 2.1, atol=1.0e-6)
    assert np.isclose(float(plan["reference_chl"][1, 1]), 1.0, atol=1.0e-6)
    assert np.isclose(float(plan["reference_kd"][1, 1]), 0.4, atol=1.0e-6)
    assert len(plan["rerun_items"]) == 1
    rerun_item = plan["rerun_items"][0]
    assert rerun_item["x"] == 1
    assert rerun_item["y"] == 1
    assert len(rerun_item["initial_guess"]) == 7
    assert len(rerun_item["bounds"]) == 7


def test_false_deep_correction_plan_grows_flat_interior_from_seed():
    settings = swampy._normalise_false_deep_correction_settings({
        "enabled": True,
        "anchor_min_sdi": 1.0,
        "anchor_max_depth_m": 4.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.01,
        "suspect_max_sdi": 0.5,
        "suspect_min_slope_percent": 10.0,
        "suspect_min_depth_jump_m": 2.0,
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
    assert len(plan["rerun_items"]) == 9
    assert np.isfinite(float(plan["reference_depth"][2, 2]))
    assert 2.0 < float(plan["reference_depth"][2, 2]) < 8.0
