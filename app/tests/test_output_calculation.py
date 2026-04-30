from pathlib import Path
import sys

import numpy as np


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import output_calculation
import sambuca as sb


def test_minimize_pixel_skips_when_input_bathymetry_is_missing():
    result = output_calculation.minimize_pixel(
        (0, 0, np.array([0.01, 0.02], dtype="float32"), np.nan, False, True)
    )

    assert result == (0, 0, False, 0, None, None, None)


def test_apply_shallow_sdi_adjustment_marks_pixel():
    class DummyRecorder:
        def __init__(self):
            self.sdi = np.zeros((1, 1), dtype="float32")
            self.depth_calls = []

        def __call__(self, x, y, observed_rrs, parameters=None, nit=None, success=None):
            self.depth_calls.append(parameters.depth)
            self.sdi[x, y] = 0.4 if parameters.depth >= 10.0 else 1.1

    recorder = DummyRecorder()
    obs_rrs = np.array([0.01, 0.02], dtype="float32")
    result_x = [0.02, 0.01, 0.5, 20.0, 1 / 3, 1 / 3, 1 / 3]

    recorder(0, 0, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=5, success=True)
    adjusted = output_calculation._apply_shallow_sdi_adjustment(
        recorder,
        0,
        0,
        obs_rrs,
        result_x,
        nit=5,
        success=True,
    )

    assert adjusted is True
    assert recorder.depth_calls == [20.0, 19.0, 20.0]


def test_initial_guess_values_from_bounds_default_to_three_points():
    values = output_calculation._initial_guess_values_from_bounds(2.0, 10.0)

    assert values == (4.0, 6.0, 8.0)


def test_initial_guess_values_from_bounds_include_min_midpoints_and_max_when_requested():
    values = output_calculation._initial_guess_values_from_bounds(2.0, 10.0, use_five_points=True)

    assert values == (2.0, 4.0, 6.0, 8.0, 10.0)


def test_select_preoptimized_initial_guess_picks_lowest_error_candidate():
    class DummyObjective:
        def __init__(self):
            self._nedr = np.ones(2, dtype="float32")
            self._error_func_name = "f"
            self.observed_rrs = None

        def _run_forward_model(self, parameters):
            class Result:
                pass

            result = Result()
            result.rrs = np.array([parameters[0], parameters[1]], dtype="float32")
            return result

        def _filter_spectrum(self, spectrum):
            return np.asarray(spectrum, dtype="float32")

    original_objective = output_calculation._WORKER_OBJECTIVE
    try:
        output_calculation._WORKER_OBJECTIVE = DummyObjective()
        p0 = np.array([0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0], dtype="float32")
        bounds = (
            (0.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
            (0.0, 0.0),
            (0.0, 0.0),
            (0.0, 0.0),
            (0.0, 0.0),
        )

        selected = output_calculation._select_preoptimized_initial_guess(
            np.array([0.75, 0.25], dtype="float32"),
            p0,
            bounds,
            use_five_points=False,
        )
    finally:
        output_calculation._WORKER_OBJECTIVE = original_objective

    assert np.allclose(selected[:2], np.array([0.75, 0.25], dtype="float32"))


def test_select_preoptimized_initial_guess_keeps_cover_sum_below_one():
    class DummyObjective:
        def __init__(self):
            self._nedr = np.ones(3, dtype="float32")
            self._error_func_name = "f"
            self.observed_rrs = None

        def _run_forward_model(self, parameters):
            class Result:
                pass

            result = Result()
            result.rrs = np.array(parameters[4:7], dtype="float32")
            return result

        def _filter_spectrum(self, spectrum):
            return np.asarray(spectrum, dtype="float32")

    original_objective = output_calculation._WORKER_OBJECTIVE
    try:
        output_calculation._WORKER_OBJECTIVE = DummyObjective()
        p0 = np.array([0.0, 0.0, 0.0, 1.0, 0.5, 0.5, 0.5], dtype="float32")
        bounds = (
            (0.0, 0.0),
            (0.0, 0.0),
            (0.0, 0.0),
            (1.0, 1.0),
            (0.0, 1.0),
            (0.0, 1.0),
            (0.0, 1.0),
        )

        selected = output_calculation._select_preoptimized_initial_guess(
            np.array([0.75, 0.75, 0.75], dtype="float32"),
            p0,
            bounds,
            use_five_points=True,
        )
    finally:
        output_calculation._WORKER_OBJECTIVE = original_objective

    assert np.sum(selected[4:7]) <= 1.0 + 1.0e-12


def test_two_target_cover_x_bounds_respects_both_active_substrate_bounds():
    bounds = (
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.1, 10.0),
        (0.2, 0.8),
        (0.1, 0.9),
        (0.0, 0.0),
    )

    assert output_calculation._two_target_cover_x_bounds(bounds) == (0.2, 0.8)


def test_minimize_pixel_uses_unconstrained_full_space_for_relaxed_case():
    class DummyObjective:
        def __init__(self):
            self._nedr = np.ones(2, dtype="float32")
            self._error_func_name = "f"
            self.observed_rrs = None

        def __call__(self, parameters):
            parameters = np.asarray(parameters, dtype=float)
            target = 0.7
            error = float((parameters[4] - target) ** 2)
            jacobian = np.zeros(7, dtype=float)
            jacobian[4] = 2.0 * (parameters[4] - target)
            return error, jacobian

    p0 = np.array([0.2, 0.1, 0.05, 2.0, 0.4, 0.6, 0.0], dtype="float32")
    bounds = (
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.5, 5.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0),
    )
    cons = ()  # relaxed mode: no constraints

    original_minimize = output_calculation.sb.minimize
    try:
        output_calculation.worker_init(
            DummyObjective(),
            p0,
            "SLSQP",
            bounds,
            cons,
            False,
            None,
            False,
            False,
            True,  # relaxed=True
        )

        call_info = {}

        def fake_minimize(objective, p0, method, bounds, constraints, options, obs_rrs):
            call_info["parameter_count"] = len(p0)
            call_info["bounds_count"] = len(bounds)
            call_info["constraints"] = constraints
            result_x = list(p0[:4]) + [0.7, 0.3, 0.0]
            return sb.minimize_result(np.array(result_x, dtype=float), 4, True)

        output_calculation.sb.minimize = fake_minimize

        result = output_calculation.minimize_pixel(
            (0, 0, np.array([0.01, 0.02], dtype="float32"), None, False, False)
        )
    finally:
        output_calculation.sb.minimize = original_minimize

    assert call_info["parameter_count"] == 7
    assert call_info["bounds_count"] == 7
    assert call_info["constraints"] == ()
    assert result[2] is True
    assert np.allclose(result[4][4:7], np.array([0.7, 0.3, 0.0], dtype=float))


def test_minimize_pixel_uses_single_fraction_mode_for_strict_two_target_case():
    class DummyObjective:
        def __init__(self):
            self._nedr = np.ones(2, dtype="float32")
            self._error_func_name = "f"
            self.observed_rrs = None

        def __call__(self, parameters):
            parameters = np.asarray(parameters, dtype=float)
            target = 0.2
            error = float((parameters[4] - target) ** 2)
            jacobian = np.zeros(7, dtype=float)
            jacobian[4] = 2.0 * (parameters[4] - target)
            return error, jacobian

    p0 = np.array([0.2, 0.1, 0.05, 2.0, 1 / 3, 1 / 3, 1 / 3], dtype="float32")
    bounds = (
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.5, 5.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0),
    )
    cons = (
        {'type': 'eq', 'fun': output_calculation.constraint_sum_to_one, 'jac': output_calculation.constraint_sum_to_one_jac},
    )

    original_minimize = output_calculation.sb.minimize
    try:
        output_calculation.worker_init(
            DummyObjective(),
            p0,
            "SLSQP",
            bounds,
            cons,
            False,
            None,
            False,
            False,
            False,
        )

        call_info = {}

        def fake_minimize(objective, p0, method, bounds, constraints, options, obs_rrs):
            call_info["parameter_count"] = len(p0)
            call_info["bounds_count"] = len(bounds)
            call_info["constraints"] = constraints
            return sb.minimize_result(np.array([p0[0], p0[1], p0[2], p0[3], 0.2], dtype=float), 4, True)

        output_calculation.sb.minimize = fake_minimize

        result = output_calculation.minimize_pixel(
            (0, 0, np.array([0.01, 0.02], dtype="float32"), None, False, False)
        )
    finally:
        output_calculation.sb.minimize = original_minimize

    assert call_info["parameter_count"] == 5
    assert call_info["bounds_count"] == 5
    assert call_info["constraints"] == ()
    assert result[2] is True
    assert np.allclose(result[4][4:7], np.array([0.2, 0.8, 0.0], dtype=float))


def test_build_rerun_setup_seeds_feasible_two_target_guess_in_strict_mode():
    siop = {
        "p_min": np.array([0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 0.0], dtype="float32"),
        "p_max": np.array([1.0, 1.0, 1.0, 10.0, 1.0, 1.0, 0.0], dtype="float32"),
        "p_bounds": (
            (0.0, 1.0),
            (0.0, 1.0),
            (0.0, 1.0),
            (0.1, 10.0),
            (0.0, 1.0),
            (0.0, 1.0),
            (0.0, 0.0),
        ),
    }

    p0, cons = output_calculation._build_rerun_setup(
        siop,
        relaxed=False,
        allow_target_sum_over_one=False,
    )

    assert np.allclose(p0[4:7], np.array([0.5, 0.5, 0.0], dtype=float))
    assert len(cons) == 1


def test_minimize_pixel_uses_substrate_multistart_when_depth_is_free():
    class DummyObjective:
        def __init__(self):
            self._nedr = np.ones(2, dtype="float32")
            self._error_func_name = "f"
            self.observed_rrs = None

        def __call__(self, parameters):
            parameters = np.asarray(parameters, dtype=float)
            target = np.array([0.8, 0.1, 0.1], dtype=float)
            error = float(np.sum((parameters[4:7] - target) ** 2))
            jacobian = np.zeros(7, dtype=float)
            jacobian[4:7] = 2.0 * (parameters[4:7] - target)
            return error, jacobian

    p0 = np.array([0.2, 0.1, 0.05, 2.0, 1 / 3, 1 / 3, 1 / 3], dtype="float32")
    bounds = (
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.5, 5.0),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
    )
    cons = (
        {'type': 'eq', 'fun': output_calculation.constraint_sum_to_one, 'jac': output_calculation.constraint_sum_to_one_jac},
    )

    original_minimize = output_calculation.sb.minimize
    try:
        output_calculation.worker_init(
            DummyObjective(),
            p0,
            "SLSQP",
            bounds,
            cons,
            False,
            None,
            False,
            False,
            False,
        )

        recorded_substrate_starts = []

        def fake_minimize(objective, p0, method, bounds, constraints, options, obs_rrs):
            recorded_substrate_starts.append(tuple(np.asarray(p0[4:7], dtype=float)))
            return sb.minimize_result(np.asarray(p0, dtype=float), 3, True)

        output_calculation.sb.minimize = fake_minimize

        result = output_calculation.minimize_pixel(
            (0, 0, np.array([0.01, 0.02], dtype="float32"), None, False, False)
        )
    finally:
        output_calculation.sb.minimize = original_minimize

    unique_starts = {tuple(np.round(start, 6)) for start in recorded_substrate_starts}
    assert result[2] is True
    assert len(unique_starts) > 1


def test_cover_sum_validation_changes_with_relaxation_mode():
    guess = np.array([0.0, 0.0, 0.0, 1.0, 0.8, 0.8, 0.0], dtype="float32")
    original_relaxed = output_calculation._WORKER_RELAXED
    try:
        output_calculation._WORKER_RELAXED = False
        assert output_calculation._cover_sum_is_valid(guess) is False

        output_calculation._WORKER_RELAXED = True
        assert output_calculation._cover_sum_is_valid(guess) is True

        output_calculation._WORKER_RELAXED = True
        assert output_calculation._cover_sum_is_valid(np.array([0, 0, 0, 1, 5, 5, 5], dtype="float32")) is True
    finally:
        output_calculation._WORKER_RELAXED = original_relaxed
