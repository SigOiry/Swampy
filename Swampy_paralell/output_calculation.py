# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 19:08:35 2017

@author: Marco
"""

import sambuca as sb
import sambuca_core as sbc
import numpy as np
import math
import os
from itertools import product
from multiprocessing import cpu_count, set_start_method
from concurrent.futures import ProcessPoolExecutor
try:
    from tqdm import tqdm
except Exception:
    # tqdm not available; provide a no-op fallback
    def tqdm(iterable, total=None):
        return iterable
import copy
from  alewriter import AleWriter
from sambuca.error import error_all

low_relax = 0.5
high_relax = 2.0


def constraint_sum_to_one(x):
    return 1 - (x[4] + x[5] + x[6])


def constraint_sum_to_one_jac(x):
    return np.array([0.0, 0.0, 0.0, 0.0, -1.0, -1.0, -1.0])


def constraint_upper_relaxed(x):
    return high_relax - (x[4] + x[5] + x[6])


def constraint_upper_relaxed_jac(x):
    return np.array([0.0, 0.0, 0.0, 0.0, -1.0, -1.0, -1.0])


def constraint_lower_relaxed(x):
    return (x[4] + x[5] + x[6]) - low_relax


def constraint_lower_relaxed_jac(x):
    return np.array([0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0])


# Module-level globals filled by worker_init to avoid pickling large objects per-task
_WORKER_OBJECTIVE = None
_WORKER_P0 = None
_WORKER_OPT_MET = None
_WORKER_BOUNDS = None
_WORKER_CONS = None
_WORKER_SHALLOW = False
_WORKER_BATHY_TOL = None
_WORKER_DEPTH_STARTS = None
_WORKER_OPTIMIZE_INITIAL_GUESSES = False
_WORKER_USE_FIVE_INITIAL_GUESSES = False
_WORKER_RELAXED = False
_WORKER_FULLY_RELAXED = False
_WORKER_SUBSTRATE_STARTS = (
    [1/3, 1/3, 1/3],
    [0.8, 0.1, 0.1],
    [0.1, 0.8, 0.1],
    [0.1, 0.1, 0.8],
)


def _input_bathy_is_missing(has_input_bathy, bathy_val):
    """Return True when the user supplied bathymetry but this pixel has no depth."""
    if not has_input_bathy:
        return False
    if bathy_val is None:
        return True
    try:
        return not np.isfinite(float(bathy_val))
    except (TypeError, ValueError):
        return True


def _apply_shallow_sdi_adjustment(result_recorder, x, y, obs_rrs, result_x, nit, success):
    """Track pixels that enter the SDI=0 shallow-water depth adjustment loop."""
    adjusted = False
    while np.floor(result_recorder.sdi[x, y]) == 0:
        adjusted = True
        old_sdi = result_recorder.sdi[x, y]
        depth_sdi0 = result_x[3]
        result_x[3] = depth_sdi0 * 0.95
        result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
        if np.floor(result_recorder.sdi[x, y]) > 0:
            # Depth reduction made bottom visible — restore previous (shallowest SDI=0) depth
            result_x[3] = depth_sdi0
            result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
            break
        if abs(old_sdi - result_recorder.sdi[x, y]) < 1e-7:
            # SDI not changing — cannot reduce further, restore and stop
            result_x[3] = depth_sdi0
            result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
            break

    return adjusted


def _dedupe_float_sequence(values, atol=1e-12):
    unique = []
    for value in values:
        fvalue = float(value)
        if not any(abs(fvalue - existing) <= atol for existing in unique):
            unique.append(fvalue)
    return tuple(unique)


def _initial_guess_values_from_bounds(lower, upper, use_five_points=False):
    """Return the initial-guess values to test inside the user bounds.

    Default mode tests exactly 3 values:
    25%, 50%, and 75% of the interval.

    Five-point mode tests exactly 5 values:
    min, 25%, 50%, 75%, and max.
    """
    lower = float(lower)
    upper = float(upper)
    midpoint = (lower + upper) / 2.0
    lower_mid = lower + 0.5 * (midpoint - lower)
    upper_mid = midpoint + 0.5 * (upper - midpoint)
    if use_five_points:
        return _dedupe_float_sequence((lower, lower_mid, midpoint, upper_mid, upper))
    return _dedupe_float_sequence((lower_mid, midpoint, upper_mid))


def _score_initial_guess(obs_rrs, closed_rrs):
    error_terms = error_all(obs_rrs, closed_rrs, _WORKER_OBJECTIVE._nedr)
    error_name = str(getattr(_WORKER_OBJECTIVE, "_error_func_name", "alpha_f")).lower()
    if error_name == 'alpha':
        return float(error_terms.alpha)
    if error_name in ('f', 'lsq'):
        return float(error_terms.f)
    return float(error_terms.alpha_f)


def _cover_sum_is_valid(guess, atol=1e-12):
    guess = np.asarray(guess, dtype=float)
    if guess.size < 7:
        return True
    cover_sum = float(np.sum(guess[4:7]))
    if _WORKER_FULLY_RELAXED:
        return True
    if _WORKER_RELAXED:
        return (low_relax - atol) <= cover_sum <= (high_relax + atol)
    return cover_sum <= (1.0 + atol)


def _enforce_cover_sum_limit(guess):
    guess = np.asarray(guess, dtype=float).copy()
    if guess.size < 7:
        return guess
    if _WORKER_FULLY_RELAXED or _WORKER_RELAXED:
        return guess
    cover_sum = float(np.sum(guess[4:7]))
    if cover_sum > 1.0 and cover_sum > 0.0:
        guess[4:7] = guess[4:7] / cover_sum
    return guess


def _normalise_target_fractions(guess):
    guess = np.asarray(guess, dtype=float).copy()
    if guess.size < 7:
        return guess
    fractions = np.clip(guess[4:7], 0.0, None)
    cover_sum = float(np.sum(fractions))
    if cover_sum > 0.0:
        fractions = fractions / cover_sum
    fractions = np.clip(fractions, 0.0, 1.0)
    guess[4:7] = fractions
    return guess


def _bound_is_fixed_zero(bound_pair, atol=1e-12):
    if bound_pair is None or len(bound_pair) < 2:
        return False
    lower, upper = bound_pair[:2]
    if lower is None or upper is None:
        return False
    return abs(float(lower)) <= atol and abs(float(upper)) <= atol


def _two_target_cover_x_bounds(bounds_local, atol=1e-12):
    if bounds_local is None or len(bounds_local) < 7:
        return None
    if not _bound_is_fixed_zero(bounds_local[6], atol=atol):
        return None
    try:
        sub1_lo, sub1_hi = map(float, bounds_local[4])
        sub2_lo, sub2_hi = map(float, bounds_local[5])
    except Exception:
        return None
    x_lo = max(sub1_lo, 1.0 - sub2_hi, 0.0)
    x_hi = min(sub1_hi, 1.0 - sub2_lo, 1.0)
    if x_hi < (x_lo - atol):
        return None
    return (x_lo, x_hi)


def _should_use_two_target_cover_mode(bounds_local, relaxed, fully_relaxed=False):
    return bool(relaxed and (not fully_relaxed) and _two_target_cover_x_bounds(bounds_local) is not None)


def _reduce_full_guess_to_two_target_mode(guess, x_bounds):
    guess = np.asarray(guess, dtype=float)
    x_lo, x_hi = map(float, x_bounds)
    if guess.size >= 6:
        denom = float(guess[4] + guess[5])
        if np.isfinite(denom) and denom > 0.0:
            x_value = float(guess[4]) / denom
        else:
            x_value = 0.5 * (x_lo + x_hi)
    else:
        x_value = 0.5 * (x_lo + x_hi)
    x_value = float(np.clip(x_value, x_lo, x_hi))
    return np.array([guess[0], guess[1], guess[2], guess[3], x_value], dtype=float)


def _expand_two_target_guess(guess):
    guess = np.asarray(guess, dtype=float)
    x_value = float(np.clip(guess[4], 0.0, 1.0))
    return np.array(
        [guess[0], guess[1], guess[2], guess[3], x_value, 1.0 - x_value, 0.0],
        dtype=float)


def _reduce_bounds_to_two_target_mode(bounds_local, x_bounds):
    bounds_list = [tuple(bounds_local[index]) for index in range(4)]
    bounds_list.append(tuple(map(float, x_bounds)))
    return tuple(bounds_list)


def _two_target_x_start_values(x_bounds):
    x_lo, x_hi = map(float, x_bounds)
    midpoint = 0.5 * (x_lo + x_hi)
    candidates = [midpoint]
    for sub_start in _WORKER_SUBSTRATE_STARTS:
        denom = float(sub_start[0] + sub_start[1])
        if denom > 0.0:
            candidates.append(np.clip(float(sub_start[0]) / denom, x_lo, x_hi))
    return _dedupe_float_sequence(candidates)


class _TwoTargetCoverObjectiveAdapter(object):
    def __init__(self, objective):
        self._objective = objective
        self._nedr = objective._nedr
        self._error_func_name = getattr(objective, "_error_func_name", "alpha_f")

    @property
    def observed_rrs(self):
        return self._objective.observed_rrs

    @observed_rrs.setter
    def observed_rrs(self, observed_rrs):
        self._objective.observed_rrs = observed_rrs

    def _expand(self, parameters):
        return _expand_two_target_guess(parameters)

    def _run_forward_model(self, parameters):
        return self._objective._run_forward_model(self._expand(parameters))

    def _filter_spectrum(self, spectra):
        return self._objective._filter_spectrum(spectra)

    def __call__(self, parameters):
        error_value, jacobian_full = self._objective(self._expand(parameters))
        jacobian_full = np.asarray(jacobian_full, dtype=float)
        jacobian_reduced = np.array([
            jacobian_full[0],
            jacobian_full[1],
            jacobian_full[2],
            jacobian_full[3],
            jacobian_full[4] - jacobian_full[5],
        ], dtype=float)
        return error_value, jacobian_reduced


def _select_preoptimized_initial_guess(obs_rrs, p0_base, bounds_local, use_five_points=False):
    objective = _WORKER_OBJECTIVE
    objective.observed_rrs = obs_rrs

    candidate_lists = []
    for index, base_value in enumerate(np.asarray(p0_base, dtype=float)):
        bound_pair = None
        if bounds_local is not None and index < len(bounds_local):
            bound_pair = bounds_local[index]
        if bound_pair is None:
            candidate_lists.append((float(base_value),))
            continue
        lower, upper = bound_pair
        if lower is None or upper is None:
            candidate_lists.append((float(base_value),))
            continue
        candidate_lists.append(_initial_guess_values_from_bounds(lower, upper, use_five_points=use_five_points))

    best_guess = _enforce_cover_sum_limit(p0_base)
    best_score = np.inf
    for guess_values in product(*candidate_lists):
        guess = np.asarray(guess_values, dtype=float)
        if not _cover_sum_is_valid(guess):
            continue
        try:
            model_results = objective._run_forward_model(guess)
            closed_rrs = objective._filter_spectrum(model_results.rrs)
            score = _score_initial_guess(obs_rrs, closed_rrs)
        except Exception:
            continue
        if np.isfinite(score) and score < best_score:
            best_score = score
            best_guess = guess
    return _enforce_cover_sum_limit(best_guess)


def _select_preoptimized_initial_guess_two_target(obs_rrs, p0_base, bounds_local, use_five_points=False):
    x_bounds = _two_target_cover_x_bounds(bounds_local)
    if x_bounds is None:
        return _enforce_cover_sum_limit(p0_base)

    objective = _TwoTargetCoverObjectiveAdapter(_WORKER_OBJECTIVE)
    objective.observed_rrs = obs_rrs
    reduced_p0 = _reduce_full_guess_to_two_target_mode(p0_base, x_bounds)
    bounds_reduced = _reduce_bounds_to_two_target_mode(bounds_local, x_bounds)

    candidate_lists = []
    for index, base_value in enumerate(reduced_p0):
        bound_pair = bounds_reduced[index]
        lower, upper = bound_pair
        if lower is None or upper is None:
            candidate_lists.append((float(base_value),))
            continue
        candidate_lists.append(_initial_guess_values_from_bounds(lower, upper, use_five_points=use_five_points))

    best_guess = reduced_p0
    best_score = np.inf
    for guess_values in product(*candidate_lists):
        guess = np.asarray(guess_values, dtype=float)
        try:
            model_results = objective._run_forward_model(guess)
            closed_rrs = objective._filter_spectrum(model_results.rrs)
            score = _score_initial_guess(obs_rrs, closed_rrs)
        except Exception:
            continue
        if np.isfinite(score) and score < best_score:
            best_score = score
            best_guess = guess
    return _expand_two_target_guess(best_guess)


def _append_unique_start(start_vectors, candidate, atol=1e-12):
    candidate = np.asarray(candidate, dtype=float)
    for existing in start_vectors:
        if np.allclose(existing, candidate, atol=atol, rtol=0.0):
            return
    start_vectors.append(candidate)


def worker_init(objective, p0, opt_met, bounds, cons, shallow, bathy_tol=None, optimize_initial_guesses=False, use_five_initial_guesses=False, relaxed=False, fully_relaxed=False):
    """Initializer called once per worker process to set globals and avoid
    pickling large objects for every task."""
    global _WORKER_OBJECTIVE, _WORKER_P0, _WORKER_OPT_MET, _WORKER_BOUNDS, _WORKER_CONS, _WORKER_SHALLOW, _WORKER_BATHY_TOL, _WORKER_DEPTH_STARTS, _WORKER_OPTIMIZE_INITIAL_GUESSES, _WORKER_USE_FIVE_INITIAL_GUESSES, _WORKER_RELAXED, _WORKER_FULLY_RELAXED
    _WORKER_OBJECTIVE = objective
    _WORKER_P0 = p0
    _WORKER_OPT_MET = opt_met
    _WORKER_BOUNDS = bounds
    _WORKER_CONS = cons
    _WORKER_SHALLOW = shallow
    _WORKER_BATHY_TOL = bathy_tol
    _WORKER_OPTIMIZE_INITIAL_GUESSES = bool(optimize_initial_guesses)
    _WORKER_USE_FIVE_INITIAL_GUESSES = bool(use_five_initial_guesses)
    _WORKER_RELAXED = bool(relaxed)
    _WORKER_FULLY_RELAXED = bool(fully_relaxed)
    if bounds is not None:
        depth_lo = max(bounds[3][0], 1e-3)
        depth_hi = bounds[3][1]
    else:
        depth_lo, depth_hi = 0.1, 30.0
    _WORKER_DEPTH_STARTS = np.logspace(np.log10(depth_lo), np.log10(depth_hi), 4)
    # Limit BLAS/OpenMP threads to 1 inside each worker to avoid memory exhaustion
    # when many worker processes are running simultaneously.
    try:
        from threadpoolctl import threadpool_limits
        threadpool_limits(limits=1)
    except ImportError:
        pass


def minimize_pixel(arg):
    """Minimize for a single pixel. arg is a small tuple
    (x, y, obs_rrs, bathy_val, bathy_exposed, has_input_bathy[, bathy_tolerance_override
    [, p0_override[, bounds_override]]]).
    Returns a compact tuple: (x, y, success_bool, nit_or_0, result_array_or_None,
    observed_rrs_or_None, selected_initial_guess_or_None)."""
    x, y, obs_rrs, bathy_val, bathy_exposed, has_input_bathy = arg[:6]
    bathy_tolerance_override = arg[6] if len(arg) > 6 else None
    p0_override = arg[7] if len(arg) > 7 else None
    bounds_override = arg[8] if len(arg) > 8 else None

    if bathy_exposed:
        return x, y, False, 0, None, None, None

    if _input_bathy_is_missing(has_input_bathy, bathy_val):
        return x, y, False, 0, None, None, None

    # cheap checks first
    if (np.allclose(obs_rrs, 0)) or (np.isnan(obs_rrs).any()):
        return x, y, False, 0, None, None, None

    depth_is_fixed = bathy_val is not None and not np.isnan(bathy_val)
    if p0_override is not None:
        p0_base = np.asarray(p0_override, dtype=float).copy()
    else:
        p0_base = _WORKER_P0.copy()
    if bounds_override is not None:
        bounds_local = tuple(bounds_override)
    else:
        bounds_local = _WORKER_BOUNDS

    if depth_is_fixed:
        # lock or constrain depth parameter (index 3) using bathymetry value
        p0_base[3] = float(bathy_val)
        base_bounds = bounds_local if bounds_local is not None else _WORKER_BOUNDS
        if base_bounds is not None:
            bounds_local = list(base_bounds)
            bathy_tol_local = _WORKER_BATHY_TOL if bathy_tolerance_override is None else bathy_tolerance_override
            if bathy_tol_local is not None and bathy_tol_local > 0:
                lo = max(0.0, float(bathy_val) - float(bathy_tol_local))
                hi = float(bathy_val) + float(bathy_tol_local)
            else:
                lo = hi = max(0.0, float(bathy_val))
            bounds_local[3] = (lo, hi)
            bounds_local = tuple(bounds_local)
        else:
            bounds_local = None

    two_target_cover_mode = _should_use_two_target_cover_mode(bounds_local, _WORKER_RELAXED, _WORKER_FULLY_RELAXED)

    selected_initial_guess = None
    if p0_override is not None and not _WORKER_OPTIMIZE_INITIAL_GUESSES:
        if two_target_cover_mode:
            selected_initial_guess = _expand_two_target_guess(
                _reduce_full_guess_to_two_target_mode(
                    p0_base,
                    _two_target_cover_x_bounds(bounds_local)))
        else:
            selected_initial_guess = np.asarray(p0_base, dtype=float).copy()
    if _WORKER_OPTIMIZE_INITIAL_GUESSES:
        if two_target_cover_mode:
            selected_initial_guess = _select_preoptimized_initial_guess_two_target(
                obs_rrs,
                p0_base,
                bounds_local,
                use_five_points=_WORKER_USE_FIVE_INITIAL_GUESSES,
            )
        else:
            selected_initial_guess = _select_preoptimized_initial_guess(
                obs_rrs,
                p0_base,
                bounds_local,
                use_five_points=_WORKER_USE_FIVE_INITIAL_GUESSES,
            )

    if two_target_cover_mode:
        x_bounds = _two_target_cover_x_bounds(bounds_local)
        reduced_objective = _TwoTargetCoverObjectiveAdapter(_WORKER_OBJECTIVE)
        reduced_bounds = _reduce_bounds_to_two_target_mode(bounds_local, x_bounds)
        if _WORKER_OPTIMIZE_INITIAL_GUESSES and selected_initial_guess is not None:
            reduced_selected_guess = _reduce_full_guess_to_two_target_mode(selected_initial_guess, x_bounds)
        else:
            reduced_selected_guess = _reduce_full_guess_to_two_target_mode(p0_base, x_bounds)
            selected_initial_guess = _expand_two_target_guess(reduced_selected_guess)
        x_start_values = _two_target_x_start_values(x_bounds)

        best_result = None
        best_error = np.inf
        if depth_is_fixed:
            if _WORKER_OPTIMIZE_INITIAL_GUESSES:
                start_vectors = []
                _append_unique_start(start_vectors, reduced_selected_guess)
                for x_start in x_start_values:
                    p0_local = np.asarray(reduced_selected_guess, dtype=float).copy()
                    p0_local[4] = float(x_start)
                    _append_unique_start(start_vectors, p0_local)
            else:
                start_vectors = []
                reduced_base = _reduce_full_guess_to_two_target_mode(p0_base, x_bounds)
                for x_start in x_start_values:
                    p0_local = reduced_base.copy()
                    p0_local[4] = float(x_start)
                    _append_unique_start(start_vectors, p0_local)
        else:
            if _WORKER_OPTIMIZE_INITIAL_GUESSES:
                start_vectors = []
                _append_unique_start(start_vectors, reduced_selected_guess)
                for depth_start in _WORKER_DEPTH_STARTS:
                    p0_local = np.asarray(reduced_selected_guess, dtype=float).copy()
                    p0_local[3] = float(depth_start)
                    _append_unique_start(start_vectors, p0_local)
            else:
                start_vectors = []
                reduced_base = _reduce_full_guess_to_two_target_mode(p0_base, x_bounds)
                for depth_start in _WORKER_DEPTH_STARTS:
                    p0_local = reduced_base.copy()
                    p0_local[3] = float(depth_start)
                    _append_unique_start(start_vectors, p0_local)

        for p0_local in start_vectors:
            res = sb.minimize(
                reduced_objective,
                p0_local,
                method=_WORKER_OPT_MET,
                bounds=reduced_bounds,
                constraints=(),
                options={'disp': False, 'maxiter': 200},
                obs_rrs=obs_rrs
            )
            if res.x is not None:
                try:
                    err_val, _ = reduced_objective(res.x)
                    if err_val < best_error:
                        best_error = err_val
                        best_result = res
                except Exception:
                    if best_result is None:
                        best_result = res

        if best_result is None:
            return x, y, False, 0, None, obs_rrs, selected_initial_guess

        result_x = _expand_two_target_guess(best_result.x)
        return x, y, best_result.success, getattr(best_result, 'nit', 0), result_x, obs_rrs, selected_initial_guess

    if depth_is_fixed:
        # Multi-start over substrate fractions: when depth is fixed the bottom
        # signal is exponentially attenuated, making substrate gradients small.
        best_result = None
        best_error = np.inf
        if _WORKER_OPTIMIZE_INITIAL_GUESSES:
            start_vectors = []
            _append_unique_start(start_vectors, selected_initial_guess)
            for sub_start in _WORKER_SUBSTRATE_STARTS:
                p0_local = np.asarray(selected_initial_guess, dtype=float).copy()
                p0_local[4:7] = sub_start
                _append_unique_start(start_vectors, p0_local)
        else:
            start_vectors = []
            for sub_start in _WORKER_SUBSTRATE_STARTS:
                p0_local = p0_base.copy()
                p0_local[4] = sub_start[0]
                p0_local[5] = sub_start[1]
                p0_local[6] = sub_start[2]
                start_vectors.append(np.asarray(p0_local, dtype=float))
        for p0_local in start_vectors:
            res = sb.minimize(
                _WORKER_OBJECTIVE,
                p0_local,
                method=_WORKER_OPT_MET,
                bounds=bounds_local,
                constraints=_WORKER_CONS,
                options={'disp': False, 'maxiter': 200},
                obs_rrs=obs_rrs
            )
            if res.x is not None:
                try:
                    err_val, _ = _WORKER_OBJECTIVE(res.x)
                    if err_val < best_error:
                        best_error = err_val
                        best_result = res
                except Exception:
                    if best_result is None:
                        best_result = res
        result = best_result
    else:
        # Depth is free: multi-start over logarithmically spaced depths to escape
        # shallow local minima caused by the depth-albedo degeneracy.
        best_result = None
        best_error = np.inf
        if _WORKER_OPTIMIZE_INITIAL_GUESSES:
            start_vectors = []
            _append_unique_start(start_vectors, selected_initial_guess)
            for depth_start in _WORKER_DEPTH_STARTS:
                p0_local = np.asarray(selected_initial_guess, dtype=float).copy()
                p0_local[3] = float(depth_start)
                _append_unique_start(start_vectors, p0_local)
        else:
            start_vectors = []
            for depth_start in _WORKER_DEPTH_STARTS:
                p0_local = p0_base.copy()
                p0_local[3] = float(depth_start)
                start_vectors.append(np.asarray(p0_local, dtype=float))
        for p0_local in start_vectors:
            res = sb.minimize(
                _WORKER_OBJECTIVE,
                p0_local,
                method=_WORKER_OPT_MET,
                bounds=bounds_local,
                constraints=_WORKER_CONS,
                options={'disp': False, 'maxiter': 200},
                obs_rrs=obs_rrs
            )
            if res.x is not None:
                try:
                    err_val, _ = _WORKER_OBJECTIVE(res.x)
                    if err_val < best_error:
                        best_error = err_val
                        best_result = res
                except Exception:
                    if best_result is None:
                        best_result = res
        result = best_result

    if result is None:
        return x, y, False, 0, None, obs_rrs, selected_initial_guess

    # Return minimal data to parent process to avoid sending heavy objects
    return x, y, result.success, getattr(result, 'nit', 0), getattr(result, 'x', None), obs_rrs, selected_initial_guess


# ALE
def analyze_results(x, y, obs_rrs, result_x, nit, success, shallow, result_recorder_height, result_recorder_width, result_recorder_sensor_filter, result_recorder_nedr, result_recorder_fixed_parameters):
    # Kept for backward-compatibility but prefer parent-side recording.
    result_recorder = AleWriter(
        result_recorder_height,
        result_recorder_width,
        result_recorder_sensor_filter,
        result_recorder_nedr,
        result_recorder_fixed_parameters)

    if result_x is not None:
        result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
        if shallow:
            _apply_shallow_sdi_adjustment(
                result_recorder,
                x,
                y,
                obs_rrs,
                result_x,
                nit,
                success)
    return x, y, result_recorder

def output_calculation(observed_rrs, objective, siop, result_recorder, image_info, opt_met, relaxed, shallow=False, free_cpu=0, optimize_initial_guesses=False, use_five_initial_guesses=False, fully_relaxed=False):
    skip_count = 0

    observed_rrs = np.asarray(observed_rrs)
    expected_band_count = int(np.asarray(getattr(objective, '_nedr', [])).shape[0])
    if observed_rrs.ndim != 3:
        raise ValueError(f"Observed RRS must be a 3D array shaped (bands, rows, cols); got shape {observed_rrs.shape}.")
    if expected_band_count and observed_rrs.shape[0] != expected_band_count:
        raise ValueError(
            "Observed RRS band count does not match the sensor/NEDR configuration: "
            f"{observed_rrs.shape[0]} observed bands vs {expected_band_count} sensor bands."
        )

    # Define a region to process in the image input
    # *****Observed data is in band, row(height, x), column(width, y)******
    xend = image_info['observed_rrs_height']
    yend = image_info['observed_rrs_width']

    xstart, ystart = 0, 0

    p0 = (np.array(siop['p_max']) + np.array(siop['p_min'])) / 2
    p0[3] = 10 ** ((math.log10(np.array(siop['p_max'][3])) + math.log10(np.array(siop['p_min'][3]))) / 2)
    if not relaxed:
        # Strict mode enforces a convex substrate mixture, so start from a
        # feasible point instead of the old 0.5/0.5/0.5 seed.
        p0[4:7] = 1.0 / 3.0

    # set some relaxed abundance constraints (RASC) after Petit et. al.(2017)******
    if fully_relaxed:
        cons = ()
    elif relaxed:
        cons = (
            {'type': 'ineq', 'fun': constraint_upper_relaxed, 'jac': constraint_upper_relaxed_jac},
            {'type': 'ineq', 'fun': constraint_lower_relaxed, 'jac': constraint_lower_relaxed_jac}
        )
    else:
        cons = (
            {'type': 'eq', 'fun': constraint_sum_to_one, 'jac': constraint_sum_to_one_jac},
        )

    print('Creating chunks...')

    bathy = image_info.get('bathymetry', None)
    bathy_exposed_mask = image_info.get('bathymetry_exposed_mask', None)
    bathy_tol = image_info.get('bathy_tolerance', None)
    has_input_bathy = bathy is not None

    def pixel_arg_generator():
        # yields minimal per-pixel args to reduce memory and pickling
        for x in range(xstart, xend):
            for y in range(ystart, yend):
                bval = None
                if bathy is not None:
                    try:
                        bval = bathy[x, y]
                    except Exception:
                        bval = None
                exposed = False
                if bathy_exposed_mask is not None:
                    try:
                        exposed = bool(bathy_exposed_mask[x, y])
                    except Exception:
                        exposed = False
                yield (x, y, observed_rrs[:, x, y], bval, exposed, has_input_bathy)


    # Limit BLAS/OpenMP threads to 1 per worker before spawning child processes.
    # Child processes inherit these env vars, preventing N_workers × N_BLAS_threads
    # memory blow-up that causes "OpenBLAS error: Memory allocation still failed".
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'

    print("Setting start method to spawn")
    set_start_method("spawn", force=True)
    print("Starting multiprocessing minimize...")

    # Initialize worker processes with shared large objects to avoid pickling them per-task
    workers = max(1, cpu_count() - free_cpu)
    with ProcessPoolExecutor(max_workers=workers, initializer=worker_init, initargs=(objective, p0, opt_met, siop['p_bounds'], cons, shallow, bathy_tol, optimize_initial_guesses, use_five_initial_guesses, relaxed, fully_relaxed)) as pool:
        # Use generator and tqdm on the futures iterator to avoid building large lists
        mapped = pool.map(minimize_pixel, pixel_arg_generator(), chunksize=200)
        for res in tqdm(mapped, total=(xend - xstart) * (yend - ystart)):
            # res is (x, y, success, nit, result_x, obs_rrs) or a skip tuple
            x, y, success, nit, result_x, obs_rrs, initial_guess = res
            if initial_guess is not None and getattr(result_recorder, 'initial_guess_stack', None) is not None:
                result_recorder.initial_guess_stack[x, y, :] = np.asarray(initial_guess, dtype=np.float32)
            if not success or result_x is None:
                # leave defaults (already set in result_recorder)
                continue

            result_x = list(result_x)  # make mutable for possible depth adjustment
            # Record results into the provided result_recorder
            result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)

            # Secondary iteration per Brando et al. (2009) Section 2.4.5:
            # For optically deep pixels (SDI=0), iteratively decrease depth while
            # maintaining SDI=0 to retrieve the "as shallow as possible" depth.
            if shallow:
                _apply_shallow_sdi_adjustment(
                    result_recorder,
                    x,
                    y,
                    obs_rrs,
                    result_x,
                    nit,
                    success)

    '''
    print('Analyzing results...')
    for res in tqdm(results):
        x, y, obs_rrs, result_x, nit, success = res
        if result_x is not None:
            result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
            if shallow:
                while np.floor(result_recorder.sdi[x, y]) == 0:
                    old_sdi = result_recorder.sdi[x, y]
                    depth_sdi0 = result_x[3]
                    result_x[3] = depth_sdi0 * 0.95
                    result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
                    if np.floor(result_recorder.sdi[x, y]) > 0:
                        result_x[3] = depth_sdi0
                        result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
                        break
                    if abs(old_sdi - result_recorder.sdi[x, y]) < 1e-7:
                        result_x[3] = depth_sdi0
                        result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
                        break
        else:
            skip_count += 1
    '''
    return result_recorder


def rerun_selected_pixels(observed_rrs, objective, siop, result_recorder, pixel_constraints,
                          opt_met, relaxed, free_cpu=0, bathy_tolerance=None,
                          optimize_initial_guesses=False, use_five_initial_guesses=False,
                          apply_shallow_adjustment=False,
                          allow_target_sum_over_one=False,
                          normalise_target_fractions=False,
                          fully_relaxed=False):
    """Re-optimise only a selected subset of pixels with depth constraints.

    Args:
        observed_rrs (ndarray): Observed reflectance cube shaped (bands, rows, cols).
        objective: Existing Sambuca objective instance.
        siop (dict): SIOP dictionary containing p_min / p_max / p_bounds.
        result_recorder (ArrayResultWriter): Recorder to update in place.
        pixel_constraints (iterable): Iterable of either:
            - (x, y, target_depth[, depth_tolerance]) tuples, or
            - dicts with keys x, y, target_depth and optional depth_tolerance,
              initial_guess, bounds.
        opt_met (str): Optimisation method.
        relaxed (bool): Whether relaxed substrate constraints are enabled.
        free_cpu (int): CPUs to leave idle.
        bathy_tolerance (float): +/- depth tolerance around target depth.
        optimize_initial_guesses (bool): Reuse the pre-search for initial guesses.
        use_five_initial_guesses (bool): Use five-point initial guess search.
        apply_shallow_adjustment (bool): Whether to apply the SDI=0 shallow-depth
            post-adjustment to corrected pixels.
        allow_target_sum_over_one (bool): Remove the cross-target sum constraint
            during the re-optimisation pass.
        normalise_target_fractions (bool): Renormalise fitted target fractions to
            sum to 1 before recording the corrected result.
    """
    observed_rrs = np.asarray(observed_rrs)
    expected_band_count = int(np.asarray(getattr(objective, '_nedr', [])).shape[0])
    if observed_rrs.ndim != 3:
        raise ValueError(f"Observed RRS must be a 3D array shaped (bands, rows, cols); got shape {observed_rrs.shape}.")
    if expected_band_count and observed_rrs.shape[0] != expected_band_count:
        raise ValueError(
            "Observed RRS band count does not match the sensor/NEDR configuration during re-optimisation: "
            f"{observed_rrs.shape[0]} observed bands vs {expected_band_count} sensor bands."
        )

    pixel_constraints = list(pixel_constraints or [])
    if not pixel_constraints:
        return result_recorder

    p0 = (np.array(siop['p_max']) + np.array(siop['p_min'])) / 2
    p0[3] = 10 ** ((math.log10(np.array(siop['p_max'][3])) + math.log10(np.array(siop['p_min'][3]))) / 2)
    if not relaxed:
        p0[4:7] = 1.0 / 3.0

    if allow_target_sum_over_one or fully_relaxed:
        cons = ()
    elif relaxed:
        cons = (
            {'type': 'ineq', 'fun': constraint_upper_relaxed, 'jac': constraint_upper_relaxed_jac},
            {'type': 'ineq', 'fun': constraint_lower_relaxed, 'jac': constraint_lower_relaxed_jac}
        )
    else:
        cons = (
            {'type': 'eq', 'fun': constraint_sum_to_one, 'jac': constraint_sum_to_one_jac},
        )

    def pixel_arg_generator():
        for item in pixel_constraints:
            p0_override = None
            bounds_override = None
            if isinstance(item, dict):
                x = int(item['x'])
                y = int(item['y'])
                target_depth = float(item['target_depth'])
                target_tolerance = item.get('depth_tolerance', bathy_tolerance)
                p0_override = item.get('initial_guess')
                bounds_override = item.get('bounds')
            else:
                if len(item) >= 6:
                    x, y, target_depth, target_tolerance, p0_override, bounds_override = item[:6]
                elif len(item) >= 4:
                    x, y, target_depth, target_tolerance = item[:4]
                else:
                    x, y, target_depth = item[:3]
                    target_tolerance = bathy_tolerance
            try:
                obs_rrs = observed_rrs[:, int(x), int(y)]
            except Exception:
                continue
            yield (
                int(x),
                int(y),
                obs_rrs,
                float(target_depth),
                False,
                False,
                target_tolerance,
                p0_override,
                bounds_override,
            )

    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'

    set_start_method("spawn", force=True)
    workers = max(1, cpu_count() - free_cpu)
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=worker_init,
        initargs=(
            objective,
            p0,
            opt_met,
            siop['p_bounds'],
            cons,
            apply_shallow_adjustment,
            bathy_tolerance,
            optimize_initial_guesses,
            use_five_initial_guesses,
            relaxed,
            fully_relaxed,
        ),
    ) as pool:
        mapped = pool.map(minimize_pixel, pixel_arg_generator(), chunksize=100)
        for res in tqdm(mapped, total=len(pixel_constraints)):
            x, y, success, nit, result_x, obs_rrs, initial_guess = res
            if initial_guess is not None and getattr(result_recorder, 'initial_guess_stack', None) is not None:
                result_recorder.initial_guess_stack[x, y, :] = np.asarray(initial_guess, dtype=np.float32)
            if not success or result_x is None:
                continue
            result_x = np.asarray(result_x, dtype=float)
            if normalise_target_fractions:
                result_x = _normalise_target_fractions(result_x)
            result_x = list(result_x)
            result_recorder(x, y, obs_rrs, parameters=sb.FreeParameters(*result_x), nit=nit, success=success)
            if apply_shallow_adjustment:
                _apply_shallow_sdi_adjustment(
                    result_recorder,
                    x,
                    y,
                    obs_rrs,
                    result_x,
                    nit,
                    success)
    return result_recorder
