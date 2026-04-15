from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

import numpy as np

import sambuca as sb


class QuadraticObjective(object):
    def __init__(self, target):
        self.target = np.asarray(target, dtype=float)
        self.observed_rrs = None

    def __call__(self, parameters):
        parameters = np.asarray(parameters, dtype=float)
        residual = parameters - self.target
        return float(np.dot(residual, residual)), (2.0 * residual)


def test_slsqp_wrapper_returns_constrained_solution():
    target = np.array([0.12, 0.004, 0.5, 4.0, 0.1, 0.2, 0.7], dtype=float)
    objective = QuadraticObjective(target)
    p0 = np.array([0.08, 0.006, 0.9, 5.0, 1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0], dtype=float)
    bounds = (
        (0.01, 0.16),
        (0.0005, 0.01),
        (0.2, 1.5),
        (3.0, 5.5),
        (0.0, 1.0),
        (0.0, 1.0),
        (0.0, 1.0),
    )
    constraints = (
        {'type': 'eq', 'fun': lambda x: 1.0 - (x[4] + x[5] + x[6])},
    )

    result = sb.minimize(
        objective,
        p0,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'maxiter': 60, 'disp': False},
        obs_rrs=np.zeros(5, dtype=float))

    assert result.success
    assert np.isclose(np.sum(result.x[4:7]), 1.0, atol=1.0e-6)
    assert np.all(result.x[4:7] >= 0.0)
    assert np.allclose(result.x, target, atol=5.0e-5)
