""" Sambuca Minimize Wrapper

    This is a wrapper for the SciPy minimize function that can be used
    with a pool of workers.
"""


from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

from collections import namedtuple
import warnings

from scipy.optimize import minimize as scipy_minimize


minimize_result = namedtuple('minimize_result', '''x,nit,success''')
""" namedtuple containing return result of sb_minimize.

Attributes:
        x (ndarray): The solution of the optimization.
        nit (int): The total number of iterations.
        success (bool): Whether the optimizer succeeded or not
"""


def minimize(objective, p0, method, bounds, constraints, options, obs_rrs):
    """
    This is a wrapper function that iterates over all the substrate combinations
    calling the SciPy minimize function for each combination.  It returns the
    result with the best fit.  It supports passing a pool of worker to
    parallelize over the substrate combinations.

    Args:
        objective (callable): the objective function
        p0 (ndarray): the initial guess
        method (str): the type of solver
        bounds (sequence): bounds for the variable solution
        constraints (sequence): optimizer constraints
        options (dict): solver options
        obs_rrs (ndarray): initial observations
    """

    objective.observed_rrs = obs_rrs
    method_name = str(method).upper() if method else ''
    use_jacobian = method_name not in ('POWELL', 'NELDER-MEAD')

    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message='Values in x were outside bounds during a minimize step, clipping to bounds',
            category=RuntimeWarning)
        results = scipy_minimize(
            objective,
            p0,
            jac=use_jacobian,
            method=method,
            bounds=bounds,
            constraints=constraints,
            options=options)

    return minimize_result(results.x, results.nit, results.success)
