""" Sambuca Error Functions.

    Used when assessing model closure during parameter estimation.
"""


from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

from collections import namedtuple

import numpy as np

ErrorTerms = namedtuple('ErrorTerms',
                        [
                            'alpha',
                            'alpha_f',
                            'f',
                            'lsq',
                        ])
# TODO: update error attribute docstrings
""" namedtuple containing the error terms.

Attributes:
    alpha (float): TODO
    alpha_f (float): TODO
    f (float): TODO
    lsq (float): TODO
"""


# pylint generates no-member warnings for valid named tuple members
# pylint: disable=no-member

PaperObjectiveTerms = namedtuple(
    'PaperObjectiveTerms',
    [
        'weights',
        'observed_rrs',
        'modelled_rrs',
        'observed_sum_weighted',
        'residual_norm',
        'model_norm',
        'observed_norm',
        'weighted_dot',
        'cosine',
        'cosine_divisor',
        'alpha',
        'lsq',
    ])


def _coerce_weight_array(observed_rrs, nedr=None):
    """Return the paper's spectral weighting array w(lambda)."""
    observed_rrs = np.asarray(observed_rrs, dtype=float)
    if nedr is None:
        return np.ones_like(observed_rrs, dtype=float)

    nedr = np.asarray(nedr, dtype=float)
    nedr = np.clip(nedr, 1e-12, None)
    return 1.0 / nedr


def compute_paper_objective_terms(observed_rrs, modelled_rrs, nedr=None):
    """Compute the Brando et al. (2009) Eq. 13-14 terms."""
    observed_rrs = np.asarray(observed_rrs, dtype=float)
    modelled_rrs = np.asarray(modelled_rrs, dtype=float)
    weights = _coerce_weight_array(observed_rrs, nedr)

    observed_sum_weighted = np.sum(weights * observed_rrs)
    observed_sum_weighted = np.clip(observed_sum_weighted, 1e-12, None)

    residual = modelled_rrs - observed_rrs
    residual_norm = np.sqrt(np.clip(np.sum(weights * residual * residual), 1e-24, None))
    model_norm = np.sqrt(np.clip(np.sum(weights * modelled_rrs * modelled_rrs), 1e-24, None))
    observed_norm = np.sqrt(np.clip(np.sum(weights * observed_rrs * observed_rrs), 1e-24, None))
    weighted_dot = np.sum(weights * observed_rrs * modelled_rrs)
    cosine = weighted_dot / (model_norm * observed_norm)
    cosine = np.clip(cosine, 0.0, 1.0)
    cosine_divisor = np.sqrt(np.clip(1.0 - cosine * cosine, 1e-24, None))
    alpha = np.arccos(cosine)
    lsq = residual_norm / observed_sum_weighted

    return PaperObjectiveTerms(
        weights=weights,
        observed_rrs=observed_rrs,
        modelled_rrs=modelled_rrs,
        observed_sum_weighted=observed_sum_weighted,
        residual_norm=residual_norm,
        model_norm=model_norm,
        observed_norm=observed_norm,
        weighted_dot=weighted_dot,
        cosine=cosine,
        cosine_divisor=cosine_divisor,
        alpha=alpha,
        lsq=lsq,
    )

def error_all(observed_rrs, modelled_rrs, nedr=None):
    """Calculates all common error terms.

    Args:
        observed_rrs (array-like): The observed reflectance(remotely-sensed).
        modelled_rrs (array-like): The modelled reflectance(remotely-sensed).
        nedr (array-like): Noise equivalent difference in reflectance.

    Returns:
        ErrorTerms: The error terms.
    """
    terms = compute_paper_objective_terms(observed_rrs, modelled_rrs, nedr)
    # Keep `f` and `lsq` as aliases for the paper's weighted LSQ term.
    return ErrorTerms(terms.alpha, terms.alpha * terms.lsq, terms.lsq, terms.lsq)


    
def distance_alpha(observed_rrs, modelled_rrs, nedr=None):
    # TODO: complete the docstring
    """Calculates TODO

    Args:
        observed_rrs: The observed reflectance(remotely-sensed).
        modelled_rrs: The modelled reflectance(remotely-sensed).
        noise: Optional spectral noise values.

    Returns: TODO
    """
    return error_all(observed_rrs, modelled_rrs, nedr).alpha


def distance_alpha_f(observed_rrs, modelled_rrs, nedr=None):
    # TODO: complete the description
    """Calculates TODO

    Args:
        observed_rrs: The observed reflectance(remotely-sensed).
        modelled_rrs: The modelled reflectance(remotely-sensed).
        noise: Optional spectral noise values.

    Returns: TODO
    """
    return error_all(observed_rrs, modelled_rrs, nedr).alpha_f


def distance_lsq(observed_rrs, modelled_rrs, nedr=None):
    # TODO: complete the description
    """Calculates TODO

    Args:
        observed_rrs: The observed reflectance(remotely-sensed).
        modelled_rrs: The modelled reflectance(remotely-sensed).
        noise: Optional spectral noise values.

    Returns: TODO
    """
    return error_all(observed_rrs, modelled_rrs, nedr).lsq


def distance_f(observed_rrs, modelled_rrs, nedr=None):
    # TODO: complete the description
    """Calculates TODO

    Args:
        observed_rrs: The observed reflectance(remotely-sensed).
        modelled_rrs: The modelled reflectance(remotely-sensed).
        noise: Optional spectral noise values.

    Returns: TODO
    """
    return error_all(observed_rrs, modelled_rrs, nedr).f

    
