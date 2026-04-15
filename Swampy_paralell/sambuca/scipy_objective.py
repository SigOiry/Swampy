""" Objective function for parameter estimation using scipy minimisation.
"""


from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

from collections.abc import Callable

import sambuca_core as sbc
import numpy as np
import math
import numba


@numba.njit(cache=True)
def _forward_model_kernel(
        chl, cdom, nap, depth, sub1_frac, sub2_frac, sub3_frac,
        a_ph_star, a_cdom_star, a_nap_star,
        bb_ph_star, bb_nap_star,
        a_water, bb_water,
        substrate1, substrate2, substrate3,
        inv_cos_theta_w, inv_cos_theta_0, one_over_pi):
    """JIT-compiled core of the forward model. All inputs must be plain
    numpy arrays (float64, C-contiguous) or Python floats. Returns a tuple
    of eight 1-D arrays: (rrs, rrs_dchl, rrs_dcdom, rrs_dnap, rrs_ddepth,
    rrs_dfrac1, rrs_dfrac2, rrs_dfrac3).
    """
    a_ph = chl * a_ph_star
    a_cdom = cdom * a_cdom_star
    a_nap = nap * a_nap_star
    a = a_water + a_ph + a_cdom + a_nap

    bb_ph = chl * bb_ph_star
    bb_nap = nap * bb_nap_star
    bb = bb_water + bb_ph + bb_nap

    r_substratum = (
        sub1_frac * substrate1 +
        sub2_frac * substrate2 +
        sub3_frac * substrate3)

    kappa = a + bb
    u = bb / kappa

    sq1 = np.power(1.00 + (2.40 * u), 0.50)
    sq2 = np.power(1.00 + (5.40 * u), 0.50)
    du_column = 1.03 * sq1
    du_bottom = 1.04 * sq2

    rrsdp = (0.084 + 0.17 * u) * u

    du_column_scaled = du_column * inv_cos_theta_0
    du_bottom_scaled = du_bottom * inv_cos_theta_0

    kappa_d = kappa * depth
    expColumnScaled = np.exp(-(inv_cos_theta_w + du_column_scaled) * kappa_d)
    expBottomScaled = np.exp(-(inv_cos_theta_w + du_bottom_scaled) * kappa_d)

    rrs = (
        rrsdp * (1.0 - expColumnScaled) +
        (one_over_pi * r_substratum * expBottomScaled)
    )

    rrs_dfrac1 = one_over_pi * substrate1 * expBottomScaled
    rrs_dfrac2 = one_over_pi * substrate2 * expBottomScaled
    rrs_dfrac3 = one_over_pi * substrate3 * expBottomScaled

    rrs_ddepth = (
        rrsdp * (inv_cos_theta_w + du_column_scaled) * kappa * expColumnScaled -
        one_over_pi * r_substratum * (inv_cos_theta_w + du_bottom_scaled) * kappa * expBottomScaled
    )

    u_dcdom = -(bb * a_cdom_star / (kappa * kappa))
    du_column_dcdom = 1.03 * 2.4 * u_dcdom / (2.0 * sq1)
    du_bottom_dcdom = 1.04 * 5.4 * u_dcdom / (2.0 * sq2)
    rrs_dcdom = (
        (0.084 * u_dcdom + 0.34 * u * u_dcdom) * (1.0 - expColumnScaled) +
        rrsdp * depth * (
            (inv_cos_theta_0 * du_column_dcdom * kappa +
             a_cdom_star * (inv_cos_theta_w + du_column_scaled)) * expColumnScaled
        ) +
        one_over_pi * r_substratum * (-depth) * (
            (inv_cos_theta_0 * du_bottom_dcdom * (a + bb) +
             a_cdom_star * (inv_cos_theta_w + du_bottom_scaled)) * expBottomScaled
        )
    )

    u_dchl = (bb_ph_star * (a + bb) - bb * (a_ph_star + bb_ph_star)) / (kappa * kappa)
    du_column_dchl = 1.03 * 2.4 * u_dchl / (2.0 * sq1)
    du_bottom_dchl = 1.04 * 5.4 * u_dchl / (2.0 * sq2)
    rrs_dchl = (
        (0.084 * u_dchl + 0.34 * u * u_dchl) * (1.0 - expColumnScaled) +
        rrsdp * depth * (
            (inv_cos_theta_0 * du_column_dchl * kappa +
             (a_ph_star + bb_ph_star) * (inv_cos_theta_w + du_column_scaled)) * expColumnScaled
        ) +
        one_over_pi * r_substratum * (-depth) * (
            (inv_cos_theta_0 * du_bottom_dchl * (a + bb) +
             (a_ph_star + bb_ph_star) * (inv_cos_theta_w + du_bottom_scaled)) * expBottomScaled
        )
    )

    u_dnap = (bb_nap_star * (a + bb) - bb * (a_nap_star + bb_nap_star)) / (kappa * kappa)
    du_column_dnap = 1.03 * 2.4 * u_dnap / (2.0 * sq1)
    du_bottom_dnap = 1.04 * 5.4 * u_dnap / (2.0 * sq2)
    rrs_dnap = (
        (0.084 * u_dnap + 0.34 * u * u_dnap) * (1.0 - expColumnScaled) +
        rrsdp * depth * (
            (inv_cos_theta_0 * du_column_dnap * (a + bb) +
             (a_nap_star + bb_nap_star) * (inv_cos_theta_w + du_column_scaled)) * expColumnScaled
        ) +
        one_over_pi * r_substratum * (-depth) * (
            (inv_cos_theta_0 * du_bottom_dnap * (a + bb) +
             (a_nap_star + bb_nap_star) * (inv_cos_theta_w + du_bottom_scaled)) * expBottomScaled
        )
    )

    return (rrs, rrs_dchl, rrs_dcdom, rrs_dnap, rrs_ddepth,
            rrs_dfrac1, rrs_dfrac2, rrs_dfrac3)


class _ObjectiveModelResults(object):
    __slots__ = (
        'rrs',
        'rrs_dchl',
        'rrs_dcdom',
        'rrs_dnap',
        'rrs_ddepth',
        'rrs_dfrac1',
        'rrs_dfrac2',
        'rrs_dfrac3',
    )

    def __init__(
            self,
            rrs,
            rrs_dchl,
            rrs_dcdom,
            rrs_dnap,
            rrs_ddepth,
            rrs_dfrac1,
            rrs_dfrac2,
            rrs_dfrac3):
        self.rrs = rrs
        self.rrs_dchl = rrs_dchl
        self.rrs_dcdom = rrs_dcdom
        self.rrs_dnap = rrs_dnap
        self.rrs_ddepth = rrs_ddepth
        self.rrs_dfrac1 = rrs_dfrac1
        self.rrs_dfrac2 = rrs_dfrac2
        self.rrs_dfrac3 = rrs_dfrac3


class SciPyObjective(Callable):
    """
    Configurable objective function for Sambuca parameter estimation, intended
    for use with the SciPy minimisation methods.

    Attributes:
        observed_rrs (array-like): The observed remotely-sensed reflectance.
            This attribute must be updated when you require the objective
            instance to use a different value.
        id (integer): The index of the substrate pair combination.
            This attribute must be updated when you require the objective
            instance to use a different substrate pair.
    """

    def __init__(
            self,
            sensor_filter,
            fixed_parameters,
            error_function_name='alpha_f',
            nedr=None):
        """
        Initialise the ArrayWriter.
        Args:
            sensor_filter (array-like): The Sambuca sensor filter.
            fixed_parameters (sambuca.AllParameters): The fixed model
                parameters.
            error_function_name (string): The error function that will be applied
                to the modelled and observed rrs.
            nedr (array-like): Noise equivalent difference in reflectance.
        """
        super().__init__()

        # check for being passed the (wavelengths, filter) tuple loaded by the
        # sambuca_core sensor_filter loading functions
        if isinstance(sensor_filter, tuple) and len(sensor_filter) == 2:
            self._sensor_filter = np.asarray(sensor_filter[1], dtype=float)
            self._nedr = np.asarray(nedr[1], dtype=float)
        else:
            self._sensor_filter = np.asarray(sensor_filter, dtype=float)
            self._nedr = np.asarray(nedr, dtype=float)

        sensor_row_sums = np.clip(self._sensor_filter.sum(axis=1), 1e-12, None)
        self._normalised_sensor_filter = self._sensor_filter / sensor_row_sums[:, None]
        self._normalised_sensor_filter_t = self._normalised_sensor_filter.T
        self._weights = 1.0 / np.clip(self._nedr, 1e-12, None)

        self._fixed_parameters = fixed_parameters
        self._error_func_name = error_function_name
        self._observed_rrs = None
        self._observed_rrs_weighted = None
        self._observed_sum_weighted = None
        self._observed_norm = None

        self._wavelengths = np.asarray(self._fixed_parameters.wavelengths, dtype=float)
        self._a_water = np.asarray(self._fixed_parameters.a_water, dtype=float)
        self._a_ph_star = np.asarray(self._fixed_parameters.a_ph_star, dtype=float)
        self._substrate1 = np.asarray(self._fixed_parameters.substrates[0], dtype=float)
        self._substrate2 = np.asarray(self._fixed_parameters.substrates[1], dtype=float)
        self._substrate3 = np.asarray(self._fixed_parameters.substrates[2], dtype=float)

        inv_refractive_index = 1.0 / self._fixed_parameters.water_refractive_index
        theta_w = math.asin(
            inv_refractive_index * math.sin(math.radians(self._fixed_parameters.theta_air)))
        theta_o = math.asin(
            inv_refractive_index * math.sin(math.radians(self._fixed_parameters.off_nadir)))
        self._inv_cos_theta_w = 1.0 / math.cos(theta_w)
        self._inv_cos_theta_0 = 1.0 / math.cos(theta_o)
        self._one_over_pi = 1.0 / math.pi

        self._bb_water = (
            (0.00194 / 2.0) *
            np.power(self._fixed_parameters.bb_lambda_ref / self._wavelengths, 4.32))
        self._a_cdom_star = (
            self._fixed_parameters.a_cdom_lambda0cdom *
            np.exp(-self._fixed_parameters.a_cdom_slope *
                   (self._wavelengths - self._fixed_parameters.lambda0cdom)))
        self._a_nap_star = (
            self._fixed_parameters.a_nap_lambda0nap *
            np.exp(-self._fixed_parameters.a_nap_slope *
                   (self._wavelengths - self._fixed_parameters.lambda0nap)))

        bb_ph_power = np.power(
            self._fixed_parameters.lambda0x / self._wavelengths,
            self._fixed_parameters.bb_ph_slope)
        self._bb_ph_star = self._fixed_parameters.x_ph_lambda0x * bb_ph_power
        if self._fixed_parameters.bb_nap_slope:
            bb_nap_power = np.power(
                self._fixed_parameters.lambda0x / self._wavelengths,
                self._fixed_parameters.bb_nap_slope)
        else:
            bb_nap_power = bb_ph_power
        self._bb_nap_star = self._fixed_parameters.x_nap_lambda0x * bb_nap_power

    @property
    def observed_rrs(self):
        return self._observed_rrs

    @observed_rrs.setter
    def observed_rrs(self, observed_rrs):
        if observed_rrs is None:
            self._observed_rrs = None
            self._observed_rrs_weighted = None
            self._observed_sum_weighted = None
            self._observed_norm = None
            return

        observed_rrs = np.asarray(observed_rrs, dtype=float)
        observed_rrs_weighted = self._weights * observed_rrs
        self._observed_rrs = observed_rrs
        self._observed_rrs_weighted = observed_rrs_weighted
        self._observed_sum_weighted = np.clip(np.sum(observed_rrs_weighted), 1e-12, None)
        self._observed_norm = np.sqrt(
            np.clip(np.sum(observed_rrs_weighted * observed_rrs), 1e-24, None))

    def _filter_spectrum(self, spectra):
        return np.dot(spectra, self._normalised_sensor_filter_t)

    def _filter_spectra_batch(self, spectra_batch):
        return np.dot(spectra_batch, self._normalised_sensor_filter_t)

    def _run_forward_model(self, parameters):
        rrs, rrs_dchl, rrs_dcdom, rrs_dnap, rrs_ddepth, rrs_dfrac1, rrs_dfrac2, rrs_dfrac3 = \
            _forward_model_kernel(
                float(parameters[0]),
                float(parameters[1]),
                float(parameters[2]),
                float(parameters[3]),
                float(parameters[4]),
                float(parameters[5]),
                float(parameters[6]),
                self._a_ph_star,
                self._a_cdom_star,
                self._a_nap_star,
                self._bb_ph_star,
                self._bb_nap_star,
                self._a_water,
                self._bb_water,
                self._substrate1,
                self._substrate2,
                self._substrate3,
                self._inv_cos_theta_w,
                self._inv_cos_theta_0,
                self._one_over_pi,
            )
        return _ObjectiveModelResults(
            rrs=rrs,
            rrs_dchl=rrs_dchl,
            rrs_dcdom=rrs_dcdom,
            rrs_dnap=rrs_dnap,
            rrs_ddepth=rrs_ddepth,
            rrs_dfrac1=rrs_dfrac1,
            rrs_dfrac2=rrs_dfrac2,
            rrs_dfrac3=rrs_dfrac3,
        )

    def __call__(self, parameters):
        """
        Returns an objective score for the given parameter set.

        Args:
            parameters (ndarray): The parameter array in the order
                (chl, cdom, nap, depth, substrate_fraction)
                as defined in the FreeParameters tuple
          
        """

        # TODO: do I need to implement this? Here or in a subclass?
        # To support algorithms without support for boundary values, we assign a high
        # score to out of range parameters. This may not be the best approach!!!
        # p_bounds is a tuple of (min, max) pairs for each parameter in p
        '''
        if p_bounds is not None:
            for _p, lu in zip(p, p_bounds):
                l, u = lu
                if _p < l or _p > u:
                    return 100000.0
        '''

        # Select the substrate pair from the list of substrates
        #id1 = self._fixed_parameters.substrate_combinations[self.id][0]
        #id2 = self._fixed_parameters.substrate_combinations[self.id][1]

        if self._observed_rrs is None:
            raise ValueError("SciPyObjective.observed_rrs must be set before evaluation.")

        model_results = self._run_forward_model(parameters)

        closed_rrs = self._filter_spectrum(model_results.rrs)
        derivative_arrays = self._filter_spectra_batch(np.vstack((
            model_results.rrs_dchl,
            model_results.rrs_dcdom,
            model_results.rrs_dnap,
            model_results.rrs_ddepth,
            model_results.rrs_dfrac1,
            model_results.rrs_dfrac2,
            model_results.rrs_dfrac3,
        )))

        observed_rrs = self._observed_rrs
        observed_rrs_weighted = self._observed_rrs_weighted
        observed_sum_weighted = self._observed_sum_weighted
        observed_norm = self._observed_norm
        weights = self._weights

        residual = closed_rrs - observed_rrs
        residual_weighted = weights * residual
        modelled_rrs_weighted = weights * closed_rrs

        residual_norm = np.sqrt(np.clip(np.sum(residual_weighted * residual), 1e-24, None))
        model_norm = np.sqrt(np.clip(np.sum(modelled_rrs_weighted * closed_rrs), 1e-24, None))
        weighted_dot = np.sum(observed_rrs_weighted * closed_rrs)
        cosine = weighted_dot / (model_norm * observed_norm)
        cosine = np.clip(cosine, 0.0, 1.0)
        cosine_divisor = np.sqrt(np.clip(1.0 - cosine * cosine, 1e-24, None))
        A = residual_norm / observed_sum_weighted
        B = np.arccos(cosine)
        F = model_norm * observed_norm

        model_norm_derivative = np.dot(derivative_arrays, modelled_rrs_weighted) / model_norm
        F_derivative = observed_norm * model_norm_derivative
        E_derivative = np.dot(derivative_arrays, observed_rrs_weighted)
        G_derivative = (E_derivative * F - F_derivative * weighted_dot) / (F * F)
        grad_alpha = -G_derivative / cosine_divisor
        residual_derivative = np.dot(derivative_arrays, residual_weighted) / residual_norm
        grad_lsq = residual_derivative / observed_sum_weighted

        if self._error_func_name == 'alpha_f':
            jacobian = (B * grad_lsq) + (A * grad_alpha)
            errorValue = A * B
        elif self._error_func_name == 'alpha':
            jacobian = grad_alpha
            errorValue = B
        elif self._error_func_name in ('f', 'lsq'):
            jacobian = grad_lsq
            errorValue = A
        else:
            raise ValueError(f"Unsupported error function '{self._error_func_name}'")

        
        #return self._error_func(self.observed_rrs, closed_rrs, self._nedr),jacobian
        #print("A*B{0}  A*B{1:.6f} A*B{2:.6f} A*B{3:.6f} A*B{4:.6f} A*B{5:.6f} A*B{6:.6f} A*B{7:.6f}".format(A*B,jacobian[0],jacobian[1],jacobian[2],jacobian[3],jacobian[4],jacobian[5],jacobian[6]))
        #print("Z{0}  {1:.6f} {2:.6f} {3:.6f} {4:.6f} {5:.6f} {6:.6f}".format(parameters[0],parameters[1],parameters[2],parameters[3],parameters[4],parameters[5],parameters[6]))
        #print("A{0}  B{1:.6f} C{2:.6f} D{3:.6f} E{4:.6f} F{5:.6f} G{6:.6f}".format(A,B,C,D,E,F,G))
        return errorValue,jacobian
