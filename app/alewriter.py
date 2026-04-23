""" Pixel result handler that writes model outputs to numpy arrays.
"""


from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

import numpy as np
import sambuca_core as sbc


from sambuca.error import error_all


def _nearest_wavelength_value(values, wavelengths, reference_wavelength):
    wavelengths = np.asarray(wavelengths, dtype=float).reshape(-1)
    values = np.asarray(values).reshape(-1)
    if wavelengths.size == 0 or values.size == 0:
        return np.nan

    index = int(np.argmin(np.abs(wavelengths - reference_wavelength)))
    return float(values[index])


class AleWriter():

    def __init__(
            self,
            height,
            width,
            sensor_filter,
            nedr,
            fixed_parameters):
        """
        Initialise the ArrayWriter.
        Args:
            width (int): Width in pixels of the modelled region.
            height (int): Height in pixels of the modelled region.
            sensor_filter (array-like): The Sambuca sensor filter.
            nedr (array-like): Noise equivalent difference in reflectance.
            fixed_parameters (sambuca.AllParameters): The fixed model
                parameters.
        """

        self._width = width
        self._height = height
        self._fixed_parameters = fixed_parameters

        # check for being passed the (wavelengths, filter) tuple loaded by the
        # sambuca_core sensor_filter loading functions
        if isinstance(sensor_filter, tuple) and len(sensor_filter) == 2:
            self._sensor_filter = sensor_filter[1]
        else:
            self._sensor_filter = sensor_filter
        if isinstance(nedr, tuple) and len(nedr) == 2:
            self._nedr = nedr[1]
        else:
            self._nedr = nedr

        self._num_modelled_bands = self._sensor_filter.shape[1]
        self._num_observed_bands = self._sensor_filter.shape[0]

        # initialise the ndarrays for the outputs.
        # Note that I am hard-coding these outputs for now, but the intent is that this class
        # support a customisable list of outputs.

    def __call__(self, x, y, observed_rrs, parameters=None, nit=None, success=None):
        """
        Called by the parameter estimator when there is a result for a pixel.

        Args:
            x (int): The pixel x coordinate.
            y (int): The pixel y coordinate.
            observed_rrs (array-like): The observed remotely-sensed reflectance
                at this pixel.
            parameters (sambuca.FreeParameters): If the pixel converged,
                this contains the final parameters; otherwise None.
            id (int): The substrate combination index
            nit (int): The number of iterations performed
            success (bool): If the optimizer exited successfully
        """


        # If this pixel did not converge, then there is nothing more to do
        if not parameters:
            return

        # Select the substrate pair from the list of substrates
        #id1 = self._fixed_parameters.substrate_combinations[id][0]
        #id2 = self._fixed_parameters.substrate_combinations[id][1]

        # Generate results from the given parameters
        model_results = sbc.forward_model(
            parameters.chl,
            parameters.cdom,
            parameters.nap,
            parameters.depth,
            parameters.sub1_frac,
            parameters.sub2_frac,
            parameters.sub3_frac,
            self._fixed_parameters.substrates[0],
            self._fixed_parameters.substrates[1],
            self._fixed_parameters.substrates[2],
            self._fixed_parameters.wavelengths,
            self._fixed_parameters.a_water,
            self._fixed_parameters.a_ph_star,
            self._fixed_parameters.num_bands,
            a_cdom_slope=self._fixed_parameters.a_cdom_slope,
            a_nap_slope=self._fixed_parameters.a_nap_slope,
            bb_ph_slope=self._fixed_parameters.bb_ph_slope,
            bb_nap_slope=self._fixed_parameters.bb_nap_slope,
            lambda0cdom=self._fixed_parameters.lambda0cdom,
            lambda0nap=self._fixed_parameters.lambda0nap,
            lambda0x=self._fixed_parameters.lambda0x,
            x_ph_lambda0x=self._fixed_parameters.x_ph_lambda0x,
            x_nap_lambda0x=self._fixed_parameters.x_nap_lambda0x,
            a_cdom_lambda0cdom=self._fixed_parameters.a_cdom_lambda0cdom,
            a_nap_lambda0nap=self._fixed_parameters.a_nap_lambda0nap,
            bb_lambda_ref=self._fixed_parameters.bb_lambda_ref,
            water_refractive_index=self._fixed_parameters.water_refractive_index,
            theta_air=self._fixed_parameters.theta_air,
            off_nadir=self._fixed_parameters.off_nadir,
            q_factor=self._fixed_parameters.q_factor)
       
        # set reference band in nm for scalar Kd / substrate outputs
        kd_out = _nearest_wavelength_value(
            model_results.kd,
            self._fixed_parameters.wavelengths,
            560.0)

        """r_substratum = sbc.apply_sensor_filter(
            model_results.r_substratum,
            self._sensor_filter)"""
        r_sub_out = _nearest_wavelength_value(
            model_results.r_substratum,
            self._fixed_parameters.wavelengths,
            560.0)
      

        closed_rrs = sbc.apply_sensor_filter(
            model_results.rrs,
            self._sensor_filter)
                
        closed_rrsdp = sbc.apply_sensor_filter(
            model_results.rrsdp,
            self._sensor_filter)
        
        sdi = np.max(np.absolute(closed_rrs - closed_rrsdp) / self._nedr)

        error = error_all(observed_rrs, closed_rrs, self._nedr)

        # Write the results into our arrays
        self.error_alpha = error.alpha
        self.error_alpha_f = error.alpha_f
        self.error_f = error.f
        self.error_lsq = error.lsq
        self.chl = parameters.chl
        self.cdom = parameters.cdom
        self.nap = parameters.nap
        self.depth = parameters.depth
        self.sub1_frac = parameters.sub1_frac
        self.sub2_frac = parameters.sub2_frac
        self.sub3_frac = parameters.sub3_frac
        self.closed_rrs = closed_rrs
        self.nit = nit
        self.success = success
        # New outputs
        self.kd = kd_out
        self.sdi = sdi
        self.r_sub=r_sub_out
