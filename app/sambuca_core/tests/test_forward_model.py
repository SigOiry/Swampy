# -*- coding: utf-8 -*-
from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

import math

import numpy as np

import sambuca_core as sbc


class TestForwardModel(object):
    def setup_method(self, method):
        self.wav = np.array([440.0, 550.0, 660.0], dtype=float)
        self.a_water = np.array([0.12, 0.08, 0.21], dtype=float)
        self.a_ph_star = np.array([0.020, 0.011, 0.006], dtype=float)
        self.substrate1 = np.array([0.40, 0.36, 0.20], dtype=float)
        self.substrate2 = np.array([0.12, 0.18, 0.22], dtype=float)
        self.substrate3 = np.array([0.05, 0.08, 0.14], dtype=float)

        self.chl = 0.9
        self.cdom = 0.07
        self.nap = 0.6
        self.depth = 3.8
        self.sub1_frac = 0.2
        self.sub2_frac = 0.3
        self.sub3_frac = 0.5

        self.a_cdom_slope = 0.0157
        self.a_nap_slope = 0.0106
        self.bb_ph_slope = 0.681
        self.bb_nap_slope = None
        self.lambda0cdom = 440.0
        self.lambda0nap = 440.0
        self.lambda0x = 542.0
        self.x_ph_lambda0x = 0.00038
        self.x_nap_lambda0x = 0.0054
        self.a_cdom_lambda0cdom = 1.0
        self.a_nap_lambda0nap = 0.0048
        self.bb_lambda_ref = 500.0
        self.water_refractive_index = 1.33784
        self.theta_air = 45.0
        self.off_nadir = 15.0
        self.q_factor = np.pi

    def _run_model(self, bb_nap_slope=None, q_factor=None):
        return sbc.forward_model(
            chl=self.chl,
            cdom=self.cdom,
            nap=self.nap,
            depth=self.depth,
            sub1_frac=self.sub1_frac,
            sub2_frac=self.sub2_frac,
            sub3_frac=self.sub3_frac,
            substrate1=self.substrate1,
            substrate2=self.substrate2,
            substrate3=self.substrate3,
            wavelengths=self.wav,
            a_water=self.a_water,
            a_ph_star=self.a_ph_star,
            num_bands=len(self.wav),
            a_cdom_slope=self.a_cdom_slope,
            a_nap_slope=self.a_nap_slope,
            bb_ph_slope=self.bb_ph_slope,
            bb_nap_slope=bb_nap_slope,
            lambda0cdom=self.lambda0cdom,
            lambda0nap=self.lambda0nap,
            lambda0x=self.lambda0x,
            x_ph_lambda0x=self.x_ph_lambda0x,
            x_nap_lambda0x=self.x_nap_lambda0x,
            a_cdom_lambda0cdom=self.a_cdom_lambda0cdom,
            a_nap_lambda0nap=self.a_nap_lambda0nap,
            bb_lambda_ref=self.bb_lambda_ref,
            water_refractive_index=self.water_refractive_index,
            theta_air=self.theta_air,
            off_nadir=self.off_nadir,
            q_factor=self.q_factor if q_factor is None else q_factor,
        )

    def _manual_solution(self, bb_nap_slope=None):
        bb_nap_slope = self.bb_ph_slope if bb_nap_slope is None else bb_nap_slope

        inv_refractive_index = 1.0 / self.water_refractive_index
        theta_w = math.asin(inv_refractive_index * math.sin(math.radians(self.theta_air)))
        theta_o = math.asin(inv_refractive_index * math.sin(math.radians(self.off_nadir)))

        bb_water = (0.00194 / 2.0) * np.power(self.bb_lambda_ref / self.wav, 4.32)
        a_cdom_star = self.a_cdom_lambda0cdom * np.exp(-self.a_cdom_slope * (self.wav - self.lambda0cdom))
        a_nap_star = self.a_nap_lambda0nap * np.exp(-self.a_nap_slope * (self.wav - self.lambda0nap))
        bb_ph_star = self.x_ph_lambda0x * np.power(self.lambda0x / self.wav, self.bb_ph_slope)
        bb_nap_star = self.x_nap_lambda0x * np.power(self.lambda0x / self.wav, bb_nap_slope)

        a_ph = self.chl * self.a_ph_star
        a_cdom = self.cdom * a_cdom_star
        a_nap = self.nap * a_nap_star
        a_total = self.a_water + a_ph + a_cdom + a_nap

        bb_ph = self.chl * bb_ph_star
        bb_nap = self.nap * bb_nap_star
        bb_total = bb_water + bb_ph + bb_nap

        r_substratum = (
            self.sub1_frac * self.substrate1
            + self.sub2_frac * self.substrate2
            + self.sub3_frac * self.substrate3
        )

        kappa = a_total + bb_total
        u = bb_total / kappa
        du_column = 1.03 * np.sqrt(1.0 + (2.40 * u))
        du_bottom = 1.04 * np.sqrt(1.0 + (5.40 * u))
        rrsdp = (0.084 + 0.17 * u) * u

        inv_cos_theta_w = 1.0 / math.cos(theta_w)
        inv_cos_theta_o = 1.0 / math.cos(theta_o)
        du_column_scaled = du_column * inv_cos_theta_o
        du_bottom_scaled = du_bottom * inv_cos_theta_o

        kd = kappa * inv_cos_theta_w
        kuc = kappa * du_column_scaled
        kub = kappa * du_bottom_scaled

        kappa_d = kappa * self.depth
        rrs = (
            rrsdp * (1.0 - np.exp(-(inv_cos_theta_w + du_column_scaled) * kappa_d))
            + ((1.0 / math.pi) * r_substratum * np.exp(-(inv_cos_theta_w + du_bottom_scaled) * kappa_d))
        )

        return {
            'bb_water': bb_water,
            'a_cdom_star': a_cdom_star,
            'a_nap_star': a_nap_star,
            'bb_ph_star': bb_ph_star,
            'bb_nap_star': bb_nap_star,
            'a_ph': a_ph,
            'a_cdom': a_cdom,
            'a_nap': a_nap,
            'a_total': a_total,
            'bb_ph': bb_ph,
            'bb_nap': bb_nap,
            'bb_total': bb_total,
            'r_substratum': r_substratum,
            'rrsdp': rrsdp,
            'kd': kd,
            'kuc': kuc,
            'kub': kub,
            'rrs': rrs,
        }

    def test_forward_model_matches_manual_equations(self):
        results = self._run_model()
        expected = self._manual_solution()

        assert np.allclose(results.bb_water, expected['bb_water'])
        assert np.allclose(results.a_cdom_star, expected['a_cdom_star'])
        assert np.allclose(results.a_nap_star, expected['a_nap_star'])
        assert np.allclose(results.bb_ph_star, expected['bb_ph_star'])
        assert np.allclose(results.bb_nap_star, expected['bb_nap_star'])
        assert np.allclose(results.a_ph, expected['a_ph'])
        assert np.allclose(results.a_cdom, expected['a_cdom'])
        assert np.allclose(results.a_nap, expected['a_nap'])
        assert np.allclose(results.a, expected['a_total'])
        assert np.allclose(results.bb_ph, expected['bb_ph'])
        assert np.allclose(results.bb_nap, expected['bb_nap'])
        assert np.allclose(results.bb, expected['bb_total'])
        assert np.allclose(results.r_substratum, expected['r_substratum'])
        assert np.allclose(results.rrsdp, expected['rrsdp'])
        assert np.allclose(results.kd, expected['kd'])
        assert np.allclose(results.kuc, expected['kuc'])
        assert np.allclose(results.kub, expected['kub'])
        assert np.allclose(results.rrs, expected['rrs'])

    def test_q_factor_scales_r_zero_minus_outputs(self):
        q_factor = 3.5
        results = self._run_model(q_factor=q_factor)
        assert np.allclose(results.r_0_minus, results.rrs * q_factor)
        assert np.allclose(results.rdp_0_minus, results.rrsdp * q_factor)

    def test_bb_nap_slope_defaults_to_bb_ph_slope(self):
        default_results = self._run_model()
        explicit_results = self._run_model(bb_nap_slope=self.bb_ph_slope)
        assert np.allclose(default_results.bb_nap_star, explicit_results.bb_nap_star)

    def test_substrate_mixture_uses_all_three_fractions(self):
        results = self._run_model()
        expected = (
            self.sub1_frac * self.substrate1
            + self.sub2_frac * self.substrate2
            + self.sub3_frac * self.substrate3
        )
        assert np.allclose(results.r_substratum, expected)
