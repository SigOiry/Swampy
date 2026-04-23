from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals)
from builtins import *

from pkg_resources import resource_filename

import numpy as np
from scipy.io import readsav

import sambuca as sb


def _expected_terms(observed, modelled, nedr):
    weights = 1.0 / np.asarray(nedr, dtype=float)
    weights = np.clip(weights, 1e-12, None)
    observed = np.asarray(observed, dtype=float)
    modelled = np.asarray(modelled, dtype=float)

    alpha_num = np.sum(weights * observed * modelled)
    alpha_den = np.sqrt(np.sum(weights * observed * observed)) * np.sqrt(np.sum(weights * modelled * modelled))
    alpha = np.arccos(np.clip(alpha_num / alpha_den, 0.0, 1.0))

    lsq = np.sqrt(np.sum(weights * (modelled - observed) ** 2)) / np.sum(weights * observed)
    return alpha, alpha * lsq, lsq


class TestErrorNoise(object):
    """ Error function tests, with noise. """

    def setup_method(self, method):
        self.data = readsav(
            resource_filename(
                sb.__name__,
                'tests/data/noise_error_data.sav'))

    def validate_data(self, data):
        observed = self.data['realrrs']
        modelled = self.data['rrs']
        noise = self.data['noiserrs']

        assert len(modelled) == len(observed)
        assert len(noise) == len(observed)

    def test_error_all(self):
        observed = self.data['realrrs']
        modelled = self.data['rrs']
        nedr = self.data['noiserrs']
        expected_distance_a, expected_distance_af, expected_lsq = _expected_terms(observed, modelled, nedr)

        actual = sb.error_all(observed, modelled, nedr)

        assert np.allclose(actual.alpha, expected_distance_a)
        assert np.allclose(actual.alpha_f, expected_distance_af)
        assert np.allclose(actual.f, expected_lsq)
        assert np.allclose(actual.lsq, expected_lsq)

    def test_distance_alpha(self):
        observed = self.data['realrrs']
        modelled = self.data['rrs']
        nedr = self.data['noiserrs']
        expected = _expected_terms(observed, modelled, nedr)[0]

        actual = sb.distance_alpha(observed, modelled, nedr)

        assert np.allclose(actual, expected)

    def test_distance_alpha_f(self):
        observed = self.data['realrrs']
        modelled = self.data['rrs']
        nedr = self.data['noiserrs']
        expected = _expected_terms(observed, modelled, nedr)[1]

        actual = sb.distance_alpha_f(observed, modelled, nedr)

        assert np.allclose(actual, expected)

    def test_distance_f(self):
        observed = self.data['realrrs']
        modelled = self.data['rrs']
        nedr = self.data['noiserrs']
        expected = _expected_terms(observed, modelled, nedr)[2]

        actual = sb.distance_f(observed, modelled, nedr)

        assert np.allclose(actual, expected)

    def test_distance_lsq(self):
        observed = self.data['realrrs']
        modelled = self.data['rrs']
        nedr = self.data['noiserrs']
        expected = _expected_terms(observed, modelled, nedr)[2]

        actual = sb.distance_lsq(observed, modelled, nedr)

        assert np.allclose(actual, expected)
