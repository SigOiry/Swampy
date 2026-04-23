from pathlib import Path
import sys

import numpy as np


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import launch_swampy as swampy


def test_scene_nedr_never_below_xml_default():
    base = np.array([0.0010, 0.0015, 0.0020], dtype="float32")[:, None, None]
    cube = np.broadcast_to(base, (3, 40, 40)).copy()
    default_nedr = (
        np.array([443.0, 560.0, 704.0], dtype="float32"),
        np.array([0.0006, 0.0007, 0.0008], dtype="float32"),
    )

    estimated, info = swampy._estimate_scene_nedr(cube, default_nedr)

    assert isinstance(estimated, tuple)
    assert np.allclose(estimated[1], default_nedr[1])
    assert info["candidate_pixel_count"] > 0
    assert not info["applied"]


def test_scene_nedr_increases_when_dark_water_is_noisy():
    rng = np.random.default_rng(42)
    cube = np.empty((3, 60, 60), dtype="float32")
    means = np.array([0.0015, 0.0010, 0.0005], dtype="float32")
    noise = np.array([0.0010, 0.0014, 0.0018], dtype="float32")
    for band in range(3):
        cube[band] = means[band] + rng.normal(0.0, noise[band], size=(60, 60)).astype("float32")

    default_nedr = (
        np.array([443.0, 560.0, 704.0], dtype="float32"),
        np.array([0.0002, 0.0002, 0.0002], dtype="float32"),
    )
    bathy = np.full((60, 60), 20.0, dtype="float32")

    estimated, info = swampy._estimate_scene_nedr(cube, default_nedr, bathy_arr=bathy)

    assert isinstance(estimated, tuple)
    assert np.all(estimated[1] >= default_nedr[1])
    assert np.any(estimated[1] > default_nedr[1])
    assert info["candidate_pixel_count"] >= info["effective_min_pixels"]
    assert info["selection_note"] == "bathymetry-assisted dark homogeneous deep-water pixels"
