from pathlib import Path
import sys

import numpy as np
from netCDF4 import Dataset


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import gui_swampy
import image_io


def _write_polymer_style_hdf(path):
    with Dataset(path, "w", format="NETCDF4") as dataset:
        dataset.createDimension("row", 4)
        dataset.createDimension("col", 5)
        lat = dataset.createVariable("latitude", "f4", ("row", "col"))
        lon = dataset.createVariable("longitude", "f4", ("row", "col"))
        rows = np.linspace(42.0, 42.3, 4, dtype="float32")[:, None]
        cols = np.linspace(-4.0, -3.6, 5, dtype="float32")[None, :]
        lat[:, :] = rows + np.zeros((4, 5), dtype="float32")
        lon[:, :] = cols + np.zeros((4, 5), dtype="float32")

        values = {
            "Rw443": 0.04,
            "Rw490": 0.10,
            "Rw560": 0.20,
            "Rw665": 0.30,
            "Rw705": 0.07,
            "Rw740": 0.05,
        }
        for name, value in values.items():
            band = dataset.createVariable(name, "f4", ("row", "col"))
            band[:, :] = np.full((4, 5), value, dtype="float32")

        dataset.createVariable("Rgli", "f4", ("row", "col"))[:, :] = 1.0
        dataset.createVariable("Rnir", "f4", ("row", "col"))[:, :] = 1.0
        dataset.createVariable("bitmask", "i2", ("row", "col"))[:, :] = 0
        dataset.createVariable("logchl", "f4", ("row", "col"))[:, :] = -1.0


def test_polymer_rw_bands_are_detected_from_hdf_extension(tmp_path):
    path = tmp_path / "polymer_scene.hdf"
    _write_polymer_style_hdf(path)

    info = gui_swampy._load_input_image_band_info(str(path))

    assert info["source_kind"] == "stacked_2d"
    assert info["band_count"] == 6
    assert info["wavelengths"] == [443.0, 490.0, 560.0, 665.0, 705.0, 740.0]
    assert all(label.startswith("Rw") for label in info["labels"])


def test_polymer_hdf_sentinel_preview_uses_rgb_wavelengths(tmp_path):
    path = tmp_path / "polymer_scene.hdf"
    _write_polymer_style_hdf(path)

    preview, preview_info = gui_swampy._load_preview_band_from_netcdf(
        str(path),
        sensor_name="Sentinel-2",
    )

    assert preview_info["preview_mode"] == "rgb"
    assert preview.shape == (4, 5, 3)
    assert np.allclose(preview[..., 0], 0.30)
    assert np.allclose(preview[..., 1], 0.20)
    assert np.allclose(preview[..., 2], 0.10)
    assert preview_info["lat_grid"].shape == (4, 5)
    assert preview_info["lon_grid"].shape == (4, 5)


def test_polymer_auxiliary_layers_are_not_reflectance_bands():
    assert image_io.is_rrs_band_variable("Rw443")
    assert not image_io.is_rrs_band_variable("Rgli")
    assert not image_io.is_rrs_band_variable("Rnir")
    assert image_io.is_auxiliary_scene_variable("logchl")
    assert image_io.stable_dimension_name("fakeDim6", "row") == "row"
