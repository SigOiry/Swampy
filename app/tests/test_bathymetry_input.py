from pathlib import Path
import sys

import numpy as np
import pytest


pytest.importorskip("future")
pytest.importorskip("scipy")
pytest.importorskip("xlrd")
rasterio = pytest.importorskip("rasterio")

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))

from rasterio.crs import CRS
from rasterio.transform import from_origin

import launch_swampy as swampy


def _write_test_bathy(path, data, transform, crs):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=-9999.0,
    ) as dataset:
        dataset.write(data.astype("float32"), 1)


def test_load_bathy_raster_to_image_grid_skips_reproject_when_crs_matches(tmp_path, monkeypatch):
    bathy_path = tmp_path / "same_crs_bathy.tif"
    src_transform = from_origin(0.0, 4.0, 1.0, 1.0)
    dst_transform = from_origin(0.0, 4.0, 2.0, 2.0)
    crs = CRS.from_epsg(4326)
    data = np.arange(16, dtype="float32").reshape(4, 4)
    _write_test_bathy(bathy_path, data, src_transform, crs)

    def fail_reproject(*args, **kwargs):
        raise AssertionError("reproject() should not be used when bathy CRS matches the image CRS")

    monkeypatch.setattr(rasterio.warp, "reproject", fail_reproject)

    with rasterio.open(bathy_path) as src:
        result = swampy._load_bathy_raster_to_image_grid(
            src,
            width=2,
            height=2,
            dst_transform=dst_transform,
            dst_crs=crs,
        )

    assert result.shape == (2, 2)
    assert result.dtype == np.float32
    assert np.all(np.isfinite(result))


def test_load_bathy_raster_to_image_grid_reprojects_when_crs_differs(tmp_path, monkeypatch):
    bathy_path = tmp_path / "different_crs_bathy.tif"
    src_transform = from_origin(0.0, 4.0, 1.0, 1.0)
    src_crs = CRS.from_epsg(4326)
    dst_crs = CRS.from_epsg(3857)
    dst_transform = from_origin(0.0, 400000.0, 200000.0, 200000.0)
    data = np.arange(16, dtype="float32").reshape(4, 4)
    _write_test_bathy(bathy_path, data, src_transform, src_crs)

    calls = {"count": 0}

    def fake_reproject(*args, **kwargs):
        calls["count"] += 1
        kwargs["destination"][:] = 7.0

    monkeypatch.setattr(rasterio.warp, "reproject", fake_reproject)

    with rasterio.open(bathy_path) as src:
        result = swampy._load_bathy_raster_to_image_grid(
            src,
            width=2,
            height=2,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
        )

    assert calls["count"] == 1
    assert np.all(result == np.float32(7.0))
