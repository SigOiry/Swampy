from pathlib import Path
import sys

import numpy as np
from netCDF4 import Dataset


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import validate_swampy_cover as validator


def test_validate_swampy_cover_smoke(tmp_path, monkeypatch):
    repo_root = Path(__file__).resolve().parents[2]
    sample_input = repo_root / "Data" / "Test" / "S2A_MSI_2019_07_11_11_18_13_T30TVT_L2W_sample.nc"
    shapefile = repo_root / "Data" / "Test" / "validation_dataset.shp"
    siop_xml = repo_root / "Data" / "Test" / "new_input_sub.xml"

    output_nc = tmp_path / "synthetic_swampy_output.nc"
    with Dataset(sample_input) as src, Dataset(output_nc, "w") as dst:
        dst.createDimension("y", len(src.dimensions["y"]))
        dst.createDimension("x", len(src.dimensions["x"]))

        lat = dst.createVariable("lat", "f4", ("y", "x"))
        lon = dst.createVariable("lon", "f4", ("y", "x"))
        lat[:] = src.variables["lat"][:]
        lon[:] = src.variables["lon"][:]

        yy, xx = np.meshgrid(
            np.linspace(0.0, 1.0, len(src.dimensions["y"]), dtype="float32"),
            np.linspace(0.0, 1.0, len(src.dimensions["x"]), dtype="float32"),
            indexing="ij",
        )
        sargassum = dst.createVariable("sargassum", "f4", ("y", "x"), fill_value=-999.0)
        zostera = dst.createVariable("100__zostera", "f4", ("y", "x"), fill_value=-999.0)
        sand = dst.createVariable("sand", "f4", ("y", "x"), fill_value=-999.0)
        sargassum.long_name = "Sargassum"
        zostera.long_name = "100% zostera"
        sand.long_name = "sand"
        sargassum[:] = 0.20 + 0.05 * xx
        zostera[:] = 0.30 + 0.10 * yy
        sand[:] = np.clip(1.0 - (sargassum[:] + zostera[:]), 0.0, 1.0)

    out_dir = tmp_path / "validation_outputs"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_swampy_cover.py",
            str(output_nc),
            "--ground-truth",
            str(shapefile),
            "--siop-xml",
            str(siop_xml),
            "--out-dir",
            str(out_dir),
        ],
    )

    exit_code = validator.main()

    assert exit_code == 0
    assert (out_dir / "vegetation_scatter.svg").is_file()
    assert (out_dir / "sand_scatter.svg").is_file()
    assert (out_dir / "validation_samples.csv").is_file()
