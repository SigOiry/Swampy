"""Validate SWAMpy substrate cover outputs against point ground truth.

This script compares:
- true vegetation cover vs retrieved vegetation cover
- true sand cover vs retrieved sand cover

Ground truth is read from the provided shapefile and SWAMpy output is sampled
at the point locations using nearest-neighbour extraction.

Outputs:
- `vegetation_scatter.svg`
- `sand_scatter.svg`
- `validation_samples.csv`
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import struct
from pathlib import Path

import numpy as np
import xmltodict
from netCDF4 import Dataset
from scipy.spatial import cKDTree


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_DIR = REPO_ROOT / "Data" / "Test"
DEFAULT_SHAPEFILE = DEFAULT_TEST_DIR / "validation_dataset.shp"
DEFAULT_SIOP_XML = DEFAULT_TEST_DIR / "new_input_sub.xml"
OUTPUT_NODATA = -999.0

SAND_KEYWORDS = ("sand",)
VEGETATION_KEYWORDS = (
    "veg",
    "vegetation",
    "seagrass",
    "grass",
    "zostera",
    "posidonia",
    "algae",
    "alga",
    "sargassum",
    "kelp",
    "macrophyte",
    "ulva",
    "caulerpa",
)
IGNORED_GROUND_TRUTH_FIELDS = {"label", "id"}


def _clean_label(label: str, fallback_index: int) -> str:
    label = "" if label is None else str(label)
    if ":" in label:
        label = label.split(":")[-1]
    label = label.strip()
    return label or f"Substrate {fallback_index + 1}"


def _make_safe_var_name(label: str, index: int) -> str:
    candidate = re.sub(r"\s+", "_", label.strip())
    candidate = re.sub(r"[^0-9A-Za-z_]", "_", candidate)
    if not candidate:
        candidate = f"{index + 1}"
    return candidate.lower()


def _parse_siop_substrates(xml_path: Path) -> list[dict]:
    with xml_path.open("rb") as stream:
        root = xmltodict.parse(stream.read()).get("root", {})
    raw_names = root.get("substrate_names", {}).get("item", [])
    if not isinstance(raw_names, list):
        raw_names = [raw_names] if raw_names else []

    substrates = []
    for index, raw_name in enumerate(raw_names):
        label = _clean_label(raw_name, index)
        substrates.append(
            {
                "index": index,
                "label": label,
                "var_name": _make_safe_var_name(label, index),
            }
        )
    return substrates


def _matches_any_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    text = text.lower()
    return any(keyword in text for keyword in keywords)


def _split_substrates_by_cover_type(substrates: list[dict]) -> tuple[list[dict], list[dict]]:
    sand = []
    vegetation = []
    for substrate in substrates:
        label = substrate["label"]
        var_name = substrate["var_name"]
        if _matches_any_keyword(label, SAND_KEYWORDS) or _matches_any_keyword(var_name, SAND_KEYWORDS):
            sand.append(substrate)
        else:
            vegetation.append(substrate)
    return vegetation, sand


def _read_dbf_table(dbf_path: Path) -> tuple[list[tuple], list[dict]]:
    with dbf_path.open("rb") as stream:
        header = stream.read(32)
        _, _, _, _, num_records, header_len, record_len = struct.unpack("<BBBBIHH20x", header)
        fields = []
        while stream.tell() < header_len:
            descriptor = stream.read(32)
            if not descriptor or descriptor[0] == 0x0D:
                break
            name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", errors="ignore")
            field_type = chr(descriptor[11])
            length = descriptor[16]
            decimals = descriptor[17]
            fields.append((name, field_type, length, decimals))

        stream.seek(header_len)
        rows = []
        for _ in range(num_records):
            record = stream.read(record_len)
            if not record:
                break
            if record[0:1] == b"*":
                continue
            position = 1
            row = {}
            for name, field_type, length, decimals in fields:
                raw = record[position:position + length]
                position += length
                text = raw.decode("latin1", errors="ignore").strip()
                if field_type == "N":
                    try:
                        row[name] = float(text) if text and "*" not in text else None
                    except ValueError:
                        row[name] = None
                else:
                    row[name] = text
            rows.append(row)
    return fields, rows


def _read_point_shapes(shp_path: Path) -> list[tuple[float, float]]:
    with shp_path.open("rb") as stream:
        stream.read(100)
        points = []
        while True:
            record_header = stream.read(8)
            if not record_header:
                break
            record_length = struct.unpack(">i", record_header[4:8])[0] * 2
            record = stream.read(record_length)
            if len(record) < 4:
                break
            shape_type = struct.unpack("<i", record[:4])[0]
            if shape_type == 1:
                x_coord, y_coord = struct.unpack("<2d", record[4:20])
                points.append((x_coord, y_coord))
            elif shape_type == 8:
                num_points = struct.unpack("<i", record[36:40])[0]
                if num_points < 1:
                    points.append((math.nan, math.nan))
                else:
                    x_coord, y_coord = struct.unpack("<2d", record[40:56])
                    points.append((x_coord, y_coord))
            else:
                raise ValueError(f"Unsupported shapefile geometry type: {shape_type}")
    return points


def _load_ground_truth_points(shapefile_path: Path) -> list[dict]:
    _, rows = _read_dbf_table(shapefile_path.with_suffix(".dbf"))
    points = _read_point_shapes(shapefile_path)
    if len(rows) != len(points):
        raise ValueError(
            f"Ground-truth table/geometry length mismatch: {len(rows)} records vs {len(points)} shapes"
        )

    samples = []
    for row, (x_coord, y_coord) in zip(rows, points):
        sample = dict(row)
        sample["lon"] = float(x_coord)
        sample["lat"] = float(y_coord)
        samples.append(sample)
    return samples


def _ground_truth_cover_fields(samples: list[dict]) -> tuple[list[str], list[str]]:
    if not samples:
        return [], []

    numeric_fields = []
    for key in samples[0]:
        if key.lower() in IGNORED_GROUND_TRUTH_FIELDS or key in {"lon", "lat"}:
            continue
        if isinstance(samples[0].get(key), (int, float)) or samples[0].get(key) is None:
            numeric_fields.append(key)

    sand_fields = [field for field in numeric_fields if _matches_any_keyword(field, SAND_KEYWORDS)]
    vegetation_fields = [field for field in numeric_fields if field not in sand_fields]
    vegetation_fields = [field for field in vegetation_fields if field.lower() not in IGNORED_GROUND_TRUTH_FIELDS]
    return vegetation_fields, sand_fields


def _find_coordinate_variables(dataset: Dataset) -> tuple[np.ndarray, np.ndarray]:
    lat_candidates = ("lat", "latitude", "lat_grid", "latitudes")
    lon_candidates = ("lon", "longitude", "lon_grid", "longitudes")

    lat = None
    lon = None
    for name in lat_candidates:
        if name in dataset.variables:
            lat = np.asarray(dataset.variables[name][:], dtype="float64")
            break
    for name in lon_candidates:
        if name in dataset.variables:
            lon = np.asarray(dataset.variables[name][:], dtype="float64")
            break

    if lat is None or lon is None:
        raise KeyError("Could not find `lat`/`lon` variables in the SWAMpy NetCDF output.")

    if lat.ndim == 1 and lon.ndim == 1:
        lon_grid, lat_grid = np.meshgrid(lon, lat)
        return lat_grid, lon_grid
    if lat.shape != lon.shape:
        raise ValueError(f"Lat/Lon shape mismatch: {lat.shape} vs {lon.shape}")
    return lat, lon


def _build_netcdf_sampler(output_path: Path) -> tuple[callable, dict]:
    dataset = Dataset(output_path)
    lat_grid, lon_grid = _find_coordinate_variables(dataset)
    tree = cKDTree(np.column_stack([lon_grid.ravel(), lat_grid.ravel()]))

    variable_map = {}
    for name, var in dataset.variables.items():
        if len(getattr(var, "dimensions", ())) != 2:
            continue
        variable_map[name.lower()] = name
        long_name = getattr(var, "long_name", "")
        if long_name:
            variable_map[str(long_name).strip().lower()] = name

    def sample_point(lon_value: float, lat_value: float, key: str) -> float:
        key_lower = key.lower()
        if key_lower not in variable_map:
            raise KeyError(f"Variable/band `{key}` not found in NetCDF output.")
        distance, flat_index = tree.query([lon_value, lat_value], k=1)
        row, col = np.unravel_index(int(flat_index), lat_grid.shape)
        value = float(dataset.variables[variable_map[key_lower]][row, col])
        if not np.isfinite(value) or value <= OUTPUT_NODATA:
            return math.nan
        return value

    return sample_point, {"close": dataset.close, "type": "netcdf"}


def _build_geotiff_sampler(output_path: Path) -> tuple[callable, dict]:
    import rasterio

    dataset = rasterio.open(output_path)
    band_map = {}
    for index, description in enumerate(dataset.descriptions, start=1):
        if description:
            band_map[description.strip().lower()] = index

    def sample_point(lon_value: float, lat_value: float, key: str) -> float:
        key_lower = key.lower()
        if key_lower not in band_map:
            raise KeyError(f"Band `{key}` not found in GeoTIFF output.")
        row, col = dataset.index(lon_value, lat_value)
        if row < 0 or row >= dataset.height or col < 0 or col >= dataset.width:
            return math.nan
        value = float(dataset.read(band_map[key_lower], window=((row, row + 1), (col, col + 1)))[0, 0])
        nodata = dataset.nodata
        if nodata is not None and np.isfinite(nodata) and value == nodata:
            return math.nan
        if not np.isfinite(value):
            return math.nan
        return value

    return sample_point, {"close": dataset.close, "type": "geotiff"}


def _build_output_sampler(output_path: Path) -> tuple[callable, dict]:
    suffix = output_path.suffix.lower()
    if suffix == ".nc":
        return _build_netcdf_sampler(output_path)
    if suffix in {".tif", ".tiff"}:
        return _build_geotiff_sampler(output_path)
    raise ValueError(f"Unsupported output type `{suffix}`. Use a SWAMpy NetCDF or GeoTIFF output.")


def _output_lookup_keys(substrate: dict, output_type: str) -> list[str]:
    if output_type == "netcdf":
        return [substrate["var_name"], substrate["label"]]
    return [substrate["label"], substrate["var_name"]]


def _sum_sampled_cover(sample_point: callable, lon_value: float, lat_value: float, key_groups: list[list[str]]) -> float:
    values = []
    for key_group in key_groups:
        value = math.nan
        for key in key_group:
            try:
                value = sample_point(lon_value, lat_value, key)
                break
            except KeyError:
                continue
        values.append(value)
    finite_values = [value for value in values if np.isfinite(value)]
    if not finite_values:
        return math.nan
    return float(np.sum(finite_values))


def _format_stat(value: float) -> str:
    return "nan" if not np.isfinite(value) else f"{value:.2f}"


def _build_stats(true_values: np.ndarray, retrieved_values: np.ndarray) -> dict:
    diff = retrieved_values - true_values
    rmse = float(np.sqrt(np.mean(diff ** 2))) if diff.size else math.nan
    bias = float(np.mean(diff)) if diff.size else math.nan
    corr = math.nan
    if diff.size >= 2:
        centered_true = true_values - np.mean(true_values)
        centered_retrieved = retrieved_values - np.mean(retrieved_values)
        denom = np.sqrt(np.sum(centered_true ** 2) * np.sum(centered_retrieved ** 2))
        if denom > 0:
            corr = float(np.sum(centered_true * centered_retrieved) / denom)
    return {"n": int(diff.size), "rmse": rmse, "bias": bias, "r": corr}


def _write_scatter_svg(path: Path, title: str, x_label: str, y_label: str,
                       true_values: np.ndarray, retrieved_values: np.ndarray) -> None:
    axis_max = max(
        100.0,
        float(np.nanmax(np.concatenate([true_values, retrieved_values]))) if true_values.size else 100.0,
    )
    axis_max = math.ceil(axis_max / 10.0) * 10.0

    width = 800
    height = 760
    margin_left = 90
    margin_right = 40
    margin_top = 70
    margin_bottom = 90
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    def x_to_svg(value: float) -> float:
        return margin_left + (value / axis_max) * plot_width

    def y_to_svg(value: float) -> float:
        return margin_top + plot_height - (value / axis_max) * plot_height

    stats = _build_stats(true_values, retrieved_values)
    ticks = np.linspace(0.0, axis_max, 6)

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2:.1f}" y="34" text-anchor="middle" font-family="Arial" font-size="24">{title}</text>',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{margin_left + plot_width}" y2="{margin_top + plot_height}" stroke="black" stroke-width="2"/>',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" stroke="black" stroke-width="2"/>',
    ]

    for tick in ticks:
        x_pos = x_to_svg(float(tick))
        y_pos = y_to_svg(float(tick))
        lines.append(
            f'<line x1="{x_pos:.2f}" y1="{margin_top}" x2="{x_pos:.2f}" y2="{margin_top + plot_height}" stroke="#dddddd" stroke-width="1"/>'
        )
        lines.append(
            f'<line x1="{margin_left}" y1="{y_pos:.2f}" x2="{margin_left + plot_width}" y2="{y_pos:.2f}" stroke="#dddddd" stroke-width="1"/>'
        )
        lines.append(
            f'<text x="{x_pos:.2f}" y="{margin_top + plot_height + 24}" text-anchor="middle" font-family="Arial" font-size="14">{int(round(tick))}</text>'
        )
        lines.append(
            f'<text x="{margin_left - 12}" y="{y_pos + 5:.2f}" text-anchor="end" font-family="Arial" font-size="14">{int(round(tick))}</text>'
        )

    lines.append(
        f'<line x1="{x_to_svg(0):.2f}" y1="{y_to_svg(0):.2f}" x2="{x_to_svg(axis_max):.2f}" y2="{y_to_svg(axis_max):.2f}" stroke="#888888" stroke-width="2" stroke-dasharray="8 6"/>'
    )

    for true_value, retrieved_value in zip(true_values, retrieved_values):
        cx = x_to_svg(float(true_value))
        cy = y_to_svg(float(retrieved_value))
        lines.append(
            f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="5.5" fill="#1f77b4" fill-opacity="0.75" stroke="#0f3d66" stroke-width="1"/>'
        )

    stats_x = margin_left + plot_width - 12
    stats_y = margin_top + 18
    lines.append(
        f'<text x="{stats_x}" y="{stats_y}" text-anchor="end" font-family="Arial" font-size="14">n = {stats["n"]}</text>'
    )
    lines.append(
        f'<text x="{stats_x}" y="{stats_y + 20}" text-anchor="end" font-family="Arial" font-size="14">RMSE = {_format_stat(stats["rmse"])}</text>'
    )
    lines.append(
        f'<text x="{stats_x}" y="{stats_y + 40}" text-anchor="end" font-family="Arial" font-size="14">Bias = {_format_stat(stats["bias"])}</text>'
    )
    lines.append(
        f'<text x="{stats_x}" y="{stats_y + 60}" text-anchor="end" font-family="Arial" font-size="14">r = {_format_stat(stats["r"])}</text>'
    )

    lines.append(
        f'<text x="{width / 2:.1f}" y="{height - 24}" text-anchor="middle" font-family="Arial" font-size="18">{x_label}</text>'
    )
    lines.append(
        f'<text transform="translate(28 {height / 2:.1f}) rotate(-90)" text-anchor="middle" font-family="Arial" font-size="18">{y_label}</text>'
    )
    lines.append("</svg>")

    path.write_text("\n".join(lines), encoding="utf-8")


def _write_samples_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = [
        "label",
        "lon",
        "lat",
        "true_vegetation",
        "retrieved_vegetation",
        "true_sand",
        "retrieved_sand",
    ]
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate SWAMpy vegetation and sand cover against point ground truth.")
    parser.add_argument("output", help="SWAMpy output NetCDF or GeoTIFF")
    parser.add_argument("--ground-truth", default=str(DEFAULT_SHAPEFILE), help="Validation shapefile path")
    parser.add_argument("--siop-xml", default=str(DEFAULT_SIOP_XML), help="SIOP XML used for the SWAMpy run")
    parser.add_argument("--out-dir", default=None, help="Directory where SVG/CSV outputs will be written")
    args = parser.parse_args()

    output_path = Path(args.output).expanduser().resolve()
    shapefile_path = Path(args.ground_truth).expanduser().resolve()
    siop_xml_path = Path(args.siop_xml).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else output_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    vegetation_scatter_path = out_dir / "vegetation_scatter.svg"
    sand_scatter_path = out_dir / "sand_scatter.svg"
    samples_csv_path = out_dir / "validation_samples.csv"

    substrates = _parse_siop_substrates(siop_xml_path)
    vegetation_substrates, sand_substrates = _split_substrates_by_cover_type(substrates)
    if not vegetation_substrates:
        raise ValueError("Could not identify any vegetation substrate from the SIOP XML.")
    if not sand_substrates:
        raise ValueError("Could not identify any sand substrate from the SIOP XML.")

    ground_truth_samples = _load_ground_truth_points(shapefile_path)
    vegetation_fields, sand_fields = _ground_truth_cover_fields(ground_truth_samples)
    if not vegetation_fields:
        raise ValueError("Could not identify vegetation fields in the validation shapefile.")
    if not sand_fields:
        raise ValueError("Could not identify a sand field in the validation shapefile.")

    sample_point, sampler_info = _build_output_sampler(output_path)
    try:
        sampled_rows = []
        for sample in ground_truth_samples:
            true_vegetation = float(sum(sample.get(field, 0.0) or 0.0 for field in vegetation_fields))
            true_sand = float(sum(sample.get(field, 0.0) or 0.0 for field in sand_fields))

            retrieved_vegetation = _sum_sampled_cover(
                sample_point,
                sample["lon"],
                sample["lat"],
                [_output_lookup_keys(entry, sampler_info["type"]) for entry in vegetation_substrates],
            )
            retrieved_sand = _sum_sampled_cover(
                sample_point,
                sample["lon"],
                sample["lat"],
                [_output_lookup_keys(entry, sampler_info["type"]) for entry in sand_substrates],
            )

            sampled_rows.append(
                {
                    "label": sample.get("Label", ""),
                    "lon": sample["lon"],
                    "lat": sample["lat"],
                    "true_vegetation": true_vegetation,
                    "retrieved_vegetation": retrieved_vegetation * 100.0 if np.isfinite(retrieved_vegetation) else math.nan,
                    "true_sand": true_sand,
                    "retrieved_sand": retrieved_sand * 100.0 if np.isfinite(retrieved_sand) else math.nan,
                }
            )
    finally:
        sampler_info["close"]()

    vegetation_pairs = np.array(
        [
            [row["true_vegetation"], row["retrieved_vegetation"]]
            for row in sampled_rows
            if np.isfinite(row["true_vegetation"]) and np.isfinite(row["retrieved_vegetation"])
        ],
        dtype="float64",
    )
    sand_pairs = np.array(
        [
            [row["true_sand"], row["retrieved_sand"]]
            for row in sampled_rows
            if np.isfinite(row["true_sand"]) and np.isfinite(row["retrieved_sand"])
        ],
        dtype="float64",
    )

    if vegetation_pairs.size == 0:
        raise ValueError("No valid vegetation comparison points were found.")
    if sand_pairs.size == 0:
        raise ValueError("No valid sand comparison points were found.")

    _write_samples_csv(samples_csv_path, sampled_rows)
    _write_scatter_svg(
        vegetation_scatter_path,
        title="True Vegetation vs Retrieved Vegetation",
        x_label="True Vegetation (%)",
        y_label="Retrieved Vegetation (%)",
        true_values=vegetation_pairs[:, 0],
        retrieved_values=vegetation_pairs[:, 1],
    )
    _write_scatter_svg(
        sand_scatter_path,
        title="True Sand vs Retrieved Sand",
        x_label="True Sand (%)",
        y_label="Retrieved Sand (%)",
        true_values=sand_pairs[:, 0],
        retrieved_values=sand_pairs[:, 1],
    )

    print(f"Vegetation fields: {', '.join(vegetation_fields)}")
    print(f"Sand fields: {', '.join(sand_fields)}")
    print(f"Retrieved vegetation substrates: {', '.join(entry['label'] for entry in vegetation_substrates)}")
    print(f"Retrieved sand substrates: {', '.join(entry['label'] for entry in sand_substrates)}")
    print(f"Wrote: {vegetation_scatter_path}")
    print(f"Wrote: {sand_scatter_path}")
    print(f"Wrote: {samples_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
