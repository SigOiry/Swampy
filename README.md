# SWAMpy

SWAMpy is a GUI-driven shallow-water inversion workflow for retrieving bathymetry, water-column properties, and benthic cover from aquatic reflectance imagery stored in NetCDF format.

The project is designed to be practical to run by non-developers: most configuration now happens through popups in the interface instead of editing XML files by hand. At the same time, the repository still keeps the original XML-driven workflow so runs can be reproduced exactly from saved log files.

## What The Workflow Does

For each pixel, SWAMpy fits a forward optical model to the observed reflectance and estimates:

- bathymetry / depth
- chlorophyll concentration
- CDOM concentration
- NAP concentration
- benthic target fractions from the selected spectral library

Depending on the options enabled, the workflow can also export modeled reflectance, spectral post-processing products, slope derived from retrieved bathymetry, and debug layers used to understand difficult areas.

## Project Layout

The main folders in this repository are:

- `Swampy_paralell/`: main code, GUI, launcher, inversion core, tests
- `Data/SRF/`: bundled sensor templates
- `Data/spectral_library/`: persistent benthic spectral library used by the GUI
- `Data/Bathy/`: bundled EMOD bathymetry
- `Data/Test/`: sample NetCDF scenes and validation data
- `docs/`: bundled papers and project references

Keep this folder structure unchanged. The GUI expects these relative paths.

## Requirements

The recommended setup uses Conda and the provided environment file:

```powershell
conda env create -f environment.yml
conda activate SwampySim
```

If the environment already exists and you want to refresh it:

```powershell
conda env update -f environment.yml --prune
conda activate SwampySim
```

The environment includes the main numerical and geospatial dependencies used by the workflow:

- Python 3.11
- NumPy
- SciPy
- pandas
- netCDF4
- rasterio
- spectral
- xmltodict
- dicttoxml
- tqdm
- numba
- tkinter

## How To Launch The App

From the repository root:

```powershell
python Swampy_paralell/launch_swampy.py
```

This opens the graphical interface.

You can also rerun a saved configuration directly from a log XML:

```powershell
python Swampy_paralell/launch_swampy.py -f path\\to\\log_file.xml
```

Useful optional command-line overrides:

- `--format netcdf|geotiff|both`
- `--bathy path\\to\\bathy.tif`
- `--nedr-mode scene|fixed`
- `-c 1`
  This keeps one CPU free for the OS. Increase it if you want SWAMpy to use fewer workers.

## Typical Workflow

1. Launch the GUI.
2. Select one or more input `.nc` images.
3. Select an output folder.
4. Open `Water & Bottom settings` and choose at least two target spectra.
5. Open `Sensor` and choose the sensor plus the bands to use.
6. Choose whether bathymetry is:
   - estimated by the inversion, or
   - provided as an input GeoTIFF, or
   - taken from the bundled EMOD bathymetry.
7. Set parameter bounds in the `Parameters` tab.
8. Configure optional processing features.
9. Run the workflow.

The `Load settings` button at the bottom of the GUI can restore a previous `log_*.xml` file and apply the same configuration to a new image.

## Input Expectations

The main workflow expects an input image in NetCDF format. In practice:

- the file should contain latitude and longitude variables
- the reflectance bands should be stored as spectral layers that SWAMpy can identify and align to the selected sensor
- if the reflectance is above-water remote-sensing reflectance (`Rrs`), enable the `Above RRS` option

Optional inputs:

- bathymetry GeoTIFF
- saved run XML / log XML

Bundled examples are available in `Data/Test/`.

## Main Features

### GUI-Driven Configuration

The interface exposes the main workflow settings through popups rather than raw XML editing:

- water and bottom settings
- sensor configuration
- bathymetry mode
- advanced processing options

### Persistent Spectral Library Management

The spectral library used for benthic targets is editable from the GUI:

- add a new spectrum from a two-column CSV
- modify an existing spectrum name
- assign tags to group spectra in the selection popup
- remove one or several spectra

These edits are written back to `Data/spectral_library/Spectral_Library.csv`, with a backup of the original library created on first modification.

### Persistent Sensor Management

The sensor popup supports:

- bundled Sentinel-2 and PRISMA templates
- adding a new sensor XML to `Data/SRF`
- removing custom sensors from the same folder
- smart band selection for sensors with many bands

### Bathymetry Modes

SWAMpy supports several bathymetry strategies:

- estimate bathymetry directly from the inversion
- use an external bathymetry raster to constrain or fix depth
- use the bundled EMOD bathymetry from `Data/Bathy/E4_2024.tif`

## Important Note About EMOD Bathymetry

The bundled EMOD bathymetry product is too large to be hosted directly in the GitHub repository.

If you are using a GitHub copy of this project, download the EMOD bathymetry from:

`[link]`

Then place the downloaded file here:

- `Data/Bathy/E4_2024.tif`

Without that file, the `EMODnet` option in the GUI will not be available.

### False-Deep Bathymetry Correction

When bathymetry is estimated, SWAMpy can apply a second correction pass to suspicious low-SDI deep pixels by using surrounding confident pixels to stabilize the solution and reduce unrealistic jumps between neighboring depths.

### Initial Guess Optimisation

The workflow can test multiple starting values before the main minimization to improve convergence on difficult pixels. Optional debug outputs can show which starting values were chosen.

### Relaxed And Fully Relaxed Substrate Modes

The workflow supports:

- strict substrate constraints
- relaxed substrate constraints
- fully relaxed mode that exports raw substrate values

### Output Products

Depending on the selected options, SWAMpy can export:

- NetCDF
- GeoTIFF
- both formats at once
- modeled reflectance in the selected sensor bands
- optional post-processing spectral outputs
- slope derived from the retrieved bathymetry

### Large Scene Handling

Image splitting by row chunks is available for scenes that are too large to process comfortably in one pass.

## Sample Data And Validation

The repository includes sample scenes and validation resources in `Data/Test/`.

There is also a validation helper script:

```powershell
python Swampy_paralell/validate_swampy_cover.py --help
```

This script compares SWAMpy substrate outputs against point-based ground truth.

## Scientific Background

This repository includes several relevant references in `docs/`. They are worth reading if you want to understand the optical background of the inversion rather than just run the software:

- [Lee et al. (2001), *Properties of the water column and bottom derived from Airborne Visible Infrared Imaging Spectrometer (AVIRIS) data*](docs/Journal%20of%20Geophysical%20Research%20%20Oceans%20-%202001%20-%20Lee%20-%20Properties%20of%20the%20water%20column%20and%20bottom%20derived%20from%20Airborne.pdf)
- [Bundled reference paper](docs/1-s2.0-S003442570800360X-main.pdf)
- [Bundled reference paper](docs/AO.38.003831.pdf)
- [Bundled reference paper](docs/s11001-005-0266-y.pdf)

These files are included directly in the repository so the theoretical background is available offline with the code.

## Notes

- The GUI prevents running when mandatory fields are missing.
- The launcher, GUI, and data layout have been updated from the older project structure, so the current entry point is `Swampy_paralell/launch_swampy.py`.
- If you share the repository, include the `Data/` folder because it contains the spectral library, sensor templates, bundled bathymetry, and sample data required by the app.
