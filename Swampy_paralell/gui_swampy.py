# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 15:14:44 2019

@author: marco
"""

import datetime
import os
import sys
import tkinter as tk
import xml.etree.ElementTree as ET
from tkinter import BooleanVar, StringVar, W
from tkinter import messagebox
from tkinter import ttk
from tkinter.filedialog import askdirectory, askopenfilename, askopenfilenames

try:
    import siop_config
except ImportError:  # pragma: no cover - fallback when imported as a package
    from Swampy_paralell import siop_config

try:
    import sensor_config
except ImportError:  # pragma: no cover - fallback when imported as a package
    from Swampy_paralell import sensor_config


PLOT_COLORS = [
    "#0f6cbd",
    "#198754",
    "#cc5500",
    "#6c757d",
]


def _xml_find_text(node, path, default=None):
    if node is None:
        return default
    found = node.find(path)
    if found is None or found.text is None:
        return default
    text = str(found.text).strip()
    return text if text != "" else default


def _xml_find_items(node, path):
    parent = node.find(path) if node is not None else None
    if parent is None:
        return []
    values = []
    for child in parent.findall("./item"):
        if child.text is None:
            continue
        text = str(child.text).strip()
        if text != "":
            values.append(text)
    return values


def _parse_bool_text(value, default=False):
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _paths_equivalent(path_a, path_b):
    if not path_a or not path_b:
        return False
    try:
        return os.path.normcase(os.path.normpath(os.path.abspath(path_a))) == os.path.normcase(os.path.normpath(os.path.abspath(path_b)))
    except Exception:
        return False


def _resolve_bundled_resource(repo_root, path):
    if not path:
        return path
    if os.path.exists(path):
        return path

    basename = os.path.basename(path)
    candidate_map = {
        "new_input_sub.xml": [
            os.path.join(repo_root, "Data", "Templates", "new_input_sub.xml"),
            os.path.join(repo_root, "Data", "Test", "new_input_sub.xml"),
        ],
        "new_inputs_posidonia_sand_sargassum.xml": [
            os.path.join(repo_root, "Data", "Templates", "new_inputs_posidonia_sand_sargassum.xml"),
        ],
        "swampy_s2_5_bands_filter_nedr.xml": [
            os.path.join(repo_root, "Data", "SRF", "swampy_s2_5_bands_filter_nedr.xml"),
            os.path.join(repo_root, "Test", "swampy_s2_5_bands_filter_nedr.xml"),
            os.path.join(repo_root, "Data", "Test", "swampy_s2_5_bands_filter_nedr.xml"),
        ],
        "swampy_s2_6_bands_filter_nedr.xml": [
            os.path.join(repo_root, "Data", "SRF", "swampy_s2_6_bands_filter_nedr.xml"),
        ],
        "swampy_prisma_63_bands_filter_nedr.xml": [
            os.path.join(repo_root, "Data", "SRF", "swampy_prisma_63_bands_filter_nedr.xml"),
        ],
        "E4_2024.tif": [
            os.path.join(repo_root, "Data", "Bathy", "E4_2024.tif"),
            os.path.join(repo_root, "Bathy", "E4_2024.tif"),
        ],
    }
    for candidate in candidate_map.get(basename, []):
        if os.path.exists(candidate):
            return candidate
    generic_candidates = [
        os.path.join(repo_root, "Data", "SRF", basename),
        os.path.join(repo_root, "Data", "Templates", basename),
        os.path.join(repo_root, "Data", "Bathy", basename),
        os.path.join(repo_root, "Test", basename),
        os.path.join(repo_root, "Data", "Test", basename),
    ]
    for candidate in generic_candidates:
        if os.path.exists(candidate):
            return candidate
    return path


def _match_clean_name(name, candidates):
    if not name:
        return None
    clean_target = siop_config.clean_label(name).lower()
    for candidate in candidates:
        if siop_config.clean_label(candidate).lower() == clean_target:
            return candidate
    return name if name in candidates else None


def _match_band_indices_by_center(template, centers, max_delta_nm=1.0):
    selected = []
    used = set()
    for center in centers:
        try:
            target_center = float(center)
        except (TypeError, ValueError):
            continue
        best_index = None
        best_delta = None
        for band in template["bands"]:
            index = int(band["index"])
            if index in used:
                continue
            delta = abs(float(band["center"]) - target_center)
            if best_delta is None or delta < best_delta:
                best_index = index
                best_delta = delta
        if best_index is not None and best_delta is not None and best_delta <= max_delta_nm:
            selected.append(best_index)
            used.add(best_index)
    return selected


def _format_sensor_centers(centers, limit=8):
    labels = [f"{center:.0f}" if float(center).is_integer() else f"{center:.1f}" for center in centers]
    if len(labels) <= limit:
        return ", ".join(labels)
    return ", ".join(labels[:limit]) + ", ..."


def _display_input_selection(files):
    files = [str(path) for path in files if str(path).strip()]
    if not files:
        return ""
    if len(files) == 1:
        return files[0]
    return f"{files[0]} (+{len(files) - 1} more)"


def _infer_output_folder_from_output_file(output_file):
    if not output_file:
        return ""
    try:
        output_file = os.path.abspath(str(output_file))
        run_dir = os.path.dirname(output_file)
        run_dir_name = os.path.basename(run_dir).lower()
        if run_dir_name.startswith("swampy_run_"):
            return os.path.dirname(run_dir)
        return run_dir
    except Exception:
        return os.path.dirname(str(output_file))


def _draw_spectra_preview(canvas, spectral_library, selected_names, hovered_name=None):
    canvas.delete("all")
    width = max(canvas.winfo_width(), 620)
    height = max(canvas.winfo_height(), 300)
    left, top, right, bottom = 58, 18, 18, 42
    plot_width = width - left - right
    plot_height = height - top - bottom

    ordered_names = []
    for name in selected_names:
        if name in spectral_library["spectra"] and name not in ordered_names:
            ordered_names.append(name)
    if hovered_name and hovered_name in spectral_library["spectra"] and hovered_name not in ordered_names:
        ordered_names.append(hovered_name)

    if not ordered_names:
        canvas.create_text(
            width / 2,
            height / 2,
            text="Hover a spectrum to preview it.",
            fill="#666666",
            font=("Segoe UI", 10),
        )
        return

    series = [(name, spectral_library["spectra"][name][1]) for name in ordered_names]
    wavelengths = spectral_library["wavelengths"]
    x_min = min(wavelengths)
    x_max = max(wavelengths)
    y_min = min(min(values) for _name, values in series)
    y_max = max(max(values) for _name, values in series)
    if y_max <= y_min:
        y_max = y_min + 1.0
    padding = (y_max - y_min) * 0.05
    if padding == 0:
        padding = 0.05
    y_min -= padding
    y_max += padding

    canvas.create_rectangle(left, top, left + plot_width, top + plot_height, outline="#cccccc", width=1)

    for tick_fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + plot_height - (tick_fraction * plot_height)
        tick_value = y_min + (tick_fraction * (y_max - y_min))
        canvas.create_line(left - 5, y, left, y, fill="#444444")
        canvas.create_text(left - 8, y, text=f"{tick_value:.2f}", anchor="e", fill="#444444", font=("Segoe UI", 8))

    for tick_fraction in (0.0, 0.5, 1.0):
        x = left + (tick_fraction * plot_width)
        tick_value = x_min + (tick_fraction * (x_max - x_min))
        canvas.create_line(x, top + plot_height, x, top + plot_height + 5, fill="#444444")
        canvas.create_text(x, top + plot_height + 16, text=f"{tick_value:.0f}", anchor="n", fill="#444444", font=("Segoe UI", 8))

    canvas.create_text(left + (plot_width / 2), height - 8, text="Wavelength (nm)", fill="#444444", font=("Segoe UI", 9))
    canvas.create_text(15, top + (plot_height / 2), text="Reflectance", angle=90, fill="#444444", font=("Segoe UI", 9))

    def scale_x(value):
        if x_max == x_min:
            return left + (plot_width / 2)
        return left + ((value - x_min) / (x_max - x_min) * plot_width)

    def scale_y(value):
        return top + ((y_max - value) / (y_max - y_min) * plot_height)

    color_map = {}
    for index, name in enumerate(selected_names):
        color_map[name] = PLOT_COLORS[index % len(PLOT_COLORS)]
    if hovered_name and hovered_name not in color_map:
        color_map[hovered_name] = PLOT_COLORS[len(selected_names) % len(PLOT_COLORS)]

    legend_y = top + 10
    for name, values in series:
        points = []
        for wavelength, value in zip(wavelengths, values):
            points.extend((scale_x(wavelength), scale_y(value)))
        is_hover_only = hovered_name == name and name not in selected_names
        line_width = 3 if is_hover_only else 2
        canvas.create_line(points, fill=color_map.get(name, "#0f6cbd"), width=line_width, smooth=False)
        canvas.create_line(left + 10, legend_y, left + 34, legend_y, fill=color_map.get(name, "#0f6cbd"), width=line_width)
        suffix = " (hover)" if is_hover_only else ""
        canvas.create_text(left + 42, legend_y, text=f"{name}{suffix}", anchor="w", fill="#222222", font=("Segoe UI", 9))
        legend_y += 16


def gui():
    root = tk.Tk()
    root.title("SWAMpy | Input Configuration")
    root.geometry("1080x760")
    root.minsize(920, 680)

    try:
        style = ttk.Style()
        if sys.platform.startswith("win"):
            style.theme_use("vista")
        else:
            style.theme_use("clam")
        style.configure("TLabel", padding=4)
        style.configure("TEntry", padding=2)
        style.configure("TButton", padding=6)
        style.configure("TLabelframe", padding=10)
        style.configure("TLabelframe.Label", font=("Segoe UI", 10, "bold"))
    except Exception:
        pass

    cancelled = False
    compiled_siop = None
    compiled_sensor = None

    def on_close():
        nonlocal cancelled
        cancelled = True
        root.destroy()

    def center_window(window):
        window.update_idletasks()
        width = window.winfo_width()
        height = window.winfo_height()
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        x_pos = int((screen_width - width) / 2)
        y_pos = int((screen_height - height) / 3)
        window.geometry(f"{width}x{height}+{x_pos}+{y_pos}")

    cwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_template_path = _resolve_bundled_resource(
        cwd,
        os.path.join(cwd, "Data", "Templates", "new_input_sub.xml"),
    )
    default_library_path = os.path.join(cwd, "Data", "spectral_library", "Spectral_Library.csv")

    try:
        template_config = siop_config.load_template_config(default_template_path)
        spectral_library = siop_config.load_spectral_library(default_library_path)
    except Exception as exc:
        messagebox.showerror("Missing defaults", f"Unable to load the default Water & Bottom Settings.\n\n{exc}")
        root.destroy()
        return None

    def load_available_sensor_templates():
        sensor_template_paths = sensor_config.default_sensor_templates(cwd)
        loaded_templates = {}
        load_errors = {}
        for sensor_name, template_path in sensor_template_paths.items():
            try:
                loaded_templates[sensor_name] = sensor_config.load_sensor_template(template_path, sensor_name)
            except Exception as exc:
                load_errors[sensor_name] = str(exc)
        return loaded_templates, load_errors

    sensor_templates, sensor_load_errors = load_available_sensor_templates()

    if not sensor_templates:
        messagebox.showerror("Missing defaults", "Unable to load any bundled sensor templates from Data/SRF.")
        root.destroy()
        return None

    now = datetime.datetime.now()
    year = str(now.year)
    month = f"{now.month:02d}"
    day = f"{now.day:02d}"
    hour = f"{now.hour:02d}"
    minute = f"{now.minute:02d}"
    second = f"{now.second:02d}"

    input_files = []
    input_image_var = StringVar(value="")
    output_folder_var = StringVar(value=os.path.join(cwd, "Output"))

    selected_target_names = []
    scalar_values = dict(template_config["scalar_fields"])
    spectrum_override_paths = {"a_water": "", "a_ph_star": ""}
    sensor_state = {
        "sensor_name": "Sentinel-2" if "Sentinel-2" in sensor_templates else next(iter(sensor_templates)),
        "selected_indices": {
            sensor_name: sensor_config.default_selected_band_indices(template)
            for sensor_name, template in sensor_templates.items()
        },
    }

    def select_file_im():
        files = askopenfilenames(
            parent=root,
            title="Choose one or more input images (.nc)",
            filetypes=[("NetCDF files", "*.nc"), ("All files", "*.*")],
        )
        nonlocal input_files
        if files:
            input_files = list(files)
            input_image_var.set(_display_input_selection(input_files))
        else:
            input_files = []
            input_image_var.set("")

    def select_folder():
        folder = askdirectory(parent=root, title="Choose the output folder")
        if folder:
            output_folder_var.set(folder)

    def build_current_siop():
        return siop_config.build_siop_config(
            template_config,
            spectral_library,
            selected_target_names,
            scalar_values,
            spectrum_override_paths=spectrum_override_paths,
        )

    def build_current_sensor():
        sensor_name = sensor_state["sensor_name"]
        if sensor_name not in sensor_templates:
            raise ValueError(sensor_load_errors.get(sensor_name, f"No template is available for {sensor_name}."))
        return sensor_config.build_sensor_config(
            sensor_templates[sensor_name],
            sensor_state["selected_indices"].get(sensor_name, []),
        )

    siop_summary_var = StringVar()
    sensor_summary_var = StringVar()
    sub3_saved_bounds = {"min": "0", "max": "1"}
    false_deep_correction_config = {
        "anchor_min_sdi": 1.5,
        "anchor_max_depth_m": 8.0,
        "anchor_max_slope_percent": 10.0,
        "anchor_max_error_f": 0.003,
        "anchor_min_depth_margin_m": 0.5,
        "suspect_max_sdi": 1.0,
        "suspect_min_slope_percent": 10.0,
        "suspect_min_depth_jump_m": 2.0,
        "search_radius_px": 12,
        "min_anchor_count": 4,
        "correction_tolerance_m": 1.5,
        "max_patch_size_px": 64,
        "treat_min_depth_as_barrier": True,
        "barrier_depth_margin_m": 0.25,
        "barrier_min_sdi": 3.0,
        "debug_export": False,
    }

    def update_substrate_ui():
        try:
            current_siop = build_current_siop()
            substrate_names = current_siop["substrate_names"]
            actual_count = len(current_siop["actual_selected_targets"])
        except Exception:
            current_siop = None
            substrate_names = ["Substrate 1", "Substrate 2", siop_config.UNUSED_SUBSTRATE_NAME]
            actual_count = len(selected_target_names)

        label_sub1.configure(text=substrate_names[0])
        label_sub2.configure(text=substrate_names[1])
        label_sub3.configure(text=substrate_names[2])

        summary_names = selected_target_names
        if current_siop is not None:
            summary_names = current_siop["actual_selected_targets"]

        if actual_count <= 1:
            if sub3_min_var.get() == "0" and sub3_max_var.get() == "0":
                sub3_min_var.set(sub3_saved_bounds["min"])
                sub3_max_var.set(sub3_saved_bounds["max"])
            try:
                sub3_min_entry.state(["!disabled"])
                sub3_max_entry.state(["!disabled"])
            except Exception:
                sub3_min_entry.configure(state="normal")
                sub3_max_entry.configure(state="normal")
            if actual_count == 0:
                siop_summary_var.set("No target spectra selected. Select at least two target spectra.")
            else:
                siop_summary_var.set(f"1 target spectrum selected: {summary_names[0]}. Select at least two target spectra.")
        elif actual_count == 2:
            if not (sub3_min_var.get() == "0" and sub3_max_var.get() == "0"):
                sub3_saved_bounds["min"] = sub3_min_var.get()
                sub3_saved_bounds["max"] = sub3_max_var.get()
            sub3_min_var.set("0")
            sub3_max_var.set("0")
            try:
                sub3_min_entry.state(["disabled"])
                sub3_max_entry.state(["disabled"])
            except Exception:
                sub3_min_entry.configure(state="disabled")
                sub3_max_entry.configure(state="disabled")
            siop_summary_var.set(
                f"{actual_count} target spectra selected: {', '.join(summary_names)}. "
                "Third substrate is fixed to zero."
            )
        else:
            if sub3_min_var.get() == "0" and sub3_max_var.get() == "0":
                sub3_min_var.set(sub3_saved_bounds["min"])
                sub3_max_var.set(sub3_saved_bounds["max"])
            try:
                sub3_min_entry.state(["!disabled"])
                sub3_max_entry.state(["!disabled"])
            except Exception:
                sub3_min_entry.configure(state="normal")
                sub3_max_entry.configure(state="normal")
            siop_summary_var.set(f"{actual_count} target spectra selected: {', '.join(summary_names)}.")
        update_run_button_state()

    def update_sensor_ui():
        try:
            current_sensor = build_current_sensor()
            centers = [band["center"] for band in current_sensor["bands"]]
            sensor_summary_var.set(
                f"{current_sensor['sensor_name']}: {len(centers)} band(s) selected "
                f"({_format_sensor_centers(centers)})."
            )
        except Exception as exc:
            sensor_name = sensor_state["sensor_name"]
            sensor_summary_var.set(f"{sensor_name}: {exc}")
        update_run_button_state()

    run_button = None

    def _set_widget_enabled(widget, enabled):
        try:
            if enabled:
                widget.state(["!disabled"])
            else:
                widget.state(["disabled"])
        except Exception:
            widget.configure(state="normal" if enabled else "disabled")

    def _current_input_file_list():
        text = input_image_var.get().strip()
        if input_files:
            return [path for path in input_files if str(path).strip()]
        return [text] if text else []

    def _sync_input_files_with_entry(*_args):
        nonlocal input_files
        if input_files and input_image_var.get() != _display_input_selection(input_files):
            input_files = []
        update_run_button_state()

    def _get_form_validation_error():
        current_files = _current_input_file_list()
        if not current_files:
            return "Please select at least one input image (.nc)."
        for path in current_files:
            if not os.path.isfile(path):
                return f"Input image not found: {path}"

        if not output_folder_var.get().strip():
            return "Please choose an output folder."

        if bathy_mode.get() == "input":
            selected_bathy = bathy_path_var.get().strip() or _resolve_bundled_resource(
                cwd,
                os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"),
            )
            if not selected_bathy or not os.path.exists(selected_bathy):
                return "Please choose a valid bathymetry file."
            try:
                float(bathy_correction.get())
                float(bathy_tolerance.get())
            except ValueError:
                return "Bathymetry correction and tolerance must be numeric."

        try:
            build_current_siop()
        except Exception as exc:
            return f"Invalid Water & Bottom Settings: {exc}"

        try:
            build_current_sensor()
        except Exception as exc:
            return f"Invalid sensor setup: {exc}"

        numeric_pairs = [
            (chl_min_var, chl_max_var, "CHL"),
            (cdom_min_var, cdom_max_var, "CDOM"),
            (nap_min_var, nap_max_var, "NAP"),
            (depth_min_var, depth_max_var, "Depth"),
            (sub1_min_var, sub1_max_var, label_sub1.cget("text")),
            (sub2_min_var, sub2_max_var, label_sub2.cget("text")),
            (sub3_min_var, sub3_max_var, label_sub3.cget("text")),
        ]
        for vmin, vmax, name in numeric_pairs:
            try:
                vmin_f = float(vmin.get())
                vmax_f = float(vmax.get())
            except ValueError:
                return f"{name} bounds must be numeric."
            if vmax_f < vmin_f:
                return f"{name} max must be greater than or equal to min."

        if allow_split.get():
            chunk_value = chunk_rows.get().strip()
            if chunk_value:
                try:
                    rows_int = int(float(chunk_value))
                    if rows_int <= 0:
                        raise ValueError
                except ValueError:
                    return "Rows per chunk must be a positive number."

        return None

    def update_run_button_state(*_args):
        if run_button is None:
            return
        _set_widget_enabled(run_button, _get_form_validation_error() is None)

    def open_feature_popup(title, description, settings_builder=None, apply_callback=None,
                           geometry="760x300", minsize=(700, 240)):
        popup = tk.Toplevel(root)
        popup.title(title)
        popup.geometry(geometry)
        popup.minsize(*minsize)
        popup.transient(root)
        popup.grab_set()
        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)

        container = ttk.Frame(popup, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)

        ttk.Label(
            container,
            text=description,
            wraplength=max(minsize[0] - 40, 620),
            justify="left",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        if settings_builder is not None:
            settings_frame = ttk.Labelframe(container, text="Settings")
            settings_frame.grid(row=1, column=0, sticky="ew")
            settings_frame.columnconfigure(0, weight=1)
            settings_frame.columnconfigure(1, weight=1)
            settings_builder(settings_frame)

        actions = ttk.Frame(container)
        actions.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        actions.columnconfigure(0, weight=1)
        if apply_callback is None:
            ttk.Button(actions, text="Close", command=popup.destroy).grid(row=0, column=1, sticky="e")
        else:
            ttk.Button(actions, text="Cancel", command=popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))

            def apply_and_close():
                try:
                    apply_callback()
                except Exception:
                    return
                popup.destroy()

            ttk.Button(actions, text="Apply", command=apply_and_close).grid(row=0, column=2, sticky="e", padx=(8, 0))

        center_window(popup)
        popup.wait_window()

    def open_above_rrs_popup():
        open_feature_popup(
            "Above-water reflectance",
            "Use this when the input reflectance is above-water remote-sensing reflectance (Rrs). "
            "When enabled, the workflow converts the input to below-surface reflectance before the inversion. "
            "Leave it disabled if the image already contains below-surface reflectance.",
            geometry="720x220",
            minsize=(660, 200),
        )

    def open_shallow_water_popup():
        open_feature_popup(
            "Shallow water adjustment",
            "This option applies a second depth adjustment after the main optimisation. "
            "For pixels that still behave as optically deep, the workflow reduces the fitted depth toward the shallowest value that keeps the bottom effectively undetectable. "
            "Use it when you want a shallow-water-oriented depth product. Disable it if you prefer to keep the raw fitted depth.",
            geometry="760x240",
            minsize=(700, 220),
        )

    def open_false_deep_correction_popup():
        debug_export_var = BooleanVar(value=bool(false_deep_correction_config["debug_export"]))
        available = bathy_mode.get() == "estimate"

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "This feature is only applied when bathymetry is estimated by the workflow."
                    if available else
                    "This feature is unavailable while input bathymetry is selected. Switch bathymetry mode to estimate to enable it."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            debug_check = ttk.Checkbutton(
                settings_frame,
                text="Export debug rasters",
                variable=debug_export_var,
            )
            debug_check.grid(row=1, column=0, sticky="w")
            _set_widget_enabled(debug_check, available)

        def apply_false_deep_correction_changes():
            false_deep_correction_config["debug_export"] = bool(debug_export_var.get())

        open_feature_popup(
            "False-deep bathymetry correction",
            "This optional second pass works automatically.\n\n"
            "1. It marks confident pixels using fit quality, depth, SDI, and local continuity.\n"
            "2. It marks suspicious low-SDI deep pixels that look inconsistent with nearby confident water.\n"
            "3. It rebuilds local starting values from surrounding confident pixels.\n"
            "4. It re-optimises suspicious pixels with tight local bounds so adjacent depths stay physically coherent.",
            settings_builder=build_settings,
            apply_callback=apply_false_deep_correction_changes,
            geometry="780x340",
            minsize=(720, 280),
        )

    def open_initial_guess_popup():
        feature_enabled = bool(optimize_initial_guesses_flag.get())
        five_var = BooleanVar(value=bool(five_initial_guess_testing_flag.get()))
        debug_var = BooleanVar(value=bool(initial_guess_debug_flag.get()))

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "These settings are used only when 'Optimise initial guesses' is enabled on the main page."
                    if feature_enabled else
                    "Enable 'Optimise initial guesses' on the main page to use the options below."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            five_check = ttk.Checkbutton(
                settings_frame,
                text="Use 5 initial guess testing",
                variable=five_var,
            )
            five_check.grid(row=1, column=0, sticky="w", pady=(0, 4))
            debug_check = ttk.Checkbutton(
                settings_frame,
                text="Export initial guess debug GeoTIFF",
                variable=debug_var,
            )
            debug_check.grid(row=2, column=0, sticky="w")
            _set_widget_enabled(five_check, feature_enabled)
            _set_widget_enabled(debug_check, feature_enabled)

        def apply_initial_guess_changes():
            if not optimize_initial_guesses_flag.get():
                five_initial_guess_testing_flag.set(False)
                initial_guess_debug_flag.set(False)
                return
            five_initial_guess_testing_flag.set(bool(five_var.get()))
            initial_guess_debug_flag.set(bool(debug_var.get()))

        open_feature_popup(
            "Initial guess optimisation",
            "Before the main optimisation, the workflow can test several starting points for each pixel and keep the combination that gives the lowest forward-model error. "
            "Standard mode tests 3 values per variable. The optional 5-point mode also includes the user minimum and maximum bounds.",
            settings_builder=build_settings,
            apply_callback=apply_initial_guess_changes,
            geometry="780x320",
            minsize=(720, 260),
        )

    def open_relaxed_constraints_popup():
        feature_enabled = bool(relaxed.get())
        fully_relaxed_var = BooleanVar(value=bool(fully_relaxed_flag.get()))

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "Standard relaxed mode keeps the internal substrate-cover sum between 0.5 and 2.0, "
                    "but the exported target-cover maps are standardized back between 0 and 1 for the user.\n\n"
                    "If only two target spectra are active, SWAMpy reduces the problem to one cover variable "
                    "(x for target 1 and 1-x for target 2) to speed up the optimisation.\n\n"
                    "Fully relaxed mode removes the cross-target cover constraint completely and exports the raw fitted target values."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            fully_relaxed_check = ttk.Checkbutton(
                settings_frame,
                text="Use fully relaxed substrate mode",
                variable=fully_relaxed_var,
            )
            fully_relaxed_check.grid(row=1, column=0, sticky="w")
            _set_widget_enabled(fully_relaxed_check, feature_enabled)

        def apply_relaxed_changes():
            if not relaxed.get():
                fully_relaxed_flag.set(False)
                return
            fully_relaxed_flag.set(bool(fully_relaxed_var.get()))

        open_feature_popup(
            "Relaxed substrate constraints",
            "When enabled, the workflow stops enforcing a strict convex substrate mixture and uses a relaxed treatment instead.",
            settings_builder=build_settings,
            apply_callback=apply_relaxed_changes,
            geometry="800x340",
            minsize=(740, 280),
        )

    def open_post_processing_popup():
        open_feature_popup(
            "Post processing",
            "This computes and exports extra spectral products after the main inversion, including modeled reflectance, deep-water reflectance, spectral kd, substrate reflectance, absorption, and backscattering. "
            "It is useful for diagnostics, but it adds runtime and creates additional output files.",
            geometry="780x230",
            minsize=(720, 210),
        )

    def open_modeled_reflectance_popup():
        open_feature_popup(
            "Output modeled reflectance",
            "This exports the final fitted modeled reflectance in the selected sensor bands as a standalone multiband product. "
            "When enabled, SWAMpy writes both a GeoTIFF and a NetCDF file with georeferencing and wavelength metadata so the result can be opened easily in SNAP, ENVI, or GIS software. "
            "The product uses the final inversion result and the same sensor-band selection as the run. "
            "If 'Above RRS' is enabled, the export is converted to above-water reflectance so it matches the input convention.",
            geometry="800x250",
            minsize=(740, 220),
        )

    def open_split_popup():
        chunk_var = StringVar(value=chunk_rows.get())

        def build_settings(settings_frame):
            ttk.Label(
                settings_frame,
                text=(
                    "Image splitting processes the scene in row chunks to reduce peak memory usage. "
                    "Leave rows per chunk blank to let the workflow choose automatically."
                ),
                wraplength=620,
                justify="left",
            ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
            ttk.Label(settings_frame, text="Rows per chunk").grid(row=1, column=0, sticky="w")
            ttk.Entry(settings_frame, textvariable=chunk_var, width=12).grid(row=1, column=1, sticky="w")

        def apply_split_changes():
            value = chunk_var.get().strip()
            if value:
                try:
                    rows_int = int(float(value))
                    if rows_int <= 0:
                        raise ValueError
                except ValueError:
                    messagebox.showerror("Invalid value", "Rows per chunk must be a positive number.", parent=root)
                    raise
            chunk_rows.set(value)

        def apply_split_changes_and_handle():
            try:
                apply_split_changes()
            except Exception:
                raise

        open_feature_popup(
            "Image splitting",
            "Enable this for large scenes or low-memory machines. "
            "The workflow will process the image chunk by chunk instead of loading the whole scene at once. "
            "Smaller chunks reduce memory use but add overhead.",
            settings_builder=build_settings,
            apply_callback=apply_split_changes_and_handle,
            geometry="760x280",
            minsize=(700, 240),
        )

    def open_sensor_popup():
        nonlocal sensor_templates, sensor_load_errors

        popup = tk.Toplevel(root)
        popup.title("Sensor Configuration")
        popup_height = max(680, int(popup.winfo_screenheight() * 0.9))
        popup.geometry(f"980x{popup_height}")
        popup.minsize(900, 680)
        popup.transient(root)
        popup.grab_set()

        local_sensor_name = tk.StringVar(value=sensor_state["sensor_name"])
        local_selected_indices = {
            sensor_name: list(indices)
            for sensor_name, indices in sensor_state["selected_indices"].items()
        }

        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)

        popup_container = ttk.Frame(popup, padding=12)
        popup_container.grid(row=0, column=0, sticky="nsew")
        popup_container.columnconfigure(0, weight=0)
        popup_container.columnconfigure(1, weight=1)
        popup_container.rowconfigure(0, weight=1)
        popup_container.rowconfigure(1, weight=0)

        sensor_frame = ttk.Labelframe(popup_container, text="Sensor")
        sensor_frame.grid(row=0, column=0, sticky="nsw", padx=(0, 10), pady=(0, 8))
        sensor_frame.columnconfigure(0, weight=1)

        sensor_choice_frame = ttk.Frame(sensor_frame)
        sensor_choice_frame.grid(row=0, column=0, sticky="nsew")
        sensor_choice_frame.columnconfigure(0, weight=1)

        sensor_actions = ttk.Frame(sensor_frame)
        sensor_actions.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        sensor_actions.columnconfigure(2, weight=1)

        sensor_error_var = StringVar()
        ttk.Label(sensor_frame, textvariable=sensor_error_var, wraplength=240, justify="left").grid(
            row=2, column=0, sticky="w", pady=(6, 0)
        )

        selection_frame = ttk.Labelframe(popup_container, text="Bands")
        selection_frame.grid(row=0, column=1, sticky="nsew", pady=(0, 8))
        selection_frame.columnconfigure(0, weight=1)
        selection_frame.rowconfigure(1, weight=1)
        selection_frame.rowconfigure(2, weight=0)

        selection_status_var = StringVar()
        ttk.Label(selection_frame, textvariable=selection_status_var, wraplength=620, justify="left").grid(
            row=0, column=0, sticky="w", padx=4, pady=(0, 4)
        )

        bands_listbox = tk.Listbox(
            selection_frame,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            height=24,
            width=24,
        )
        bands_listbox.grid(row=1, column=0, sticky="nsew", padx=(4, 0), pady=(0, 8))
        bands_scroll = ttk.Scrollbar(selection_frame, orient="vertical", command=bands_listbox.yview)
        bands_scroll.grid(row=1, column=1, sticky="ns", pady=(0, 8))
        bands_listbox.configure(yscrollcommand=bands_scroll.set)

        quick_frame = ttk.Frame(selection_frame)
        quick_frame.grid(row=2, column=0, columnspan=2, sticky="ew", padx=4)
        for col_index in range(5):
            quick_frame.columnconfigure(col_index, weight=0)
        quick_frame.columnconfigure(5, weight=1)

        range_min_var = StringVar(value="400")
        range_max_var = StringVar(value="700")
        range_step_var = StringVar(value="1")

        smart_frame = ttk.Labelframe(selection_frame, text="Smart Selection")
        smart_frame.grid(row=3, column=0, columnspan=2, sticky="ew", padx=4, pady=(8, 0))
        for col_index in range(7):
            smart_frame.columnconfigure(col_index, weight=0)

        ttk.Label(smart_frame, text="Min nm").grid(row=0, column=0, sticky="w")
        ttk.Entry(smart_frame, textvariable=range_min_var, width=8).grid(row=0, column=1, sticky="w", padx=(4, 10))
        ttk.Label(smart_frame, text="Max nm").grid(row=0, column=2, sticky="w")
        ttk.Entry(smart_frame, textvariable=range_max_var, width=8).grid(row=0, column=3, sticky="w", padx=(4, 10))
        ttk.Label(smart_frame, text="Every nth band").grid(row=0, column=4, sticky="w")
        ttk.Entry(smart_frame, textvariable=range_step_var, width=6).grid(row=0, column=5, sticky="w", padx=(4, 10))

        def _ensure_local_selection_defaults():
            for sensor_name, template in sensor_templates.items():
                local_selected_indices.setdefault(
                    sensor_name,
                    sensor_config.default_selected_band_indices(template),
                )
            for sensor_name in list(local_selected_indices.keys()):
                if sensor_name not in sensor_templates:
                    del local_selected_indices[sensor_name]
            if sensor_templates and local_sensor_name.get() not in sensor_templates:
                local_sensor_name.set(next(iter(sensor_templates)))

        def current_template():
            sensor_name = local_sensor_name.get()
            return sensor_templates.get(sensor_name)

        def sync_current_selection():
            template = current_template()
            if template is None:
                return
            local_selected_indices[template["sensor_name"]] = list(bands_listbox.curselection())

        def update_selection_status():
            template = current_template()
            if template is None:
                selection_status_var.set("No template is available for this sensor in the current workspace.")
                sensor_error_var.set(sensor_load_errors.get(local_sensor_name.get(), ""))
                return
            selected = [template["bands"][idx]["center"] for idx in local_selected_indices.get(template["sensor_name"], [])]
            if selected:
                selection_status_var.set(
                    f"{template['sensor_name']}: {len(selected)} band(s) selected ({_format_sensor_centers(selected)})."
                )
            else:
                selection_status_var.set(f"{template['sensor_name']}: no bands selected.")
            sensor_error_var.set("")

        def refresh_sensor_buttons():
            for child in sensor_choice_frame.winfo_children():
                child.destroy()
            if not sensor_templates and not sensor_load_errors:
                ttk.Label(sensor_choice_frame, text="No sensor templates are available.").grid(row=0, column=0, sticky="w")
                return

            row_index = 0
            for sensor_name in sensor_templates.keys():
                ttk.Radiobutton(
                    sensor_choice_frame,
                    text=sensor_name,
                    value=sensor_name,
                    variable=local_sensor_name,
                ).grid(row=row_index, column=0, sticky="w", pady=(0, 4))
                row_index += 1

            for sensor_name, error_text in sensor_load_errors.items():
                ttk.Radiobutton(
                    sensor_choice_frame,
                    text=f"{sensor_name} (template unavailable)",
                    value=sensor_name,
                    variable=local_sensor_name,
                    state="disabled",
                ).grid(row=row_index, column=0, sticky="w", pady=(0, 4))
                row_index += 1

        def refresh_band_list():
            _ensure_local_selection_defaults()
            refresh_sensor_buttons()
            template = current_template()
            bands_listbox.delete(0, tk.END)
            if template is None:
                smart_frame.grid_remove()
                update_selection_status()
                return

            for band in template["bands"]:
                bands_listbox.insert(tk.END, band["label"])

            selected_indices = local_selected_indices.get(template["sensor_name"], [])
            for idx in selected_indices:
                if 0 <= idx < len(template["bands"]):
                    bands_listbox.selection_set(idx)

            if sensor_config.supports_smart_selection(template):
                smart_frame.grid()
            else:
                smart_frame.grid_remove()
            update_selection_status()

        def apply_selection(indices):
            template = current_template()
            if template is None:
                return
            deduped = sorted({int(idx) for idx in indices if 0 <= int(idx) < len(template["bands"])})
            local_selected_indices[template["sensor_name"]] = deduped
            bands_listbox.selection_clear(0, tk.END)
            for idx in deduped:
                bands_listbox.selection_set(idx)
            update_selection_status()

        def select_all_bands():
            template = current_template()
            if template is None:
                return
            apply_selection(range(len(template["bands"])))

        def clear_bands():
            apply_selection([])

        def apply_range_selection():
            template = current_template()
            if template is None:
                return
            try:
                min_nm = float(range_min_var.get().strip())
                max_nm = float(range_max_var.get().strip())
                step = int(float(range_step_var.get().strip()))
            except ValueError:
                messagebox.showerror("Invalid value", "PRISMA range and step values must be numeric.", parent=popup)
                return
            if step <= 0:
                messagebox.showerror("Invalid value", "Every nth band must be positive.", parent=popup)
                return
            if max_nm < min_nm:
                messagebox.showerror("Invalid value", "Max nm must be greater than or equal to Min nm.", parent=popup)
                return
            apply_selection(sensor_config.select_bands_by_range(template, min_nm, max_nm, step=step))

        def select_visible_all():
            template = current_template()
            if template is None:
                return
            apply_selection(sensor_config.select_bands_by_range(template, 400.0, 700.0, step=1))

        def select_visible_every_other():
            template = current_template()
            if template is None:
                return
            apply_selection(sensor_config.select_bands_by_range(template, 400.0, 700.0, step=2))

        def reset_sensor_defaults():
            template = current_template()
            if template is None:
                return
            apply_selection(sensor_config.default_selected_band_indices(template))

        def _sync_outer_sensor_state_after_template_change():
            for sensor_name in list(sensor_state["selected_indices"].keys()):
                if sensor_name not in sensor_templates:
                    del sensor_state["selected_indices"][sensor_name]
            for sensor_name, template in sensor_templates.items():
                sensor_state["selected_indices"][sensor_name] = list(
                    local_selected_indices.get(
                        sensor_name,
                        sensor_config.default_selected_band_indices(template),
                    )
                )
            if sensor_templates and sensor_state["sensor_name"] not in sensor_templates:
                if local_sensor_name.get() in sensor_templates:
                    sensor_state["sensor_name"] = local_sensor_name.get()
                else:
                    sensor_state["sensor_name"] = next(iter(sensor_templates))
            update_sensor_ui()

        def _refresh_sensor_templates_from_disk(preferred_sensor_name=None):
            nonlocal sensor_templates, sensor_load_errors

            previous_selection = dict(local_selected_indices)
            sensor_templates, sensor_load_errors = load_available_sensor_templates()
            for sensor_name, template in sensor_templates.items():
                if sensor_name not in local_selected_indices:
                    local_selected_indices[sensor_name] = previous_selection.get(
                        sensor_name,
                        sensor_config.default_selected_band_indices(template),
                    )
            for sensor_name in list(local_selected_indices.keys()):
                if sensor_name not in sensor_templates:
                    del local_selected_indices[sensor_name]

            if preferred_sensor_name and preferred_sensor_name in sensor_templates:
                local_sensor_name.set(preferred_sensor_name)
            elif sensor_templates and local_sensor_name.get() not in sensor_templates:
                local_sensor_name.set(next(iter(sensor_templates)))

            _sync_outer_sensor_state_after_template_change()
            refresh_band_list()

        def open_add_sensor_popup():
            add_popup = tk.Toplevel(popup)
            add_popup.title("Add sensor")
            add_popup.geometry("780x220")
            add_popup.minsize(720, 210)
            add_popup.transient(popup)
            add_popup.grab_set()
            add_popup.columnconfigure(0, weight=1)
            add_popup.rowconfigure(0, weight=1)

            name_var = StringVar()
            xml_path_var = StringVar()

            container = ttk.Frame(add_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(1, weight=1)
            container.rowconfigure(3, weight=1)

            ttk.Label(
                container,
                text=(
                    "Choose a sensor template XML containing the sensor response functions and fixed NEDR values. "
                    "The sensor will be copied into Data/SRF and will remain available in future runs."
                ),
                wraplength=700,
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            ttk.Label(container, text="Sensor name").grid(row=1, column=0, sticky="w")
            ttk.Entry(container, textvariable=name_var).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0))

            ttk.Label(container, text="Sensor XML").grid(row=2, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=xml_path_var).grid(row=2, column=1, sticky="ew", padx=(8, 6), pady=(10, 0))

            def choose_xml():
                path = askopenfilename(
                    parent=add_popup,
                    title="Choose sensor template XML",
                    filetypes=[("XML files", "*.xml"), ("All files", "*.*")],
                )
                if path:
                    xml_path_var.set(path)

            ttk.Button(container, text="Browse", command=choose_xml).grid(row=2, column=2, sticky="e", pady=(10, 0))

            def apply_add_sensor():
                xml_path = xml_path_var.get().strip()
                if not xml_path:
                    messagebox.showerror("Missing XML", "Choose a sensor XML file.", parent=add_popup)
                    return
                try:
                    add_result = sensor_config.add_sensor_template(cwd, name_var.get(), xml_path)
                except Exception as exc:
                    messagebox.showerror("Invalid sensor", str(exc), parent=add_popup)
                    return

                preferred_sensor_name = add_result["template"]["sensor_name"]
                _refresh_sensor_templates_from_disk(preferred_sensor_name=preferred_sensor_name)

                if add_result["backup_created"]:
                    messagebox.showinfo(
                        "Sensor added",
                        f"Added sensor '{preferred_sensor_name}'.\n\nA backup of the original templates was created in:\n{add_result['backup_dir']}",
                        parent=popup,
                    )
                else:
                    messagebox.showinfo(
                        "Sensor added",
                        f"Added sensor '{preferred_sensor_name}'.",
                        parent=popup,
                    )
                add_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(14, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=add_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="Add", command=apply_add_sensor).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(add_popup)
            add_popup.wait_window()

        def open_remove_sensor_popup():
            remove_popup = tk.Toplevel(popup)
            remove_popup.title("Remove sensors")
            remove_popup.geometry("500x420")
            remove_popup.minsize(440, 360)
            remove_popup.transient(popup)
            remove_popup.grab_set()
            remove_popup.columnconfigure(0, weight=1)
            remove_popup.rowconfigure(0, weight=1)

            container = ttk.Frame(remove_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(0, weight=1)
            container.rowconfigure(1, weight=1)

            ttk.Label(
                container,
                text="Select one or several sensors to remove from the app.",
                wraplength=440,
                justify="left",
            ).grid(row=0, column=0, sticky="w", pady=(0, 8))

            remove_listbox = tk.Listbox(
                container,
                selectmode=tk.MULTIPLE,
                exportselection=False,
                height=16,
            )
            remove_listbox.grid(row=1, column=0, sticky="nsew")
            remove_scroll = ttk.Scrollbar(container, orient="vertical", command=remove_listbox.yview)
            remove_scroll.grid(row=1, column=1, sticky="ns")
            remove_listbox.configure(yscrollcommand=remove_scroll.set)

            for sensor_name in sensor_templates.keys():
                remove_listbox.insert(tk.END, sensor_name)

            def apply_remove_sensor():
                selected_names = [remove_listbox.get(index) for index in remove_listbox.curselection()]
                if not selected_names:
                    messagebox.showerror("No sensors selected", "Select at least one sensor to remove.", parent=remove_popup)
                    return
                confirm_message = (
                    "You are about to permanently remove the following sensors from the app:\n\n"
                    + "\n".join(f"- {name}" for name in selected_names)
                    + "\n\nDo you want to continue?"
                )
                if not messagebox.askyesno("Confirm deletion", confirm_message, parent=remove_popup):
                    return
                try:
                    remove_result = sensor_config.remove_sensor_templates(cwd, sensor_templates, selected_names)
                except Exception as exc:
                    messagebox.showerror("Cannot remove sensors", str(exc), parent=remove_popup)
                    return

                remaining_names = [name for name in sensor_templates.keys() if name not in set(remove_result["removed_names"])]
                preferred_sensor_name = remaining_names[0] if remaining_names else None
                _refresh_sensor_templates_from_disk(preferred_sensor_name=preferred_sensor_name)

                if remove_result["backup_created"]:
                    messagebox.showinfo(
                        "Sensors removed",
                        f"Removed {len(remove_result['removed_names'])} sensor(s).\n\nA backup of the original templates was created in:\n{remove_result['backup_dir']}",
                        parent=popup,
                    )
                else:
                    messagebox.showinfo(
                        "Sensors removed",
                        f"Removed {len(remove_result['removed_names'])} sensor(s).",
                        parent=popup,
                    )
                remove_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=remove_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="OK", command=apply_remove_sensor).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(remove_popup)
            remove_popup.wait_window()

        ttk.Button(quick_frame, text="All", command=select_all_bands).grid(row=0, column=0, sticky="w", padx=(0, 6))
        ttk.Button(quick_frame, text="Clear", command=clear_bands).grid(row=0, column=1, sticky="w", padx=(0, 6))
        ttk.Button(quick_frame, text="Reset", command=reset_sensor_defaults).grid(row=0, column=2, sticky="w", padx=(0, 6))

        ttk.Button(sensor_actions, text="Add", command=open_add_sensor_popup).grid(row=0, column=0, sticky="w")
        ttk.Button(sensor_actions, text="Remove", command=open_remove_sensor_popup).grid(row=0, column=1, sticky="w", padx=(8, 0))

        ttk.Button(smart_frame, text="Apply Range", command=apply_range_selection).grid(row=0, column=6, sticky="w")
        ttk.Button(smart_frame, text="Visible 400-700", command=select_visible_all).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))
        ttk.Button(smart_frame, text="Visible Every 2nd", command=select_visible_every_other).grid(row=1, column=3, columnspan=3, sticky="w", padx=(10, 0), pady=(8, 0))

        def on_sensor_changed(*_args):
            refresh_band_list()

        def on_band_selection(_event=None):
            sync_current_selection()
            update_selection_status()

        def apply_sensor_changes():
            sensor_name = local_sensor_name.get()
            if sensor_name not in sensor_templates:
                messagebox.showerror("Unavailable sensor", sensor_load_errors.get(sensor_name, "Template not available."), parent=popup)
                return
            sensor_state["sensor_name"] = sensor_name
            for key in list(sensor_state["selected_indices"].keys()):
                if key not in sensor_templates:
                    del sensor_state["selected_indices"][key]
            for key, indices in local_selected_indices.items():
                if key in sensor_templates:
                    sensor_state["selected_indices"][key] = list(indices)
            try:
                sensor_config.build_sensor_config(
                    sensor_templates[sensor_name],
                    sensor_state["selected_indices"][sensor_name],
                )
            except Exception as exc:
                messagebox.showerror("Invalid sensor setup", str(exc), parent=popup)
                return
            update_sensor_ui()
            popup.destroy()

        local_sensor_name.trace_add("write", on_sensor_changed)
        bands_listbox.bind("<<ListboxSelect>>", on_band_selection)

        popup_actions = ttk.Frame(popup_container)
        popup_actions.grid(row=1, column=0, columnspan=2, sticky="ew")
        popup_actions.columnconfigure(0, weight=1)
        ttk.Button(popup_actions, text="Cancel", command=popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Button(popup_actions, text="Apply", command=apply_sensor_changes).grid(row=0, column=2, sticky="e", padx=(8, 0))

        _ensure_local_selection_defaults()
        refresh_band_list()
        center_window(popup)
        popup.wait_window()

    def open_siop_popup():
        nonlocal spectral_library, selected_target_names

        popup = tk.Toplevel(root)
        popup.title("Water & Bottom Settings")
        popup_height = max(680, int(popup.winfo_screenheight() * 0.9))
        popup.geometry(f"1140x{popup_height}")
        popup.minsize(980, 680)
        popup.transient(root)
        popup.grab_set()

        local_scalar_values = dict(scalar_values)
        local_selected_names = list(selected_target_names)
        local_spectrum_paths = dict(spectrum_override_paths)

        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)

        popup_container = ttk.Frame(popup, padding=12)
        popup_container.grid(row=0, column=0, sticky="nsew")
        popup_container.columnconfigure(0, weight=0)
        popup_container.columnconfigure(1, weight=1)
        popup_container.rowconfigure(0, weight=1)
        popup_container.rowconfigure(1, weight=0)

        targets_frame = ttk.Labelframe(popup_container, text="Target Spectra")
        targets_frame.grid(row=0, column=0, sticky="nsw", padx=(0, 10), pady=(0, 8))
        targets_frame.columnconfigure(0, weight=1)
        targets_frame.rowconfigure(1, weight=1)

        ttk.Label(
            targets_frame,
            text="Select 2 or 3 spectra. Hover a name to preview it. Spectra are grouped by tag.",
            wraplength=260,
        ).grid(row=0, column=0, sticky="w")

        spectra_listbox = tk.Listbox(
            targets_frame,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            width=32,
            height=20,
        )
        spectra_listbox.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        for name in spectral_library["names"]:
            spectra_listbox.insert(tk.END, name)

        listbox_scroll = ttk.Scrollbar(targets_frame, orient="vertical", command=spectra_listbox.yview)
        listbox_scroll.grid(row=1, column=1, sticky="ns", pady=(6, 0))
        spectra_listbox.configure(yscrollcommand=listbox_scroll.set)

        library_actions = ttk.Frame(targets_frame)
        library_actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        library_actions.columnconfigure(2, weight=1)

        preview_frame = ttk.Labelframe(popup_container, text="Preview & Advanced Parameters")
        preview_frame.grid(row=0, column=1, sticky="nsew", pady=(0, 8))
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(0, weight=1)
        preview_frame.rowconfigure(1, weight=0)
        preview_frame.rowconfigure(2, weight=0)
        preview_frame.rowconfigure(3, weight=0)

        preview_canvas = tk.Canvas(preview_frame, bg="white", highlightthickness=1, highlightbackground="#d7d7d7")
        preview_canvas.grid(row=0, column=0, sticky="nsew", padx=4, pady=(4, 8))

        absorption_status_var = StringVar()

        def refresh_absorption_summary():
            has_water_override = bool(local_spectrum_paths.get("a_water", "").strip())
            has_chl_override = bool(local_spectrum_paths.get("a_ph_star", "").strip())
            if has_water_override and has_chl_override:
                absorption_status_var.set("Custom water and chlorophyll absorption CSV files selected.")
            elif has_water_override:
                absorption_status_var.set("Custom pure-water absorption CSV selected.")
            elif has_chl_override:
                absorption_status_var.set("Custom chlorophyll absorption CSV selected.")
            else:
                absorption_status_var.set(
                    f"Using the default absorption spectra from {os.path.basename(default_template_path)}."
                )

        def open_absorption_popup():
            absorption_popup = tk.Toplevel(popup)
            absorption_popup.title("Modify absorption of chl and water")
            absorption_popup.geometry("900x260")
            absorption_popup.minsize(760, 240)
            absorption_popup.transient(popup)
            absorption_popup.grab_set()
            absorption_popup.columnconfigure(0, weight=1)
            absorption_popup.rowconfigure(0, weight=1)

            draft_paths = dict(local_spectrum_paths)
            a_water_path_var = StringVar(value=draft_paths.get("a_water", ""))
            a_ph_star_path_var = StringVar(value=draft_paths.get("a_ph_star", ""))

            def choose_spectrum_csv(target_key, target_var, title_text):
                path = askopenfilename(
                    parent=absorption_popup,
                    title=title_text,
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                )
                if path:
                    draft_paths[target_key] = path
                    target_var.set(path)

            def clear_spectrum_csv(target_key, target_var):
                draft_paths[target_key] = ""
                target_var.set("")

            absorption_container = ttk.Frame(absorption_popup, padding=12)
            absorption_container.grid(row=0, column=0, sticky="nsew")
            absorption_container.columnconfigure(1, weight=1)
            absorption_container.rowconfigure(3, weight=1)

            ttk.Label(
                absorption_container,
                text="CSV format: wavelength, value. One header row is allowed.",
                wraplength=820,
            ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 8))

            ttk.Label(
                absorption_container,
                text="Absorption coefficient of pure seawater [m^-1]",
            ).grid(row=1, column=0, sticky="w")
            ttk.Entry(
                absorption_container,
                textvariable=a_water_path_var,
            ).grid(row=1, column=1, sticky="ew", padx=(0, 6))
            ttk.Button(
                absorption_container,
                text="Browse",
                command=lambda: choose_spectrum_csv(
                    "a_water",
                    a_water_path_var,
                    "Choose pure seawater absorption CSV",
                ),
            ).grid(row=1, column=2, sticky="e", padx=(0, 6))
            ttk.Button(
                absorption_container,
                text="Default",
                command=lambda: clear_spectrum_csv("a_water", a_water_path_var),
            ).grid(row=1, column=3, sticky="e")

            ttk.Label(
                absorption_container,
                text="Chlorophyll-specific absorption spectrum [m^2 mg^-1]",
            ).grid(row=2, column=0, sticky="w", pady=(8, 0))
            ttk.Entry(
                absorption_container,
                textvariable=a_ph_star_path_var,
            ).grid(row=2, column=1, sticky="ew", padx=(0, 6), pady=(8, 0))
            ttk.Button(
                absorption_container,
                text="Browse",
                command=lambda: choose_spectrum_csv(
                    "a_ph_star",
                    a_ph_star_path_var,
                    "Choose chlorophyll-specific absorption CSV",
                ),
            ).grid(row=2, column=2, sticky="e", padx=(0, 6), pady=(8, 0))
            ttk.Button(
                absorption_container,
                text="Default",
                command=lambda: clear_spectrum_csv("a_ph_star", a_ph_star_path_var),
            ).grid(row=2, column=3, sticky="e", pady=(8, 0))

            def apply_absorption_changes():
                local_spectrum_paths["a_water"] = a_water_path_var.get().strip()
                local_spectrum_paths["a_ph_star"] = a_ph_star_path_var.get().strip()
                refresh_absorption_summary()
                absorption_popup.destroy()

            absorption_actions = ttk.Frame(absorption_container)
            absorption_actions.grid(row=3, column=0, columnspan=4, sticky="sew", pady=(14, 0))
            absorption_actions.columnconfigure(0, weight=1)

            ttk.Button(
                absorption_actions,
                text="Cancel",
                command=absorption_popup.destroy,
            ).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(
                absorption_actions,
                text="Apply",
                command=apply_absorption_changes,
            ).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(absorption_popup)
            absorption_popup.wait_window()

        absorption_frame = ttk.Frame(preview_frame)
        absorption_frame.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 8))
        absorption_frame.columnconfigure(1, weight=1)

        ttk.Button(
            absorption_frame,
            text="Modify absorption of chl and water",
            command=open_absorption_popup,
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            absorption_frame,
            textvariable=absorption_status_var,
            justify="left",
            wraplength=430,
        ).grid(row=0, column=1, sticky="w", padx=(10, 0))

        scalar_frame = ttk.Frame(preview_frame)
        scalar_frame.grid(row=2, column=0, sticky="ew", padx=4, pady=(0, 4))
        for column_index in range(4):
            scalar_frame.columnconfigure(column_index, weight=1)

        scalar_vars = {}
        for idx, (key, label, _required) in enumerate(siop_config.SIOP_SCALAR_FIELDS):
            row = idx // 2
            col = (idx % 2) * 2
            ttk.Label(
                scalar_frame,
                text=label,
                justify="left",
                wraplength=220,
            ).grid(row=row, column=col, sticky="w", padx=(0, 6), pady=2)
            var = StringVar(value=local_scalar_values.get(key, ""))
            scalar_vars[key] = var
            ttk.Entry(scalar_frame, textvariable=var, justify="right").grid(row=row, column=col + 1, sticky="ew", pady=2)

        popup_status_var = StringVar()
        hovered_spectrum_name = {"name": None}
        display_rows = []

        def get_local_selection():
            chosen = []
            seen = set()
            for index in spectra_listbox.curselection():
                if 0 <= index < len(display_rows):
                    row = display_rows[index]
                    if row.get("type") == "spectrum":
                        name = row["name"]
                        if name not in seen:
                            chosen.append(name)
                            seen.add(name)
            return chosen

        def _sanitize_selection(names):
            available_names = set(spectral_library["names"])
            chosen = []
            seen = set()
            for name in names:
                if name in available_names and name not in seen:
                    chosen.append(name)
                    seen.add(name)
            return chosen[:3]

        def _build_grouped_display_rows():
            grouped_names = {}
            for name in spectral_library["names"]:
                tag = siop_config.display_tag(spectral_library.get("tags", {}).get(name, ""))
                grouped_names.setdefault(tag, []).append(name)

            ordered_tags = sorted(
                grouped_names.keys(),
                key=lambda tag: (tag == siop_config.UNTAGGED_TAG, tag.lower()),
            )

            rows = []
            for tag in ordered_tags:
                rows.append({
                    "type": "header",
                    "label": f"[{tag}]",
                })
                for name in grouped_names[tag]:
                    rows.append({
                        "type": "spectrum",
                        "name": name,
                        "label": f"  {name}",
                    })
            return rows

        def _set_listbox_selection(names):
            wanted = set(names)
            spectra_listbox.selection_clear(0, tk.END)
            for index, row in enumerate(display_rows):
                if row.get("type") == "spectrum" and row.get("name") in wanted:
                    spectra_listbox.selection_set(index)

        def refresh_popup_summary():
            chosen = get_local_selection()
            if len(chosen) == 2:
                popup_status_var.set("2 targets selected. The third substrate will be fixed to zero.")
            elif len(chosen) == 3:
                popup_status_var.set("3 targets selected.")
            elif len(chosen) < 2:
                popup_status_var.set("Select at least two target spectra.")
            else:
                popup_status_var.set("Select at most three target spectra.")

        def redraw_preview():
            _draw_spectra_preview(
                preview_canvas,
                spectral_library,
                get_local_selection(),
                hovered_name=hovered_spectrum_name["name"],
            )

        def refresh_spectra_listbox(selected_names=None):
            chosen = list(selected_names) if selected_names is not None else get_local_selection()
            chosen = _sanitize_selection(chosen)
            local_selected_names[:] = list(chosen)
            if hovered_spectrum_name["name"] not in spectral_library["spectra"]:
                hovered_spectrum_name["name"] = None
            spectra_listbox.delete(0, tk.END)
            display_rows.clear()
            display_rows.extend(_build_grouped_display_rows())
            for row in display_rows:
                spectra_listbox.insert(tk.END, row["label"])
            _set_listbox_selection(chosen)
            refresh_popup_summary()
            redraw_preview()

        def _sync_outer_selection_after_library_change(chosen_names=None):
            chosen = _sanitize_selection(chosen_names if chosen_names is not None else selected_target_names)
            selected_target_names[:] = chosen
            update_substrate_ui()
            return chosen

        def _persist_library_update(updated_library, action_description, selected_names=None):
            nonlocal spectral_library

            backup_path = siop_config.spectral_library_backup_path(updated_library["path"])
            backup_created = not os.path.exists(backup_path)
            siop_config.write_spectral_library(updated_library)
            spectral_library = siop_config.load_spectral_library(updated_library["path"])

            chosen = _sanitize_selection(selected_names if selected_names is not None else local_selected_names)
            local_selected_names[:] = chosen
            chosen = _sync_outer_selection_after_library_change(chosen)
            refresh_spectra_listbox(chosen)

            if backup_created:
                messagebox.showinfo(
                    "Spectral library updated",
                    f"{action_description}\n\nA backup of the original library was created at:\n{backup_path}",
                    parent=popup,
                )
            else:
                messagebox.showinfo(
                    "Spectral library updated",
                    action_description,
                    parent=popup,
                )

        def on_selection_change(_event=None):
            selected_indices = list(spectra_listbox.curselection())
            invalid_indices = []
            valid_indices = []
            for index in selected_indices:
                if 0 <= index < len(display_rows) and display_rows[index].get("type") == "spectrum":
                    valid_indices.append(index)
                else:
                    invalid_indices.append(index)
            for index in invalid_indices:
                spectra_listbox.selection_clear(index)
            if len(valid_indices) > 3:
                for index in valid_indices[3:]:
                    spectra_listbox.selection_clear(index)
                messagebox.showwarning("Too many spectra", "Select at most three target spectra.", parent=popup)
            local_selected_names[:] = get_local_selection()
            refresh_popup_summary()
            redraw_preview()

        def on_hover(event):
            if spectra_listbox.size() == 0:
                return
            index = spectra_listbox.nearest(event.y)
            if 0 <= index < spectra_listbox.size():
                if 0 <= index < len(display_rows) and display_rows[index].get("type") == "spectrum":
                    hovered_spectrum_name["name"] = display_rows[index]["name"]
                else:
                    hovered_spectrum_name["name"] = None
                redraw_preview()

        def on_hover_leave(_event):
            hovered_spectrum_name["name"] = None
            redraw_preview()

        def open_add_spectrum_popup():
            add_popup = tk.Toplevel(popup)
            add_popup.title("Add spectrum to library")
            add_popup.geometry("760x260")
            add_popup.minsize(700, 240)
            add_popup.transient(popup)
            add_popup.grab_set()
            add_popup.columnconfigure(0, weight=1)
            add_popup.rowconfigure(0, weight=1)

            name_var = StringVar()
            csv_path_var = StringVar()
            tag_var = StringVar()

            container = ttk.Frame(add_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(1, weight=1)
            container.rowconfigure(4, weight=1)

            ttk.Label(
                container,
                text=(
                    "Choose a two-column CSV containing wavelength and reflectance. "
                    "The file may include a single header row."
                ),
                wraplength=680,
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            ttk.Label(container, text="Spectrum name").grid(row=1, column=0, sticky="w")
            ttk.Entry(container, textvariable=name_var).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 0))

            ttk.Label(container, text="Tag").grid(row=2, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=tag_var).grid(row=2, column=1, columnspan=2, sticky="ew", padx=(8, 0), pady=(10, 0))

            ttk.Label(container, text="CSV file").grid(row=3, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=csv_path_var).grid(row=3, column=1, sticky="ew", padx=(8, 6), pady=(10, 0))

            def choose_csv():
                path = askopenfilename(
                    parent=add_popup,
                    title="Choose spectrum CSV",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                )
                if path:
                    csv_path_var.set(path)

            ttk.Button(container, text="Browse", command=choose_csv).grid(row=3, column=2, sticky="e", pady=(10, 0))

            def apply_add():
                csv_path = csv_path_var.get().strip()
                if not csv_path:
                    messagebox.showerror("Missing CSV", "Choose a CSV file for the new spectrum.", parent=add_popup)
                    return
                try:
                    wavelengths, values = siop_config.load_two_column_spectrum_csv(csv_path)
                    updated_library = siop_config.add_spectrum_to_library(
                        spectral_library,
                        name_var.get(),
                        wavelengths,
                        values,
                    )
                    updated_library = siop_config.modify_spectrum_in_library(
                        updated_library,
                        updated_library["names"][-1],
                        updated_library["names"][-1],
                        tag_var.get(),
                    )
                except Exception as exc:
                    messagebox.showerror("Invalid spectrum", str(exc), parent=add_popup)
                    return

                _persist_library_update(
                    updated_library,
                    f"Added spectrum '{updated_library['names'][-1]}' to the spectral library.",
                )
                add_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(14, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=add_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="Add", command=apply_add).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(add_popup)
            add_popup.wait_window()

        def open_modify_spectrum_popup():
            modify_popup = tk.Toplevel(popup)
            modify_popup.title("Modify spectrum")
            modify_popup.geometry("760x360")
            modify_popup.minsize(700, 320)
            modify_popup.transient(popup)
            modify_popup.grab_set()
            modify_popup.columnconfigure(0, weight=1)
            modify_popup.rowconfigure(0, weight=1)

            current_selection = get_local_selection()
            initial_name = current_selection[0] if current_selection else (spectral_library["names"][0] if spectral_library["names"] else "")
            selected_name_var = StringVar(value=initial_name)
            rename_var = StringVar(value=initial_name)
            tag_var = StringVar(value=spectral_library.get("tags", {}).get(initial_name, ""))

            container = ttk.Frame(modify_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(1, weight=1)
            container.rowconfigure(1, weight=1)

            ttk.Label(
                container,
                text="Choose one spectrum, then edit its name and tag.",
                wraplength=700,
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))

            modify_listbox = tk.Listbox(
                container,
                selectmode=tk.SINGLE,
                exportselection=False,
                height=10,
            )
            modify_listbox.grid(row=1, column=0, columnspan=3, sticky="nsew")
            modify_scroll = ttk.Scrollbar(container, orient="vertical", command=modify_listbox.yview)
            modify_scroll.grid(row=1, column=3, sticky="ns")
            modify_listbox.configure(yscrollcommand=modify_scroll.set)

            for name in spectral_library["names"]:
                tag_text = siop_config.display_tag(spectral_library.get("tags", {}).get(name, ""))
                modify_listbox.insert(tk.END, f"{name}  [{tag_text}]")
            if initial_name in spectral_library["names"]:
                modify_listbox.selection_set(spectral_library["names"].index(initial_name))

            ttk.Label(container, text="Name").grid(row=2, column=0, sticky="w", pady=(12, 0))
            ttk.Entry(container, textvariable=rename_var).grid(row=2, column=1, columnspan=3, sticky="ew", padx=(8, 0), pady=(12, 0))

            ttk.Label(container, text="Tag").grid(row=3, column=0, sticky="w", pady=(10, 0))
            ttk.Entry(container, textvariable=tag_var).grid(row=3, column=1, columnspan=3, sticky="ew", padx=(8, 0), pady=(10, 0))

            def load_selected_fields(name):
                selected_name_var.set(name)
                rename_var.set(name)
                tag_var.set(spectral_library.get("tags", {}).get(name, ""))

            def on_modify_selection(_event=None):
                selection = modify_listbox.curselection()
                if not selection:
                    return
                selected_name = spectral_library["names"][selection[0]]
                load_selected_fields(selected_name)

            def apply_modify():
                current_name = selected_name_var.get()
                if not current_name:
                    messagebox.showerror("No spectrum selected", "Choose a spectrum to modify.", parent=modify_popup)
                    return
                try:
                    updated_library = siop_config.modify_spectrum_in_library(
                        spectral_library,
                        current_name,
                        rename_var.get(),
                        tag_var.get(),
                    )
                except Exception as exc:
                    messagebox.showerror("Invalid spectrum update", str(exc), parent=modify_popup)
                    return

                new_name = rename_var.get().strip()
                updated_selection = [
                    new_name if name == current_name else name
                    for name in local_selected_names
                ]
                _persist_library_update(
                    updated_library,
                    f"Updated spectrum '{current_name}'.",
                    selected_names=updated_selection,
                )
                modify_popup.destroy()

            modify_listbox.bind("<<ListboxSelect>>", on_modify_selection)

            actions = ttk.Frame(container)
            actions.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(14, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=modify_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="Apply", command=apply_modify).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(modify_popup)
            modify_popup.wait_window()

        def open_remove_spectra_popup():
            remove_popup = tk.Toplevel(popup)
            remove_popup.title("Remove spectra from library")
            remove_popup.geometry("500x420")
            remove_popup.minsize(440, 360)
            remove_popup.transient(popup)
            remove_popup.grab_set()
            remove_popup.columnconfigure(0, weight=1)
            remove_popup.rowconfigure(0, weight=1)

            container = ttk.Frame(remove_popup, padding=12)
            container.grid(row=0, column=0, sticky="nsew")
            container.columnconfigure(0, weight=1)
            container.rowconfigure(1, weight=1)

            ttk.Label(
                container,
                text="Select one or several spectra to remove from the library.",
                wraplength=440,
                justify="left",
            ).grid(row=0, column=0, sticky="w", pady=(0, 8))

            remove_listbox = tk.Listbox(
                container,
                selectmode=tk.MULTIPLE,
                exportselection=False,
                height=16,
            )
            remove_listbox.grid(row=1, column=0, sticky="nsew")
            remove_scroll = ttk.Scrollbar(container, orient="vertical", command=remove_listbox.yview)
            remove_scroll.grid(row=1, column=1, sticky="ns")
            remove_listbox.configure(yscrollcommand=remove_scroll.set)

            for name in spectral_library["names"]:
                remove_listbox.insert(tk.END, name)

            def apply_remove():
                selected_names = [remove_listbox.get(index) for index in remove_listbox.curselection()]
                if not selected_names:
                    messagebox.showerror("No spectra selected", "Select at least one spectrum to remove.", parent=remove_popup)
                    return
                confirm_message = (
                    "You are about to permanently remove the following spectra from the spectral library:\n\n"
                    + "\n".join(f"- {name}" for name in selected_names)
                    + "\n\nDo you want to continue?"
                )
                if not messagebox.askyesno("Confirm deletion", confirm_message, parent=remove_popup):
                    return
                try:
                    updated_library = siop_config.remove_spectra_from_library(spectral_library, selected_names)
                except Exception as exc:
                    messagebox.showerror("Cannot remove spectra", str(exc), parent=remove_popup)
                    return

                _persist_library_update(
                    updated_library,
                    f"Removed {len(selected_names)} spectrum(s) from the spectral library.",
                )
                remove_popup.destroy()

            actions = ttk.Frame(container)
            actions.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))
            actions.columnconfigure(0, weight=1)
            ttk.Button(actions, text="Cancel", command=remove_popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
            ttk.Button(actions, text="OK", command=apply_remove).grid(row=0, column=2, sticky="e", padx=(8, 0))

            center_window(remove_popup)
            remove_popup.wait_window()

        def apply_popup_changes():
            chosen = get_local_selection()
            override_paths = {
                "a_water": str(local_spectrum_paths.get("a_water", "")).strip(),
                "a_ph_star": str(local_spectrum_paths.get("a_ph_star", "")).strip(),
            }
            try:
                siop_config.build_siop_config(
                    template_config,
                    spectral_library,
                    chosen,
                    {key: var.get() for key, var in scalar_vars.items()},
                    spectrum_override_paths=override_paths,
                )
            except Exception as exc:
                messagebox.showerror("Invalid Water & Bottom Settings", str(exc), parent=popup)
                return

            scalar_values.clear()
            for key, var in scalar_vars.items():
                scalar_values[key] = var.get().strip()

            selected_target_names.clear()
            selected_target_names.extend(chosen)
            spectrum_override_paths["a_water"] = override_paths["a_water"]
            spectrum_override_paths["a_ph_star"] = override_paths["a_ph_star"]
            update_substrate_ui()
            popup.destroy()

        spectra_listbox.bind("<<ListboxSelect>>", on_selection_change)
        spectra_listbox.bind("<Motion>", on_hover)
        spectra_listbox.bind("<Leave>", on_hover_leave)
        preview_canvas.bind("<Configure>", lambda _event: redraw_preview())

        ttk.Button(library_actions, text="Add", command=open_add_spectrum_popup).grid(row=0, column=0, sticky="w")
        ttk.Button(library_actions, text="Modify", command=open_modify_spectrum_popup).grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Button(library_actions, text="Remove", command=open_remove_spectra_popup).grid(row=0, column=2, sticky="w", padx=(8, 0))

        popup_actions = ttk.Frame(popup_container)
        popup_actions.grid(row=1, column=0, columnspan=2, sticky="ew")
        popup_actions.columnconfigure(0, weight=1)

        ttk.Label(popup_actions, textvariable=popup_status_var).grid(row=0, column=0, sticky="w")
        ttk.Button(popup_actions, text="Cancel", command=popup.destroy).grid(row=0, column=1, sticky="e", padx=(8, 0))
        ttk.Button(popup_actions, text="Apply", command=apply_popup_changes).grid(row=0, column=2, sticky="e", padx=(8, 0))

        refresh_spectra_listbox(local_selected_names)
        refresh_absorption_summary()
        center_window(popup)
        popup.wait_window()

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    container = ttk.Frame(root, padding=12)
    container.grid(row=0, column=0, sticky="nsew")
    container.columnconfigure(0, weight=1)
    container.rowconfigure(0, weight=1)
    container.rowconfigure(1, weight=0)

    notebook = ttk.Notebook(container)
    notebook.grid(row=0, column=0, sticky="nsew")

    input_tab = ttk.Frame(notebook, padding=8)
    bathy_tab = ttk.Frame(notebook, padding=8)
    params_tab = ttk.Frame(notebook, padding=8)
    notebook.add(input_tab, text="Inputs & Options")
    notebook.add(bathy_tab, text="Bathymetry")
    notebook.add(params_tab, text="Parameters")

    for tab in (input_tab, bathy_tab, params_tab):
        tab.columnconfigure(0, weight=1)

    input_tab.rowconfigure(0, weight=0)
    input_tab.rowconfigure(1, weight=1)
    input_tab.rowconfigure(2, weight=0)
    bathy_tab.rowconfigure(0, weight=1)
    params_tab.rowconfigure(0, weight=1)

    files_frame = ttk.Labelframe(input_tab, text="Files")
    files_frame.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
    for i in range(3):
        files_frame.columnconfigure(i, weight=1)

    ttk.Label(files_frame, text="Input image(s) (.nc)").grid(row=0, column=0, sticky="w")
    ttk.Entry(files_frame, textvariable=input_image_var).grid(row=0, column=1, sticky="ew", padx=(0, 6))
    ttk.Button(files_frame, text="Browse", command=select_file_im).grid(row=0, column=2, sticky="e")

    ttk.Label(files_frame, text="Output folder").grid(row=1, column=0, sticky="w")
    ttk.Entry(files_frame, textvariable=output_folder_var).grid(row=1, column=1, sticky="ew", padx=(0, 6))
    ttk.Button(files_frame, text="Choose", command=select_folder).grid(row=1, column=2, sticky="e")

    ttk.Label(files_frame, text="Water & Bottom settings").grid(row=2, column=0, sticky="nw")
    ttk.Label(files_frame, textvariable=siop_summary_var, wraplength=560, justify="left").grid(row=2, column=1, sticky="w", padx=(0, 6))
    ttk.Button(files_frame, text="Configure", command=open_siop_popup).grid(row=2, column=2, sticky="e")

    ttk.Label(files_frame, text="Sensor").grid(row=3, column=0, sticky="nw")
    ttk.Label(files_frame, textvariable=sensor_summary_var, wraplength=560, justify="left").grid(row=3, column=1, sticky="w", padx=(0, 6))
    ttk.Button(files_frame, text="Configure", command=open_sensor_popup).grid(row=3, column=2, sticky="e")

    flags_frame = ttk.Labelframe(input_tab, text="Options")
    flags_frame.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
    flags_frame.columnconfigure(0, weight=0)
    flags_frame.columnconfigure(1, weight=1)

    above_rrs_flag = BooleanVar(value=True)
    shallow_flag = BooleanVar(value=True)
    false_deep_correction_flag = BooleanVar(value=False)
    optimize_initial_guesses_flag = BooleanVar(value=False)
    five_initial_guess_testing_flag = BooleanVar(value=False)
    initial_guess_debug_flag = BooleanVar(value=False)
    output_modeled_reflectance_flag = BooleanVar(value=False)
    relaxed = BooleanVar(value=False)
    fully_relaxed_flag = BooleanVar(value=False)
    pp = BooleanVar(value=False)
    allow_split = BooleanVar(value=False)
    chunk_rows = StringVar(value="512")
    bathy_mode = tk.StringVar(value="estimate")
    bathy_path_var = tk.StringVar(value="")
    bathy_info_var = tk.StringVar(value="")
    bathy_correction = tk.StringVar(value="0")
    bathy_tolerance = tk.StringVar(value="0")
    use_emodnet_var = BooleanVar(value=False)
    user_defined_var = BooleanVar(value=False)

    def add_option_row(row_index, label, variable, popup_command, pady=(0, 2)):
        info_button = ttk.Button(flags_frame, text="Info", width=6, command=popup_command)
        info_button.grid(
            row=row_index,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=pady,
        )
        checkbutton = ttk.Checkbutton(flags_frame, text=label, variable=variable)
        checkbutton.grid(
            row=row_index,
            column=1,
            sticky=W,
            pady=pady,
        )
        return info_button, checkbutton

    add_option_row(0, "Above RRS", above_rrs_flag, open_above_rrs_popup)
    add_option_row(1, "Shallow water", shallow_flag, open_shallow_water_popup)
    false_deep_info_button, false_deep_checkbutton = add_option_row(
        2,
        "Correct steep false-deep bathymetry",
        false_deep_correction_flag,
        open_false_deep_correction_popup,
    )
    add_option_row(3, "Optimise initial guesses", optimize_initial_guesses_flag, open_initial_guess_popup)
    add_option_row(4, "Output modeled reflectance", output_modeled_reflectance_flag, open_modeled_reflectance_popup)
    add_option_row(5, "Relaxed constraints", relaxed, open_relaxed_constraints_popup)
    add_option_row(6, "Post processing", pp, open_post_processing_popup)
    add_option_row(7, "Allow image splitting", allow_split, open_split_popup, pady=(4, 0))

    def update_initial_guess_controls(*_args):
        if not optimize_initial_guesses_flag.get():
            five_initial_guess_testing_flag.set(False)
            initial_guess_debug_flag.set(False)

    def update_split_controls(*_args):
        return

    def update_relaxed_controls(*_args):
        if not relaxed.get():
            fully_relaxed_flag.set(False)

    def update_false_deep_correction_controls(*_args):
        available = bathy_mode.get() == "estimate"
        if not available and false_deep_correction_flag.get():
            false_deep_correction_flag.set(False)
        _set_widget_enabled(false_deep_checkbutton, available)

    optimize_initial_guesses_flag.trace_add("write", update_initial_guess_controls)
    allow_split.trace_add("write", update_split_controls)
    relaxed.trace_add("write", update_relaxed_controls)
    false_deep_correction_flag.trace_add("write", update_false_deep_correction_controls)
    update_initial_guess_controls()
    update_split_controls()
    update_relaxed_controls()
    update_false_deep_correction_controls()

    bathy_frame = ttk.Labelframe(bathy_tab, text="Bathymetry")
    bathy_frame.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
    bathy_frame.columnconfigure(0, weight=1)
    ttk.Radiobutton(bathy_frame, text="Estimate bathymetry", value="estimate", variable=bathy_mode).grid(row=0, column=0, sticky=W)
    ttk.Radiobutton(bathy_frame, text="Use input bathymetry", value="input", variable=bathy_mode).grid(row=1, column=0, sticky=W)

    bathy_input_frame = ttk.Frame(bathy_frame)
    bathy_input_frame.grid(row=2, column=0, sticky="nsew", padx=(12, 0), pady=(6, 0))
    bathy_input_frame.columnconfigure(1, weight=1)

    def on_emodnet_toggle():
        if use_emodnet_var.get():
            if user_defined_var.get():
                user_defined_var.set(False)
            path = _resolve_bundled_resource(cwd, os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"))
            bathy_path_var.set(path)
            bathy_info_var.set("EMODnet: E4_2024.tif")
        elif not user_defined_var.get():
            bathy_path_var.set("")
            bathy_info_var.set("")

    def on_user_defined_toggle():
        if user_defined_var.get():
            if use_emodnet_var.get():
                use_emodnet_var.set(False)
            path = askopenfilename(
                parent=root,
                title="Choose bathymetry GeoTIFF",
                filetypes=[("GeoTIFF", "*.tif *.tiff"), ("All files", "*.*")],
            )
            if path:
                bathy_path_var.set(path)
                bathy_info_var.set(os.path.basename(path))
            else:
                user_defined_var.set(False)
                if not use_emodnet_var.get():
                    bathy_path_var.set("")
                    bathy_info_var.set("")
        elif not use_emodnet_var.get():
            bathy_path_var.set("")
            bathy_info_var.set("")

    ttk.Checkbutton(bathy_input_frame, text="EMODnet", variable=use_emodnet_var, command=on_emodnet_toggle).grid(row=0, column=0, sticky=W)
    ttk.Checkbutton(bathy_input_frame, text="User defined", variable=user_defined_var, command=on_user_defined_toggle).grid(row=0, column=1, sticky=W, padx=(12, 0))
    ttk.Label(bathy_input_frame, textvariable=bathy_info_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 0))

    ttk.Label(bathy_input_frame, text="Water level correction (m)").grid(row=2, column=0, sticky=W, pady=(6, 0))
    ttk.Entry(bathy_input_frame, textvariable=bathy_correction, width=10).grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(6, 0))

    ttk.Label(bathy_input_frame, text="Depth bounds around bathy (\u00b1 m)").grid(row=3, column=0, sticky=W, pady=(6, 0))
    ttk.Entry(bathy_input_frame, textvariable=bathy_tolerance, width=10).grid(row=3, column=1, sticky="w", padx=(8, 0), pady=(6, 0))

    def update_bathy_visibility(*_args):
        if bathy_mode.get() == "input":
            bathy_input_frame.grid()
        else:
            bathy_input_frame.grid_remove()

    bathy_mode.trace_add("write", update_bathy_visibility)
    update_bathy_visibility()

    params_frame = ttk.Labelframe(params_tab, text="Parameter Bounds (min / max)")
    params_frame.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
    for i in range(3):
        params_frame.columnconfigure(i, weight=1)

    ttk.Label(params_frame, text="").grid(row=0, column=0, sticky=W)
    ttk.Label(params_frame, text="Min").grid(row=0, column=1, sticky=W)
    ttk.Label(params_frame, text="Max").grid(row=0, column=2, sticky=W)

    chl_min_var = StringVar(value="0.01")
    chl_max_var = StringVar(value="0.16")
    cdom_min_var = StringVar(value="0.0005")
    cdom_max_var = StringVar(value="0.01")
    nap_min_var = StringVar(value="0.2")
    nap_max_var = StringVar(value="1.5")
    depth_min_var = StringVar(value="0.1")
    depth_max_var = StringVar(value="30")
    sub1_min_var = StringVar(value="0")
    sub1_max_var = StringVar(value="1")
    sub2_min_var = StringVar(value="0")
    sub2_max_var = StringVar(value="1")
    sub3_min_var = StringVar(value="0")
    sub3_max_var = StringVar(value="1")

    ttk.Label(params_frame, text="CHL").grid(row=1, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=chl_min_var, justify="right").grid(row=1, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=chl_max_var, justify="right").grid(row=1, column=2, sticky="ew")

    ttk.Label(params_frame, text="CDOM").grid(row=2, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=cdom_min_var, justify="right").grid(row=2, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=cdom_max_var, justify="right").grid(row=2, column=2, sticky="ew")

    ttk.Label(params_frame, text="NAP").grid(row=3, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=nap_min_var, justify="right").grid(row=3, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=nap_max_var, justify="right").grid(row=3, column=2, sticky="ew")

    ttk.Label(params_frame, text="Depth").grid(row=4, column=0, sticky=W)
    depth_min_entry = ttk.Entry(params_frame, textvariable=depth_min_var, justify="right")
    depth_min_entry.grid(row=4, column=1, sticky="ew", padx=(0, 6))
    depth_max_entry = ttk.Entry(params_frame, textvariable=depth_max_var, justify="right")
    depth_max_entry.grid(row=4, column=2, sticky="ew")

    label_sub1 = ttk.Label(params_frame, text="Substrate 1")
    label_sub1.grid(row=5, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=sub1_min_var, justify="right").grid(row=5, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=sub1_max_var, justify="right").grid(row=5, column=2, sticky="ew")

    label_sub2 = ttk.Label(params_frame, text="Substrate 2")
    label_sub2.grid(row=6, column=0, sticky=W)
    ttk.Entry(params_frame, textvariable=sub2_min_var, justify="right").grid(row=6, column=1, sticky="ew", padx=(0, 6))
    ttk.Entry(params_frame, textvariable=sub2_max_var, justify="right").grid(row=6, column=2, sticky="ew")

    label_sub3 = ttk.Label(params_frame, text="Substrate 3")
    label_sub3.grid(row=7, column=0, sticky=W)
    sub3_min_entry = ttk.Entry(params_frame, textvariable=sub3_min_var, justify="right")
    sub3_min_entry.grid(row=7, column=1, sticky="ew", padx=(0, 6))
    sub3_max_entry = ttk.Entry(params_frame, textvariable=sub3_max_var, justify="right")
    sub3_max_entry.grid(row=7, column=2, sticky="ew")

    for tracked_var in (
        input_image_var,
        output_folder_var,
        chl_min_var,
        chl_max_var,
        cdom_min_var,
        cdom_max_var,
        nap_min_var,
        nap_max_var,
        depth_min_var,
        depth_max_var,
        sub1_min_var,
        sub1_max_var,
        sub2_min_var,
        sub2_max_var,
        sub3_min_var,
        sub3_max_var,
        chunk_rows,
        bathy_path_var,
        bathy_correction,
        bathy_tolerance,
    ):
        tracked_var.trace_add("write", update_run_button_state)
    input_image_var.trace_add("write", _sync_input_files_with_entry)
    allow_split.trace_add("write", update_run_button_state)
    bathy_mode.trace_add("write", update_run_button_state)

    def update_depth_state(*_args):
        if bathy_mode.get() == "input":
            try:
                depth_min_entry.state(["disabled"])
                depth_max_entry.state(["disabled"])
            except Exception:
                depth_min_entry.configure(state="disabled")
                depth_max_entry.configure(state="disabled")
        else:
            try:
                depth_min_entry.state(["!disabled"])
                depth_max_entry.state(["!disabled"])
            except Exception:
                depth_min_entry.configure(state="normal")
                depth_max_entry.configure(state="normal")

    bathy_mode.trace_add("write", update_depth_state)
    bathy_mode.trace_add("write", update_false_deep_correction_controls)
    update_depth_state()
    update_false_deep_correction_controls()

    def load_previous_run_settings():
        nonlocal template_config, spectral_library, compiled_siop, compiled_sensor, input_files

        xml_path = askopenfilename(
            parent=root,
            title="Load previous run log XML",
            filetypes=[("Run log XML", "*.xml"), ("All files", "*.*")],
        )
        if not xml_path:
            return

        try:
            xml_root = ET.parse(xml_path).getroot()

            new_template_config = template_config
            new_spectral_library = spectral_library
            new_selected_target_names = list(selected_target_names)
            new_scalar_values = dict(scalar_values)
            new_spectrum_override_paths = dict(spectrum_override_paths)
            new_sensor_name = sensor_state["sensor_name"]
            new_sensor_indices = {
                sensor_name: list(indices)
                for sensor_name, indices in sensor_state["selected_indices"].items()
            }

            siop_popup_node = xml_root.find("siop_popup")
            if siop_popup_node is not None:
                template_source = _resolve_bundled_resource(cwd, _xml_find_text(siop_popup_node, "template_source"))
                if template_source and os.path.exists(template_source):
                    new_template_config = siop_config.load_template_config(template_source)

                spectral_library_source = _xml_find_text(siop_popup_node, "spectral_library")
                if spectral_library_source and os.path.exists(spectral_library_source):
                    new_spectral_library = siop_config.load_spectral_library(spectral_library_source)

                raw_selected_targets = _xml_find_items(siop_popup_node, "selected_targets")
                if not raw_selected_targets:
                    raw_selected_targets = [
                        name for name in _xml_find_items(siop_popup_node, "xml_substrate_names")
                        if siop_config.clean_label(name) != siop_config.UNUSED_SUBSTRATE_NAME
                    ]
                matched_targets = []
                seen_targets = set()
                for raw_name in raw_selected_targets:
                    matched_name = _match_clean_name(raw_name, new_spectral_library["names"])
                    if matched_name and matched_name not in seen_targets:
                        matched_targets.append(matched_name)
                        seen_targets.add(matched_name)
                if matched_targets:
                    new_selected_target_names = matched_targets

                for key, _label, _required in siop_config.SIOP_SCALAR_FIELDS:
                    loaded_value = _xml_find_text(siop_popup_node, key)
                    if loaded_value is not None:
                        new_scalar_values[key] = loaded_value

                for spectrum_key in ("a_water", "a_ph_star"):
                    source_key = f"{spectrum_key}_source"
                    source_path = _resolve_bundled_resource(cwd, _xml_find_text(siop_popup_node, source_key, ""))
                    if source_path and os.path.exists(source_path) and not _paths_equivalent(source_path, new_template_config["template_path"]):
                        new_spectrum_override_paths[spectrum_key] = source_path
                    else:
                        new_spectrum_override_paths[spectrum_key] = ""

            compiled_siop_candidate = siop_config.build_siop_config(
                new_template_config,
                new_spectral_library,
                new_selected_target_names,
                new_scalar_values,
                spectrum_override_paths=new_spectrum_override_paths,
            )

            sensor_popup_node = xml_root.find("sensor_popup")
            if sensor_popup_node is not None:
                requested_sensor_name = _xml_find_text(sensor_popup_node, "sensor_name", new_sensor_name)
                template_source = _resolve_bundled_resource(cwd, _xml_find_text(sensor_popup_node, "sensor_template_source", ""))
                if template_source and os.path.exists(template_source):
                    sensor_templates[requested_sensor_name] = sensor_config.load_sensor_template(template_source, requested_sensor_name)
                if requested_sensor_name not in sensor_templates:
                    raise ValueError(f"No sensor template is available for '{requested_sensor_name}'.")
                selected_centers = _xml_find_items(sensor_popup_node, "selected_band_centers")
                matched_indices = _match_band_indices_by_center(sensor_templates[requested_sensor_name], selected_centers)
                if not matched_indices:
                    matched_indices = sensor_config.default_selected_band_indices(sensor_templates[requested_sensor_name])
                new_sensor_name = requested_sensor_name
                new_sensor_indices[new_sensor_name] = matched_indices

            compiled_sensor_candidate = sensor_config.build_sensor_config(
                sensor_templates[new_sensor_name],
                new_sensor_indices.get(new_sensor_name, []),
            )
        except Exception as exc:
            messagebox.showerror("Invalid run log", f"Unable to load settings from:\n{xml_path}\n\n{exc}")
            return

        template_config = new_template_config
        spectral_library = new_spectral_library
        selected_target_names[:] = list(compiled_siop_candidate["actual_selected_targets"])
        scalar_values.clear()
        scalar_values.update(new_scalar_values)
        spectrum_override_paths.clear()
        spectrum_override_paths.update(new_spectrum_override_paths)
        sensor_state["sensor_name"] = compiled_sensor_candidate["sensor_name"]
        sensor_state["selected_indices"].update(new_sensor_indices)
        sensor_state["selected_indices"][compiled_sensor_candidate["sensor_name"]] = list(compiled_sensor_candidate["selected_indices"])
        compiled_siop = None
        compiled_sensor = None

        loaded_images = _xml_find_items(xml_root, "images")
        if not loaded_images:
            loaded_image = _xml_find_text(xml_root, "image", "")
            if loaded_image:
                loaded_images = [loaded_image]
        if loaded_images:
            input_files = list(loaded_images)
            input_image_var.set(_display_input_selection(input_files))

        loaded_output_folder = _xml_find_text(xml_root, "output_folder", "")
        if not loaded_output_folder:
            loaded_output_folder = _infer_output_folder_from_output_file(_xml_find_text(xml_root, "output_file", ""))
        if loaded_output_folder:
            output_folder_var.set(loaded_output_folder)

        above_rrs_flag.set(_parse_bool_text(_xml_find_text(xml_root, "rrs_flag"), above_rrs_flag.get()))
        shallow_flag.set(_parse_bool_text(_xml_find_text(xml_root, "shallow"), shallow_flag.get()))
        optimize_initial_guesses_flag.set(_parse_bool_text(_xml_find_text(xml_root, "optimize_initial_guesses"), optimize_initial_guesses_flag.get()))
        five_initial_guess_testing_flag.set(_parse_bool_text(_xml_find_text(xml_root, "use_five_initial_guesses"), five_initial_guess_testing_flag.get()))
        initial_guess_debug_flag.set(_parse_bool_text(_xml_find_text(xml_root, "initial_guess_debug"), initial_guess_debug_flag.get()))
        pp.set(_parse_bool_text(_xml_find_text(xml_root, "post_processing", _xml_find_text(xml_root, "pproc")), pp.get()))
        fully_relaxed_flag.set(_parse_bool_text(_xml_find_text(xml_root, "fully_relaxed"), fully_relaxed_flag.get()))
        output_modeled_reflectance_flag.set(_parse_bool_text(_xml_find_text(xml_root, "output_modeled_reflectance"), output_modeled_reflectance_flag.get()))
        false_deep_correction_flag.set(_parse_bool_text(_xml_find_text(xml_root, "false_deep_correction_enabled"), false_deep_correction_flag.get()))
        relaxed.set(_parse_bool_text(_xml_find_text(xml_root, "relaxed"), relaxed.get()))
        allow_split.set(_parse_bool_text(_xml_find_text(xml_root, "allow_split"), allow_split.get()))
        loaded_chunk_rows = _xml_find_text(xml_root, "split_chunk_rows", chunk_rows.get())
        chunk_rows.set("" if loaded_chunk_rows is None else str(loaded_chunk_rows))

        loaded_output_format = (_xml_find_text(xml_root, "output_format", output_format.get()) or output_format.get()).lower()
        if loaded_output_format in {"netcdf", "geotiff", "both"}:
            output_format.set(loaded_output_format)

        loaded_pmin = _xml_find_items(xml_root, "pmin")
        loaded_pmax = _xml_find_items(xml_root, "pmax")
        if len(loaded_pmin) >= 7 and len(loaded_pmax) >= 7:
            chl_min_var.set(loaded_pmin[0]); chl_max_var.set(loaded_pmax[0])
            cdom_min_var.set(loaded_pmin[1]); cdom_max_var.set(loaded_pmax[1])
            nap_min_var.set(loaded_pmin[2]); nap_max_var.set(loaded_pmax[2])
            depth_min_var.set(loaded_pmin[3]); depth_max_var.set(loaded_pmax[3])
            sub1_min_var.set(loaded_pmin[4]); sub1_max_var.set(loaded_pmax[4])
            sub2_min_var.set(loaded_pmin[5]); sub2_max_var.set(loaded_pmax[5])
            sub3_saved_bounds["min"] = loaded_pmin[6]
            sub3_saved_bounds["max"] = loaded_pmax[6]
            sub3_min_var.set(loaded_pmin[6]); sub3_max_var.set(loaded_pmax[6])

        loaded_false_deep_values = {}
        for config_key, current_value in false_deep_correction_config.items():
            loaded_text = _xml_find_text(xml_root, f"false_deep_{config_key}")
            if loaded_text is None:
                continue
            if isinstance(current_value, bool):
                loaded_false_deep_values[config_key] = _parse_bool_text(loaded_text, current_value)
            elif isinstance(current_value, int) and not isinstance(current_value, bool):
                try:
                    loaded_false_deep_values[config_key] = int(float(loaded_text))
                except (TypeError, ValueError):
                    loaded_false_deep_values[config_key] = current_value
            else:
                try:
                    loaded_false_deep_values[config_key] = float(loaded_text)
                except (TypeError, ValueError):
                    loaded_false_deep_values[config_key] = current_value
        false_deep_correction_config.update(loaded_false_deep_values)

        use_bathy = _parse_bool_text(_xml_find_text(xml_root, "use_bathy"), False)
        default_emodnet_path = _resolve_bundled_resource(cwd, os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"))
        if use_bathy:
            loaded_bathy_path = _resolve_bundled_resource(cwd, _xml_find_text(xml_root, "bathy_path", "") or "")
            loaded_bathy_reference = (_xml_find_text(xml_root, "bathy_reference", "depth") or "depth").strip().lower()
            bathy_mode.set("input")
            bathy_path_var.set(loaded_bathy_path)
            bathy_correction.set(str(_xml_find_text(xml_root, "bathy_correction_m", bathy_correction.get()) or "0"))
            bathy_tolerance.set(str(_xml_find_text(xml_root, "bathy_tolerance_m", bathy_tolerance.get()) or "0"))
            is_emodnet = _paths_equivalent(loaded_bathy_path, default_emodnet_path) and loaded_bathy_reference == "depth"
            use_emodnet_var.set(is_emodnet)
            user_defined_var.set(not is_emodnet and bool(loaded_bathy_path))
            if is_emodnet:
                bathy_info_var.set("EMODnet: E4_2024.tif")
            else:
                bathy_info_var.set(os.path.basename(loaded_bathy_path) if loaded_bathy_path else "")
        else:
            bathy_mode.set("estimate")
            bathy_path_var.set("")
            bathy_info_var.set("")
            bathy_correction.set("0")
            bathy_tolerance.set("0")
            use_emodnet_var.set(False)
            user_defined_var.set(False)

        update_substrate_ui()
        update_sensor_ui()
        update_initial_guess_controls()
        update_false_deep_correction_controls()

        messagebox.showinfo(
            "Settings loaded",
            "Run settings were loaded from the selected log XML.\n\n"
            "The input image(s), output folder, and processing options were restored from that run. "
            "You can still change the input image or output folder before starting a new run.",
            parent=root,
        )

    fmt_frame = ttk.Labelframe(input_tab, text="Output Format")
    fmt_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=4)
    fmt_frame.columnconfigure(0, weight=1)

    output_format = tk.StringVar(value="both")
    ttk.Radiobutton(fmt_frame, text="NetCDF", value="netcdf", variable=output_format).grid(row=0, column=0, sticky=W)
    ttk.Radiobutton(fmt_frame, text="GeoTIFF", value="geotiff", variable=output_format).grid(row=1, column=0, sticky=W)
    ttk.Radiobutton(fmt_frame, text="Both", value="both", variable=output_format).grid(row=2, column=0, sticky=W)

    def validate_and_close():
        nonlocal compiled_siop, compiled_sensor

        validation_error = _get_form_validation_error()
        if validation_error is not None:
            messagebox.showerror("Incomplete run settings", validation_error)
            return
        try:
            compiled_siop = build_current_siop()
        except Exception as exc:
            messagebox.showerror("Invalid Water & Bottom Settings", str(exc))
            return

        try:
            compiled_sensor = build_current_sensor()
        except Exception as exc:
            messagebox.showerror("Invalid sensor setup", str(exc))
            return

        root.destroy()

    actions = ttk.Frame(container)
    actions.grid(row=1, column=0, sticky="ew", padx=4, pady=(8, 0))
    actions.columnconfigure(0, weight=1)
    actions.columnconfigure(1, weight=0)
    actions.columnconfigure(2, weight=0)
    ttk.Button(actions, text="Cancel", command=on_close).grid(row=0, column=0, sticky="w")
    ttk.Button(actions, text="Load settings", command=load_previous_run_settings).grid(row=0, column=1, sticky="e", padx=(0, 8))
    run_button = ttk.Button(actions, text="Run", command=validate_and_close)
    run_button.grid(row=0, column=2, sticky="e")

    update_substrate_ui()
    update_sensor_ui()
    update_run_button_state()
    root.protocol("WM_DELETE_WINDOW", on_close)
    center_window(root)
    root.mainloop()

    if cancelled:
        return None

    if compiled_siop is None:
        try:
            compiled_siop = build_current_siop()
        except Exception:
            return None

    if compiled_sensor is None:
        try:
            compiled_sensor = build_current_sensor()
        except Exception:
            return None

    file_list = input_files if input_files else ([input_image_var.get()] if input_image_var.get() else [])
    out_folder = output_folder_var.get()

    pmin = [
        float(chl_min_var.get()),
        float(cdom_min_var.get()),
        float(nap_min_var.get()),
        float(depth_min_var.get()),
        float(sub1_min_var.get()),
        float(sub2_min_var.get()),
        float(sub3_min_var.get()),
    ]
    pmax = [
        float(chl_max_var.get()),
        float(cdom_max_var.get()),
        float(nap_max_var.get()),
        float(depth_max_var.get()),
        float(sub1_max_var.get()),
        float(sub2_max_var.get()),
        float(sub3_max_var.get()),
    ]

    filename_im = os.path.basename(file_list[0]) if file_list else ""
    run_dir = os.path.join(out_folder, f"swampy_run_{year}{month}{day}_{hour}{minute}{second}")
    if not os.path.isdir(run_dir):
        os.makedirs(run_dir)
    input_base = os.path.splitext(filename_im)[0] if filename_im else f"run_{year}{month}{day}_{hour}{minute}{second}"
    ofile = os.path.join(run_dir, f"swampy_{input_base}.nc")

    file_iop = os.path.join(run_dir, "generated_siop.xml")
    siop_config.write_siop_xml(file_iop, compiled_siop)
    file_sensor = os.path.join(run_dir, "generated_sensor_filter.xml")
    sensor_config.write_sensor_xml(file_sensor, compiled_sensor)

    input_dict = {
        "image": file_list[0] if file_list else "",
        "images": list(file_list),
        "SIOPS": file_iop,
        "sensor_filter": file_sensor,
        "nedr_mode": "fixed",
        "pmin": pmin,
        "pmax": pmax,
        "rrs_flag": above_rrs_flag.get(),
        "shallow": shallow_flag.get(),
        "optimize_initial_guesses": optimize_initial_guesses_flag.get(),
        "use_five_initial_guesses": five_initial_guess_testing_flag.get(),
        "initial_guess_debug": initial_guess_debug_flag.get(),
        "post_processing": bool(pp.get()),
        "fully_relaxed": fully_relaxed_flag.get(),
        "output_modeled_reflectance": output_modeled_reflectance_flag.get(),
        "false_deep_correction_enabled": false_deep_correction_flag.get(),
        "false_deep_anchor_min_sdi": false_deep_correction_config["anchor_min_sdi"],
        "false_deep_anchor_max_depth_m": false_deep_correction_config["anchor_max_depth_m"],
        "false_deep_anchor_max_slope_percent": false_deep_correction_config["anchor_max_slope_percent"],
        "false_deep_anchor_max_error_f": false_deep_correction_config["anchor_max_error_f"],
        "false_deep_anchor_min_depth_margin_m": false_deep_correction_config["anchor_min_depth_margin_m"],
        "false_deep_suspect_max_sdi": false_deep_correction_config["suspect_max_sdi"],
        "false_deep_suspect_min_slope_percent": false_deep_correction_config["suspect_min_slope_percent"],
        "false_deep_suspect_min_depth_jump_m": false_deep_correction_config["suspect_min_depth_jump_m"],
        "false_deep_search_radius_px": false_deep_correction_config["search_radius_px"],
        "false_deep_min_anchor_count": false_deep_correction_config["min_anchor_count"],
        "false_deep_correction_tolerance_m": false_deep_correction_config["correction_tolerance_m"],
        "false_deep_max_patch_size_px": false_deep_correction_config["max_patch_size_px"],
        "false_deep_treat_min_depth_as_barrier": false_deep_correction_config["treat_min_depth_as_barrier"],
        "false_deep_barrier_depth_margin_m": false_deep_correction_config["barrier_depth_margin_m"],
        "false_deep_barrier_min_sdi": false_deep_correction_config["barrier_min_sdi"],
        "false_deep_debug_export": false_deep_correction_config["debug_export"],
        "relaxed": relaxed.get(),
        "output_folder": out_folder,
        "output_file": ofile,
        "output_format": output_format.get(),
        "allow_split": allow_split.get(),
        "split_chunk_rows": chunk_rows.get().strip(),
        "siop_popup": siop_config.build_log_payload(compiled_siop, template_config, spectral_library),
        "sensor_popup": sensor_config.build_log_payload(compiled_sensor),
    }

    bathy_path = ""
    if bathy_mode.get() == "input":
        bathy_path = bathy_path_var.get() or _resolve_bundled_resource(cwd, os.path.join(cwd, "Data", "Bathy", "E4_2024.tif"))
        input_dict["use_bathy"] = True
        input_dict["bathy_path"] = bathy_path
        input_dict["bathy_reference"] = "hydrographic_zero" if user_defined_var.get() else "depth"
        try:
            input_dict["bathy_correction_m"] = float(bathy_correction.get())
        except Exception:
            input_dict["bathy_correction_m"] = 0.0
        try:
            input_dict["bathy_tolerance_m"] = float(bathy_tolerance.get())
        except Exception:
            input_dict["bathy_tolerance_m"] = 0.0
    else:
        input_dict["use_bathy"] = False
        input_dict["bathy_path"] = ""
        input_dict["bathy_reference"] = "depth"
        input_dict["bathy_correction_m"] = 0.0
        input_dict["bathy_tolerance_m"] = 0.0

    xml_file = os.path.join(run_dir, f"log_{input_base}.xml")

    return (
        file_list,
        ofile,
        file_iop,
        file_sensor,
        above_rrs_flag.get(),
        relaxed.get(),
        shallow_flag.get(),
        optimize_initial_guesses_flag.get(),
        five_initial_guess_testing_flag.get(),
        initial_guess_debug_flag.get(),
        fully_relaxed_flag.get(),
        output_modeled_reflectance_flag.get(),
        {
            "enabled": false_deep_correction_flag.get(),
            **false_deep_correction_config,
        },
        pmin,
        pmax,
        xml_file,
        input_dict,
        output_format.get(),
        bathy_path,
        bool(pp.get()),
        allow_split.get(),
        chunk_rows.get().strip(),
    )
