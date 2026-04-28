import csv
import json
import math
import os
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET

import numpy as np
import xmltodict


SIOP_SCALAR_FIELDS = [
    ("a_cdom_slope", "Spectral slope of CDOM absorption [nm^-1]", True),
    ("a_nap_slope", "Spectral slope of NAP absorption [nm^-1]", True),
    ("bb_ph_slope", "Power law exponent for algal particle backscattering [-]", True),
    ("bb_nap_slope", "Power law exponent for NAP backscattering [-]", False),
    ("lambda0cdom", "Reference wavelength for CDOM absorption [nm]", True),
    ("lambda0nap", "Reference wavelength for NAP absorption [nm]", True),
    ("lambda0x", "Reference wavelength for backscattering [nm]", True),
    ("x_ph_lambda0x", "Specific phytoplankton backscattering at the reference wavelength [m^2 mg^-1]", True),
    ("x_nap_lambda0x", "Specific NAP backscattering at the reference wavelength [m^2 g^-1]", True),
    ("a_cdom_lambda0cdom", "Specific CDOM absorption at the reference wavelength [m^-1]", True),
    ("a_nap_lambda0nap", "Specific NAP absorption at the reference wavelength [m^2 g^-1]", True),
    ("bb_lambda_ref", "Reference wavelength used for pure-water backscattering [nm]", True),
    ("water_refractive_index", "Water refractive index [-]", True),
]

UNUSED_SUBSTRATE_NAME = "Unused substrate"
UNTAGGED_TAG = "Untagged"


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _extract_items(node):
    if isinstance(node, dict) and "item" in node:
        return _as_list(node["item"])
    return _as_list(node)


def clean_label(label):
    text = "" if label is None else str(label)
    text = text.replace("\\", "/").split("/")[-1]
    if ":" in text:
        text = text.split(":")[-1]
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_float_list(node):
    return [float(item) for item in _extract_items(node)]


def _parse_spectrum(node):
    items = _extract_items(node)
    if len(items) < 2:
        raise ValueError("Invalid spectrum definition in SIOP template.")
    wavelengths = _parse_float_list(items[0])
    values = _parse_float_list(items[1])
    if len(wavelengths) != len(values):
        raise ValueError("Spectrum wavelengths and values have different lengths.")
    return wavelengths, values


def load_template_config(template_path):
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"SIOP template not found: {template_path}")
    with open(template_path, "rb") as handle:
        data = xmltodict.parse(handle.read())
    root = data["root"]
    scalar_fields = {}
    for key, _label, _required in SIOP_SCALAR_FIELDS:
        value = root.get(key)
        scalar_fields[key] = "" if value is None else str(value)

    raw_names = _extract_items(root.get("substrate_names"))
    template_substrate_names = [clean_label(name) for name in raw_names if clean_label(name)]

    return {
        "template_path": template_path,
        "a_water": _parse_spectrum(root["a_water"]),
        "a_ph_star": _parse_spectrum(root["a_ph_star"]),
        "scalar_fields": scalar_fields,
        "template_substrate_names": template_substrate_names,
    }


def load_spectral_library(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Spectral library not found: {csv_path}")

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if len(fieldnames) < 3:
            raise ValueError("Spectral library must contain wavelength and at least two spectra.")
        wavelength_key = fieldnames[0]
        names = fieldnames[1:]
        wavelengths = []
        spectra_values = {name: [] for name in names}
        for row in reader:
            wavelengths.append(float(row[wavelength_key]))
            for name in names:
                spectra_values[name].append(float(row[name]))

    spectra = {
        name: (list(wavelengths), spectra_values[name])
        for name in names
    }
    tags = load_spectral_library_tags(csv_path, names)
    return {
        "path": csv_path,
        "wavelength_key": wavelength_key,
        "wavelengths": list(wavelengths),
        "names": names,
        "spectra": spectra,
        "tags": tags,
    }


def load_two_column_spectrum_csv(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Spectrum CSV not found: {csv_path}")

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.reader(handle, dialect)

        rows = []
        skipped_header = False
        for line_number, row in enumerate(reader, start=1):
            meaningful = [cell.strip() for cell in row if cell is not None and str(cell).strip() != ""]
            if not meaningful:
                continue
            if len(meaningful) < 2:
                raise ValueError(
                    f"Spectrum CSV '{os.path.basename(csv_path)}' must contain two columns on line {line_number}."
                )
            try:
                wavelength = float(meaningful[0])
                value = float(meaningful[1])
            except ValueError:
                if rows or skipped_header:
                    raise ValueError(
                        f"Spectrum CSV '{os.path.basename(csv_path)}' contains a non-numeric row at line {line_number}."
                    )
                skipped_header = True
                continue
            if not math.isfinite(wavelength) or not math.isfinite(value):
                raise ValueError(
                    f"Spectrum CSV '{os.path.basename(csv_path)}' contains non-finite values at line {line_number}."
                )
            rows.append((wavelength, value))

    if len(rows) < 2:
        raise ValueError(f"Spectrum CSV '{os.path.basename(csv_path)}' must contain at least two numeric rows.")

    rows.sort(key=lambda item: item[0])
    wavelengths = [item[0] for item in rows]
    if len(set(wavelengths)) != len(wavelengths):
        raise ValueError(f"Spectrum CSV '{os.path.basename(csv_path)}' contains duplicate wavelengths.")
    values = [item[1] for item in rows]
    return wavelengths, values


def spectral_library_backup_path(csv_path):
    root, ext = os.path.splitext(csv_path)
    if not ext:
        ext = ".csv"
    return f"{root}.original_backup{ext}"


def spectral_library_metadata_path(csv_path):
    root, _ext = os.path.splitext(csv_path)
    return f"{root}.metadata.json"


def spectral_library_metadata_backup_path(csv_path):
    root, _ext = os.path.splitext(csv_path)
    return f"{root}.metadata.original_backup.json"


def _clean_tag(tag):
    return re.sub(r"\s+", " ", ("" if tag is None else str(tag)).strip())


def display_tag(tag):
    cleaned = _clean_tag(tag)
    return cleaned if cleaned else UNTAGGED_TAG


def load_spectral_library_tags(csv_path, names):
    metadata_path = spectral_library_metadata_path(csv_path)
    tags = {name: "" for name in names}
    if not os.path.exists(metadata_path):
        return tags

    with open(metadata_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    raw_tags = data.get("tags", {}) if isinstance(data, dict) else {}
    if not isinstance(raw_tags, dict):
        return tags
    for name in names:
        if name in raw_tags:
            tags[name] = _clean_tag(raw_tags[name])
    return tags


def ensure_spectral_library_backup(csv_path):
    backup_path = spectral_library_backup_path(csv_path)
    if not os.path.exists(backup_path):
        shutil.copy2(csv_path, backup_path)
    metadata_path = spectral_library_metadata_path(csv_path)
    metadata_backup_path = spectral_library_metadata_backup_path(csv_path)
    if os.path.exists(metadata_path) and not os.path.exists(metadata_backup_path):
        shutil.copy2(metadata_path, metadata_backup_path)
    return backup_path


def _validate_spectrum_name(name, existing_names):
    cleaned_name = "" if name is None else str(name).strip()
    if not cleaned_name:
        raise ValueError("Spectrum name is required.")

    cleaned_key = clean_label(cleaned_name).lower()
    if cleaned_key == "wl":
        raise ValueError("Spectrum name 'wl' is reserved.")

    for existing_name in existing_names:
        if clean_label(existing_name).lower() == cleaned_key:
            raise ValueError(f"A spectrum named '{cleaned_name}' already exists in the library.")
    return cleaned_name


def _interpolate_to_reference_wavelengths(source_wavelengths, source_values, reference_wavelengths):
    source_x = np.asarray(source_wavelengths, dtype=float)
    source_y = np.asarray(source_values, dtype=float)
    target_x = np.asarray(reference_wavelengths, dtype=float)

    if source_x.ndim != 1 or source_y.ndim != 1 or target_x.ndim != 1:
        raise ValueError("Spectra must be one-dimensional.")
    if len(source_x) != len(source_y):
        raise ValueError("Spectrum wavelengths and values have different lengths.")
    if len(source_x) < 2:
        raise ValueError("Spectrum must contain at least two samples.")
    if np.min(source_x) > np.min(target_x) or np.max(source_x) < np.max(target_x):
        raise ValueError(
            "The provided spectrum does not cover the full wavelength range of the spectral library."
        )

    interpolated = np.interp(target_x, source_x, source_y)
    return [float(value) for value in interpolated]


def add_spectrum_to_library(spectral_library, name, source_wavelengths, source_values):
    validated_name = _validate_spectrum_name(name, spectral_library["names"])
    interpolated_values = _interpolate_to_reference_wavelengths(
        source_wavelengths,
        source_values,
        spectral_library["wavelengths"],
    )

    new_names = list(spectral_library["names"]) + [validated_name]
    new_spectra = dict(spectral_library["spectra"])
    new_spectra[validated_name] = (list(spectral_library["wavelengths"]), interpolated_values)
    new_tags = dict(spectral_library.get("tags", {}))
    new_tags[validated_name] = ""
    return {
        "path": spectral_library["path"],
        "wavelength_key": spectral_library.get("wavelength_key", "wl"),
        "wavelengths": list(spectral_library["wavelengths"]),
        "names": new_names,
        "spectra": new_spectra,
        "tags": new_tags,
    }


def remove_spectra_from_library(spectral_library, names_to_remove):
    names_to_remove = [name for name in names_to_remove if name in spectral_library["spectra"]]
    if not names_to_remove:
        raise ValueError("Select at least one spectrum to remove.")

    remaining_names = [name for name in spectral_library["names"] if name not in set(names_to_remove)]
    if len(remaining_names) < 2:
        raise ValueError("The spectral library must keep at least two spectra.")

    new_spectra = {
        name: spectral_library["spectra"][name]
        for name in remaining_names
    }
    old_tags = spectral_library.get("tags", {})
    new_tags = {
        name: _clean_tag(old_tags.get(name, ""))
        for name in remaining_names
    }
    return {
        "path": spectral_library["path"],
        "wavelength_key": spectral_library.get("wavelength_key", "wl"),
        "wavelengths": list(spectral_library["wavelengths"]),
        "names": remaining_names,
        "spectra": new_spectra,
        "tags": new_tags,
    }


def modify_spectrum_in_library(spectral_library, current_name, new_name, new_tag):
    if current_name not in spectral_library["spectra"]:
        raise ValueError("The selected spectrum is not available in the library.")

    cleaned_tag = _clean_tag(new_tag)
    validated_name = _validate_spectrum_name(
        new_name,
        [name for name in spectral_library["names"] if name != current_name],
    )

    new_names = []
    new_spectra = {}
    old_tags = dict(spectral_library.get("tags", {}))
    new_tags = {}

    for name in spectral_library["names"]:
        target_name = validated_name if name == current_name else name
        new_names.append(target_name)
        new_spectra[target_name] = spectral_library["spectra"][name]
        if name == current_name:
            new_tags[target_name] = cleaned_tag
        else:
            new_tags[target_name] = _clean_tag(old_tags.get(name, ""))

    return {
        "path": spectral_library["path"],
        "wavelength_key": spectral_library.get("wavelength_key", "wl"),
        "wavelengths": list(spectral_library["wavelengths"]),
        "names": new_names,
        "spectra": new_spectra,
        "tags": new_tags,
    }


def write_spectral_library(spectral_library):
    csv_path = spectral_library["path"]
    if not csv_path:
        raise ValueError("Spectral library path is not defined.")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Spectral library not found: {csv_path}")

    ensure_spectral_library_backup(csv_path)

    fieldnames = [spectral_library.get("wavelength_key", "wl")] + list(spectral_library["names"])
    directory = os.path.dirname(csv_path) or "."
    handle = tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        delete=False,
        dir=directory,
        prefix="spectral_library_",
        suffix=".csv",
    )
    temp_path = handle.name
    try:
        writer = csv.writer(handle)
        writer.writerow(fieldnames)
        reference_wavelengths = list(spectral_library["wavelengths"])
        spectra = spectral_library["spectra"]
        for row_index, wavelength in enumerate(reference_wavelengths):
            row = [wavelength]
            for name in spectral_library["names"]:
                values = spectra[name][1]
                row.append(values[row_index])
            writer.writerow(row)
        handle.close()
        os.replace(temp_path, csv_path)
        write_spectral_library_metadata(spectral_library)
    except Exception:
        try:
            handle.close()
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        raise


def write_spectral_library_metadata(spectral_library):
    csv_path = spectral_library["path"]
    metadata_path = spectral_library_metadata_path(csv_path)
    directory = os.path.dirname(metadata_path) or "."
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        delete=False,
        dir=directory,
        prefix="spectral_library_",
        suffix=".json",
    )
    temp_path = handle.name
    try:
        payload = {
            "tags": {
                name: _clean_tag(spectral_library.get("tags", {}).get(name, ""))
                for name in spectral_library["names"]
            }
        }
        json.dump(payload, handle, indent=2, ensure_ascii=True)
        handle.write("\n")
        handle.close()
        os.replace(temp_path, metadata_path)
    except Exception:
        try:
            handle.close()
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        raise


def _label_tokens(label):
    return re.findall(r"[a-z0-9]+", clean_label(label).lower())


def default_selected_target_names(template_names, available_names, max_targets=3):
    selections = []
    used = set()

    for template_name in template_names:
        template_tokens = set(_label_tokens(template_name))
        best_name = None
        best_score = 0
        for candidate in available_names:
            if candidate in used:
                continue
            candidate_tokens = set(_label_tokens(candidate))
            if not candidate_tokens:
                continue
            score = len(template_tokens & candidate_tokens)
            if clean_label(template_name).lower() == clean_label(candidate).lower():
                score += 100
            elif clean_label(template_name).lower() in clean_label(candidate).lower():
                score += 50
            if score > best_score:
                best_score = score
                best_name = candidate
        if best_name is not None and best_score > 0:
            selections.append(best_name)
            used.add(best_name)
        if len(selections) == max_targets:
            break

    for candidate in available_names:
        if candidate in used:
            continue
        selections.append(candidate)
        used.add(candidate)
        if len(selections) == max_targets:
            break

    return selections


def validate_scalar_values(scalar_values):
    validated = {}
    for key, label, required in SIOP_SCALAR_FIELDS:
        raw_value = scalar_values.get(key, "")
        text = "" if raw_value is None else str(raw_value).strip()
        if not text:
            if required:
                raise ValueError(f"{label} is required.")
            validated[key] = None
            continue
        try:
            validated[key] = float(text)
        except ValueError as exc:
            raise ValueError(f"{label} must be numeric.") from exc
    return validated


def prepare_selected_targets(spectral_library, selected_names):
    cleaned = []
    seen = set()
    for name in selected_names:
        if name in spectral_library["spectra"] and name not in seen:
            cleaned.append(name)
            seen.add(name)

    if len(cleaned) < 2:
        raise ValueError("Select at least two target spectra.")
    if len(cleaned) > 3:
        raise ValueError("Select at most three target spectra.")

    prepared = [(name, spectral_library["spectra"][name]) for name in cleaned]
    generated_unused = False
    if len(prepared) == 2:
        wavelengths = list(spectral_library["wavelengths"])
        prepared.append((UNUSED_SUBSTRATE_NAME, (wavelengths, [0.0] * len(wavelengths))))
        generated_unused = True

    return {
        "actual_selected_names": cleaned,
        "generated_unused_substrate": generated_unused,
        "names": [name for name, _spectrum in prepared],
        "spectra": [spectrum for _name, spectrum in prepared],
    }


def build_siop_config(template_config, spectral_library, selected_names, scalar_values, spectrum_override_paths=None):
    validated_scalars = validate_scalar_values(scalar_values)
    target_info = prepare_selected_targets(spectral_library, selected_names)
    spectrum_override_paths = spectrum_override_paths or {}
    a_water_path = str(spectrum_override_paths.get("a_water", "") or "").strip()
    a_ph_star_path = str(spectrum_override_paths.get("a_ph_star", "") or "").strip()
    a_water = load_two_column_spectrum_csv(a_water_path) if a_water_path else template_config["a_water"]
    a_ph_star = load_two_column_spectrum_csv(a_ph_star_path) if a_ph_star_path else template_config["a_ph_star"]
    siop_config = {
        "a_water": a_water,
        "a_ph_star": a_ph_star,
        "substrates": target_info["spectra"],
        "substrate_names": target_info["names"],
        "actual_selected_targets": target_info["actual_selected_names"],
        "generated_unused_substrate": target_info["generated_unused_substrate"],
        "a_water_source": a_water_path or template_config["template_path"],
        "a_ph_star_source": a_ph_star_path or template_config["template_path"],
    }
    siop_config.update(validated_scalars)
    return siop_config


def _append_sequence(parent, values):
    container = ET.SubElement(parent, "item")
    for value in values:
        item = ET.SubElement(container, "item")
        item.text = _format_value(value)
    return container


def _append_spectrum(parent, tag, spectrum):
    element = ET.SubElement(parent, tag)
    wavelengths, values = spectrum
    _append_sequence(element, wavelengths)
    _append_sequence(element, values)
    return element


def _format_value(value):
    if value is None:
        return None
    if isinstance(value, float):
        return format(value, ".15g")
    return str(value)


def write_siop_xml(output_path, siop_config):
    root = ET.Element("root")

    _append_spectrum(root, "a_water", siop_config["a_water"])
    _append_spectrum(root, "a_ph_star", siop_config["a_ph_star"])

    substrates = ET.SubElement(root, "substrates")
    for spectrum in siop_config["substrates"]:
        substrate = ET.SubElement(substrates, "item")
        _append_sequence(substrate, spectrum[0])
        _append_sequence(substrate, spectrum[1])

    names_element = ET.SubElement(root, "substrate_names")
    for name in siop_config["substrate_names"]:
        item = ET.SubElement(names_element, "item")
        item.text = clean_label(name)

    for key, _label, _required in SIOP_SCALAR_FIELDS:
        element = ET.SubElement(root, key)
        value = siop_config.get(key)
        if value is not None:
            element.text = _format_value(value)

    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(output_path, encoding="utf-8", xml_declaration=True)


def build_log_payload(siop_config, template_config, spectral_library):
    payload = {
        "template_source": template_config["template_path"],
        "spectral_library": spectral_library["path"],
        "selected_targets": list(siop_config["actual_selected_targets"]),
        "xml_substrate_names": list(siop_config["substrate_names"]),
        "generated_unused_substrate": bool(siop_config["generated_unused_substrate"]),
        "a_water_source": siop_config.get("a_water_source", template_config["template_path"]),
        "a_ph_star_source": siop_config.get("a_ph_star_source", template_config["template_path"]),
    }
    for key, _label, _required in SIOP_SCALAR_FIELDS:
        payload[key] = siop_config.get(key)
    return payload
