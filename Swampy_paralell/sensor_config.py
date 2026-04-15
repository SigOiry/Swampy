import os
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET


BUILTIN_SENSOR_CANDIDATES = {
    "Sentinel-2": [
        os.path.join("Data", "SRF", "swampy_s2_6_bands_filter_nedr.xml"),
        os.path.join("Data", "SRF", "swampy_s2_5_bands_filter_nedr.xml"),
        os.path.join("Test", "swampy_s2_5_bands_filter_nedr.xml"),
        os.path.join("Data", "Test", "swampy_s2_5_bands_filter_nedr.xml"),
    ],
    "PRISMA": [
        os.path.join("Data", "SRF", "swampy_prisma_63_bands_filter_nedr.xml"),
    ],
}


def _read_float_items(node, xpath):
    return [float(item.text) for item in node.findall(xpath)]


def _find_existing_template(candidates):
    for path in candidates:
        if path and os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    return None


def _clean_sensor_display_name(name):
    text = "" if name is None else str(name)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _read_sensor_name_from_xml(template_path):
    try:
        root = ET.parse(template_path).getroot()
    except Exception:
        return None
    node = root.find("./sensor_name")
    if node is None or node.text is None:
        return None
    text = _clean_sensor_display_name(node.text)
    return text or None


def _infer_sensor_name_from_path(template_path):
    basename = os.path.splitext(os.path.basename(template_path))[0]
    lowered = basename.lower()
    if "prisma" in lowered:
        return "PRISMA"
    if "sentinel" in lowered or re.search(r"(^|[_-])s2([_-]|$)", lowered):
        return "Sentinel-2"
    label = re.sub(r"[_-]+", " ", basename).strip()
    return label.title() if label else basename


def _safe_sensor_filename(sensor_name):
    slug = re.sub(r"[^0-9A-Za-z]+", "_", _clean_sensor_display_name(sensor_name)).strip("_").lower()
    return slug or "sensor"


def _sensor_template_dir(repo_root):
    return os.path.join(repo_root, "Data", "SRF")


def sensor_templates_backup_dir(repo_root):
    return os.path.join(_sensor_template_dir(repo_root), "original_backup")


def ensure_sensor_templates_backup(repo_root):
    sensor_dir = _sensor_template_dir(repo_root)
    backup_dir = sensor_templates_backup_dir(repo_root)
    if os.path.isdir(backup_dir) and any(os.scandir(backup_dir)):
        return backup_dir, False

    os.makedirs(backup_dir, exist_ok=True)
    if os.path.isdir(sensor_dir):
        for entry in os.scandir(sensor_dir):
            if not entry.is_file():
                continue
            if not entry.name.lower().endswith(".xml"):
                continue
            shutil.copy2(entry.path, os.path.join(backup_dir, entry.name))
    return backup_dir, True


def default_sensor_templates(repo_root):
    templates = {}
    assigned_paths = set()

    for sensor_name, relative_candidates in BUILTIN_SENSOR_CANDIDATES.items():
        path = _find_existing_template([os.path.join(repo_root, candidate) for candidate in relative_candidates])
        if path:
            templates[sensor_name] = path
            assigned_paths.add(os.path.normcase(os.path.normpath(os.path.abspath(path))))

    sensor_dir = _sensor_template_dir(repo_root)
    if os.path.isdir(sensor_dir):
        for entry in sorted(os.scandir(sensor_dir), key=lambda item: item.name.lower()):
            if not entry.is_file() or not entry.name.lower().endswith(".xml"):
                continue
            full_path = os.path.normcase(os.path.normpath(os.path.abspath(entry.path)))
            if full_path in assigned_paths:
                continue
            if os.path.getsize(entry.path) == 0:
                continue
            sensor_name = _read_sensor_name_from_xml(entry.path) or _infer_sensor_name_from_path(entry.path)
            if sensor_name in templates:
                sensor_name = f"{sensor_name} ({os.path.splitext(entry.name)[0]})"
            templates[sensor_name] = entry.path
    return templates


def load_sensor_template(template_path, sensor_name):
    if not template_path:
        raise FileNotFoundError(f"No bundled template is available for {sensor_name}.")
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Sensor template not found: {template_path}")
    if os.path.getsize(template_path) == 0:
        raise ValueError(f"Sensor template is empty: {template_path}")

    root = ET.parse(template_path).getroot()
    wavelengths = _read_float_items(root, "./sensor_filter/item[1]/item")
    filter_rows = []
    for node in root.findall("./sensor_filter/item[2]/item"):
        filter_rows.append([float(item.text) for item in node.findall("./item")])
    nedr_wavelengths = _read_float_items(root, "./nedr/item[1]/item")
    nedr_values = _read_float_items(root, "./nedr/item[2]/item")

    if len(filter_rows) != len(nedr_wavelengths) or len(nedr_wavelengths) != len(nedr_values):
        raise ValueError(f"Sensor template '{template_path}' has inconsistent band counts.")

    bands = []
    for index, (center, nedr_value, response) in enumerate(zip(nedr_wavelengths, nedr_values, filter_rows)):
        if len(response) != len(wavelengths):
            raise ValueError(f"Band {index + 1} in '{template_path}' does not match the wavelength grid.")
        bands.append({
            "index": index,
            "center": float(center),
            "nedr": float(nedr_value),
            "response": response,
            "label": f"{center:.0f} nm" if float(center).is_integer() else f"{center:.1f} nm",
        })

    return {
        "sensor_name": sensor_name,
        "template_path": template_path,
        "wavelengths": wavelengths,
        "bands": bands,
    }


def default_selected_band_indices(template):
    if supports_smart_selection(template):
        visible = select_bands_by_range(template, min_wavelength=400.0, max_wavelength=700.0, step=1)
        if visible:
            return visible
    return [band["index"] for band in template["bands"]]


def supports_smart_selection(template):
    return len(template["bands"]) > 20 or template["sensor_name"] == "PRISMA"


def select_bands_by_range(template, min_wavelength=None, max_wavelength=None, step=1):
    step = max(1, int(step))
    filtered = []
    for band in template["bands"]:
        center = band["center"]
        if min_wavelength is not None and center < min_wavelength:
            continue
        if max_wavelength is not None and center > max_wavelength:
            continue
        filtered.append(band["index"])
    return filtered[::step]


def build_sensor_config(template, selected_band_indices):
    wanted = []
    seen = set()
    for index in selected_band_indices:
        if index in seen:
            continue
        seen.add(index)
        wanted.append(int(index))

    selected_bands = [band for band in template["bands"] if band["index"] in seen]
    if len(selected_bands) < 4:
        raise ValueError("Select at least four bands.")

    return {
        "sensor_name": template["sensor_name"],
        "template_path": template["template_path"],
        "wavelengths": list(template["wavelengths"]),
        "bands": selected_bands,
        "selected_indices": wanted,
    }


def write_sensor_xml(output_path, sensor_config):
    root = ET.Element("root")

    sensor_filter = ET.SubElement(root, "sensor_filter")
    wavelengths_item = ET.SubElement(sensor_filter, "item")
    for wavelength in sensor_config["wavelengths"]:
        item = ET.SubElement(wavelengths_item, "item")
        item.text = _format_value(wavelength)

    filter_bands_item = ET.SubElement(sensor_filter, "item")
    for band in sensor_config["bands"]:
        band_item = ET.SubElement(filter_bands_item, "item")
        for value in band["response"]:
            value_item = ET.SubElement(band_item, "item")
            value_item.text = _format_value(value)

    nedr = ET.SubElement(root, "nedr")
    centers_item = ET.SubElement(nedr, "item")
    values_item = ET.SubElement(nedr, "item")
    for band in sensor_config["bands"]:
        center_item = ET.SubElement(centers_item, "item")
        center_item.text = _format_value(band["center"])
        nedr_item = ET.SubElement(values_item, "item")
        nedr_item.text = _format_value(band["nedr"])

    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(output_path, encoding="utf-8", xml_declaration=True)


def build_log_payload(sensor_config):
    return {
        "sensor_name": sensor_config["sensor_name"],
        "sensor_template_source": sensor_config["template_path"],
        "selected_band_centers": [band["center"] for band in sensor_config["bands"]],
        "selected_band_count": len(sensor_config["bands"]),
    }


def _format_value(value):
    if isinstance(value, float):
        return format(value, ".15g")
    return str(value)


def add_sensor_template(repo_root, sensor_name, source_xml_path):
    sensor_name = _clean_sensor_display_name(sensor_name)
    if not sensor_name:
        raise ValueError("Sensor name is required.")

    existing_names = default_sensor_templates(repo_root).keys()
    if any(_clean_sensor_display_name(name).lower() == sensor_name.lower() for name in existing_names):
        raise ValueError(f"A sensor named '{sensor_name}' already exists.")

    # Validate the source template before copying.
    load_sensor_template(source_xml_path, sensor_name)

    sensor_dir = _sensor_template_dir(repo_root)
    os.makedirs(sensor_dir, exist_ok=True)
    backup_dir, backup_created = ensure_sensor_templates_backup(repo_root)

    base_name = _safe_sensor_filename(sensor_name)
    destination_path = os.path.join(sensor_dir, f"{base_name}.xml")
    counter = 2
    while os.path.exists(destination_path):
        destination_path = os.path.join(sensor_dir, f"{base_name}_{counter}.xml")
        counter += 1

    tree = ET.parse(source_xml_path)
    root = tree.getroot()
    sensor_name_node = root.find("./sensor_name")
    if sensor_name_node is None:
        sensor_name_node = ET.SubElement(root, "sensor_name")
    sensor_name_node.text = sensor_name
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass
    tree.write(destination_path, encoding="utf-8", xml_declaration=True)

    return {
        "template": load_sensor_template(destination_path, sensor_name),
        "backup_dir": backup_dir,
        "backup_created": backup_created,
    }


def remove_sensor_templates(repo_root, loaded_sensor_templates, sensor_names):
    names = [name for name in sensor_names if name in loaded_sensor_templates]
    if not names:
        raise ValueError("Select at least one sensor to remove.")
    if len(loaded_sensor_templates) - len(names) < 1:
        raise ValueError("At least one sensor must remain available.")

    sensor_dir = os.path.normcase(os.path.normpath(os.path.abspath(_sensor_template_dir(repo_root))))
    for name in names:
        template_path = loaded_sensor_templates[name]["template_path"]
        resolved_path = os.path.normcase(os.path.normpath(os.path.abspath(template_path)))
        if not resolved_path.startswith(sensor_dir):
            raise ValueError(f"Sensor '{name}' is not managed from Data/SRF and cannot be removed automatically.")

    backup_dir, backup_created = ensure_sensor_templates_backup(repo_root)
    removed_paths = []
    for name in names:
        template_path = loaded_sensor_templates[name]["template_path"]
        if os.path.exists(template_path):
            os.remove(template_path)
            removed_paths.append(template_path)

    return {
        "removed_names": names,
        "removed_paths": removed_paths,
        "backup_dir": backup_dir,
        "backup_created": backup_created,
    }
