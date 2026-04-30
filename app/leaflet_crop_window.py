# -*- coding: utf-8 -*-
import json
import os
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import tkinter as tk
from tkinter import Tk, messagebox, ttk
from tkinter.filedialog import askopenfilename
from urllib.parse import urlparse


def _load_vector_mask_geometries(path, point_buffer_m=50.0):
    import fiona
    from rasterio.warp import transform_geom
    from shapely.geometry import mapping, shape
    from shapely.ops import transform as shapely_transform
    from pyproj import Transformer

    def _transform_with_optional_point_buffer(geometry, src_crs, dst_crs, point_buffer_m=50.0):
        geom_type = str(geometry.get("type") or "")
        if geom_type in {"Point", "MultiPoint"}:
            source_geom = shape(geometry)
            to_metric = Transformer.from_crs(src_crs, "EPSG:3857", always_xy=True)
            to_target = Transformer.from_crs("EPSG:3857", dst_crs, always_xy=True)
            buffered_geom = shapely_transform(to_metric.transform, source_geom).buffer(float(point_buffer_m))
            return mapping(shapely_transform(to_target.transform, buffered_geom))
        return transform_geom(src_crs, dst_crs, geometry, precision=8)

    geometries = []
    with fiona.open(path, "r") as src:
        src_crs = src.crs_wkt or src.crs
        if not src_crs:
            raise RuntimeError("The shapefile has no CRS information.")
        for feature in src:
            geometry = feature.get("geometry")
            if not geometry:
                continue
            geometries.append(_transform_with_optional_point_buffer(
                geometry,
                src_crs,
                "EPSG:4326",
                point_buffer_m=point_buffer_m,
            ))
    if not geometries:
        raise RuntimeError("The shapefile does not contain any valid geometry.")
    return geometries


def _inspect_vector_mask_file(path):
    import fiona
    from rasterio.warp import transform_geom
    from shapely.geometry import box, mapping, shape

    geometry_types = set()
    with fiona.open(path, "r") as src:
        src_crs = src.crs_wkt or src.crs
        if not src_crs:
            raise RuntimeError("The shapefile has no CRS information.")

        bbox_geometry = transform_geom(src_crs, "EPSG:4326", mapping(box(*src.bounds)), precision=8)
        min_lon, min_lat, max_lon, max_lat = shape(bbox_geometry).bounds
        bbox = {
            "min_lon": float(min_lon),
            "max_lon": float(max_lon),
            "min_lat": float(min_lat),
            "max_lat": float(max_lat),
        }

        has_geometry = False
        for feature in src:
            geometry = feature.get("geometry")
            if not geometry:
                continue
            has_geometry = True
            geom_type = str(geometry.get("type") or "").strip()
            if geom_type:
                geometry_types.add(geom_type)

    if not has_geometry:
        raise RuntimeError("The shapefile does not contain any valid geometry.")

    return {
        "bbox": bbox,
        "geometry_types": geometry_types,
        "point_only": bool(geometry_types) and geometry_types.issubset({"Point", "MultiPoint"}),
    }


def _choose_point_mask_mode(parent, default_buffer_m=50.0):
    parent_is_visible = False
    if parent is not None:
        try:
            parent_is_visible = str(parent.state()).lower() not in {"withdrawn", "iconic"}
        except Exception:
            parent_is_visible = False

    dialog = tk.Toplevel(parent if parent_is_visible else None)
    dialog.title("Point Shapefile Options")
    if parent_is_visible:
        dialog.transient(parent)
    dialog.resizable(False, False)
    dialog.attributes("-topmost", True)

    choice_var = tk.StringVar(value="buffer")
    buffer_var = tk.StringVar(value=f"{float(default_buffer_m):g}")
    result = {"value": None}

    container = ttk.Frame(dialog, padding=14)
    container.grid(row=0, column=0, sticky="nsew")
    container.columnconfigure(0, weight=1)

    ttk.Label(
        container,
        text=(
            "This shapefile contains point geometries.\n\n"
            "Choose whether to crop around each point using a buffer, or to use the "
            "overall shapefile extent as a bounding box."
        ),
        justify="left",
        wraplength=420,
    ).grid(row=0, column=0, sticky="w")

    ttk.Radiobutton(
        container,
        text="Use exact point locations with a buffer",
        variable=choice_var,
        value="buffer",
    ).grid(row=1, column=0, sticky="w", pady=(12, 2))

    buffer_frame = ttk.Frame(container)
    buffer_frame.grid(row=2, column=0, sticky="ew", padx=(24, 0))
    ttk.Label(buffer_frame, text="Buffer around each point (m)").grid(row=0, column=0, sticky="w")
    buffer_entry = ttk.Entry(buffer_frame, textvariable=buffer_var, width=12)
    buffer_entry.grid(row=1, column=0, sticky="w", pady=(4, 0))

    ttk.Radiobutton(
        container,
        text="Use shapefile extent as a bounding-box crop",
        variable=choice_var,
        value="bbox",
    ).grid(row=3, column=0, sticky="w", pady=(12, 0))

    def _refresh_buffer_controls(*_args):
        state = "normal" if choice_var.get() == "buffer" else "disabled"
        buffer_entry.configure(state=state)

    def _accept():
        if choice_var.get() == "buffer":
            buffer_text = str(buffer_var.get()).strip()
            try:
                buffer_m = float(buffer_text)
            except (TypeError, ValueError):
                messagebox.showerror(
                    "Invalid buffer size",
                    "Enter a valid numeric buffer size in meters.",
                    parent=dialog,
                )
                buffer_entry.focus_set()
                return
            if buffer_m <= 0.0:
                messagebox.showerror(
                    "Invalid buffer size",
                    "The buffer size must be greater than 0 meters.",
                    parent=dialog,
                )
                buffer_entry.focus_set()
                return
            result["value"] = {
                "mode": "geometry",
                "mask_buffer_m": buffer_m,
            }
        else:
            result["value"] = {
                "mode": "bbox",
                "mask_buffer_m": None,
            }
        dialog.destroy()

    def _cancel():
        result["value"] = None
        dialog.destroy()

    actions = ttk.Frame(container)
    actions.grid(row=4, column=0, sticky="e", pady=(14, 0))
    ttk.Button(actions, text="Cancel", command=_cancel).grid(row=0, column=0, padx=(0, 8))
    ttk.Button(actions, text="OK", command=_accept).grid(row=0, column=1)

    dialog.protocol("WM_DELETE_WINDOW", _cancel)
    choice_var.trace_add("write", _refresh_buffer_controls)
    _refresh_buffer_controls()

    dialog.update_idletasks()
    width = dialog.winfo_width()
    height = dialog.winfo_height()
    screen_width = dialog.winfo_screenwidth()
    screen_height = dialog.winfo_screenheight()
    x_pos = max((screen_width - width) // 2, 0)
    y_pos = max((screen_height - height) // 2, 0)
    dialog.geometry(f"{width}x{height}+{x_pos}+{y_pos}")
    dialog.deiconify()
    dialog.lift()
    try:
        dialog.wait_visibility()
    except Exception:
        pass
    dialog.grab_set()
    dialog.focus_force()

    dialog.wait_window()
    return result["value"]


def _choose_mask_file(default_point_buffer_m=50.0):
    dialog_root = Tk()
    dialog_root.withdraw()
    dialog_root.attributes("-topmost", True)
    try:
        path = askopenfilename(
            parent=dialog_root,
            title="Choose shapefile mask",
            filetypes=[("Shapefile", "*.shp"), ("All files", "*.*")],
        )
        if not path:
            return None
        inspection = _inspect_vector_mask_file(path)
        if inspection.get("point_only"):
            point_options = _choose_point_mask_mode(
                dialog_root,
                default_buffer_m=default_point_buffer_m,
            )
            if not point_options:
                return None
            if point_options.get("mode") == "bbox":
                return {
                    "path": path,
                    "mode": "bbox",
                    "bbox": inspection.get("bbox"),
                    "mask_buffer_m": None,
                }
            return {
                "path": path,
                "mode": "geometry",
                "mask_buffer_m": point_options.get("mask_buffer_m"),
            }
        return {
            "path": path,
            "mode": "geometry",
            "mask_buffer_m": None,
        }
    finally:
        dialog_root.destroy()


def _build_html(payload):
    payload_json = json.dumps(payload)
    mode = str(payload.get("mode", "crop") or "crop")
    needs_leaflet_draw = (mode == "polygons") or bool(payload.get("allow_polygon")) or bool(payload.get("enable_edit_toolbar"))
    leaflet_draw_css = '<link rel="stylesheet" href="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.css">' if needs_leaflet_draw else ''
    leaflet_draw_js = '<script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>' if needs_leaflet_draw else ''
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{payload.get("title", "Spatial selection")}</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  {leaflet_draw_css}
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      font-family: "Segoe UI", sans-serif;
      background: #f3f4f6;
      color: #1f2937;
    }}
    #app {{
      display: flex;
      flex-direction: column;
      width: 100%;
      height: 100%;
      position: relative;
    }}
    #header {{
      padding: 12px 14px 8px 14px;
      background: #ffffff;
      border-bottom: 1px solid #d1d5db;
    }}
    #title {{
      font-size: 15px;
      font-weight: 600;
      margin-bottom: 6px;
    }}
    #subtitle {{
      font-size: 12px;
      color: #4b5563;
      line-height: 1.45;
    }}
    #toolbar {{
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
      padding: 10px 14px;
      background: #ffffff;
      border-bottom: 1px solid #d1d5db;
    }}
    #toolbar button {{
      border: 1px solid #cbd5e1;
      background: #ffffff;
      color: #111827;
      border-radius: 6px;
      padding: 7px 10px;
      font-size: 12px;
      cursor: pointer;
    }}
    #toolbar button:disabled {{
      opacity: 0.55;
      cursor: default;
    }}
    #toolbar button.primary {{
      background: #0f766e;
      color: #ffffff;
      border-color: #0f766e;
    }}
    #toolbar button.warn {{
      background: #f9fafb;
      color: #7f1d1d;
      border-color: #fecaca;
    }}
    #toolbar .spacer {{
      flex: 1 1 auto;
    }}
    #toolbar-options {{
      display: none;
      align-items: center;
      gap: 10px;
      padding: 2px 0;
    }}
    #toolbar-options label {{
      display: flex;
      align-items: flex-start;
      gap: 8px;
      font-size: 12px;
      color: #374151;
      cursor: pointer;
      max-width: 420px;
    }}
    #toolbar-options input {{
      margin-top: 1px;
    }}
    #toolbar-options .option-text {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}
    #toolbar-options .option-hint {{
      font-size: 11px;
      color: #6b7280;
      line-height: 1.35;
    }}
    #summary {{
      padding: 8px 14px;
      font-size: 12px;
      color: #374151;
      background: #f9fafb;
      border-bottom: 1px solid #e5e7eb;
    }}
    #map {{
      flex: 1 1 auto;
      min-height: 0;
    }}
    #startup-overlay {{
      position: absolute;
      inset: 0;
      z-index: 2000;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 12px;
      background: rgba(243, 244, 246, 0.96);
      color: #1f2937;
      transition: opacity 0.18s ease-out;
      pointer-events: all;
    }}
    #startup-overlay.hidden {{
      opacity: 0;
      pointer-events: none;
    }}
    #startup-spinner {{
      width: 34px;
      height: 34px;
      border-radius: 999px;
      border: 3px solid #cbd5e1;
      border-top-color: #0f766e;
      animation: startup-spin 0.9s linear infinite;
    }}
    #startup-text {{
      font-size: 13px;
      color: #4b5563;
    }}
    @keyframes startup-spin {{
      from {{ transform: rotate(0deg); }}
      to {{ transform: rotate(360deg); }}
    }}
    .leaflet-container {{
      background: #e5e7eb;
    }}
  </style>
</head>
<body>
  <div id="app">
    <div id="header">
      <div id="title">{payload.get("title", "Spatial selection")}</div>
      <div id="subtitle">
        {payload.get("subtitle", payload.get("preview_description", "Previewing the first selected image."))}
      </div>
    </div>
    <div id="toolbar">
      <button id="draw-rect-btn">Draw rectangle</button>
      <button id="clear-rect-btn" class="warn">Clear rectangle</button>
      <button id="draw-poly-btn">Draw polygon</button>
      <button id="clear-poly-btn" class="warn">Clear polygons</button>
      <button id="import-mask-btn">Import</button>
      <button id="clear-mask-btn" class="warn">Clear mask</button>
      <div id="toolbar-options"></div>
      <div class="spacer"></div>
      <button id="cancel-btn">Cancel</button>
      <button id="ok-btn" class="primary">OK</button>
    </div>
    <div id="summary"></div>
    <div id="map"></div>
    <div id="startup-overlay">
      <div id="startup-spinner"></div>
      <div id="startup-text">Preparing map tools...</div>
    </div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  {leaflet_draw_js}
  <script>
    const payload = {payload_json};
    const mode = String(payload.mode || 'crop');
    const allowRectangle = mode === 'crop' || payload.allow_rectangle === true;
    const allowPolygons = mode === 'polygons' || payload.allow_polygon === true;
    const allowMaskImport = payload.allow_mask_import !== false && mode === 'crop';
    const hasLeafletDraw = typeof L.Draw !== 'undefined' && L.Draw && L.Draw.Event;
    const optionCheckboxes = Array.isArray(payload.option_checkboxes)
      ? payload.option_checkboxes.filter(function(config) {{
          return config && config.id;
        }})
      : [];

    function coerceBool(value, defaultValue) {{
      if (value === null || value === undefined || value === '') {{
        return defaultValue;
      }}
      if (typeof value === 'boolean') {{
        return value;
      }}
      if (typeof value === 'number') {{
        return value !== 0;
      }}
      const text = String(value).trim().toLowerCase();
      if (['1', 'true', 'yes', 'on'].includes(text)) {{
        return true;
      }}
      if (['0', 'false', 'no', 'off'].includes(text)) {{
        return false;
      }}
      return defaultValue;
    }}

    let currentBBox = payload.selection && payload.selection.bbox ? payload.selection.bbox : null;
    let currentMaskPath = payload.selection && payload.selection.mask_path ? payload.selection.mask_path : '';
    let currentMaskBuffer = null;
    if (payload.selection && payload.selection.mask_buffer_m !== null && payload.selection.mask_buffer_m !== undefined && payload.selection.mask_buffer_m !== '') {{
      const parsedMaskBuffer = Number(payload.selection.mask_buffer_m);
      currentMaskBuffer = Number.isFinite(parsedMaskBuffer) && parsedMaskBuffer > 0 ? parsedMaskBuffer : null;
    }}
    let currentMaskGeometries = payload.selection && payload.selection.mask_geometries ? payload.selection.mask_geometries : [];
    let currentPolygons = payload.selection && Array.isArray(payload.selection.polygons) ? payload.selection.polygons : [];
    const optionValues = {{}};
    optionCheckboxes.forEach(function(config) {{
      const optionId = String(config.id);
      const selectionValue = payload.selection ? payload.selection[optionId] : undefined;
      optionValues[optionId] = coerceBool(selectionValue, coerceBool(config.value, false));
    }});

    const imageBounds = [
      [payload.lat_min, payload.lon_min],
      [payload.lat_max, payload.lon_max]
    ];

    const map = L.map('map', {{
      crs: L.CRS.EPSG4326,
      zoomSnap: 0,
      attributionControl: false,
      zoomAnimation: false,
      fadeAnimation: false,
      markerZoomAnimation: false,
      inertia: false,
      preferCanvas: true
    }});

    const imageOverlay = L.imageOverlay(payload.image_url || payload.image_data_url, imageBounds, {{
      interactive: false,
      opacity: 1.0
    }}).addTo(map);

    map.fitBounds(imageBounds, {{ padding: [20, 20] }});
    L.control.scale({{ metric: true, imperial: false }}).addTo(map);

    const rectangleGroup = new L.FeatureGroup().addTo(map);
    const polygonGroup = new L.FeatureGroup().addTo(map);
    let maskLayer = null;
    let startupReleased = false;
    let selectionSubmitted = false;
    let rectangleDrawActive = false;
    let rectangleStartLatLng = null;
    let rectangleDraftLayer = null;

    function setInteractionEnabled(enabled) {{
      const idsToToggle = [
        'draw-rect-btn',
        'clear-rect-btn',
        'draw-poly-btn',
        'clear-poly-btn',
        'import-mask-btn',
        'clear-mask-btn',
        'ok-btn',
      ];
      idsToToggle.forEach(function(id) {{
        const element = document.getElementById(id);
        if (element) {{
          element.disabled = !enabled;
        }}
      }});
      document.querySelectorAll('.selection-option-checkbox').forEach(function(element) {{
        element.disabled = !enabled;
      }});
      if (enabled) {{
        map.dragging.enable();
        map.touchZoom.enable();
        map.doubleClickZoom.enable();
        map.scrollWheelZoom.enable();
        map.boxZoom.enable();
        map.keyboard.enable();
      }} else {{
        map.dragging.disable();
        map.touchZoom.disable();
        map.doubleClickZoom.disable();
        map.scrollWheelZoom.disable();
        map.boxZoom.disable();
        map.keyboard.disable();
      }}
    }}

    setInteractionEnabled(false);

    function postJson(path, body) {{
      return fetch(path, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body || {{}})
      }}).then(response => response.json());
    }}

    function formatBufferMeters(value) {{
      if (!Number.isFinite(value) || value <= 0) {{
        return '';
      }}
      if (Math.abs(value - Math.round(value)) < 1e-9) {{
        return String(Math.round(value));
      }}
      return value.toFixed(3).replace(/\\.?0+$/, '');
    }}

    function formatSummary() {{
      const parts = [];
      if (currentBBox) {{
        parts.push(
          `BBox lon ${{
            currentBBox.min_lon.toFixed(5)
          }} to ${{
            currentBBox.max_lon.toFixed(5)
          }}, lat ${{
            currentBBox.min_lat.toFixed(5)
          }} to ${{
            currentBBox.max_lat.toFixed(5)
          }}`
        );
      }}
      if (currentMaskPath) {{
        const split = currentMaskPath.split(/[/\\\\]/);
        let maskText = `Mask ${{split[split.length - 1]}}`;
        if (Number.isFinite(currentMaskBuffer) && currentMaskBuffer > 0) {{
          maskText += ` (buffer ${{formatBufferMeters(currentMaskBuffer)}} m)`;
        }}
        parts.push(maskText);
      }}
      if (currentPolygons.length) {{
        parts.push(`${{currentPolygons.length}} polygon(s)`);
      }}
      optionCheckboxes.forEach(function(config) {{
        const optionId = String(config.id);
        const summaryText = optionValues[optionId]
          ? String(config.summary_when_true || '').trim()
          : String(config.summary_when_false || '').trim();
        if (summaryText) {{
          parts.push(summaryText);
        }}
      }});
      document.getElementById('summary').textContent = parts.length ? parts.join(' | ') : (mode === 'polygons' ? 'No deep-water polygons selected' : 'Full scene');
    }}

    function configureOptions() {{
      const container = document.getElementById('toolbar-options');
      if (!container) {{
        return;
      }}
      if (!optionCheckboxes.length) {{
        container.style.display = 'none';
        container.innerHTML = '';
        return;
      }}
      container.style.display = 'flex';
      container.innerHTML = '';
      optionCheckboxes.forEach(function(config) {{
        const optionId = String(config.id);
        const label = document.createElement('label');

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = optionId;
        checkbox.className = 'selection-option-checkbox';
        checkbox.checked = !!optionValues[optionId];
        checkbox.addEventListener('change', function() {{
          optionValues[optionId] = checkbox.checked;
          formatSummary();
        }});

        const textWrap = document.createElement('span');
        textWrap.className = 'option-text';

        const labelText = document.createElement('span');
        labelText.textContent = String(config.label || optionId);
        textWrap.appendChild(labelText);

        const hintText = String(config.hint || '').trim();
        if (hintText) {{
          const hint = document.createElement('span');
          hint.className = 'option-hint';
          hint.textContent = hintText;
          textWrap.appendChild(hint);
        }}

        label.appendChild(checkbox);
        label.appendChild(textWrap);
        container.appendChild(label);
      }});
    }}

    function bboxFromLeaflet(bounds) {{
      return {{
        min_lon: bounds.getWest(),
        max_lon: bounds.getEast(),
        min_lat: bounds.getSouth(),
        max_lat: bounds.getNorth()
      }};
    }}

    function setRectangle(bbox) {{
      rectangleGroup.clearLayers();
      currentBBox = bbox;
      if (bbox) {{
        const rect = L.rectangle(
          [[bbox.min_lat, bbox.min_lon], [bbox.max_lat, bbox.max_lon]],
          {{
            color: '#ff3366',
            weight: 2,
            fill: false
          }}
        );
        rect.addTo(rectangleGroup);
      }}
      formatSummary();
    }}

    function clearRectangleDraft() {{
      if (rectangleDraftLayer) {{
        map.removeLayer(rectangleDraftLayer);
        rectangleDraftLayer = null;
      }}
      rectangleStartLatLng = null;
    }}

    function setRectangleDrawMode(enabled) {{
      rectangleDrawActive = !!enabled;
      const mapContainer = map.getContainer();
      const drawButton = document.getElementById('draw-rect-btn');
      if (drawButton) {{
        if (rectangleDrawActive) {{
          drawButton.classList.add('primary');
          drawButton.textContent = rectangleStartLatLng ? 'Click opposite corner...' : 'Click first corner...';
        }} else {{
          drawButton.classList.remove('primary');
          drawButton.textContent = 'Draw rectangle';
        }}
      }}
      if (mapContainer) {{
        mapContainer.style.cursor = rectangleDrawActive ? 'crosshair' : '';
      }}
      if (!rectangleDrawActive) {{
        clearRectangleDraft();
        if (startupReleased) {{
          map.dragging.enable();
        }}
      }}
    }}

    function polygonsFromGroup() {{
      const geometries = [];
      polygonGroup.eachLayer(function(layer) {{
        const geojson = layer.toGeoJSON();
        if (geojson && geojson.geometry) {{
          geometries.push(geojson.geometry);
        }}
      }});
      currentPolygons = geometries;
      formatSummary();
    }}

    function setPolygons(geometries) {{
      polygonGroup.clearLayers();
      currentPolygons = [];
      (geometries || []).forEach(function(geometry) {{
        const layer = L.geoJSON(geometry, {{
          style: function() {{
            return {{
              color: '#2563eb',
              weight: 2,
              fillOpacity: 0.08
            }};
          }}
        }});
        layer.eachLayer(function(innerLayer) {{
          polygonGroup.addLayer(innerLayer);
        }});
      }});
      polygonsFromGroup();
    }}

    function setMask(geometries, path, bufferMeters) {{
      if (maskLayer) {{
        map.removeLayer(maskLayer);
        maskLayer = null;
      }}
      currentMaskGeometries = geometries || [];
      currentMaskPath = path || '';
      currentMaskBuffer = Number.isFinite(bufferMeters) && Number(bufferMeters) > 0 ? Number(bufferMeters) : null;
      if (currentMaskGeometries.length) {{
        maskLayer = L.geoJSON(currentMaskGeometries, {{
          style: function() {{
            return {{
              color: '#00bcd4',
              weight: 2,
              fillOpacity: 0
            }};
          }}
        }}).addTo(map);
      }}
      formatSummary();
    }}

    if (hasLeafletDraw) {{
      map.on(L.Draw.Event.CREATED, function(event) {{
        const layer = event.layer;
        if (event.layerType === 'polygon') {{
          polygonGroup.addLayer(layer);
          polygonsFromGroup();
        }}
      }});
    }}

    map.on('click', function(event) {{
      if (!rectangleDrawActive || !allowRectangle) {{
        return;
      }}
      if (!rectangleStartLatLng) {{
        rectangleStartLatLng = event.latlng;
        clearRectangleDraft();
        rectangleStartLatLng = event.latlng;
        rectangleDraftLayer = L.rectangle(
          [rectangleStartLatLng, rectangleStartLatLng],
          {{
            color: '#ff3366',
            weight: 2,
            fill: false,
            interactive: false
          }}
        ).addTo(map);
        map.dragging.disable();
        setRectangleDrawMode(true);
        return;
      }}
      finishRectangleDrag(event.latlng);
    }});

    map.on('mousemove', function(event) {{
      if (!rectangleDrawActive || !rectangleStartLatLng || !rectangleDraftLayer) {{
        return;
      }}
      rectangleDraftLayer.setBounds(L.latLngBounds(rectangleStartLatLng, event.latlng));
    }});

    function finishRectangleDrag(latlng) {{
      if (!rectangleDrawActive || !rectangleStartLatLng) {{
        return;
      }}
      const finalBounds = L.latLngBounds(rectangleStartLatLng, latlng);
      setRectangle(bboxFromLeaflet(finalBounds));
      setRectangleDrawMode(false);
      if (startupReleased) {{
        map.dragging.enable();
      }}
    }}

    document.getElementById('draw-rect-btn').addEventListener('click', function() {{
      setRectangleDrawMode(!rectangleDrawActive);
    }});

    document.getElementById('clear-rect-btn').addEventListener('click', function() {{
      setRectangleDrawMode(false);
      setRectangle(null);
    }});

    document.getElementById('draw-poly-btn').addEventListener('click', function() {{
      if (!hasLeafletDraw) {{
        window.alert('Polygon drawing tools are not available in this window.');
        return;
      }}
      const drawer = new L.Draw.Polygon(map, {{
        allowIntersection: false,
        showArea: false,
        shapeOptions: {{
          color: '#2563eb',
          weight: 2
        }}
      }});
      drawer.enable();
    }});

    document.getElementById('clear-poly-btn').addEventListener('click', function() {{
      setPolygons([]);
    }});

    document.getElementById('import-mask-btn').addEventListener('click', async function() {{
      if (!allowMaskImport) {{
        return;
      }}
      const response = await postJson('/choose-mask', {{
        current_mask_buffer_m: currentMaskBuffer
      }});
      if (!response || response.cancelled) {{
        return;
      }}
      if (!response.ok) {{
        window.alert(response.error || 'Unable to load the selected shapefile.');
        return;
      }}
      if (response.mode === 'bbox' && response.bbox) {{
        setMask([], '', null);
        setRectangle(response.bbox);
        return;
      }}
      setMask(response.geometries || [], response.path || '', response.mask_buffer_m);
    }});

    document.getElementById('clear-mask-btn').addEventListener('click', function() {{
      setMask([], '', null);
    }});

    function configureToolbar() {{
      document.getElementById('draw-rect-btn').style.display = allowRectangle ? '' : 'none';
      document.getElementById('clear-rect-btn').style.display = allowRectangle ? '' : 'none';
      document.getElementById('draw-poly-btn').style.display = allowPolygons ? '' : 'none';
      document.getElementById('clear-poly-btn').style.display = allowPolygons ? '' : 'none';
      document.getElementById('import-mask-btn').style.display = allowMaskImport ? '' : 'none';
      document.getElementById('clear-mask-btn').style.display = allowMaskImport ? '' : 'none';
    }}

    document.getElementById('cancel-btn').addEventListener('click', async function() {{
      selectionSubmitted = true;
      await postJson('/cancel', {{}});
      document.body.innerHTML = '<p style="font-family: Segoe UI, sans-serif; padding: 24px;">Selection cancelled. You can close this tab.</p>';
    }});

    document.getElementById('ok-btn').addEventListener('click', async function() {{
      selectionSubmitted = true;
      const acceptedSelection = {{
        bbox: currentBBox,
        mask_path: currentMaskPath,
        mask_buffer_m: currentMaskBuffer,
        polygons: currentPolygons
      }};
      optionCheckboxes.forEach(function(config) {{
        const optionId = String(config.id);
        acceptedSelection[optionId] = !!optionValues[optionId];
      }});
      await postJson('/accept', acceptedSelection);
      document.body.innerHTML = '<p style="font-family: Segoe UI, sans-serif; padding: 24px;">Selection saved. You can close this tab.</p>';
    }});

    window.addEventListener('beforeunload', function() {{
      if (!selectionSubmitted && navigator.sendBeacon) {{
        navigator.sendBeacon('/cancel', new Blob(['{{}}'], {{ type: 'application/json' }}));
      }}
    }});

    if (currentBBox) {{
      setRectangle(currentBBox);
    }} else {{
      formatSummary();
    }}
    if (currentMaskGeometries.length) {{
      setMask(currentMaskGeometries, currentMaskPath, currentMaskBuffer);
    }}
    if (currentPolygons.length) {{
      setPolygons(currentPolygons);
    }}
    configureOptions();
    configureToolbar();

    const startupOverlay = document.getElementById('startup-overlay');
    let startupTimerElapsed = false;
    let mapReady = false;

    function releaseStartupOverlay() {{
      if (startupReleased || !startupTimerElapsed || !mapReady) {{
        return;
      }}
      startupReleased = true;
      setInteractionEnabled(true);
      window.setTimeout(function() {{
        map.invalidateSize(true);
      }}, 40);
      startupOverlay.classList.add('hidden');
      window.setTimeout(function() {{
        if (startupOverlay && startupOverlay.parentNode) {{
          startupOverlay.parentNode.removeChild(startupOverlay);
        }}
      }}, 220);
    }}

    map.whenReady(function() {{
      mapReady = true;
      releaseStartupOverlay();
    }});

    imageOverlay.on('load', function() {{
      mapReady = true;
      releaseStartupOverlay();
    }});

    imageOverlay.on('error', function() {{
      mapReady = true;
      releaseStartupOverlay();
    }});

    window.setTimeout(function() {{
      startupTimerElapsed = true;
      releaseStartupOverlay();
    }}, 1000);
  </script>
</body>
</html>
"""


class _SelectionServer(ThreadingHTTPServer):
    daemon_threads = True


def _make_handler(payload, response_path, stop_event, preview_image_path):
    html = _build_html(payload).encode("utf-8")
    image_path = Path(preview_image_path) if preview_image_path else None

    class SelectionHandler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            return

        def _send_json(self, data, status=200):
            body = json.dumps(data).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path in {"/", "/index.html"}:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return
            if parsed.path == "/preview":
                if image_path is None or not image_path.exists():
                    self.send_error(404)
                    return
                suffix = image_path.suffix.lower()
                if suffix == ".webp":
                    content_type = "image/webp"
                elif suffix in {".jpg", ".jpeg"}:
                    content_type = "image/jpeg"
                else:
                    content_type = "image/png"
                data = image_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            self.send_error(404)

        def do_POST(self):
            parsed = urlparse(self.path)
            length = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(length) if length > 0 else b"{}"
            try:
                data = json.loads(body.decode("utf-8")) if body else {}
            except Exception:
                data = {}
            if parsed.path == "/choose-mask":
                default_buffer_m = data.get("current_mask_buffer_m")
                try:
                    default_buffer_m = float(default_buffer_m)
                except (TypeError, ValueError):
                    default_buffer_m = 50.0
                selection = _choose_mask_file(default_point_buffer_m=default_buffer_m)
                if not selection:
                    self._send_json({"ok": False, "cancelled": True})
                    return
                if selection.get("mode") == "bbox":
                    self._send_json({
                        "ok": True,
                        "mode": "bbox",
                        "bbox": selection.get("bbox"),
                        "path": "",
                        "geometries": [],
                        "mask_buffer_m": None,
                    })
                    return
                path = selection.get("path")
                point_buffer_m = selection.get("mask_buffer_m")
                try:
                    geometries = _load_vector_mask_geometries(
                        path,
                        point_buffer_m=50.0 if point_buffer_m in (None, "") else float(point_buffer_m),
                    )
                except Exception as exc:
                    self._send_json({"ok": False, "error": str(exc)})
                    return
                self._send_json({
                    "ok": True,
                    "mode": "geometry",
                    "path": path,
                    "geometries": geometries,
                    "mask_buffer_m": point_buffer_m,
                })
                return
            if parsed.path == "/accept":
                with open(response_path, "w", encoding="utf-8") as response_file:
                    json.dump({"cancelled": False, "selection": data}, response_file)
                self._send_json({"ok": True})
                stop_event.set()
                return
            if parsed.path == "/cancel":
                with open(response_path, "w", encoding="utf-8") as response_file:
                    json.dump({"cancelled": True}, response_file)
                self._send_json({"ok": True})
                stop_event.set()
                return
            self.send_error(404)

    return SelectionHandler


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: leaflet_crop_window.py <request.json> <response.json>")

    request_path = sys.argv[1]
    response_path = sys.argv[2]

    with open(request_path, "r", encoding="utf-8") as request_file:
        payload = json.load(request_file)
    preview_image_path = str(payload.get("image_url") or "").strip()
    if preview_image_path:
        payload["image_url"] = "/preview"

    stop_event = threading.Event()
    server = _SelectionServer(
        ("127.0.0.1", 0),
        _make_handler(payload, response_path, stop_event, preview_image_path),
    )
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    webbrowser.open(url, new=1, autoraise=True)

    try:
        while not stop_event.is_set():
            time.sleep(0.1)
    finally:
        server.shutdown()
        server.server_close()

    if not os.path.exists(response_path):
        with open(response_path, "w", encoding="utf-8") as response_file:
            json.dump({"cancelled": True}, response_file)


if __name__ == "__main__":
    main()
