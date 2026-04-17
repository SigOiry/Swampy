# -*- coding: utf-8 -*-
import json
import os
import sys
from pathlib import Path
from tkinter import Tk
from tkinter.filedialog import askopenfilename

import webview


def _load_vector_mask_geometries(path):
    import fiona
    from rasterio.warp import transform_geom

    geometries = []
    with fiona.open(path, "r") as src:
        src_crs = src.crs_wkt or src.crs
        if not src_crs:
            raise RuntimeError("The shapefile has no CRS information.")
        for feature in src:
            geometry = feature.get("geometry")
            if not geometry:
                continue
            transformed = transform_geom(src_crs, "EPSG:4326", geometry, precision=8)
            geometries.append(transformed)
    if not geometries:
        raise RuntimeError("The shapefile does not contain any valid polygon geometry.")
    return geometries


class CropWindowApi:
    def __init__(self):
        self.window = None
        self.result = {"cancelled": True}

    def choose_mask(self):
        dialog_root = Tk()
        dialog_root.withdraw()
        dialog_root.attributes("-topmost", True)
        try:
            path = askopenfilename(
                parent=dialog_root,
                title="Choose shapefile mask",
                filetypes=[("Shapefile", "*.shp"), ("All files", "*.*")],
            )
        finally:
            dialog_root.destroy()
        if not path:
            return {"ok": False, "cancelled": True}
        try:
            geometries = _load_vector_mask_geometries(path)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "path": path, "geometries": geometries}

    def accept_selection(self, selection):
        self.result = {
            "cancelled": False,
            "selection": selection,
        }
        if self.window is not None:
            self.window.destroy()
        return True

    def cancel(self):
        self.result = {"cancelled": True}
        if self.window is not None:
            self.window.destroy()
        return True


def _build_html(payload):
    payload_json = json.dumps(payload)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Crop area</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <link rel="stylesheet" href="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.css">
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
    .leaflet-container {{
      background: #e5e7eb;
    }}
  </style>
</head>
<body>
  <div id="app">
    <div id="header">
      <div id="title">Crop area</div>
      <div id="subtitle">
        {payload.get("preview_description", "Previewing the first selected image.")}. Draw a rectangle to define a geographic bounding box,
        or import a shapefile mask. The saved selection is stored as coordinates, not pixel indices.
      </div>
    </div>
    <div id="toolbar">
      <button id="draw-btn">Draw rectangle</button>
      <button id="clear-rect-btn" class="warn">Clear rectangle</button>
      <button id="import-mask-btn">Import</button>
      <button id="clear-mask-btn" class="warn">Clear mask</button>
      <div class="spacer"></div>
      <button id="cancel-btn">Cancel</button>
      <button id="ok-btn" class="primary">OK</button>
    </div>
    <div id="summary"></div>
    <div id="map"></div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>
  <script>
    const payload = {payload_json};
    let currentBBox = payload.selection && payload.selection.bbox ? payload.selection.bbox : null;
    let currentMaskPath = payload.selection && payload.selection.mask_path ? payload.selection.mask_path : '';
    let currentMaskGeometries = payload.selection && payload.selection.mask_geometries ? payload.selection.mask_geometries : [];

    const imageBounds = [
      [payload.lat_min, payload.lon_min],
      [payload.lat_max, payload.lon_max]
    ];

    const map = L.map('map', {{
      crs: L.CRS.EPSG4326,
      zoomSnap: 0,
      attributionControl: false
    }});

    L.imageOverlay(payload.image_url || payload.image_data_url, imageBounds, {{
      interactive: false,
      opacity: 1.0
    }}).addTo(map);

    map.fitBounds(imageBounds, {{ padding: [20, 20] }});
    L.control.scale({{ metric: true, imperial: false }}).addTo(map);

    const rectangleGroup = new L.FeatureGroup().addTo(map);
    let maskLayer = null;

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
        parts.push(`Mask ${{split[split.length - 1]}}`);
      }}
      document.getElementById('summary').textContent = parts.length ? parts.join(' | ') : 'Full scene';
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

    function setMask(geometries, path) {{
      if (maskLayer) {{
        map.removeLayer(maskLayer);
        maskLayer = null;
      }}
      currentMaskGeometries = geometries || [];
      currentMaskPath = path || '';
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

    map.on(L.Draw.Event.CREATED, function(event) {{
      const layer = event.layer;
      const bbox = bboxFromLeaflet(layer.getBounds());
      setRectangle(bbox);
    }});

    document.getElementById('draw-btn').addEventListener('click', function() {{
      const drawer = new L.Draw.Rectangle(map, {{
        shapeOptions: {{
          color: '#ff3366',
          weight: 2
        }}
      }});
      drawer.enable();
    }});

    document.getElementById('clear-rect-btn').addEventListener('click', function() {{
      setRectangle(null);
    }});

    document.getElementById('import-mask-btn').addEventListener('click', async function() {{
      if (!window.pywebview || !window.pywebview.api) {{
        return;
      }}
      const response = await window.pywebview.api.choose_mask();
      if (!response || response.cancelled) {{
        return;
      }}
      if (!response.ok) {{
        window.alert(response.error || 'Unable to load the selected shapefile.');
        return;
      }}
      setMask(response.geometries || [], response.path || '');
    }});

    document.getElementById('clear-mask-btn').addEventListener('click', function() {{
      setMask([], '');
    }});

    document.getElementById('cancel-btn').addEventListener('click', async function() {{
      if (window.pywebview && window.pywebview.api) {{
        await window.pywebview.api.cancel();
      }}
    }});

    document.getElementById('ok-btn').addEventListener('click', async function() {{
      if (window.pywebview && window.pywebview.api) {{
        await window.pywebview.api.accept_selection({{
          bbox: currentBBox,
          mask_path: currentMaskPath
        }});
      }}
    }});

    if (currentBBox) {{
      setRectangle(currentBBox);
    }} else {{
      formatSummary();
    }}
    if (currentMaskGeometries.length) {{
      setMask(currentMaskGeometries, currentMaskPath);
    }}
  </script>
</body>
</html>
"""


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: leaflet_crop_window.py <request.json> <response.json>")

    request_path = sys.argv[1]
    response_path = sys.argv[2]

    with open(request_path, "r", encoding="utf-8") as request_file:
        payload = json.load(request_file)
    image_url = str(payload.get("image_url") or "").strip()
    if image_url and os.path.exists(image_url):
        payload["image_url"] = Path(image_url).resolve().as_uri()

    api = CropWindowApi()
    html = _build_html(payload)
    html_path = Path(response_path).with_name("leaflet_crop_window.html")
    with open(html_path, "w", encoding="utf-8") as html_file:
        html_file.write(html)
    api.window = webview.create_window(
        payload.get("title", "Crop area"),
        url=html_path.resolve().as_uri(),
        js_api=api,
        width=1200,
        height=860,
        min_size=(900, 680),
    )
    webview.start()

    with open(response_path, "w", encoding="utf-8") as response_file:
        json.dump(api.result, response_file)


if __name__ == "__main__":
    main()
