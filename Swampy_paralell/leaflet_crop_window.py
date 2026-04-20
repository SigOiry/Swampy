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
  <title>{payload.get("title", "Spatial selection")}</title>
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
  <script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>
  <script>
    const payload = {payload_json};
    const mode = String(payload.mode || 'crop');
    const allowRectangle = mode === 'crop' || payload.allow_rectangle === true;
    const allowPolygons = mode === 'polygons' || payload.allow_polygon === true;
    const allowMaskImport = payload.allow_mask_import !== false && mode === 'crop';
    let currentBBox = payload.selection && payload.selection.bbox ? payload.selection.bbox : null;
    let currentMaskPath = payload.selection && payload.selection.mask_path ? payload.selection.mask_path : '';
    let currentMaskGeometries = payload.selection && payload.selection.mask_geometries ? payload.selection.mask_geometries : [];
    let currentPolygons = payload.selection && Array.isArray(payload.selection.polygons) ? payload.selection.polygons : [];

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
      if (currentPolygons.length) {{
        parts.push(`${{currentPolygons.length}} polygon(s)`);
      }}
      document.getElementById('summary').textContent = parts.length ? parts.join(' | ') : (mode === 'polygons' ? 'No deep-water polygons selected' : 'Full scene');
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
      if (event.layerType === 'polygon') {{
        polygonGroup.addLayer(layer);
        polygonsFromGroup();
      }}
    }});

    map.on(L.Draw.Event.EDITED, function() {{
      if (allowRectangle && rectangleGroup.getLayers().length) {{
        const rectLayer = rectangleGroup.getLayers()[0];
        currentBBox = bboxFromLeaflet(rectLayer.getBounds());
      }}
      if (allowPolygons) {{
        polygonsFromGroup();
      }}
    }});

    map.on(L.Draw.Event.DELETED, function() {{
      if (!rectangleGroup.getLayers().length) {{
        currentBBox = null;
      }}
      polygonsFromGroup();
      formatSummary();
    }});

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

    function configureToolbar() {{
      document.getElementById('draw-rect-btn').style.display = allowRectangle ? '' : 'none';
      document.getElementById('clear-rect-btn').style.display = allowRectangle ? '' : 'none';
      document.getElementById('draw-poly-btn').style.display = allowPolygons ? '' : 'none';
      document.getElementById('clear-poly-btn').style.display = allowPolygons ? '' : 'none';
      document.getElementById('import-mask-btn').style.display = allowMaskImport ? '' : 'none';
      document.getElementById('clear-mask-btn').style.display = allowMaskImport ? '' : 'none';
      if (payload.enable_edit_toolbar) {{
        new L.Control.Draw({{
          draw: false,
          edit: {{
            featureGroup: new L.FeatureGroup([rectangleGroup, polygonGroup]),
            remove: true
          }}
        }});
      }}
    }}

    document.getElementById('cancel-btn').addEventListener('click', async function() {{
      if (window.pywebview && window.pywebview.api) {{
        await window.pywebview.api.cancel();
      }}
    }});

    document.getElementById('ok-btn').addEventListener('click', async function() {{
      if (window.pywebview && window.pywebview.api) {{
        await window.pywebview.api.accept_selection({{
          bbox: currentBBox,
          mask_path: currentMaskPath,
          polygons: currentPolygons
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
    if (currentPolygons.length) {{
      setPolygons(currentPolygons);
    }}
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
