import L from "leaflet";
import "proj4leaflet";
import proj4 from "proj4";
import { ColorScale } from "./colormap";
import { StoreData } from "./store";
import { Hemisphere, VariableInfo } from "./config";

interface HemisphereConfig {
  epsg: string;
  projDef: string;
  // GIBS polar stereo tile grids share the same powers-of-two scheme.
  resolutions: number[];
  origin: [number, number];
  bounds: L.Bounds;
  center: [number, number];
  zoom: number;
  basemaps: Record<string, { url: string; maxNativeZoom: number }>;
}

const GIBS_RESOLUTIONS = [8192, 4096, 2048, 1024, 512, 256, 128, 64, 32];
const GIBS_ORIGIN: [number, number] = [-4194304, 4194304];
const GIBS_BOUNDS = L.bounds([-4194304, -4194304], [4194304, 4194304]);

const HEMISPHERES: Record<Hemisphere, HemisphereConfig> = {
  antarctic: {
    epsg: "EPSG:3031",
    projDef:
      "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs",
    resolutions: GIBS_RESOLUTIONS,
    origin: GIBS_ORIGIN,
    bounds: GIBS_BOUNDS,
    center: [-76, 162],
    zoom: 2,
    basemaps: {
      "Blue Marble": {
        url: "https://gibs.earthdata.nasa.gov/wmts/epsg3031/best/BlueMarble_ShadedRelief_Bathymetry/default/2004-08-01/500m/{z}/{y}/{x}.jpeg",
        maxNativeZoom: 4,
      },
      "Land / Water": {
        url: "https://gibs.earthdata.nasa.gov/wmts/epsg3031/best/SCAR_Land_Water_Map/default/2024-01-01/250m/{z}/{y}/{x}.png",
        maxNativeZoom: 5,
      },
    },
  },
  arctic: {
    epsg: "EPSG:3413",
    projDef:
      "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs",
    resolutions: GIBS_RESOLUTIONS,
    origin: GIBS_ORIGIN,
    bounds: GIBS_BOUNDS,
    // Greenland-centric.
    center: [72, -40],
    zoom: 3,
    basemaps: {
      "Blue Marble": {
        url: "https://gibs.earthdata.nasa.gov/wmts/epsg3413/best/BlueMarble_ShadedRelief_Bathymetry/default/2004-08-01/500m/{z}/{y}/{x}.jpeg",
        maxNativeZoom: 4,
      },
      "Land / Water": {
        url: "https://gibs.earthdata.nasa.gov/wmts/epsg3413/best/OSM_Land_Water_Map/default/250m/{z}/{y}/{x}.png",
        maxNativeZoom: 5,
      },
    },
  },
};

let map: L.Map;
let baseLayers: Record<string, L.TileLayer>;
let currentHemisphere: Hemisphere;
let onViewChange: (() => void) | null = null;

export function setOnViewChange(cb: (() => void) | null): void {
  onViewChange = cb;
}

export function isLatLonVisible(lat: number, lon: number): boolean {
  if (!map) return true;
  return map.getBounds().contains([lat, lon]);
}

// Canvas overlay for fast point rendering
let canvasOverlay: L.Layer | null = null;

// Maximum points to render — subsample if over this
const MAX_RENDER_POINTS = 20000;

// Hover state
let renderedPoints: Array<{ lat: number; lon: number; idx: number }> = [];
let currentFrameIds: string[] | null = null;
let currentValues: Float64Array | null = null;
let currentVarInfo: VariableInfo | null = null;
let tooltipEl: HTMLDivElement | null = null;
const HOVER_THRESHOLD_PX = 12;

export function formatScaledValue(value: number, info: VariableInfo): string {
  const scaled = (info.displayScale ?? 1) * value;
  return scaled.toPrecision(4);
}

export function initMap(containerId: string, hemisphere: Hemisphere): L.Map {
  const cfg = HEMISPHERES[hemisphere];
  currentHemisphere = hemisphere;

  proj4.defs(cfg.epsg, cfg.projDef);
  const crs = new L.Proj.CRS(cfg.epsg, cfg.projDef, {
    resolutions: cfg.resolutions,
    origin: cfg.origin,
    bounds: cfg.bounds,
  });

  map = L.map(containerId, {
    crs,
    center: cfg.center,
    zoom: cfg.zoom,
    minZoom: 0,
    maxZoom: 8,
    preferCanvas: true,
  });

  const tileOpts: L.TileLayerOptions = {
    tileSize: 512,
    attribution: "NASA GIBS",
    noWrap: true,
  };

  baseLayers = {};
  for (const [name, info] of Object.entries(cfg.basemaps)) {
    baseLayers[name] = L.tileLayer(info.url, {
      ...tileOpts,
      maxZoom: 8,
      maxNativeZoom: info.maxNativeZoom,
    });
  }

  const firstBasemap = Object.keys(baseLayers)[0];
  baseLayers[firstBasemap].addTo(map);

  L.control.scale({ imperial: false }).addTo(map);

  map.on("moveend", () => {
    if (onViewChange) onViewChange();
  });

  map.on("mousemove", (e: L.LeafletMouseEvent) => {
    if (renderedPoints.length === 0) return;
    const tooltip = getOrCreateTooltip();
    const containerPt = e.containerPoint;

    let minDist = Infinity;
    let nearestPt: { lat: number; lon: number; idx: number } | null = null;
    for (const pt of renderedPoints) {
      const px = map.latLngToContainerPoint([pt.lat, pt.lon]);
      const dx = px.x - containerPt.x;
      const dy = px.y - containerPt.y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < minDist) {
        minDist = dist;
        nearestPt = pt;
      }
    }

    if (minDist <= HOVER_THRESHOLD_PX && nearestPt !== null) {
      const frameLabel = currentFrameIds
        ? (currentFrameIds[nearestPt.idx] ?? "unknown")
        : `trace ${nearestPt.idx}`;
      tooltip.replaceChildren();
      const line1 = document.createElement("div");
      line1.textContent = frameLabel;
      tooltip.appendChild(line1);
      if (currentValues && currentVarInfo) {
        const v = currentValues[nearestPt.idx];
        if (!isNaN(v)) {
          const line2 = document.createElement("div");
          line2.textContent = `${currentVarInfo.label} [${currentVarInfo.unit}]: ${formatScaledValue(v, currentVarInfo)}`;
          tooltip.appendChild(line2);
        }
      }
      tooltip.style.display = "block";
      tooltip.style.left = `${containerPt.x + 14}px`;
      tooltip.style.top = `${containerPt.y - 28}px`;
    } else {
      tooltip.style.display = "none";
    }
  });

  map.on("mouseout", () => {
    if (tooltipEl) tooltipEl.style.display = "none";
  });

  return map;
}

export function getHemisphere(): Hemisphere {
  return currentHemisphere;
}

export function destroyMap(): void {
  if (canvasOverlay) {
    map.removeLayer(canvasOverlay);
    canvasOverlay = null;
  }
  renderedPoints = [];
  currentFrameIds = null;
  currentValues = null;
  currentVarInfo = null;
  if (tooltipEl && tooltipEl.parentNode) {
    tooltipEl.parentNode.removeChild(tooltipEl);
    tooltipEl = null;
  }
  if (map) map.remove();
}

function getOrCreateTooltip(): HTMLDivElement {
  if (!tooltipEl) {
    tooltipEl = document.createElement("div");
    Object.assign(tooltipEl.style, {
      position: "absolute",
      background: "rgba(0,0,0,0.72)",
      color: "#fff",
      padding: "3px 8px",
      borderRadius: "4px",
      fontSize: "12px",
      fontFamily: "monospace",
      pointerEvents: "none",
      zIndex: "1000",
      display: "none",
      whiteSpace: "nowrap",
    });
    map.getContainer().appendChild(tooltipEl);
  }
  return tooltipEl;
}

export function setBasemap(name: string): void {
  for (const [key, layer] of Object.entries(baseLayers)) {
    if (key === name) {
      if (!map.hasLayer(layer)) map.addLayer(layer);
    } else {
      if (map.hasLayer(layer)) map.removeLayer(layer);
    }
  }
}

export function getBasemapNames(): string[] {
  return Object.keys(baseLayers);
}

interface PointData {
  lat: number;
  lon: number;
  color: string;
  idx: number;
}

// Custom canvas layer that draws all points in one pass
const CanvasPointsLayer = L.Layer.extend({
  _points: [] as PointData[],
  _canvas: null as HTMLCanvasElement | null,

  initialize(points: PointData[]) {
    this._points = points;
  },

  onAdd(map: L.Map) {
    this._canvas = L.DomUtil.create(
      "canvas",
      "leaflet-layer"
    ) as HTMLCanvasElement;
    this._canvas.style.position = "absolute";
    this._canvas.style.pointerEvents = "none";
    const pane = map.getPane("overlayPane")!;
    pane.appendChild(this._canvas);

    map.on("moveend", this._redraw, this);
    map.on("zoomend", this._redraw, this);
    this._redraw();
    return this;
  },

  onRemove(map: L.Map) {
    if (this._canvas && this._canvas.parentNode) {
      this._canvas.parentNode.removeChild(this._canvas);
    }
    map.off("moveend", this._redraw, this);
    map.off("zoomend", this._redraw, this);
    return this;
  },

  _redraw() {
    if (!this._canvas) return;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const map = (this as any)._map as L.Map;
    if (!map) return;

    const size = map.getSize();
    const topLeft = map.containerPointToLayerPoint([0, 0]);
    L.DomUtil.setPosition(this._canvas, topLeft);
    this._canvas.width = size.x;
    this._canvas.height = size.y;

    const ctx = this._canvas.getContext("2d")!;
    ctx.clearRect(0, 0, size.x, size.y);

    const radius = Math.max(2, Math.min(4, map.getZoom()));

    for (const pt of this._points) {
      const px = map.latLngToContainerPoint([pt.lat, pt.lon]);
      ctx.fillStyle = pt.color;
      ctx.beginPath();
      ctx.arc(px.x, px.y, radius, 0, Math.PI * 2);
      ctx.fill();
    }
  },
});

export function renderPoints(
  data: StoreData,
  variableName: string,
  varInfo: VariableInfo,
  scale: ColorScale,
  seasonPredicate: ((traceIdx: number) => boolean) | null = null,
): void {
  if (canvasOverlay) {
    map.removeLayer(canvasOverlay);
    canvasOverlay = null;
  }

  const values = data.variables[variableName];
  if (!values) return;

  // Stable subsample over qc-passing traces with valid lat/lon — *not* filtered
  // by variable-NaN or season. Sampling the same set every render keeps the
  // unrelated seasons' visible points fixed when one is toggled.
  const baseIndices: number[] = [];
  for (let i = 0; i < data.numTraces; i++) {
    if (data.qcPass && !data.qcPass[i]) continue;
    if (isNaN(data.latitude[i]) || isNaN(data.longitude[i])) continue;
    baseIndices.push(i);
  }

  let indices = baseIndices;
  if (baseIndices.length > MAX_RENDER_POINTS) {
    const step = baseIndices.length / MAX_RENDER_POINTS;
    indices = [];
    for (let j = 0; j < MAX_RENDER_POINTS; j++) {
      indices.push(baseIndices[Math.floor(j * step)]);
    }
  }

  const points: PointData[] = [];
  for (const i of indices) {
    if (isNaN(values[i])) continue;
    if (seasonPredicate && !seasonPredicate(i)) continue;
    points.push({
      lat: data.latitude[i],
      lon: data.longitude[i],
      color: scale.getColor(values[i]),
      idx: i,
    });
  }

  // Update hover state
  renderedPoints = points.map((p) => ({ lat: p.lat, lon: p.lon, idx: p.idx }));
  currentFrameIds = data.frameId;
  currentValues = values;
  currentVarInfo = varInfo;
  if (tooltipEl) tooltipEl.style.display = "none";

  canvasOverlay = new (CanvasPointsLayer as unknown as new (
    points: PointData[]
  ) => L.Layer)(points);
  canvasOverlay.addTo(map);
}

export function fitToData(data: StoreData): void {
  let minLat = Infinity,
    maxLat = -Infinity,
    minLon = Infinity,
    maxLon = -Infinity;
  for (let i = 0; i < data.numTraces; i++) {
    if (isNaN(data.latitude[i]) || isNaN(data.longitude[i])) continue;
    if (data.qcPass && !data.qcPass[i]) continue;
    minLat = Math.min(minLat, data.latitude[i]);
    maxLat = Math.max(maxLat, data.latitude[i]);
    minLon = Math.min(minLon, data.longitude[i]);
    maxLon = Math.max(maxLon, data.longitude[i]);
  }

  if (isFinite(minLat)) {
    map.fitBounds([
      [minLat, minLon],
      [maxLat, maxLon],
    ]);
  }
}
