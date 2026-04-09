import L from "leaflet";
import "proj4leaflet";
import proj4 from "proj4";
import { ColorScale } from "./colormap";
import { StoreData } from "./store";

// EPSG:3031 Antarctic Polar Stereographic
const PROJ_DEF =
  "+proj=stere +lat_0=-90 +lat_ts=-71 +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs";
proj4.defs("EPSG:3031", PROJ_DEF);

const GIBS_RESOLUTIONS = [8192, 4096, 2048, 1024, 512, 256];
const GIBS_ORIGIN: [number, number] = [-4194304, 4194304];
const GIBS_BOUNDS = L.bounds([-4194304, -4194304], [4194304, 4194304]);

const EPSG3031 = new L.Proj.CRS("EPSG:3031", PROJ_DEF, {
  resolutions: GIBS_RESOLUTIONS,
  origin: GIBS_ORIGIN,
  bounds: GIBS_BOUNDS,
});

let map: L.Map;
let baseLayers: Record<string, L.TileLayer>;

// Canvas overlay for fast point rendering
let canvasOverlay: L.Layer | null = null;

// Maximum points to render — subsample if over this
const MAX_RENDER_POINTS = 20000;

export function initMap(containerId: string): L.Map {
  map = L.map(containerId, {
    crs: EPSG3031,
    center: [-76, 162],
    zoom: 2,
    minZoom: 0,
    maxZoom: 5,
    preferCanvas: true,
  });

  const tileOpts: L.TileLayerOptions = {
    tileSize: 512,
    attribution: "NASA GIBS",
    noWrap: true,
  };

  baseLayers = {
    "Blue Marble": L.tileLayer(
      "https://gibs.earthdata.nasa.gov/wmts/epsg3031/best/BlueMarble_ShadedRelief_Bathymetry/default/2004-08-01/500m/{z}/{y}/{x}.jpeg",
      { ...tileOpts, maxZoom: 4 }
    ),
    "Land / Water": L.tileLayer(
      "https://gibs.earthdata.nasa.gov/wmts/epsg3031/best/SCAR_Land_Water_Map/default/2024-01-01/250m/{z}/{y}/{x}.png",
      { ...tileOpts, maxZoom: 5 }
    ),
  };

  baseLayers["Blue Marble"].addTo(map);
  return map;
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
  scale: ColorScale
): void {
  if (canvasOverlay) {
    map.removeLayer(canvasOverlay);
    canvasOverlay = null;
  }

  const values = data.variables[variableName];
  if (!values) return;

  // Build list of valid point indices
  const validIndices: number[] = [];
  for (let i = 0; i < data.numTraces; i++) {
    if (data.qcPass && !data.qcPass[i]) continue;
    if (isNaN(values[i])) continue;
    if (isNaN(data.latitude[i]) || isNaN(data.longitude[i])) continue;
    validIndices.push(i);
  }

  // Subsample if too many points
  let indices = validIndices;
  if (validIndices.length > MAX_RENDER_POINTS) {
    const step = validIndices.length / MAX_RENDER_POINTS;
    indices = [];
    for (let j = 0; j < MAX_RENDER_POINTS; j++) {
      indices.push(validIndices[Math.floor(j * step)]);
    }
  }

  const points: PointData[] = indices.map((i) => ({
    lat: data.latitude[i],
    lon: data.longitude[i],
    color: scale.getColor(values[i]),
  }));

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
