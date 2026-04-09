import chroma from "chroma-js";

const COLORMAPS: Record<string, string[]> = {
  viridis: [
    "#440154",
    "#482777",
    "#3e4989",
    "#31688e",
    "#26828e",
    "#1f9e89",
    "#35b779",
    "#6ece58",
    "#b5de2b",
    "#fde725",
  ],
  terrain: [
    "#333399",
    "#0080ff",
    "#00cc66",
    "#66ff33",
    "#ccff33",
    "#ffff00",
    "#ffcc00",
    "#cc6600",
    "#993300",
    "#ffffff",
  ],
};

export interface ColorScale {
  getColor: (value: number) => string;
  vmin: number;
  vmax: number;
}

export function createColorScale(
  values: Float64Array,
  cmapName: string,
  qcPass: Int8Array | null
): ColorScale {
  // Collect valid values (not NaN, passes QC)
  const valid: number[] = [];
  for (let i = 0; i < values.length; i++) {
    if (isNaN(values[i])) continue;
    if (qcPass && !qcPass[i]) continue;
    valid.push(values[i]);
  }

  if (valid.length === 0) {
    return { getColor: () => "#888888", vmin: 0, vmax: 1 };
  }

  valid.sort((a, b) => a - b);
  const p02 = valid[Math.floor(valid.length * 0.02)];
  const p98 = valid[Math.floor(valid.length * 0.98)];
  const vmin = p02;
  const vmax = p98 === p02 ? p02 + 1 : p98;

  const colors = COLORMAPS[cmapName] || COLORMAPS.viridis;
  const scale = chroma.scale(colors).domain([vmin, vmax]);

  return {
    getColor: (value: number) => scale(value).hex(),
    vmin,
    vmax,
  };
}

export function drawLegend(
  canvas: HTMLCanvasElement,
  scale: ColorScale,
  cmapName: string
): void {
  const ctx = canvas.getContext("2d")!;
  const h = canvas.height;
  const w = canvas.width;
  const colors = COLORMAPS[cmapName] || COLORMAPS.viridis;
  const chromaScale = chroma.scale(colors).domain([0, 1]);

  for (let y = 0; y < h; y++) {
    const t = 1 - y / h; // top = max
    ctx.fillStyle = chromaScale(t).hex();
    ctx.fillRect(0, y, w, 1);
  }
}
