import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import chroma from "chroma-js";

// Stable categorical season -> color. Sorted so the same season always gets
// the same color regardless of which subset is currently shown.
const SEASON_PALETTE = chroma.brewer.Set2.concat(chroma.brewer.Set1);
const ALL_COLOR = "#e8e8e8";

export function seasonColors(seasons: string[]): Map<string, string> {
  const sorted = Array.from(new Set(seasons)).sort();
  const m = new Map<string, string>();
  sorted.forEach((s, i) => m.set(s, SEASON_PALETTE[i % SEASON_PALETTE.length]));
  return m;
}

const GRID_N = 256;

function percentile(sorted: number[], p: number): number {
  const i = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * p)));
  return sorted[i];
}

// Gaussian KDE over `grid`, normalized to unit area across the grid so curve
// shapes are comparable regardless of sample size.
function kde(values: number[], grid: number[]): number[] {
  const n = values.length;
  const out = new Array(grid.length).fill(0);
  if (n < 2) return out;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  let varSum = 0;
  for (const v of values) varSum += (v - mean) ** 2;
  const std = Math.sqrt(varSum / (n - 1));
  if (std === 0) return out;
  const bw = 1.06 * std * Math.pow(n, -0.2);
  const inv = 1 / (bw * Math.sqrt(2 * Math.PI));
  for (let g = 0; g < grid.length; g++) {
    let s = 0;
    const x = grid[g];
    for (let i = 0; i < n; i++) {
      const z = (x - values[i]) / bw;
      s += Math.exp(-0.5 * z * z);
    }
    out[g] = (s * inv) / n;
  }
  // Trapezoidal renormalization to unit area over the plotted range.
  let area = 0;
  for (let g = 1; g < grid.length; g++) {
    area += ((out[g] + out[g - 1]) / 2) * (grid[g] - grid[g - 1]);
  }
  if (area > 0) for (let g = 0; g < out.length; g++) out[g] /= area;
  return out;
}

export interface HistSeries {
  label: string;
  color: string;
  values: number[];
}

let plot: uPlot | null = null;

export function clearHistogram(legendEl?: HTMLElement): void {
  if (plot) {
    plot.destroy();
    plot = null;
  }
  if (legendEl) legendEl.replaceChildren();
}

// Returns the row elements in series order so the chart's focus hook can
// highlight the matching entry on hover.
function buildLegend(legendEl: HTMLElement, series: HistSeries[]): HTMLElement[] {
  legendEl.replaceChildren();
  const rows: HTMLElement[] = [];
  for (const s of series) {
    const isAll = s.label === "All";
    const row = document.createElement("div");
    row.className = isAll ? "leg-row leg-all" : "leg-row";
    const sw = document.createElement("span");
    sw.className = "leg-swatch";
    sw.style.borderTopColor = isAll ? ALL_COLOR : s.color;
    sw.style.borderTopWidth = isAll ? "3px" : "2px";
    row.appendChild(sw);
    row.append(s.label);
    legendEl.appendChild(row);
    rows.push(row);
  }
  return rows;
}

// Rebuild the chart. `series` order is drawn as given; pass the cumulative
// "All" series last so it sits on top. A custom fixed legend is rendered into
// `legendEl` (uPlot's own legend is disabled because its live values reflow
// and shift the canvas under the cursor).
export function renderHistogram(
  container: HTMLElement,
  legendEl: HTMLElement,
  title: string,
  series: HistSeries[],
  // true: each season curve independently normalized to unit area.
  // false: common normalization — season curves scaled by their share of
  // the pooled sample so heights reflect relative abundance.
  perSeasonNorm: boolean,
): void {
  clearHistogram(legendEl);
  container.replaceChildren();

  // "All" is the union of the shown seasons; use it for the axis range so a
  // single-season store (no per-season series) still works.
  const allSeries = series.find((s) => s.label === "All") ?? series[series.length - 1];
  const usable = (allSeries?.values ?? []).filter((v) => !isNaN(v));
  if (usable.length < 2) {
    const msg = document.createElement("div");
    msg.className = "region-empty";
    msg.textContent = "No data for the selected seasons in this region.";
    container.appendChild(msg);
    return;
  }

  usable.sort((a, b) => a - b);
  const lo = percentile(usable, 0.02);
  const hi = percentile(usable, 0.98);
  const xmin = lo;
  const xmax = hi === lo ? lo + 1 : hi;
  const step = (xmax - xmin) / (GRID_N - 1);
  const grid = Array.from({ length: GRID_N }, (_, i) => xmin + i * step);

  // Common-norm reference: size of the pooled ("All") sample.
  const nAll = usable.length;
  const data: uPlot.AlignedData = [
    grid,
    ...series.map((s) => {
      const vals = s.values.filter((v) => !isNaN(v));
      const density = kde(vals, grid);
      // "All" is always unit area; season curves are scaled by their share
      // of the pooled sample when common-norm is selected.
      if (perSeasonNorm || s.label === "All" || nAll === 0) return density;
      const w = vals.length / nAll;
      return density.map((d) => d * w);
    }),
  ];

  const uSeries: uPlot.Series[] = [
    {},
    ...series.map((s) => {
      const isAll = s.label === "All";
      return {
        label: s.label,
        stroke: isAll ? ALL_COLOR : s.color,
        width: isAll ? 3 : 1.5,
        points: { show: false },
      };
    }),
  ];

  const legRows = buildLegend(legendEl, series);

  const w = container.clientWidth || 460;
  const h = 260;
  plot = new uPlot(
    {
      title,
      width: w,
      height: h,
      // Custom legend (see buildLegend); focus dims other series on hover.
      cursor: { drag: { x: false, y: false }, focus: { prox: 24 } },
      focus: { alpha: 0.25 },
      legend: { show: false },
      hooks: {
        setSeries: [
          (_u, sIdx) => {
            legRows.forEach((r, i) =>
              r.classList.toggle("leg-hi", sIdx != null && i === sIdx - 1),
            );
          },
        ],
      },
      scales: { x: { time: false } },
      axes: [
        { stroke: "#aaa", grid: { stroke: "rgba(255,255,255,0.08)" }, ticks: { stroke: "rgba(255,255,255,0.15)" } },
        {
          stroke: "#aaa",
          grid: { stroke: "rgba(255,255,255,0.08)" },
          ticks: { stroke: "rgba(255,255,255,0.15)" },
          size: 50,
        },
      ],
      series: uSeries,
    },
    data,
    container,
  );
}
