import { IcechunkStore } from "@carbonplan/icechunk-js";
import { VARIABLES, STORES } from "./config";
import { createColorScale, drawLegend } from "./colormap";
import { getCommitLog, CommitEntry, formatDate } from "./history";
import {
  initMap,
  renderPoints,
  fitToData,
  setBasemap,
  destroyMap,
  getHemisphere,
  getBasemapNames,
  formatScaledValue,
  setOnViewChange,
  isLatLonVisible,
} from "./map";
import { openStore, loadEssentials, loadVariables, StoreData } from "./store";

const datasetSelect = document.getElementById(
  "dataset-select"
) as HTMLSelectElement;
const basemapSelect = document.getElementById(
  "basemap-select"
) as HTMLSelectElement;
const variableSelect = document.getElementById(
  "variable-select"
) as HTMLSelectElement;
const historyList = document.getElementById("history-list") as HTMLUListElement;
const seasonsSection = document.getElementById("seasons-section") as HTMLDetailsElement;
const seasonList = document.getElementById("season-list") as HTMLDivElement;
const statusEl = document.getElementById("status") as HTMLDivElement;
const loadingOverlay = document.getElementById(
  "loading-overlay"
) as HTMLDivElement;
const legendTitle = document.getElementById("legend-title") as HTMLDivElement;
const legendMax = document.getElementById("legend-max") as HTMLSpanElement;
const legendMid = document.getElementById("legend-mid") as HTMLSpanElement;
const legendMin = document.getElementById("legend-min") as HTMLSpanElement;
const legendCanvas = document.getElementById("legend-bar") as HTMLCanvasElement;
const legendAdaptiveCb = document.getElementById("legend-adaptive-cb") as HTMLInputElement;

let currentData: StoreData | null = null;
let currentStore: IcechunkStore | null = null;
let currentSnapshotId: string | undefined;
let currentStoreIndex: number = 0;
let commitLog: CommitEntry[] = [];

// Season filter: per-trace collection name (e.g. "2018_Greenland_P3") read
// from the store's frame_collections attribute. Null when the store predates
// the backfill. enabledSeasons holds the collection names currently checked.
let traceSeasons: string[] | null = null;
let enabledSeasons: Set<string> = new Set();

function buildSeasonState(data: StoreData): void {
  traceSeasons = data.frameCollection;
  if (!traceSeasons) {
    enabledSeasons = new Set();
    seasonsSection.hidden = true;
    seasonList.replaceChildren();
    return;
  }
  const unique = Array.from(new Set(traceSeasons)).sort();
  enabledSeasons = new Set(unique);
  if (unique.length <= 1) {
    seasonsSection.hidden = true;
    seasonList.replaceChildren();
    return;
  }
  seasonsSection.hidden = false;
  seasonList.replaceChildren();
  for (const season of unique) {
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = true;
    cb.value = season;
    cb.addEventListener("change", () => {
      if (cb.checked) enabledSeasons.add(season);
      else enabledSeasons.delete(season);
      renderCurrentVariable();
    });
    label.appendChild(cb);
    label.append(` ${season}`);
    seasonList.appendChild(label);
  }
}

function seasonPredicate(): ((traceIdx: number) => boolean) | null {
  if (!traceSeasons) return null;
  // No filter when every season is enabled.
  const total = new Set(traceSeasons).size;
  if (enabledSeasons.size === total) return null;
  const enabled = enabledSeasons;
  const seasons = traceSeasons;
  return (i) => enabled.has(seasons[i]);
}

function setStatus(msg: string) {
  statusEl.textContent = msg;
}

function showLoading(msg: string) {
  loadingOverlay.textContent = msg;
  loadingOverlay.classList.remove("hidden");
}

function hideLoading() {
  loadingOverlay.classList.add("hidden");
}

function updateLegend(variableName: string, scale: ReturnType<typeof createColorScale>) {
  const varInfo = VARIABLES[variableName];
  if (!varInfo) return;
  legendTitle.textContent = `${varInfo.label} [${varInfo.unit}]`;
  legendMax.textContent = formatScaledValue(scale.vmax, varInfo);
  legendMid.textContent = formatScaledValue((scale.vmin + scale.vmax) / 2, varInfo);
  legendMin.textContent = formatScaledValue(scale.vmin, varInfo);
  drawLegend(legendCanvas, scale, varInfo.cmap);
}

function renderCurrentVariable() {
  if (!currentData) return;
  const variableName = variableSelect.value;
  const varInfo = VARIABLES[variableName];
  if (!varInfo) return;

  const values = currentData.variables[variableName];
  if (!values) {
    setStatus(`Variable ${variableName} not available`);
    return;
  }

  const seasonPred = seasonPredicate();
  // Adaptive scale: stretch color range to values currently visible on the map
  // (also respecting any season filter so they agree with what's drawn).
  // Otherwise use the global dataset range (the prior default).
  const scaleIncludeFn = legendAdaptiveCb.checked
    ? (i: number) => {
        if (seasonPred && !seasonPred(i)) return false;
        return isLatLonVisible(currentData!.latitude[i], currentData!.longitude[i]);
      }
    : undefined;

  const scale = createColorScale(values, varInfo.cmap, currentData.qcPass, scaleIncludeFn);
  renderPoints(currentData, variableName, varInfo, scale, seasonPred);
  updateLegend(variableName, scale);

  const validCount = Array.from(values).filter((v) => !isNaN(v)).length;
  setStatus(
    `${currentData.numTraces.toLocaleString()} traces, ${validCount.toLocaleString()} valid`
  );
}

async function ensureVariable(variableName: string): Promise<void> {
  if (!currentData || !currentStore) return;
  if (variableName in currentData.variables) return;
  setStatus(`Loading ${VARIABLES[variableName]?.label ?? variableName}...`);
  await loadVariables(currentStore, currentData, [variableName]);
}

async function loadSnapshot(snapshotId?: string) {
  showLoading("Loading data from S3...");
  currentSnapshotId = snapshotId;
  currentData = null;
  currentStore = null;

  try {
    currentStore = await openStore(STORES[currentStoreIndex].url, snapshotId);
    currentData = await loadEssentials(currentStore);
    buildSeasonState(currentData);

    // Load only the initially selected variable
    const initialVar = variableSelect.value;
    showLoading(`Loading ${VARIABLES[initialVar]?.label ?? initialVar}...`);
    await loadVariables(currentStore, currentData, [initialVar]);

    // Fit before rendering so the adaptive color scale (when on) sees the
    // post-fit bounds rather than whatever default view the map was at.
    fitToData(currentData);
    renderCurrentVariable();
    hideLoading();
  } catch (err) {
    hideLoading();
    setStatus(`Error loading data: ${err}`);
    console.error(err);
  }
}

function renderHistoryList() {
  historyList.innerHTML = "";
  for (const entry of commitLog) {
    const li = document.createElement("li");
    if (entry.id === (currentSnapshotId ?? commitLog[0]?.id)) {
      li.classList.add("active");
    }

    const msgSpan = document.createElement("span");
    msgSpan.className = "commit-msg";
    msgSpan.textContent = entry.message || "(no message)";

    const metaSpan = document.createElement("span");
    metaSpan.className = "commit-meta";
    metaSpan.textContent = `${formatDate(entry.date)} | ${entry.id.slice(0, 8)}...`;

    li.appendChild(msgSpan);
    li.appendChild(metaSpan);

    li.addEventListener("click", () => {
      loadSnapshot(entry.id);
      historyList
        .querySelectorAll("li")
        .forEach((el) => el.classList.remove("active"));
      li.classList.add("active");
    });

    historyList.appendChild(li);
  }
}

function populateDatasetSelect() {
  datasetSelect.innerHTML = "";
  for (const store of STORES) {
    const opt = document.createElement("option");
    opt.value = store.label;
    opt.textContent = store.label;
    datasetSelect.appendChild(opt);
  }
}

function populateBasemapSelect() {
  basemapSelect.innerHTML = "";
  for (const name of getBasemapNames()) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    basemapSelect.appendChild(opt);
  }
}

async function init() {
  populateDatasetSelect();
  initMap("map", STORES[currentStoreIndex].hemisphere);
  populateBasemapSelect();

  // Browsers autofill checkbox state across reloads but do not fire the change
  // event for the restored value, so sync onViewChange manually before any
  // initial render.
  if (legendAdaptiveCb.checked) {
    setOnViewChange(renderCurrentVariable);
  }

  async function switchDataset(index: number) {
    const store = STORES[index];
    if (!store) return;

    // Rebuild the map if the new dataset is in a different hemisphere
    // (Leaflet doesn't allow changing CRS on a live map).
    if (store.hemisphere !== getHemisphere()) {
      destroyMap();
      initMap("map", store.hemisphere);
      populateBasemapSelect();
    }

    currentStoreIndex = index;
    currentSnapshotId = undefined;

    historyList.innerHTML = '<li class="commit-meta">Loading...</li>';
    const [logResult] = await Promise.allSettled([getCommitLog(store.url)]);
    if (logResult.status === "fulfilled") {
      commitLog = logResult.value;
      renderHistoryList();
    } else {
      historyList.innerHTML =
        '<li class="commit-meta">Failed to load history</li>';
      console.error("History error:", logResult.reason);
    }

    await loadSnapshot();
  }

  await switchDataset(currentStoreIndex);

  datasetSelect.addEventListener("change", () => {
    switchDataset(datasetSelect.selectedIndex);
  });

  basemapSelect.addEventListener("change", () => {
    setBasemap(basemapSelect.value);
  });

  legendAdaptiveCb.addEventListener("change", () => {
    setOnViewChange(legendAdaptiveCb.checked ? renderCurrentVariable : null);
    renderCurrentVariable();
  });

  variableSelect.addEventListener("change", async () => {
    if (!currentData || !currentStore) return;
    const variableName = variableSelect.value;
    if (!(variableName in currentData.variables)) {
      showLoading(`Loading ${VARIABLES[variableName]?.label ?? variableName}...`);
      await ensureVariable(variableName);
      hideLoading();
    }
    renderCurrentVariable();
  });
}

init().catch(console.error);
