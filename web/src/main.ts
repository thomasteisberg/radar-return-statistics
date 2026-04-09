import { IcechunkStore } from "@carbonplan/icechunk-js";
import { VARIABLES, VARIABLE_DEPS } from "./config";
import { createColorScale, drawLegend } from "./colormap";
import { getCommitLog, CommitEntry, formatDate } from "./history";
import { initMap, renderPoints, fitToData, setBasemap } from "./map";
import { openStore, loadEssentials, loadVariables, StoreData } from "./store";

const basemapSelect = document.getElementById(
  "basemap-select"
) as HTMLSelectElement;
const variableSelect = document.getElementById(
  "variable-select"
) as HTMLSelectElement;
const historyList = document.getElementById("history-list") as HTMLUListElement;
const statusEl = document.getElementById("status") as HTMLDivElement;
const loadingOverlay = document.getElementById(
  "loading-overlay"
) as HTMLDivElement;
const legendTitle = document.getElementById("legend-title") as HTMLDivElement;
const legendMax = document.getElementById("legend-max") as HTMLSpanElement;
const legendMid = document.getElementById("legend-mid") as HTMLSpanElement;
const legendMin = document.getElementById("legend-min") as HTMLSpanElement;
const legendCanvas = document.getElementById("legend-bar") as HTMLCanvasElement;

let currentData: StoreData | null = null;
let currentStore: IcechunkStore | null = null;
let currentSnapshotId: string | undefined;
let commitLog: CommitEntry[] = [];

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

function updateLegend(variableName: string) {
  if (!currentData) return;
  const varInfo = VARIABLES[variableName];
  const values = currentData.variables[variableName];
  if (!values || !varInfo) return;

  const scale = createColorScale(values, varInfo.cmap, currentData.qcPass);
  legendTitle.textContent = `${varInfo.label} [${varInfo.unit}]`;
  legendMax.textContent = scale.vmax.toPrecision(4);
  legendMid.textContent = ((scale.vmin + scale.vmax) / 2).toPrecision(4);
  legendMin.textContent = scale.vmin.toPrecision(4);
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

  const scale = createColorScale(values, varInfo.cmap, currentData.qcPass);
  renderPoints(currentData, variableName, scale);
  updateLegend(variableName);

  const validCount = Array.from(values).filter((v) => !isNaN(v)).length;
  setStatus(
    `${currentData.numTraces.toLocaleString()} traces, ${validCount.toLocaleString()} valid`
  );
}

function depsFor(variableName: string): string[] {
  return VARIABLE_DEPS[variableName] ?? [variableName];
}

async function ensureVariable(variableName: string): Promise<void> {
  if (!currentData || !currentStore) return;
  const deps = depsFor(variableName);
  const missing = deps.filter((d) => !(d in currentData!.variables));
  if (missing.length === 0) return;

  setStatus(`Loading ${VARIABLES[variableName]?.label ?? variableName}...`);
  await loadVariables(currentStore, currentData, deps);
}

async function loadSnapshot(snapshotId?: string) {
  showLoading("Loading data from S3...");
  currentSnapshotId = snapshotId;
  currentData = null;
  currentStore = null;

  try {
    currentStore = await openStore(snapshotId);
    currentData = await loadEssentials(currentStore);

    // Load only the initially selected variable
    const initialVar = variableSelect.value;
    showLoading(`Loading ${VARIABLES[initialVar]?.label ?? initialVar}...`);
    await loadVariables(currentStore, currentData, depsFor(initialVar));

    renderCurrentVariable();
    fitToData(currentData);
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

async function init() {
  initMap("map");

  const [logResult] = await Promise.allSettled([getCommitLog()]);

  if (logResult.status === "fulfilled") {
    commitLog = logResult.value;
    renderHistoryList();
  } else {
    historyList.innerHTML =
      '<li class="commit-meta">Failed to load history</li>';
    console.error("History error:", logResult.reason);
  }

  await loadSnapshot();

  basemapSelect.addEventListener("change", () => {
    setBasemap(basemapSelect.value);
  });

  variableSelect.addEventListener("change", async () => {
    if (!currentData || !currentStore) return;
    const variableName = variableSelect.value;
    const deps = depsFor(variableName);
    const missing = deps.filter((d) => !(d in currentData!.variables));
    if (missing.length > 0) {
      showLoading(`Loading ${VARIABLES[variableName]?.label ?? variableName}...`);
      await ensureVariable(variableName);
      hideLoading();
    }
    renderCurrentVariable();
  });
}

init().catch(console.error);
