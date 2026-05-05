import { IcechunkStore } from "@carbonplan/icechunk-js";
import * as zarr from "zarrita";
import { STORE_URL, C, ICE_PERMITTIVITY } from "./config";

export interface StoreData {
  latitude: Float64Array;
  longitude: Float64Array;
  qcPass: Int8Array | null;
  frameId: string[] | null;
  variables: Record<string, Float64Array>;
  numTraces: number;
}

export async function openStore(snapshotId?: string): Promise<IcechunkStore> {
  const opts = snapshotId ? { snapshot: snapshotId } : { branch: "main" };
  return IcechunkStore.open(STORE_URL, opts);
}

async function loadArray(
  store: IcechunkStore,
  name: string
): Promise<zarr.Chunk<zarr.DataType>> {
  const root = zarr.root(store);
  const arr = await zarr.open(root.resolve(`/${name}`), { kind: "array" });
  return zarr.get(arr);
}

function toFloat64Array(chunk: zarr.Chunk<zarr.DataType>): Float64Array {
  const data = chunk.data;
  if (data instanceof Float64Array) return data;
  if (data instanceof Float32Array) return new Float64Array(data);
  if (ArrayBuffer.isView(data))
    return new Float64Array(data.buffer, data.byteOffset, data.byteLength / 8);
  return new Float64Array(data as unknown as ArrayLike<number>);
}

function toInt8Array(chunk: zarr.Chunk<zarr.DataType>): Int8Array {
  const data = chunk.data;
  if (data instanceof Int8Array) return data;
  if (ArrayBuffer.isView(data))
    return new Int8Array(data.buffer, data.byteOffset, data.byteLength);
  return new Int8Array(data as unknown as ArrayLike<number>);
}

// Load frame IDs via frame_index (uint16 per trace) + frame_names group attribute.
// The native frame_id array uses zarr-python v3's numpy.str_ dtype which
// zarrita cannot parse. Run scripts/add_frame_index.py once to populate these.
async function loadFrameIds(store: IcechunkStore): Promise<string[] | null> {
  // Load frame_names from the root group attributes
  const rootGrp = await zarr.open(zarr.root(store), { kind: "group" });
  const frameNames = (rootGrp.attrs as Record<string, unknown>)[
    "frame_names"
  ] as string[] | undefined;
  if (!frameNames?.length) return null;

  // Load per-trace frame index
  const idxArr = await zarr.open(
    zarr.root(store).resolve("/frame_index"),
    { kind: "array" }
  );
  const chunk = await zarr.get(idxArr);
  const indices = chunk.data as Uint16Array;

  return Array.from(indices, (i) => frameNames[i] ?? "unknown");
}

export async function loadEssentials(store: IcechunkStore): Promise<StoreData> {
  const [latChunk, lonChunk] = await Promise.all([
    loadArray(store, "latitude"),
    loadArray(store, "longitude"),
  ]);

  const latitude = toFloat64Array(latChunk);
  const longitude = toFloat64Array(lonChunk);

  let qcPass: Int8Array | null = null;
  try {
    const qcChunk = await loadArray(store, "qc_pass");
    qcPass = toInt8Array(qcChunk);
  } catch {
    // qc_pass may not exist
  }

  let frameId: string[] | null = null;
  try {
    frameId = await loadFrameIds(store);
  } catch (err) {
    console.warn("frame_id not loaded:", err);
  }

  return { latitude, longitude, qcPass, frameId, variables: {}, numTraces: latitude.length };
}

export async function loadVariables(
  store: IcechunkStore,
  data: StoreData,
  names: string[]
): Promise<void> {
  const missing = names.filter((n) => !(n in data.variables));
  if (missing.length === 0) return;

  const results = await Promise.all(
    missing.map(async (name) => {
      try {
        const chunk = await loadArray(store, name);
        return [name, toFloat64Array(chunk)] as const;
      } catch {
        return null;
      }
    })
  );

  for (const entry of results) {
    if (entry) data.variables[entry[0]] = entry[1];
  }

  computeRSSNR(data.variables);
}

function computeRSSNR(variables: Record<string, Float64Array>): void {
  const surfTwtt = variables["surface_twtt"];
  const bedTwtt = variables["bed_twtt"];
  const surfPower = variables["surface_power_dB"];
  const bedPower = variables["bed_power_dB"];
  if (!surfTwtt || !bedTwtt || !surfPower || !bedPower) return;

  const n = Math.sqrt(ICE_PERMITTIVITY);
  const speedInIce = C / n;
  const rssnr = new Float64Array(surfTwtt.length);

  for (let i = 0; i < surfTwtt.length; i++) {
    const h = (surfTwtt[i] * C) / 2;
    const z = ((bedTwtt[i] - surfTwtt[i]) * speedInIce) / 2;

    if (isNaN(h) || isNaN(z) || h <= 0) {
      rssnr[i] = NaN;
      continue;
    }

    const geomSurf = 10 * Math.log10(1 / (h * h));
    const hPlusZOverN = h + z / n;
    const geomBed = 10 * Math.log10(1 / (hPlusZOverN * hPlusZOverN));

    rssnr[i] = surfPower[i] - geomSurf - (bedPower[i] - geomBed);
  }

  variables["rssnr"] = rssnr;
}
