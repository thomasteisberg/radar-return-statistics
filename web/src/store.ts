import { IcechunkStore } from "@carbonplan/icechunk-js";
import * as zarr from "zarrita";
import { VARIABLE_SOURCE } from "./config";

export interface StoreData {
  latitude: Float64Array;
  longitude: Float64Array;
  qcPass: Int8Array | null;
  frameId: string[] | null;
  // Collection name (e.g. "2018_Greenland_P3") per trace, parallel to frameId.
  // Null when the store predates frame_collections backfill.
  frameCollection: string[] | null;
  variables: Record<string, Float64Array>;
  numTraces: number;
}

export async function openStore(storeUrl: string, snapshotId?: string): Promise<IcechunkStore> {
  const opts = snapshotId ? { snapshot: snapshotId } : { branch: "main" };
  return IcechunkStore.open(storeUrl, opts);
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

// Load frame IDs (and per-trace collection if available) via frame_index
// (uint16 per trace) plus the frame_names / frame_collections group
// attributes. The native frame_id array uses zarr-python v3's numpy.str_
// dtype which zarrita cannot parse.
async function loadFrameInfo(
  store: IcechunkStore
): Promise<{ frameId: string[] | null; frameCollection: string[] | null }> {
  const rootGrp = await zarr.open(zarr.root(store), { kind: "group" });
  const attrs = rootGrp.attrs as Record<string, unknown>;
  const frameNames = attrs["frame_names"] as string[] | undefined;
  const frameCollections = attrs["frame_collections"] as string[] | undefined;
  if (!frameNames?.length) return { frameId: null, frameCollection: null };

  const idxArr = await zarr.open(
    zarr.root(store).resolve("/frame_index"),
    { kind: "array" }
  );
  const chunk = await zarr.get(idxArr);
  const indices = chunk.data as Uint16Array;

  const frameId = Array.from(indices, (i) => frameNames[i] ?? "unknown");
  const frameCollection =
    frameCollections && frameCollections.length === frameNames.length
      ? Array.from(indices, (i) => frameCollections[i] ?? "")
      : null;
  return { frameId, frameCollection };
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
  let frameCollection: string[] | null = null;
  try {
    ({ frameId, frameCollection } = await loadFrameInfo(store));
  } catch (err) {
    console.warn("frame info not loaded:", err);
  }

  return {
    latitude,
    longitude,
    qcPass,
    frameId,
    frameCollection,
    variables: {},
    numTraces: latitude.length,
  };
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
      const source = VARIABLE_SOURCE[name] ?? name;
      try {
        const chunk = await loadArray(store, source);
        return [name, toFloat64Array(chunk)] as const;
      } catch {
        return null;
      }
    })
  );

  for (const entry of results) {
    if (entry) data.variables[entry[0]] = entry[1];
  }
}
