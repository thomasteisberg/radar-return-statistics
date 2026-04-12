# Plan: Browser visualization with icechunk-js

Status: planned

## Goal

A static webpage (deployable to GitHub Pages) that reads the icechunk store from S3 and renders any of the 6 variables on an interactive polar map. No backend server — all data fetching and rendering happens client-side.

## Stack

- **icechunk-js** (`npm install icechunk-js`) — read-only access to the icechunk store on S3
- **zarrita.js** (`npm install zarrita`) — decode zarr arrays returned by icechunk-js
- **Leaflet** + **Leaflet.heat** or **deck.gl** — map rendering (see discussion below)
- **Vite** — build tool (fast, zero-config for TypeScript)

## Data flow

```
S3 bucket (public read)
  → icechunk-js opens store via HTTPS (anonymous, no credentials)
  → zarrita.js reads latitude, longitude, qc_pass, and selected variable arrays
  → filter to qc_pass == 1, drop NaN
  → render points on a polar stereographic map
```

## Key design decisions

### Map library

**Option A: Leaflet + proj4leaflet (simpler)**
- Leaflet is lightweight and widely used
- proj4leaflet plugin adds support for custom CRS (EPSG:3031, EPSG:3413)
- Points rendered as CircleMarkers or via a canvas layer
- Works well up to ~100K points; beyond that needs tiling or aggregation
- Antarctic/Arctic base tiles available from NSIDC or GIBS

**Option B: deck.gl (better for large datasets)**
- WebGL-accelerated, handles millions of points natively
- ScatterplotLayer with custom projections
- More complex setup, heavier bundle
- Better choice if the dataset grows to 100K+ frames (~50M traces)

**Recommendation: Start with Leaflet + proj4leaflet.** It's simpler, and the current dataset (57K points) is well within its limits. Can migrate to deck.gl later if needed for the full catalog.

### Antarctic/Arctic base maps

For EPSG:3031 (Antarctic):
- NASA GIBS: `https://gibs.earthdata.nasa.gov/wmts/epsg3031/best/...`
- Quantarctica WMS layers
- Or a simple coastline GeoJSON rendered on a blank map

For EPSG:3413 (Arctic/Greenland):
- NASA GIBS EPSG:3413 endpoint
- Same approach as Antarctic

### Data loading strategy

The store has ~57K traces (current dataset). At 100K frames this could be ~50M traces. Strategy:

1. **Current scale (<100K traces):** Load all latitude, longitude, qc_pass, and the selected variable into memory at once. Filter client-side. Fast and simple.

2. **Large scale (>1M traces):** Need chunked loading or server-side pre-processing:
   - Pre-compute spatial tiles (e.g., quadtree bins) as a separate zarr group
   - Or use zarr chunk boundaries to load data spatially
   - Or pre-generate a static GeoJSON/protobuf tileset during the pipeline run

**Start with approach 1.** Revisit when data grows.

### Variable selection

A dropdown or radio buttons to select which variable to display:
- Surface Elevation [m WGS84]
- Bed Elevation [m WGS84]
- Surface Power [dB]
- Bed Power [dB]
- Surface TWTT [s]
- Bed TWTT [s]

Changing the variable re-colors the existing points without re-fetching coordinates (they're shared across variables). Load all 6 variable arrays upfront since they're small at current scale.

### Color mapping

Use a JS colormap library (e.g., `chroma-js` or `d3-scale-chromatic`):
- Elevation variables: terrain-like colormap
- Power variables: viridis
- TWTT variables: viridis
- Percentile-based bounds (2nd–98th) to handle outliers
- Color legend/bar on the map

## Version history panel

icechunk-js exposes version information through the `Snapshot` type, which has:
- `id` — snapshot ID (Crockford Base32)
- `parentId` — parent snapshot ID (forms the commit chain)
- `message` — commit message (e.g., "Processed 104 frames (57105 traces)")
- `flushedAt` — ISO timestamp of when the commit was made
- `metadata` — key-value pairs

The store's `getSnapshot()` method returns the current snapshot. To build a commit log, walk the `parentId` chain: open the store at each parent snapshot and read its metadata.

### Version history UI

A collapsible sidebar or panel showing:
- **Commit log** — list of snapshots with timestamp, message, and abbreviated ID
- **Snapshot picker** — click a snapshot to load the store at that point in time, re-rendering the map with the data as it existed at that commit
- **Diff summary** — show the number of traces at each snapshot (read from array shape metadata) so you can see how the dataset grew over time

### Walking the commit chain

```typescript
import { IcechunkStore } from 'icechunk-js';

interface CommitEntry {
  id: string;
  parentId: string | null;
  message: string;
  timestamp: string;
}

async function getCommitLog(storeUrl: string, branch: string): Promise<CommitEntry[]> {
  const log: CommitEntry[] = [];
  let ref: string | undefined = branch;
  let snapshotId: string | undefined;

  // Walk parent chain
  while (true) {
    const store = await IcechunkStore.open(storeUrl,
      snapshotId ? { snapshot: snapshotId } : { ref: ref! }
    );
    const snapshot = store.getSnapshot();
    log.push({
      id: snapshot.id,
      parentId: snapshot.parentId ?? null,
      message: snapshot.message,
      timestamp: snapshot.flushedAt,
    });
    if (!snapshot.parentId) break;
    snapshotId = snapshot.parentId;
    ref = undefined;
  }
  return log;
}
```

### Time travel

When the user selects a historical snapshot, re-open the store at that snapshot ID and reload the data:

```typescript
// User clicks on a commit entry
async function loadAtSnapshot(storeUrl: string, snapshotId: string) {
  const store = await IcechunkStore.open(storeUrl, { snapshot: snapshotId });
  // Reload arrays from this version of the store
  const lat = await loadVariable(store, 'latitude');
  const lon = await loadVariable(store, 'longitude');
  // ... re-render map
}
```

## File structure

```
web/
  index.html
  src/
    main.ts          # entry point
    store.ts         # icechunk-js store opening, array reading
    history.ts       # commit log walking, snapshot metadata
    map.ts           # Leaflet map setup with polar projection
    colormap.ts      # color scale utilities
    ui.ts            # variable picker, version history panel
  package.json
  vite.config.ts
  tsconfig.json
```

## Implementation steps

### 1. Project setup
- `npm init` in `web/` directory
- Install: `icechunk-js`, `zarrita`, `leaflet`, `proj4`, `proj4leaflet`, `chroma-js`
- Vite + TypeScript config

### 2. Store reading (`store.ts`)
```typescript
import { IcechunkStore } from 'icechunk-js';
import * as zarr from 'zarrita';

const STORE_URL = 'https://opr-radar-metrics.s3.us-west-2.amazonaws.com/icechunk/david-drygalski/';

export async function openStore(snapshotId?: string) {
  const opts = snapshotId ? { snapshot: snapshotId } : { ref: 'main' };
  return IcechunkStore.open(STORE_URL, opts);
}

export async function loadVariable(store: IcechunkStore, name: string) {
  const arr = await zarr.open(store.resolve(name), { kind: 'array' });
  return zarr.get(arr);
}
```

Load `latitude`, `longitude`, `qc_pass` once. Load selected variable on demand (or all 6 upfront).

### 3. Version history (`history.ts`)
- Walk the parent chain from HEAD to build the commit log
- Cache the log (it only changes when the page is reloaded)
- Provide `loadAtSnapshot()` to re-open the store at a historical version

### 4. Map setup (`map.ts`)
- Configure Leaflet with EPSG:3031 CRS using proj4leaflet
- Add Antarctic coastline layer (GeoJSON or GIBS tiles)
- Create a canvas-rendered point layer for the data

### 5. Rendering and interaction
- Color points by selected variable using chroma-js scale
- Dropdown to switch variables (re-color without re-fetching)
- Color legend
- Tooltip on hover showing lat, lon, variable value, frame_id
- Loading indicator while fetching from S3
- Version history sidebar with clickable commit entries
- Selecting a commit reloads data from that snapshot

### 6. Build and deploy
- `vite build` produces static files in `web/dist/`
- Deploy to GitHub Pages via GitHub Actions:
  ```yaml
  - uses: actions/upload-pages-artifact@v3
    with:
      path: web/dist
  - uses: actions/deploy-pages@v4
  ```

## Prerequisites

- S3 bucket must have **public read access** and **CORS configured** (already documented in `docs/s3-setup.md`)
- The bucket policy and CORS are already set up for anonymous GET/HEAD from any origin

## Open questions

- **Hemisphere detection:** If the store eventually contains both Antarctic and Greenland data, need to either auto-detect hemisphere from lat/lon or let user toggle. For now, hardcode Antarctic.
- **Performance at scale:** At 50M traces, loading all data client-side won't work. Will need spatial pre-aggregation or tiling. Cross that bridge when we get there.
- **Base map tiles:** Need to pick a reliable EPSG:3031 tile source. GIBS is free but can be slow. Alternatively, render just coastlines from a static GeoJSON.
