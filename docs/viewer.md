# Interactive Viewer

The `web/` directory contains a browser-based map viewer built with Vite + TypeScript. It reads radar return statistics directly from the icechunk store over HTTP and renders them as a color-mapped Leaflet map.

## Configuration

Edit `web/src/config.ts` to change the store URL or add/remove display variables. The `STORE_URL` must point to an icechunk store accessible via HTTP range requests (e.g., an S3 bucket with CORS enabled).

## Development

```bash
cd web
npm install
npm run dev
```

Then open the local URL printed by Vite (typically `http://localhost:5173`).

## Production build

```bash
cd web
npm run build
```

Output goes to `web/dist/`. Serve it with any static file host.
