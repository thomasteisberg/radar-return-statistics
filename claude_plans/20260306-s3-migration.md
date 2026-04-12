# Plan: Migrate icechunk store to AWS S3

Status: complete (code changes done, full pipeline run to S3 in progress)

## Overview

Move the icechunk store from local filesystem to S3 so it can be shared across machines, used in CI, and accessed by the planned GitHub Pages visualization (icechunk-js).

## Code changes

### 1. Update `config.yaml` store section

```yaml
store:
  backend: "s3"            # "local" or "s3"
  # Local settings (used when backend: local)
  path: "outputs/icechunk_store"
  # S3 settings (used when backend: s3)
  s3_bucket: "opr-radar-return-statistics"
  s3_prefix: "icechunk/david-drygalski"
  s3_region: "us-west-2"
```

### 2. Update `store.py` — `open_or_create_repo()`

Replace `local_filesystem_storage` with a factory that picks backend based on config:

```python
def _make_storage(store_config: dict):
    backend = store_config.get("backend", "local")
    if backend == "s3":
        return icechunk.s3_storage(
            bucket=store_config["s3_bucket"],
            prefix=store_config.get("s3_prefix"),
            region=store_config.get("s3_region"),
            from_env=True,
        )
    else:
        return icechunk.local_filesystem_storage(str(store_config["path"]))
```

Key: use `from_env=True` so credentials come from standard AWS credential chain (env vars, ~/.aws/credentials, IAM role, etc).

### 3. Update `open_or_create_repo` signature

Currently takes a `path` string. Change to take the full `store_config` dict so it can pick backend.

Callers to update:
- `runner.py` — `run()` function
- `visualize_frame.py` — `load_frame_data()` (currently takes `store_path` string)
- `visualize_map.py` — `load_all_data()` (currently takes `store_path` string)

For the visualization scripts, the simplest approach: accept either a local path or an `s3://bucket/prefix` URI and parse accordingly. Alternatively, accept a config file path.

### 4. Update `config.py` defaults

Add defaults for new store fields:
```python
config["store"].setdefault("backend", "local")
```

### 5. Read-only access for visualizations

`visualize_frame.py` and `visualize_map.py` currently open the store directly with zarr. They should use the same `_make_storage()` helper, with `anonymous=True` option for public read access (if the bucket is configured for it).

## AWS setup required

See `docs/s3-setup.md` for full details (created alongside this plan).

## Testing

1. Create bucket, configure IAM
2. Run pipeline with `backend: s3` config
3. Verify visualizations can read from S3
4. Test GitHub Actions workflow with OIDC credentials
5. Test anonymous read access for icechunk-js
