# Vault — Distributed Object Storage System

A minimal, single-machine distributed object storage system with SHA-256 integrity verification, automatic failover, self-healing replication, and a real-time web dashboard built with Python and FastAPI.

## Architecture

- **Storage Nodes (`node.py`)**:
  - Run on configurable ports (`python node.py <PORT>`, defaults: `8001`, `8002`, `8003`).
  - Persist binary blobs locally in `storage/node_<PORT>/`.
  - Expose `POST /store/{filename}`, `GET /retrieve/{filename}`, and `GET /health`.
  - Compute and return SHA-256 checksums for stored and retrieved objects.
- **Coordinator (`coordinator.py`)**:
  - Routes requests across storage nodes (`8001`, `8002`, `8003`).
  - `POST /upload`: Writes files concurrently to all active nodes and verifies returned SHA-256 hashes match the source digest.
  - `GET /download/{filename}`: Iterates through replica nodes and serves the first HTTP 200 copy with a verified SHA-256 checksum, automatically failing over if a node is offline or returns corrupted bytes.
  - `POST /repair/{filename}`: Fetches a verified healthy replica and replicates it to any active node missing or holding a corrupted copy of the file.
  - `GET /status`: Queries every node's `/health` endpoint with a 1-second timeout and returns cluster health and replica metadata.
  - `GET /`: Serves the real-time dark-mode web dashboard (`static/index.html`).

## Quick Start

```bash
chmod +x run.sh
./run.sh
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000) in your browser.
