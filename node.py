import argparse
import hashlib
import os
from pathlib import Path
from typing import Dict

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

app = FastAPI(title="Vault Storage Node")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

NODE_PORT: int = 8001
STORAGE_DIR: Path = Path("storage/node_8001")
SIMULATED_OFFLINE: bool = False


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def check_online():
    if SIMULATED_OFFLINE:
        raise HTTPException(status_code=503, detail=f"Node {NODE_PORT} is currently offline")


def safe_path(filename: str) -> Path:
    clean_name = os.path.basename(filename)
    if not clean_name or clean_name in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return STORAGE_DIR / clean_name


@app.post("/store/{filename}")
async def store_blob(filename: str, request: Request):
    """Store binary blob locally in storage/node_<PORT>/ and return its SHA-256 hash."""
    check_online()
    target = safe_path(filename)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        upload = form.get("file")
        if upload is None:
            for val in form.values():
                if hasattr(val, "read"):
                    upload = val
                    break
        if upload is None or not hasattr(upload, "read"):
            raise HTTPException(status_code=400, detail="No file field found in multipart payload")
        data = await upload.read()
    else:
        data = await request.body()

    target.write_bytes(data)
    digest = compute_sha256(data)

    return {
        "status": "ok",
        "filename": target.name,
        "sha256": digest,
        "size": len(data),
        "node_port": NODE_PORT,
    }


@app.get("/retrieve/{filename}")
async def retrieve_blob(filename: str):
    """Retrieve a stored binary blob and include its current on-disk SHA-256 hash."""
    check_online()
    target = safe_path(filename)
    if not target.is_file():
        raise HTTPException(status_code=404, detail=f"Object '{filename}' not found on node {NODE_PORT}")

    data = target.read_bytes()
    digest = compute_sha256(data)

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "X-SHA256": digest,
            "X-Node-Port": str(NODE_PORT),
            "Content-Disposition": f'attachment; filename="{target.name}"',
        },
    )


@app.get("/health")
async def health_check():
    """Return node health and on-disk file SHA-256 digests."""
    check_online()
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    file_hashes: Dict[str, str] = {}
    file_sizes: Dict[str, int] = {}
    for entry in STORAGE_DIR.iterdir():
        if entry.is_file():
            try:
                raw = entry.read_bytes()
                file_hashes[entry.name] = compute_sha256(raw)
                file_sizes[entry.name] = len(raw)
            except OSError:
                continue

    return {
        "status": "ONLINE",
        "port": NODE_PORT,
        "storage_dir": str(STORAGE_DIR),
        "stored_files": sorted(file_hashes.keys()),
        "file_hashes": file_hashes,
        "file_sizes": file_sizes,
    }


@app.post("/admin/toggle")
async def toggle_node_state():
    """Toggle simulated offline/online state so users can test node failures from the dashboard."""
    global SIMULATED_OFFLINE
    SIMULATED_OFFLINE = not SIMULATED_OFFLINE
    return {
        "port": NODE_PORT,
        "status": "DEAD" if SIMULATED_OFFLINE else "ONLINE",
        "simulated_offline": SIMULATED_OFFLINE,
    }


@app.post("/admin/corrupt/{filename}")
async def corrupt_blob(filename: str):
    """Simulate silent bit-rot/corruption on disk for a stored file on this node."""
    check_online()
    target = safe_path(filename)
    if not target.is_file():
        raise HTTPException(status_code=404, detail=f"Object '{filename}' not found on node {NODE_PORT}")

    data = bytearray(target.read_bytes())
    if len(data) == 0:
        data.extend(b"CORRUPTED")
    else:
        data[0] ^= 0xFF
        data.extend(b"_CORRUPTED_BYTES")

    target.write_bytes(bytes(data))
    corrupted_hash = compute_sha256(bytes(data))
    return {
        "status": "corrupted",
        "filename": target.name,
        "node_port": NODE_PORT,
        "corrupted_sha256": corrupted_hash,
    }


@app.delete("/admin/delete/{filename}")
async def delete_blob(filename: str):
    """Delete a stored blob from this node's local storage directory."""
    target = safe_path(filename)
    if target.is_file():
        target.unlink()
        return {"status": "deleted", "filename": target.name, "node_port": NODE_PORT}
    raise HTTPException(status_code=404, detail=f"Object '{filename}' not found on node {NODE_PORT}")


def main():
    global NODE_PORT, STORAGE_DIR
    parser = argparse.ArgumentParser(description="Vault Distributed Storage Node")
    parser.add_argument("port", type=int, help="Port number to bind the storage node (e.g. 8001)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host address to bind")
    args = parser.parse_args()

    NODE_PORT = args.port
    STORAGE_DIR = Path(f"storage/node_{NODE_PORT}")
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    uvicorn.run(app, host=args.host, port=NODE_PORT, log_level="warning")


if __name__ == "__main__":
    main()
