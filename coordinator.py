import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

NODE_PORTS: List[int] = [8001, 8002, 8003]
NODES: Dict[int, str] = {port: f"http://127.0.0.1:{port}" for port in NODE_PORTS}

# In-memory dictionary tracking file metadata and replica node ports:
# { filename: { "filename": str, "sha256": str, "size": int, "replicas": List[int], "uploaded_at": str } }
FILE_REGISTRY: Dict[str, Dict[str, Any]] = {}

# Recent cluster activity log for real-time visibility in the dashboard
EVENT_LOG: List[Dict[str, str]] = []


def log_event(level: str, message: str):
    EVENT_LOG.insert(
        0,
        {
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "message": message,
        },
    )
    del EVENT_LOG[40:]


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def probe_node_health(client: httpx.AsyncClient, port: int) -> Tuple[int, Dict[str, Any]]:
    url = f"{NODES[port]}/health"
    try:
        resp = await client.get(url, timeout=1.0)
        if resp.status_code == 200:
            payload = resp.json()
            return port, {
                "port": port,
                "url": NODES[port],
                "status": "ONLINE",
                "stored_files": payload.get("stored_files", []),
                "file_hashes": payload.get("file_hashes", {}),
                "file_sizes": payload.get("file_sizes", {}),
            }
    except Exception:
        pass

    return port, {
        "port": port,
        "url": NODES[port],
        "status": "DEAD",
        "stored_files": [],
        "file_hashes": {},
        "file_sizes": {},
    }


async def store_on_node(
    client: httpx.AsyncClient, port: int, filename: str, data: bytes, expected_sha256: str
) -> Tuple[int, bool, Optional[str]]:
    url = f"{NODES[port]}/store/{quote(filename)}"
    try:
        resp = await client.post(
            url,
            content=data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=5.0,
        )
        if resp.status_code == 200:
            body = resp.json()
            node_sha256 = body.get("sha256")
            if node_sha256 == expected_sha256:
                return port, True, None
            return port, False, f"SHA-256 mismatch on node {port}: expected {expected_sha256}, got {node_sha256}"
        return port, False, f"HTTP {resp.status_code} from node {port}"
    except Exception as exc:
        return port, False, str(exc)


async def fetch_verified_from_node(
    client: httpx.AsyncClient, port: int, filename: str, expected_sha256: str
) -> Optional[bytes]:
    url = f"{NODES[port]}/retrieve/{quote(filename)}"
    try:
        resp = await client.get(url, timeout=5.0)
        if resp.status_code == 200:
            actual_sha256 = compute_sha256(resp.content)
            if actual_sha256 == expected_sha256:
                return resp.content
    except Exception:
        pass
    return None


async def sync_or_seed_initial_data():
    """Sync existing files on storage nodes or seed a welcome object so the cluster is immediately usable."""
    await asyncio.sleep(0.6)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*(probe_node_health(client, p) for p in NODE_PORTS))
        online = [p for p, info in results if info["status"] == "ONLINE"]

        # Reconstruct registry from existing node files if any exist on disk
        for port, info in results:
            if info["status"] != "ONLINE":
                continue
            for fname, fhash in info["file_hashes"].items():
                fsize = info.get("file_sizes", {}).get(fname, 0)
                if fname not in FILE_REGISTRY:
                    FILE_REGISTRY[fname] = {
                        "filename": fname,
                        "sha256": fhash,
                        "size": fsize,
                        "replicas": [port],
                        "uploaded_at": datetime.now(timezone.utc).isoformat(),
                    }
                elif port not in FILE_REGISTRY[fname]["replicas"]:
                    FILE_REGISTRY[fname]["replicas"].append(port)
                    FILE_REGISTRY[fname]["replicas"].sort()

        if not FILE_REGISTRY and online:
            sample_name = "vault-architecture.txt"
            sample_bytes = (
                b"Vault Distributed Object Storage\n"
                b"================================\n"
                b"Replication Factor: 3 (Nodes 8001, 8002, 8003)\n"
                b"Integrity Verification: SHA-256 content-addressable verification\n"
                b"Self-Healing: Automatic replica reconstruction via Coordinator\n"
            )
            sample_sha = compute_sha256(sample_bytes)
            store_res = await asyncio.gather(
                *(store_on_node(client, p, sample_name, sample_bytes, sample_sha) for p in online)
            )
            verified = [p for p, ok, _ in store_res if ok]
            if verified:
                FILE_REGISTRY[sample_name] = {
                    "filename": sample_name,
                    "sha256": sample_sha,
                    "size": len(sample_bytes),
                    "replicas": sorted(verified),
                    "uploaded_at": datetime.now(timezone.utc).isoformat(),
                }
                log_event(
                    "SUCCESS",
                    f"Cluster initialized. Seeded '{sample_name}' across nodes {verified} (SHA-256: {sample_sha[:12]}...).",
                )
        else:
            log_event("INFO", f"Coordinator connected to storage nodes {online}.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(sync_or_seed_initial_data())
    yield


app = FastAPI(title="Vault Coordinator", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def serve_dashboard():
    """Serve the static HTML dashboard from static/index.html."""
    index_path = STATIC_DIR / "index.html"
    if not index_path.is_file():
        raise HTTPException(status_code=404, detail="Dashboard static/index.html not found")
    return FileResponse(index_path)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Write uploaded file concurrently to all active nodes and verify SHA-256 checksums."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    clean_name = os.path.basename(file.filename)
    if not clean_name or clean_name in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename")

    data = await file.read()
    source_sha256 = compute_sha256(data)

    async with httpx.AsyncClient() as client:
        health_results = await asyncio.gather(*(probe_node_health(client, p) for p in NODE_PORTS))
        active_ports = [port for port, info in health_results if info["status"] == "ONLINE"]

        if not active_ports:
            log_event("ERROR", f"Upload of '{clean_name}' failed: all storage nodes are DEAD.")
            raise HTTPException(status_code=503, detail="No active storage nodes available")

        store_results = await asyncio.gather(
            *(store_on_node(client, port, clean_name, data, source_sha256) for port in active_ports)
        )

    verified_replicas = [port for port, ok, _ in store_results if ok]
    errors = {port: err for port, ok, err in store_results if not ok and err}

    if not verified_replicas:
        log_event("ERROR", f"Upload of '{clean_name}' failed hash verification on all active nodes.")
        raise HTTPException(
            status_code=502,
            detail={"message": "Failed to store verified replica on any active node", "errors": errors},
        )

    record = {
        "filename": clean_name,
        "sha256": source_sha256,
        "size": len(data),
        "replicas": sorted(verified_replicas),
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    FILE_REGISTRY[clean_name] = record

    log_event(
        "SUCCESS",
        f"Uploaded '{clean_name}' ({len(data)} B, SHA-256: {source_sha256[:12]}...) to nodes {record['replicas']}.",
    )

    return {
        "status": "uploaded",
        "filename": clean_name,
        "sha256": source_sha256,
        "size": len(data),
        "replicas": record["replicas"],
        "active_replica_count": len(record["replicas"]),
        "total_nodes": len(NODE_PORTS),
        "errors": errors,
    }


@app.get("/download/{filename}")
async def download_file(filename: str):
    """Iterate through replica nodes and return the first HTTP 200 copy with a valid SHA-256 checksum."""
    clean_name = os.path.basename(filename)
    meta = FILE_REGISTRY.get(clean_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"File '{clean_name}' not found in coordinator registry")

    expected_sha256 = meta["sha256"]
    candidate_ports: List[int] = list(meta["replicas"]) + [
        p for p in NODE_PORTS if p not in meta["replicas"]
    ]

    failed_nodes: List[Dict[str, Any]] = []

    async with httpx.AsyncClient() as client:
        for port in candidate_ports:
            url = f"{NODES[port]}/retrieve/{quote(clean_name)}"
            try:
                resp = await client.get(url, timeout=3.0)
                if resp.status_code != 200:
                    reason = f"HTTP {resp.status_code}"
                    failed_nodes.append({"port": port, "reason": reason})
                    log_event(
                        "WARN",
                        f"Download '{clean_name}': Node :{port} returned {reason}, failing over to next node...",
                    )
                    continue

                actual_sha256 = compute_sha256(resp.content)
                if actual_sha256 != expected_sha256:
                    if port in meta["replicas"]:
                        meta["replicas"].remove(port)
                    reason = f"SHA-256 mismatch (expected {expected_sha256[:10]}..., got {actual_sha256[:10]}...)"
                    failed_nodes.append({"port": port, "reason": reason})
                    log_event(
                        "WARN",
                        f"Corruption detected on Node :{port} for '{clean_name}' ({reason})! Automatic failover triggered.",
                    )
                    continue

                if port not in meta["replicas"]:
                    meta["replicas"].append(port)
                    meta["replicas"].sort()

                if failed_nodes:
                    log_event(
                        "SUCCESS",
                        f"Failover succeeded for '{clean_name}': served verified replica from Node :{port} after {len(failed_nodes)} failed attempt(s).",
                    )
                else:
                    log_event(
                        "INFO",
                        f"Served verified download of '{clean_name}' from Node :{port} (SHA-256 verified).",
                    )

                return Response(
                    content=resp.content,
                    media_type="application/octet-stream",
                    headers={
                        "Content-Disposition": f'attachment; filename="{clean_name}"',
                        "X-SHA256": actual_sha256,
                        "X-Served-By-Node": str(port),
                    },
                )
            except Exception as exc:
                failed_nodes.append({"port": port, "reason": str(exc)})
                log_event(
                    "WARN",
                    f"Download '{clean_name}': Node :{port} unreachable, failing over to next node...",
                )
                continue

    log_event("ERROR", f"Download failed for '{clean_name}': all replicas unreachable or corrupted.")
    raise HTTPException(
        status_code=502,
        detail={
            "message": f"All nodes failed or returned corrupted data for '{clean_name}'",
            "failures": failed_nodes,
        },
    )


@app.post("/repair/{filename}")
async def repair_file(filename: str):
    """Retrieve a verified healthy copy and replicate it to any active node missing or corrupting the file."""
    clean_name = os.path.basename(filename)
    meta = FILE_REGISTRY.get(clean_name)
    if not meta:
        raise HTTPException(status_code=404, detail=f"File '{clean_name}' not found in coordinator registry")

    expected_sha256 = meta["sha256"]

    async with httpx.AsyncClient() as client:
        health_results = await asyncio.gather(*(probe_node_health(client, p) for p in NODE_PORTS))
        online_ports = [port for port, info in health_results if info["status"] == "ONLINE"]

        if not online_ports:
            log_event("ERROR", f"Self-heal failed for '{clean_name}': no online nodes available.")
            raise HTTPException(status_code=503, detail="No online storage nodes available for repair")

        healthy_bytes: Optional[bytes] = None
        source_node: Optional[int] = None
        verified_online_ports: List[int] = []
        ports_needing_repair: List[int] = []

        ordered_online = sorted(online_ports, key=lambda p: (0 if p in meta["replicas"] else 1, p))

        for port in ordered_online:
            blob = await fetch_verified_from_node(client, port, clean_name, expected_sha256)
            if blob is not None:
                verified_online_ports.append(port)
                if healthy_bytes is None:
                    healthy_bytes = blob
                    source_node = port
            else:
                ports_needing_repair.append(port)

        if healthy_bytes is None:
            log_event(
                "ERROR",
                f"Self-heal failed for '{clean_name}': no online node holds a valid copy matching {expected_sha256[:12]}...",
            )
            raise HTTPException(
                status_code=502,
                detail=f"Cannot repair '{clean_name}': no online node holds a verified copy matching SHA-256 {expected_sha256}",
            )

        repaired_ports: List[int] = []
        repair_errors: Dict[int, str] = {}

        if ports_needing_repair:
            repair_results = await asyncio.gather(
                *(
                    store_on_node(client, port, clean_name, healthy_bytes, expected_sha256)
                    for port in ports_needing_repair
                )
            )
            for port, ok, err in repair_results:
                if ok:
                    repaired_ports.append(port)
                    verified_online_ports.append(port)
                elif err:
                    repair_errors[port] = err

        meta["replicas"] = sorted(set(verified_online_ports))

    if repaired_ports:
        log_event(
            "SUCCESS",
            f"Self-healed '{clean_name}' from Node :{source_node} -> replicated verified copy to Node(s) {repaired_ports}.",
        )
    else:
        log_event(
            "INFO",
            f"Self-heal check complete for '{clean_name}': all online nodes {verified_online_ports} already hold verified copies.",
        )

    return {
        "status": "repaired",
        "filename": clean_name,
        "sha256": expected_sha256,
        "source_node": source_node,
        "repaired_nodes": sorted(repaired_ports),
        "active_replicas": meta["replicas"],
        "active_replica_count": len(meta["replicas"]),
        "total_nodes": len(NODE_PORTS),
        "errors": repair_errors,
    }


@app.post("/node/{port}/toggle")
async def toggle_node(port: int):
    """Proxy endpoint for the dashboard to simulate bringing a storage node offline or back online."""
    if port not in NODES:
        raise HTTPException(status_code=404, detail=f"Unknown node port {port}")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"{NODES[port]}/admin/toggle", timeout=2.0)
            payload = resp.json()
            state = payload.get("status", "UNKNOWN")
            log_event(
                "WARN" if state == "DEAD" else "SUCCESS",
                f"Node :{port} state switched to {state}.",
            )
            return payload
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Could not reach node {port}: {exc}")


@app.post("/node/{port}/corrupt/{filename}")
async def corrupt_file_on_node(port: int, filename: str):
    """Simulate bit-rot corruption on a specific storage node for testing automatic failover and self-healing."""
    if port not in NODES:
        raise HTTPException(status_code=404, detail=f"Unknown node port {port}")
    clean_name = os.path.basename(filename)
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{NODES[port]}/admin/corrupt/{quote(clean_name)}", timeout=2.0)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        payload = resp.json()
        log_event(
            "WARN",
            f"Injected byte corruption into '{clean_name}' on Node :{port} (new hash: {payload['corrupted_sha256'][:10]}...).",
        )
        return payload


@app.delete("/node/{port}/file/{filename}")
async def delete_file_on_node(port: int, filename: str):
    """Delete a file replica from a single storage node to test under-replication and self-healing."""
    if port not in NODES:
        raise HTTPException(status_code=404, detail=f"Unknown node port {port}")
    clean_name = os.path.basename(filename)
    async with httpx.AsyncClient() as client:
        resp = await client.delete(f"{NODES[port]}/admin/delete/{quote(clean_name)}", timeout=2.0)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        if clean_name in FILE_REGISTRY and port in FILE_REGISTRY[clean_name]["replicas"]:
            FILE_REGISTRY[clean_name]["replicas"].remove(port)
        log_event("WARN", f"Deleted replica of '{clean_name}' from Node :{port}.")
        return resp.json()


@app.get("/status")
async def cluster_status():
    """Query every node's /health endpoint with a 1-second timeout and return node health + file metadata."""
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*(probe_node_health(client, p) for p in NODE_PORTS))

    node_map: Dict[int, Dict[str, Any]] = dict(results)
    nodes_list: List[Dict[str, Any]] = []
    for port in NODE_PORTS:
        info = node_map[port]
        nodes_list.append(
            {
                "port": port,
                "name": f"node_{port}",
                "url": info["url"],
                "status": info["status"],
                "stored_files": info["stored_files"],
                "stored_files_count": len(info["stored_files"]),
            }
        )

    files_list: List[Dict[str, Any]] = []
    for filename, meta in FILE_REGISTRY.items():
        expected_sha256 = meta["sha256"]
        active_replicas: List[int] = []
        corrupted_replicas: List[int] = []
        node_states: Dict[int, str] = {}

        for port in NODE_PORTS:
            node_info = node_map.get(port)
            if not node_info or node_info["status"] != "ONLINE":
                node_states[port] = "OFFLINE"
                continue

            on_disk_hash = node_info["file_hashes"].get(filename)
            if on_disk_hash is None:
                node_states[port] = "MISSING"
                if port in meta["replicas"]:
                    meta["replicas"].remove(port)
            elif on_disk_hash == expected_sha256:
                node_states[port] = "HEALTHY"
                active_replicas.append(port)
                if port not in meta["replicas"]:
                    meta["replicas"].append(port)
                    meta["replicas"].sort()
            else:
                node_states[port] = "CORRUPTED"
                corrupted_replicas.append(port)

        files_list.append(
            {
                "filename": filename,
                "sha256": expected_sha256,
                "size": meta["size"],
                "uploaded_at": meta["uploaded_at"],
                "replicas": sorted(meta["replicas"]),
                "active_replicas": sorted(active_replicas),
                "corrupted_replicas": sorted(corrupted_replicas),
                "node_states": node_states,
                "active_replica_count": len(active_replicas),
                "total_nodes": len(NODE_PORTS),
            }
        )

    return {
        "coordinator": "ONLINE",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "nodes": nodes_list,
        "files": files_list,
        "events": EVENT_LOG[:15],
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
