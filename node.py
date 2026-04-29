"""
node.py — Dynamo Node (Full Implementation)

Implements all 6 Dynamo paper concepts:
  1. Consistent Hashing    — hashing.py (ConsistentHashRing)
  2. Replication           — preference list built from ring
  3. Vector Clocks         — vector_clock.py (VectorClock)
  4. Sloppy Quorum (R,W,N) — this file (PUT/GET handlers)
  5. Merkle Tree Anti-Entropy — merkle.py + background loop below
  6. Gossip Protocol       — gossip.py + background loop below

New endpoints added:
  GET  /gossip/members           → current membership table
  POST /gossip                   → receive gossip from a peer
  GET  /merkle                   → return Merkle tree summary (root + bucket hashes)
  GET  /merkle/bucket/{bucket_id}→ return all keys in a specific bucket
  GET  /metrics                  → performance metrics report
"""

import os
import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Body
from typing import Optional, Dict, Any, List
import httpx
from pydantic import BaseModel

from hashing import ConsistentHashRing
from vector_clock import VectorClock
from gossip import GossipManager
from merkle import MerkleTree, should_adopt_remote
from metrics import metrics

# ─── Configuration ────────────────────────────────────────────────────────────
NODE_ID = os.environ.get("NODE_ID", "localhost:8000")
NODES   = os.environ.get("NODES", "localhost:8000").split(",")
N       = int(os.environ.get("N", 3))
W       = int(os.environ.get("W", 2))
R       = int(os.environ.get("R", 2))
ANTI_ENTROPY_INTERVAL = float(os.environ.get("ANTI_ENTROPY_INTERVAL_SECS", 10.0))

# ─── State ────────────────────────────────────────────────────────────────────
# In-memory KV store: { key: {"value": any, "context": {node: counter}} }
store: Dict[str, Dict[str, Any]] = {}

# Consistent hash ring (seeded with static nodes; gossip refines liveness)
ring = ConsistentHashRing(nodes=NODES, replicas=3)

# Gossip manager — seeded from NODES env, then dynamic
gossip = GossipManager(node_id=NODE_ID, seed_nodes=NODES)

# Shared async HTTP client
client = httpx.AsyncClient(timeout=5.0)


# ─── Background Tasks ─────────────────────────────────────────────────────────

async def gossip_loop():
    """Run the gossip protocol forever. Wired into gossip.py."""
    while True:
        await asyncio.sleep(1.0)
        gossip._tick()
        await gossip._gossip_once()
        metrics.record_gossip_round()


async def anti_entropy_loop():
    """
    Merkle-tree-based anti-entropy loop.
    Every ANTI_ENTROPY_INTERVAL seconds:
      1. Pick a random alive peer.
      2. Compare Merkle tree summaries.
      3. Sync differing buckets key-by-key.
    """
    await asyncio.sleep(5.0)  # let cluster stabilize first
    while True:
        await asyncio.sleep(ANTI_ENTROPY_INTERVAL)
        await run_anti_entropy_round()


async def run_anti_entropy_round():
    peers = gossip.get_alive_nodes()
    if not peers:
        return

    import random
    peer = random.choice(peers)

    try:
        # Step 1: Fetch peer's Merkle summary
        resp = await client.get(f"http://{peer}/merkle")
        if resp.status_code != 200:
            return
        peer_summary = resp.json()
        peer_bucket_hashes = peer_summary.get("bucket_hashes", [])

        # Step 2: Build our Merkle tree and compare
        our_tree = MerkleTree(store)
        if our_tree.root == peer_summary.get("root"):
            return  # Trees match — nothing to do

        differing = our_tree.differing_buckets(peer_bucket_hashes)
        if not differing:
            return

        keys_synced = 0

        # Step 3: For each differing bucket, fetch peer's keys and sync
        for bucket_id in differing:
            resp2 = await client.get(f"http://{peer}/merkle/bucket/{bucket_id}")
            if resp2.status_code != 200:
                continue
            peer_bucket = resp2.json()  # { key: {value, context} }

            for key, remote_entry in peer_bucket.items():
                local_entry = store.get(key)
                if should_adopt_remote(local_entry, remote_entry):
                    store[key] = remote_entry
                    keys_synced += 1
                    print(f"[{NODE_ID}][anti-entropy] Synced key '{key}' from {peer}")

        metrics.record_anti_entropy(
            keys_synced=keys_synced,
            buckets_differed=len(differing),
        )

        if keys_synced:
            print(f"[{NODE_ID}][anti-entropy] Round complete — {keys_synced} keys synced from {peer}")

    except Exception as e:
        print(f"[{NODE_ID}][anti-entropy] Error during round with {peer}: {e}")


# ─── App Lifecycle ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background tasks on startup
    gossip_task        = asyncio.create_task(gossip_loop())
    anti_entropy_task  = asyncio.create_task(anti_entropy_loop())
    yield
    # Cleanup on shutdown
    gossip_task.cancel()
    anti_entropy_task.cancel()


app = FastAPI(lifespan=lifespan)


# ─── Models ───────────────────────────────────────────────────────────────────

class KVItem(BaseModel):
    value: Any
    context: Optional[Dict[str, int]] = None
    is_replica_request: bool = False


# ─── KV Endpoints ─────────────────────────────────────────────────────────────

@app.put("/kv/{key}")
async def put_key(key: str, item: KVItem):
    # ── Replica path: just write and return ──────────────────────────────────
    if item.is_replica_request:
        store[key] = {"value": item.value, "context": item.context}
        print(f"[{NODE_ID}] Replica wrote key: {key}")
        return {"status": "success", "node": NODE_ID}

    # ── Coordinator path ─────────────────────────────────────────────────────
    t_start = time.perf_counter()
    print(f"[{NODE_ID}] Coordinator received PUT for key: {key}")

    preference_list = ring.get_preference_list(key, N)
    # Filter to alive/suspected nodes via gossip (prefer alive)
    preference_list = _filter_by_gossip(preference_list)
    print(f"[{NODE_ID}] Preference list for {key}: {preference_list}")

    new_context = VectorClock.increment(item.context, NODE_ID)

    payload = {
        "value": item.value,
        "context": new_context,
        "is_replica_request": True,
    }

    success_count = 0
    tasks = []

    for target_node in preference_list:
        if target_node == NODE_ID:
            store[key] = {"value": item.value, "context": new_context}
            success_count += 1
            print(f"[{NODE_ID}] Coordinator wrote locally")
        else:
            tasks.append(send_replica_write(target_node, key, payload))

    if tasks:
        for coro in asyncio.as_completed(tasks):
            try:
                ok = await coro
                if ok:
                    success_count += 1
                else:
                    metrics.record_replica_failure()
                if success_count >= W:
                    break
            except Exception as e:
                metrics.record_replica_failure()
                print(f"[{NODE_ID}] Error in remote write: {e}")

    elapsed_ms = (time.perf_counter() - t_start) * 1000
    succeeded = success_count >= W
    metrics.record_put(elapsed_ms, succeeded)

    if succeeded:
        return {
            "status": "success",
            "message": f"Wrote to {success_count}/{N} nodes",
            "context": new_context,
        }
    raise HTTPException(
        status_code=503,
        detail=f"Failed to reach write quorum W={W}. Reached {success_count} nodes.",
    )


@app.get("/kv/{key}")
async def get_key(key: str, is_replica_request: bool = False):
    # ── Replica path ─────────────────────────────────────────────────────────
    if is_replica_request:
        if key in store:
            print(f"[{NODE_ID}] Replica returning key: {key}")
            return store[key]
        raise HTTPException(status_code=404, detail="Key not found")

    # ── Coordinator path ─────────────────────────────────────────────────────
    t_start = time.perf_counter()
    print(f"[{NODE_ID}] Coordinator received GET for key: {key}")

    preference_list = ring.get_preference_list(key, N)
    preference_list = _filter_by_gossip(preference_list)

    success_count = 0
    tasks = []
    responses = []

    for target_node in preference_list:
        if target_node == NODE_ID:
            if key in store:
                responses.append(store[key])
                success_count += 1
                print(f"[{NODE_ID}] Coordinator read locally")
        else:
            tasks.append(send_replica_read(target_node, key))

    if tasks:
        for coro in asyncio.as_completed(tasks):
            try:
                res = await coro
                if res:
                    responses.append(res)
                    success_count += 1
                if success_count >= R:
                    break
            except Exception as e:
                print(f"[{NODE_ID}] Error in remote read: {e}")

    elapsed_ms = (time.perf_counter() - t_start) * 1000

    if success_count < R:
        metrics.record_get(elapsed_ms, success=False)
        raise HTTPException(
            status_code=503,
            detail=f"Failed to reach read quorum R={R}. Reached {success_count} nodes.",
        )

    if not responses:
        metrics.record_get(elapsed_ms, success=False)
        raise HTTPException(status_code=404, detail="Key not found")

    # ── Conflict resolution via vector clocks ─────────────────────────────────
    latest = responses[0]
    conflicts = []
    for r in responses[1:]:
        if VectorClock.is_conflict(latest.get("context"), r.get("context")):
            conflicts.append(r)
        else:
            merged_ctx = VectorClock.merge(latest.get("context"), r.get("context"))
            if merged_ctx == r.get("context") and merged_ctx != latest.get("context"):
                latest = r

    has_conflict = bool(conflicts)
    metrics.record_get(elapsed_ms, success=True, conflict=has_conflict)

    if has_conflict:
        return {
            "status": "conflict",
            "versions": [latest] + conflicts,
            "message": "Multiple concurrent versions found — client must reconcile",
        }

    return {
        "status": "success",
        "value": latest.get("value"),
        "context": latest.get("context"),
    }


# ─── Gossip Endpoints ─────────────────────────────────────────────────────────

@app.post("/gossip")
async def receive_gossip(payload: Dict[str, Any] = Body(...)):
    """Receive a gossip message from a peer and merge membership tables."""
    gossip.merge(payload)
    return {"status": "ok"}


@app.get("/gossip/members")
async def get_members():
    """Return current membership table with liveness status."""
    return {
        "node": NODE_ID,
        "members": gossip.get_members(),
        "alive_count": len(gossip.get_alive_nodes()) + 1,  # +1 for self
    }


# ─── Merkle / Anti-Entropy Endpoints ─────────────────────────────────────────

@app.get("/merkle")
async def get_merkle():
    """Return this node's Merkle tree summary (root + bucket hashes)."""
    tree = MerkleTree(store)
    return {
        "node": NODE_ID,
        "store_size": len(store),
        **tree.to_summary(),
    }


@app.get("/merkle/bucket/{bucket_id}")
async def get_merkle_bucket(bucket_id: int):
    """Return all KV entries in the given bucket for anti-entropy sync."""
    tree = MerkleTree(store)
    bucket_data = tree.get_bucket_keys(bucket_id)
    return bucket_data


# ─── Metrics Endpoint ─────────────────────────────────────────────────────────

@app.get("/metrics")
async def get_metrics():
    """Full performance metrics report for this node."""
    report = metrics.report()
    report["node"] = NODE_ID
    report["store_size"] = len(store)
    report["gossip_members"] = gossip.get_members()
    return report


# ─── Health Check ────────────────────────────────────────────────────────────

@app.get("/")
def health_check():
    return {
        "status": "ok",
        "node": NODE_ID,
        "store_size": len(store),
        "ring_size": len(ring.ring),
        "alive_peers": gossip.get_alive_nodes(),
        "uptime_seconds": metrics.uptime_seconds,
    }


# ─── Internal Helpers ─────────────────────────────────────────────────────────

def _filter_by_gossip(preference_list: List[str]) -> List[str]:
    """
    Re-order preference list: alive nodes first, suspected nodes after, dead last.
    This implements the sloppy quorum — we try to use healthy nodes but won't
    block on dead ones.
    """
    member_info = gossip.get_members()

    def priority(node_id: str) -> int:
        status = member_info.get(node_id, {}).get("status", "alive")
        if node_id == NODE_ID:
            return 0  # self is always first priority
        return {"alive": 1, "suspected": 2, "dead": 3}.get(status, 1)

    return sorted(preference_list, key=priority)


async def send_replica_write(node: str, key: str, payload: dict) -> bool:
    try:
        response = await client.put(f"http://{node}/kv/{key}", json=payload)
        return response.status_code == 200
    except Exception:
        return False


async def send_replica_read(node: str, key: str):
    try:
        response = await client.get(f"http://{node}/kv/{key}?is_replica_request=true")
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None
