import os
import asyncio
from fastapi import FastAPI, Request, HTTPException, Body
from typing import Optional, Dict, Any, List
import httpx
from pydantic import BaseModel

from hashing import ConsistentHashRing
from vector_clock import VectorClock

app = FastAPI()

NODE_ID = os.environ.get("NODE_ID", "localhost:8000")
NODES = os.environ.get("NODES", "localhost:8000").split(",")
N = int(os.environ.get("N", 3))
W = int(os.environ.get("W", 2))
R = int(os.environ.get("R", 2))

# In-memory storage: {key: {"value": ..., "context": {...}}}
store = {}

# Initialize hashing ring
ring = ConsistentHashRing(nodes=NODES, replicas=3)
client = httpx.AsyncClient(timeout=5.0)

class KVItem(BaseModel):
    value: Any
    context: Optional[Dict[str, int]] = None
    is_replica_request: bool = False

@app.put("/kv/{key}")
async def put_key(key: str, item: KVItem):
    # If this is a direct request to a replica from a coordinator
    if item.is_replica_request:
        store[key] = {
            "value": item.value,
            "context": item.context
        }
        print(f"[{NODE_ID}] Replica wrote key: {key}")
        return {"status": "success", "node": NODE_ID}

    # Otherwise, this node acts as a coordinator
    print(f"[{NODE_ID}] Coordinator received PUT for key: {key}")
    preference_list = ring.get_preference_list(key, N)
    print(f"[{NODE_ID}] Preference list for {key}: {preference_list}")
    
    # Increment the vector clock for the coordinator
    new_context = VectorClock.increment(item.context, NODE_ID)
    
    # Send replication requests
    success_count = 0
    tasks = []
    
    payload = {
        "value": item.value,
        "context": new_context,
        "is_replica_request": True
    }
    
    for target_node in preference_list:
        if target_node == NODE_ID:
            # Write locally
            store[key] = {"value": item.value, "context": new_context}
            success_count += 1
            print(f"[{NODE_ID}] Coordinator wrote locally")
        else:
            # Write remotely
            tasks.append(send_replica_write(target_node, key, payload))

    # Wait for remaining tasks to fulfill W
    if tasks:
        for coro in asyncio.as_completed(tasks):
            try:
                res = await coro
                if res:
                    success_count += 1
                if success_count >= W:
                    break
            except Exception as e:
                print(f"[{NODE_ID}] Error in remote write: {e}")

    if success_count >= W:
        return {"status": "success", "message": f"Wrote to {success_count} nodes", "context": new_context}
    else:
        raise HTTPException(status_code=503, detail=f"Failed to reach write quorum. Reached {success_count}/{W}")

async def send_replica_write(node, key, payload):
    try:
        response = await client.put(f"http://{node}/kv/{key}", json=payload)
        return response.status_code == 200
    except Exception:
        return False

@app.get("/kv/{key}")
async def get_key(key: str, is_replica_request: bool = False):
    if is_replica_request:
        if key in store:
            print(f"[{NODE_ID}] Replica returning key: {key}")
            return store[key]
        raise HTTPException(status_code=404, detail="Key not found")

    print(f"[{NODE_ID}] Coordinator received GET for key: {key}")
    preference_list = ring.get_preference_list(key, N)
    
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

    if success_count < R:
        raise HTTPException(status_code=503, detail=f"Failed to reach read quorum. Reached {success_count}/{R}")
        
    if not responses:
        raise HTTPException(status_code=404, detail="Key not found")

    # Resolve conflicts
    latest = responses[0]
    conflicts = []
    for r in responses[1:]:
        if VectorClock.is_conflict(latest.get("context"), r.get("context")):
            conflicts.append(r)
        else:
            merged_ctx = VectorClock.merge(latest.get("context"), r.get("context"))
            if merged_ctx == r.get("context") and merged_ctx != latest.get("context"):
                latest = r
                
    if conflicts:
        return {
            "status": "conflict", 
            "versions": [latest] + conflicts, 
            "message": "Multiple conflicting versions found"
        }
        
    return {
        "status": "success",
        "value": latest.get("value"),
        "context": latest.get("context")
    }

async def send_replica_read(node, key):
    try:
        response = await client.get(f"http://{node}/kv/{key}?is_replica_request=true")
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None

@app.get("/")
def health_check():
    return {"status": "ok", "node": NODE_ID, "ring_size": len(ring.ring)}
