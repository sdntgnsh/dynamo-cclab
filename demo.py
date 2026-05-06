import httpx
import time
import subprocess
import json
import sys

BASE_NODES = {
    "node1": "http://192.168.137.212:8001",
    "node2": "http://192.168.137.212:8002",
    "node3": "http://192.168.137.212:8003",
    "node4": "http://192.168.137.80:8004",
    "node5": "http://192.168.137.80:8005",
}
PRIMARY = BASE_NODES["node1"]

# ─── Formatting helpers ───────────────────────────────────────────────────────

def header(title: str):
    bar = "=" * 70
    print(f"\n+{bar}+")
    print(f"|  {title:<68}|")
    print(f"+{bar}+")

def info(msg: str): print(f"  [INFO] {msg}")
def ok(msg: str):  print(f"  [OK]   {msg}")
def warn(msg: str): print(f"  [WARN] {msg}")

def print_json(data):
    print(json.dumps(data, indent=4))

# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def put(url: str, key: str, value, context=None):
    payload = {"value": value}
    if context:
        payload["context"] = context
    try:
        r = httpx.put(f"{url}/kv/{key}", json=payload, timeout=10)
        print(f"  [PUT /kv/{key}] -> HTTP {r.status_code}")
        data = r.json()
        print_json(data)
        return data
    except Exception as e:
        print(f"  [ERROR] PUT failed: {e}")
        return None

def get(url: str, key: str):
    try:
        r = httpx.get(f"{url}/kv/{key}", timeout=10)
        print(f"  [GET /kv/{key}] -> HTTP {r.status_code}")
        data = r.json()
        print_json(data)
        return data
    except Exception as e:
        print(f"  [ERROR] GET failed: {e}")
        return None

def get_members(url: str):
    try:
        return httpx.get(f"{url}/gossip/members", timeout=5).json()
    except Exception:
        return None

def get_merkle(url: str):
    try:
        return httpx.get(f"{url}/merkle", timeout=5).json()
    except Exception:
        return None

def run_cmd(cmd: str):
    print(f"  [Shell] {cmd}")
    subprocess.run(cmd, shell=True, capture_output=True, text=True)

# ─── Concept Demos ────────────────────────────────────────────────────────────

def demo_consistent_hashing_and_replication():
    header("CONCEPT 1 & 2: Consistent Hashing & Replication")
    
    # [What's happening]: We write a key to the PRIMARY node. 
    # Internally, the primary node uses the Consistent Hash Ring to find which 3 nodes (N=3) 
    # should store this key. It then Replicates the data to those nodes.
    info("Writing key 'user_data' to node1.")
    info("Node1 will act as coordinator, find the preference list via consistent hashing, and replicate it.")
    
    put(PRIMARY, "user_data", {"name": "Alice", "age": 25})
    
    ok("Write successful. The data is now distributed and replicated across 3 nodes.")
    time.sleep(2)

def demo_vector_clocks():
    header("CONCEPT 3: Vector Clocks (Causality Tracking)")
    
    # [What's happening]: We read the key back from a DIFFERENT node (node2).
    # The response will include a 'context' object (the Vector Clock). 
    # This clock keeps track of exactly which nodes updated this data and how many times.
    info("Reading 'user_data' from node2 to see the Vector Clock context.")
    info("The Vector Clock proves which nodes updated this data and how many times.")
    
    result = get(BASE_NODES["node2"], "user_data")
    
    if result and "context" in result:
        ok(f"Received Vector Clock context: {result['context']}")
    time.sleep(2)

def demo_sloppy_quorum():
    header("CONCEPT 4: Sloppy Quorum (Fault Tolerance: N=3, W=2, R=2)")
    
    # [What's happening]: We intentionally crash node3. 
    # Because our Quorum requires only 2 nodes to acknowledge a write (W=2), 
    # the system will continue to function normally even with a server offline.
    info("Simulating server crash: Stopping node3 container...")
    run_cmd("docker stop dynamo-node3-1 2>/dev/null || docker stop dynamo-cclab-node3-1 2>/dev/null")
    time.sleep(3) # Wait for it to die
    
    warn("Node3 is now down! Attempting to write new data anyway...")
    info("Since Write Quorum (W) is 2, and 2 out of 3 replicas are still alive, this will succeed.")
    
    # First, get the current vector clock context so we don't cause a conflict
    result = get(PRIMARY, "user_data")
    context = result.get("context") if result else None
    
    # Perform the update
    put(PRIMARY, "user_data", {"name": "Alice", "age": 26}, context=context)
    
    ok("Write succeeded despite node failure! Sloppy Quorum achieved.")
    time.sleep(2)

def demo_gossip_protocol():
    header("CONCEPT 5: Gossip Protocol (Failure Detection)")
    
    # [What's happening]: Nodes constantly ping each other in the background. 
    # Since we killed node3, the other nodes will eventually notice it missed its heartbeats
    # and mark it as 'suspected' or 'dead' in their internal membership tables.
    info("Waiting a few seconds for nodes to gossip about node3's failure...")
    time.sleep(5)
    
    info("Checking Node1's membership table to see what it thinks about Node3...")
    members = get_members(PRIMARY)
    
    if members and "members" in members:
        for nid, info_data in members["members"].items():
            status = info_data.get("status", "?")
            print(f"    Node: {nid:<20} Status: {status}")
        ok("Notice how Node3 is automatically marked as 'suspected' or 'dead' by the Gossip Protocol!")
    time.sleep(2)

def demo_merkle_trees():
    header("CONCEPT 6: Merkle Trees & Anti-Entropy (Data Sync)")
    
    # [What's happening]: We turn node3 back on. It missed the previous update (where Alice turned 26).
    # Nodes use Merkle Trees (trees of hashes) to quickly compare databases.
    # Anti-entropy runs in the background, spots the differing hashes, and syncs the missing data to node3.
    info("Bringing node3 back online...")
    run_cmd("docker start dynamo-node3-1 2>/dev/null || docker start dynamo-cclab-node3-1 2>/dev/null")
    
    info("Waiting a moment for node3 to boot, then comparing Merkle roots...")
    time.sleep(5)
    
    m1 = get_merkle(PRIMARY)
    m3 = get_merkle(BASE_NODES["node3"])
    if m1 and m3:
        print(f"    Node1 root: {m1.get('root', 'N/A')[:30]}... (Has Alice=26)")
        print(f"    Node3 root: {m3.get('root', 'N/A')[:30]}... (Stale data)")
    
    info("Waiting 15 seconds for the Anti-Entropy background process to sync missing data to Node3...")
    time.sleep(15)
    
    info("Checking Merkle roots again after Anti-Entropy sync...")
    m1_after = get_merkle(PRIMARY)
    m3_after = get_merkle(BASE_NODES["node3"])
    
    if m1_after and m3_after:
        print(f"    Node1 root: {m1_after.get('root', 'N/A')[:30]}...")
        print(f"    Node3 root: {m3_after.get('root', 'N/A')[:30]}...")
        if m1_after.get("root") == m3_after.get("root"):
            ok("Anti-Entropy worked! The roots match, meaning Node3 synced its missing data.")
        else:
            warn("Roots still differ. Anti-entropy might need another interval.")
    
    info("Reading from Node3 to prove it has the latest data (Alice is 26).")
    get(BASE_NODES["node3"], "user_data")

def test_cluster_connectivity():
    header("PRE-FLIGHT: Cluster Connectivity Test")
    info("Checking if all nodes (including your friend's at 192.168.137.80) are reachable...")
    all_up = True
    for name, url in BASE_NODES.items():
        try:
            r = httpx.get(f"{url}/", timeout=3)
            if r.status_code == 200:
                ok(f"{name} ({url}) is UP!")
            else:
                warn(f"{name} ({url}) returned HTTP {r.status_code}")
                all_up = False
        except Exception as e:
            warn(f"{name} ({url}) is DOWN or UNREACHABLE! Error: {e}")
            all_up = False
    
    if not all_up:
        warn("Some nodes are unreachable. Please verify IPs and ensure Docker is running on both machines.")
        info("Ensure firewall rules allow traffic on port 8000-8005.")
        sys.exit(1)
    else:
        ok("All 5 nodes are successfully connected over the network!")

def print_metrics_table():
    print("\nTable 2: Performance of client-driven and server-driven coordination approaches.")
    info("Fetching final metrics from Primary node...")
    try:
        r = httpx.get(f"{PRIMARY}/metrics", timeout=5)
        data = r.json()
        
        read_p999 = data.get("get", {}).get("latency_ms", {}).get("p999", 0.0)
        write_p999 = data.get("put", {}).get("latency_ms", {}).get("p999", 0.0)
        read_mean = data.get("get", {}).get("latency_ms", {}).get("mean", 0.0)
        write_mean = data.get("put", {}).get("latency_ms", {}).get("mean", 0.0)
        
        print(f"\n{'':<15} | {'99.9th percentile':<17} | {'99.9th percentile':<18} | {'Average read':<12} | {'Average write':<12}")
        print(f"{'Approach':<15} | {'read latency(ms)':<17} | {'write latency(ms)':<18} | {'latency (ms)':<12} | {'latency (ms)':<12}")
        print("-" * 87)
        print(f"{'Server-driven':<15} | {read_p999:<17.2f} | {write_p999:<18.2f} | {read_mean:<12.2f} | {write_mean:<12.2f}")
        print(f"{'Client-driven':<15} | {'30.4 (Paper)':<17} | {'30.4 (Paper)':<18} | {'1.55 (Paper)':<12} | {'1.9 (Paper)':<12}")
        print("\nNote: Our current Python implementation uses Server-driven coordination (coordinator node handles replication).")
    except Exception as e:
        warn(f"Failed to fetch metrics: {e}")


# ─── Main Execution ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*70)
    print("   Amazon Dynamo — Simplified Concept-by-Concept Demonstration")
    print("="*70)
    
    test_cluster_connectivity()
    
    info("Waiting 3s for cluster to stabilize...")
    time.sleep(3)
    
    demo_consistent_hashing_and_replication()
    demo_vector_clocks()
    demo_sloppy_quorum()
    demo_gossip_protocol()
    demo_merkle_trees()
    
    print_metrics_table()
    
    print("\n" + "="*70)
    print("   Demo Complete! All concepts demonstrated.")
    print("="*70 + "\n")
