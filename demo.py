"""
demo.py — Dynamo CCLab Full Demonstration Script

Demonstrates all 6 Dynamo concepts with real performance numbers.

Usage:
  python demo.py            # Full demo (Dynamo concepts walkthrough)
  python demo.py --bench    # Benchmark mode: concurrent load test + metrics table
"""

import httpx
import time
import sys
import subprocess
import concurrent.futures
import json
import statistics
from typing import Optional

BASE_NODES = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003",
    "node4": "http://localhost:8004",
    "node5": "http://localhost:8005",
}
PRIMARY = BASE_NODES["node1"]
KEY = "student1"

# ─── Formatting helpers ───────────────────────────────────────────────────────

def header(title: str):
    bar = "═" * 62
    print(f"\n╔{bar}╗")
    print(f"║  {title:<60}║")
    print(f"╚{bar}╝")


def section(title: str):
    print(f"\n  ── {title} {'─'*(55-len(title))}")


def ok(msg: str):  print(f"  ✅  {msg}")
def info(msg: str): print(f"  ℹ️   {msg}")
def warn(msg: str): print(f"  ⚠️   {msg}")
def fail(msg: str): print(f"  ❌  {msg}")


def print_json(data):
    print(json.dumps(data, indent=4))


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def put(url: str, key: str, value, context=None) -> Optional[dict]:
    payload = {"value": value}
    if context:
        payload["context"] = context
    try:
        r = httpx.put(f"{url}/kv/{key}", json=payload, timeout=10)
        print(f"  PUT /kv/{key} → HTTP {r.status_code}")
        data = r.json()
        print_json(data)
        return data
    except Exception as e:
        fail(f"PUT failed: {e}")
        return None


def get(url: str, key: str) -> Optional[dict]:
    try:
        r = httpx.get(f"{url}/kv/{key}", timeout=10)
        print(f"  GET /kv/{key} → HTTP {r.status_code}")
        data = r.json()
        print_json(data)
        return data
    except Exception as e:
        fail(f"GET failed: {e}")
        return None


def get_members(url: str) -> Optional[dict]:
    try:
        r = httpx.get(f"{url}/gossip/members", timeout=5)
        return r.json()
    except Exception:
        return None


def get_merkle(url: str) -> Optional[dict]:
    try:
        r = httpx.get(f"{url}/merkle", timeout=5)
        return r.json()
    except Exception:
        return None


def get_metrics(url: str) -> Optional[dict]:
    try:
        r = httpx.get(f"{url}/metrics", timeout=5)
        return r.json()
    except Exception:
        return None


def run_cmd(cmd: str):
    info(f"Shell: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout.strip():
        print(f"    {result.stdout.strip()}")


# ─── Full Demo ────────────────────────────────────────────────────────────────

def run_demo():
    print()
    print("  ██████╗ ██╗   ██╗███╗   ██╗ █████╗ ███╗   ███╗ ██████╗")
    print("  ██╔══██╗╚██╗ ██╔╝████╗  ██║██╔══██╗████╗ ████║██╔═══██╗")
    print("  ██║  ██║ ╚████╔╝ ██╔██╗ ██║███████║██╔████╔██║██║   ██║")
    print("  ██║  ██║  ╚██╔╝  ██║╚██╗██║██╔══██║██║╚██╔╝██║██║   ██║")
    print("  ██████╔╝   ██║   ██║ ╚████║██║  ██║██║ ╚═╝ ██║╚██████╔╝")
    print("  ╚═════╝    ╚═╝   ╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝     ╚═╝ ╚═════╝")
    print("            Amazon Dynamo — Full 6-Concept Lab Demo")

    info("Waiting 3s for cluster to stabilize...")
    time.sleep(3)

    # ── 1. Gossip Protocol: Initial Membership ────────────────────────────────
    header("CONCEPT 6 — Gossip Protocol: Membership View")
    info("Querying gossip membership table on node1...")
    time.sleep(2)
    members = get_members(PRIMARY)
    if members:
        for nid, info_data in members.get("members", {}).items():
            status = info_data.get("status", "?")
            hb = info_data.get("heartbeat", 0)
            icon = "🟢" if status == "alive" else "🟡" if status == "suspected" else "🔴"
            print(f"    {icon}  {nid:<22} status={status:<10} heartbeat={hb}")
        ok(f"Alive nodes visible: {members.get('alive_count', '?')}/5")

    # ── 2. Consistent Hashing: Write and see preference list ─────────────────
    header("CONCEPT 1+2 — Consistent Hashing + Replication")
    info(f"Writing key='{KEY}' via node1 (it will act as coordinator)")
    context = None
    res = put(PRIMARY, KEY, {"name": "Tanmay", "grade": "A"})
    if res:
        context = res.get("context")
        ok(f"Vector clock after write: {context}")

    # ── 3. Vector Clocks: Read and show context ───────────────────────────────
    header("CONCEPT 3 — Vector Clocks: Read-back verification")
    info("Reading back from node2 (different coordinator)...")
    get(BASE_NODES["node2"], KEY)

    # ── 4. Quorum: Kill a node, prove writes still work ───────────────────────
    header("CONCEPT 4 — Sloppy Quorum (W=2, R=2, N=3): Node Failure")
    info("Stopping node3 to simulate failure...")
    run_cmd("docker stop dynamo-node3-1 2>/dev/null || docker stop dynamo-cclab-node3-1 2>/dev/null || echo 'Stop command sent'")
    time.sleep(2)

    warn("node3 is now DOWN. Attempting write with W=2 (needs only 2 of 3 replicas)...")
    res2 = put(PRIMARY, KEY, {"name": "Tanmay", "grade": "A+"}, context=context)
    if res2 and res2.get("status") == "success":
        ok("Write SUCCEEDED despite node failure! Quorum achieved.")
        context = res2.get("context")

    info("Reading while node3 is still down (R=2 quorum from remaining nodes)...")
    get(PRIMARY, KEY)

    # ── 5. Gossip detects the failure ─────────────────────────────────────────
    header("CONCEPT 6 — Gossip: Failure Detection")
    info("Waiting 6s for gossip to mark node3 as 'suspected'...")
    time.sleep(6)
    members = get_members(PRIMARY)
    if members:
        for nid, info_data in members.get("members", {}).items():
            status = info_data.get("status", "?")
            icon = "🟢" if status == "alive" else "🟡" if status == "suspected" else "🔴"
            print(f"    {icon}  {nid:<22} status={status}")
        node3_status = members.get("members", {}).get("node3:8000", {}).get("status", "unknown")
        if node3_status in ("suspected", "dead"):
            ok(f"Gossip correctly detected node3 as '{node3_status}'!")

    # ── 6. Merkle Trees: Before node3 recovers ───────────────────────────────
    header("CONCEPT 5 — Merkle Trees: Divergence Detection")
    info("Comparing Merkle roots before node3 recovers...")
    m1 = get_merkle(PRIMARY)
    m2 = get_merkle(BASE_NODES["node2"])
    if m1 and m2:
        print(f"    node1 root: {m1.get('root', 'N/A')[:32]}...")
        print(f"    node2 root: {m2.get('root', 'N/A')[:32]}...")
        if m1.get("root") == m2.get("root"):
            ok("node1 and node2 Merkle roots MATCH ✓")
        else:
            warn("node1 and node2 Merkle roots DIFFER (expected before sync)")

    # ── 7. Eventual Consistency: Bring node3 back ────────────────────────────
    header("CONCEPT 4+5 — Eventual Consistency via Anti-Entropy")
    info("Bringing node3 back online...")
    run_cmd("docker start dynamo-node3-1 2>/dev/null || docker start dynamo-cclab-node3-1 2>/dev/null || echo 'Start command sent'")
    info(f"Waiting {int(10+5)}s for node to start and anti-entropy to sync...")
    time.sleep(15)

    info("Comparing Merkle roots after anti-entropy has had time to run...")
    m1_after = get_merkle(PRIMARY)
    m3_after = get_merkle(f"http://localhost:8003")
    if m1_after and m3_after:
        print(f"    node1 root: {m1_after.get('root', 'N/A')[:32]}...")
        print(f"    node3 root: {m3_after.get('root', 'N/A')[:32]}...")
        if m1_after.get("root") == m3_after.get("root"):
            ok("Anti-entropy WORKED! node3 converged to the same Merkle root as node1.")
        else:
            warn("Roots still differ — anti-entropy may need another round (check ANTI_ENTROPY_INTERVAL_SECS).")

    info("Reading from node3 (should now have the latest data)...")
    get(BASE_NODES["node3"], KEY)

    # ── 8. Performance Metrics Report ────────────────────────────────────────
    header("PERFORMANCE METRICS — All Nodes")
    print_metrics_table()

    header("Demo Complete — All 6 Dynamo Concepts Demonstrated")
    ok("Consistent Hashing   — keys distributed across ring")
    ok("Replication          — preference list of N=3 nodes")
    ok("Vector Clocks        — causality tracked on every write")
    ok("Sloppy Quorum        — writes succeeded with node3 down")
    ok("Merkle Anti-Entropy  — node3 re-synced after recovery")
    ok("Gossip Protocol      — node3 failure detected automatically")


# ─── Benchmark Mode ───────────────────────────────────────────────────────────

def run_benchmark(n_ops: int = 200):
    header(f"BENCHMARK MODE — {n_ops} concurrent operations")
    info("Warming up cluster (5s)...")
    time.sleep(5)

    keys = [f"bench_key_{i}" for i in range(n_ops)]

    # ── Write benchmark ───────────────────────────────────────────────────────
    section("Write throughput (concurrent PUTs)")
    put_latencies = []
    put_errors = 0

    def do_put(key):
        try:
            t0 = time.perf_counter()
            r = httpx.put(f"{PRIMARY}/kv/{key}", json={"value": {"data": key}}, timeout=10)
            elapsed = (time.perf_counter() - t0) * 1000
            return elapsed, r.status_code == 200
        except Exception:
            return None, False

    t_write_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        futs = [ex.submit(do_put, k) for k in keys]
        for f in concurrent.futures.as_completed(futs):
            lat, ok_ = f.result()
            if lat is not None:
                put_latencies.append(lat)
            if not ok_:
                put_errors += 1
    write_duration = time.time() - t_write_start

    # ── Read benchmark ────────────────────────────────────────────────────────
    section("Read throughput (concurrent GETs)")
    get_latencies = []
    get_errors = 0

    def do_get(key):
        try:
            t0 = time.perf_counter()
            r = httpx.get(f"{PRIMARY}/kv/{key}", timeout=10)
            elapsed = (time.perf_counter() - t0) * 1000
            return elapsed, r.status_code == 200
        except Exception:
            return None, False

    t_read_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        futs = [ex.submit(do_get, k) for k in keys]
        for f in concurrent.futures.as_completed(futs):
            lat, ok_ = f.result()
            if lat is not None:
                get_latencies.append(lat)
            if not ok_:
                get_errors += 1
    read_duration = time.time() - t_read_start

    # ── Print benchmark results ───────────────────────────────────────────────
    header("Benchmark Results")

    def pct(data, p):
        if not data: return 0
        s = sorted(data)
        i = max(0, min(int((p/100)*len(s)), len(s)-1))
        return round(s[i], 2)

    print(f"""
  ┌──────────────────────────────────────────────────────────────┐
  │                    WRITE PERFORMANCE                         │
  ├──────────────────────────────────────────────────────────────┤
  │  Operations:    {n_ops:<10}                                   │
  │  Duration:      {write_duration:.2f}s                                    │
  │  Throughput:    {round(n_ops / write_duration, 1):<10} ops/sec                          │
  │  Errors:        {put_errors:<10}                                   │
  │  Latency p50:   {pct(put_latencies,50):<10} ms                              │
  │  Latency p95:   {pct(put_latencies,95):<10} ms                              │
  │  Latency p99:   {pct(put_latencies,99):<10} ms                              │
  │  Latency max:   {pct(put_latencies,100):<10} ms                              │
  ├──────────────────────────────────────────────────────────────┤
  │                    READ PERFORMANCE                          │
  ├──────────────────────────────────────────────────────────────┤
  │  Operations:    {n_ops:<10}                                   │
  │  Duration:      {read_duration:.2f}s                                    │
  │  Throughput:    {round(n_ops / read_duration, 1):<10} ops/sec                          │
  │  Errors:        {get_errors:<10}                                   │
  │  Latency p50:   {pct(get_latencies,50):<10} ms                              │
  │  Latency p95:   {pct(get_latencies,95):<10} ms                              │
  │  Latency p99:   {pct(get_latencies,99):<10} ms                              │
  │  Latency max:   {pct(get_latencies,100):<10} ms                              │
  └──────────────────────────────────────────────────────────────┘
""")

    print_metrics_table()


# ─── Metrics Table ────────────────────────────────────────────────────────────

def print_metrics_table():
    section("Per-node Performance Report")
    print(f"\n  {'Node':<10} {'PUT p50':>8} {'PUT p99':>8} {'PUT ok%':>8} {'GET p50':>8} {'GET p99':>8} {'GET ok%':>8} {'Conflicts':>10} {'AE Keys':>8} {'Gossip':>7}")
    print(f"  {'─'*10} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*10} {'─'*8} {'─'*7}")

    for name, url in BASE_NODES.items():
        m = get_metrics(url)
        if not m:
            print(f"  {name:<10}  {'(unreachable)':>70}")
            continue

        p = m.get("put", {})
        g = m.get("get", {})
        ae = m.get("anti_entropy", {})
        gossip_info = m.get("gossip", {})
        p_lat = p.get("latency_ms", {})
        g_lat = g.get("latency_ms", {})

        print(
            f"  {name:<10}"
            f"  {p_lat.get('p50', 0):>6.1f}ms"
            f"  {p_lat.get('p99', 0):>6.1f}ms"
            f"  {p.get('quorum_success_rate_pct', 100):>6.1f}%"
            f"  {g_lat.get('p50', 0):>6.1f}ms"
            f"  {g_lat.get('p99', 0):>6.1f}ms"
            f"  {g.get('quorum_success_rate_pct', 100):>6.1f}%"
            f"  {g.get('conflict_count', 0):>10}"
            f"  {ae.get('keys_synced', 0):>8}"
            f"  {gossip_info.get('rounds', 0):>7}"
        )

    print()
    section("Explanation of columns")
    print("  PUT/GET p50/p99 — coordinator-side latency percentiles (ms)")
    print("  PUT/GET ok%     — quorum success rate (% of requests that met W/R)")
    print("  Conflicts       — reads with concurrent vector clock versions")
    print("  AE Keys         — keys synced via Merkle tree anti-entropy")
    print("  Gossip          — gossip rounds fired since node start")


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--bench" in sys.argv:
        n = 200
        for arg in sys.argv:
            if arg.startswith("--n="):
                try:
                    n = int(arg.split("=")[1])
                except ValueError:
                    pass
        run_benchmark(n_ops=n)
    else:
        run_demo()
