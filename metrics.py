"""
metrics.py — Performance Metrics Collector

In-memory singleton that instruments the Dynamo node with real performance data.
Exposed via GET /metrics as a JSON report.

Tracked metrics:
  - PUT / GET latency (ms): p50, p95, p99
  - PUT / GET success / failure counts
  - Quorum success rate (%)
  - Conflict rate (% of reads with vector clock conflicts)
  - Replica write failures
  - Anti-entropy keys synced
  - Gossip rounds fired
  - Uptime (seconds)
  - Throughput (ops / sec since start)
"""

import time
import statistics
from typing import List, Dict, Any


class MetricsCollector:
    def __init__(self):
        self.start_time: float = time.time()

        # Latency buckets (milliseconds)
        self._put_latencies: List[float] = []
        self._get_latencies: List[float] = []

        # Operation counters
        self.put_success: int = 0
        self.put_failure: int = 0
        self.get_success: int = 0
        self.get_failure: int = 0

        # Conflict tracking
        self.conflict_count: int = 0

        # Replica-level failures (individual node unreachable during replication)
        self.replica_write_failures: int = 0

        # Anti-entropy
        self.anti_entropy_rounds: int = 0
        self.anti_entropy_keys_synced: int = 0
        self.anti_entropy_buckets_differed: int = 0

        # Gossip
        self.gossip_rounds: int = 0

    # ── Recording helpers ─────────────────────────────────────────────────────

    def record_put(self, latency_ms: float, success: bool):
        self._put_latencies.append(latency_ms)
        if success:
            self.put_success += 1
        else:
            self.put_failure += 1

    def record_get(self, latency_ms: float, success: bool, conflict: bool = False):
        self._get_latencies.append(latency_ms)
        if success:
            self.get_success += 1
        else:
            self.get_failure += 1
        if conflict:
            self.conflict_count += 1

    def record_replica_failure(self):
        self.replica_write_failures += 1

    def record_anti_entropy(self, keys_synced: int, buckets_differed: int):
        self.anti_entropy_rounds += 1
        self.anti_entropy_keys_synced += keys_synced
        self.anti_entropy_buckets_differed += buckets_differed

    def record_gossip_round(self):
        self.gossip_rounds += 1

    # ── Computed metrics ──────────────────────────────────────────────────────

    @property
    def uptime_seconds(self) -> float:
        return round(time.time() - self.start_time, 2)

    @property
    def total_put_ops(self) -> int:
        return self.put_success + self.put_failure

    @property
    def total_get_ops(self) -> int:
        return self.get_success + self.get_failure

    @property
    def total_ops(self) -> int:
        return self.total_put_ops + self.total_get_ops

    @property
    def throughput_ops_per_sec(self) -> float:
        elapsed = self.uptime_seconds
        if elapsed <= 0:
            return 0.0
        return round(self.total_ops / elapsed, 2)

    def _percentiles(self, data: List[float]) -> Dict[str, float]:
        if not data:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "min": 0.0, "max": 0.0, "mean": 0.0}
        import math
        sorted_data = sorted(data)
        n = len(sorted_data)

        def percentile(p: float) -> float:
            idx = int(math.ceil(p / 100.0 * n)) - 1
            idx = max(0, min(idx, n - 1))
            return round(sorted_data[idx], 3)

        return {
            "p50":  percentile(50),
            "p95":  percentile(95),
            "p99":  percentile(99),
            "p999": percentile(99.9),
            "min":  round(sorted_data[0], 3),
            "max":  round(sorted_data[-1], 3),
            "mean": round(statistics.mean(sorted_data), 3),
        }

    # ── Report ────────────────────────────────────────────────────────────────

    def report(self) -> Dict[str, Any]:
        """Full metrics report — returned by GET /metrics."""
        put_lat = self._percentiles(self._put_latencies)
        get_lat = self._percentiles(self._get_latencies)

        put_qsr = (
            round(self.put_success / self.total_put_ops * 100, 1)
            if self.total_put_ops > 0 else 100.0
        )
        get_qsr = (
            round(self.get_success / self.total_get_ops * 100, 1)
            if self.total_get_ops > 0 else 100.0
        )
        conflict_rate = (
            round(self.conflict_count / self.get_success * 100, 1)
            if self.get_success > 0 else 0.0
        )

        return {
            "uptime_seconds": self.uptime_seconds,
            "throughput_ops_per_sec": self.throughput_ops_per_sec,

            "put": {
                "total": self.total_put_ops,
                "success": self.put_success,
                "failure": self.put_failure,
                "quorum_success_rate_pct": put_qsr,
                "latency_ms": put_lat,
                "replica_write_failures": self.replica_write_failures,
            },

            "get": {
                "total": self.total_get_ops,
                "success": self.get_success,
                "failure": self.get_failure,
                "quorum_success_rate_pct": get_qsr,
                "conflict_count": self.conflict_count,
                "conflict_rate_pct": conflict_rate,
                "latency_ms": get_lat,
            },

            "anti_entropy": {
                "rounds": self.anti_entropy_rounds,
                "keys_synced": self.anti_entropy_keys_synced,
                "buckets_differed": self.anti_entropy_buckets_differed,
            },

            "gossip": {
                "rounds": self.gossip_rounds,
            },
        }


# ── Module-level singleton ────────────────────────────────────────────────────
metrics = MetricsCollector()
