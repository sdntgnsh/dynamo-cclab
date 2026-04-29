"""
merkle.py — Merkle Tree Anti-Entropy Implementation

Mirrors §4.7 of the Dynamo paper. Each node builds a Merkle tree over its
key-value store. Two nodes can compare their trees root-first, then narrow
down to only the differing key buckets — avoiding full data transfer.

Architecture:
  - The key space is divided into NUM_BUCKETS fixed buckets using key hash.
  - Each bucket leaf = SHA256( sorted(key+str(value)) for all keys in bucket )
  - Internal nodes = SHA256( left_child_hash + right_child_hash )
  - Root hash = single hash representing the entire store state.

Anti-Entropy Loop (runs in node.py as background task):
  Every ANTI_ENTROPY_INTERVAL seconds:
    1. Pick a random alive peer (from gossip).
    2. GET /merkle from peer → compare bucket hashes.
    3. For differing buckets, GET /merkle/bucket/{id} from peer → get keys.
    4. Sync keys where peer has a higher vector clock version.
    5. Record synced key count in metrics.
"""

import hashlib
import math
from typing import Dict, Any, List, Tuple, Optional

NUM_BUCKETS = 16  # power of 2; determines anti-entropy granularity


def _sha256(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


class MerkleTree:
    """
    Builds a Merkle tree over a snapshot of the node's key-value store.

    store format: { key: {"value": any, "context": dict} }
    """

    def __init__(self, store: Dict[str, Dict[str, Any]]):
        self.num_buckets = NUM_BUCKETS
        # Assign each key to a bucket based on its hash
        self.buckets: List[Dict[str, Any]] = [{} for _ in range(self.num_buckets)]
        for key, entry in store.items():
            bucket_idx = self._key_to_bucket(key)
            self.buckets[bucket_idx][key] = entry

        # Build leaf hashes (one per bucket)
        self.leaf_hashes: List[str] = [
            self._hash_bucket(i) for i in range(self.num_buckets)
        ]

        # Build the full tree bottom-up
        self.tree: List[str] = self._build_tree(self.leaf_hashes)

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def root(self) -> str:
        """The root hash of the Merkle tree."""
        return self.tree[0] if self.tree else _sha256("")

    def get_bucket_hashes(self) -> List[str]:
        """Return the hash for each bucket (leaf level)."""
        return list(self.leaf_hashes)

    def get_bucket_keys(self, bucket_id: int) -> Dict[str, Any]:
        """Return all key-value entries in the given bucket."""
        if 0 <= bucket_id < self.num_buckets:
            return dict(self.buckets[bucket_id])
        return {}

    def differing_buckets(self, other_bucket_hashes: List[str]) -> List[int]:
        """
        Compare our bucket hashes against the peer's.
        Returns list of bucket indices that differ.
        """
        differing = []
        for i, (ours, theirs) in enumerate(
            zip(self.leaf_hashes, other_bucket_hashes)
        ):
            if ours != theirs:
                differing.append(i)
        return differing

    def to_summary(self) -> Dict[str, Any]:
        """Serializable summary for GET /merkle endpoint."""
        return {
            "root": self.root,
            "num_buckets": self.num_buckets,
            "bucket_hashes": self.leaf_hashes,
        }

    # ── Internals ─────────────────────────────────────────────────────────────

    def _key_to_bucket(self, key: str) -> int:
        """Hash the key and map to a bucket index."""
        h = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
        return h % self.num_buckets

    def _hash_bucket(self, bucket_idx: int) -> str:
        """
        Deterministically hash all entries in a bucket.
        Sort keys for consistency across nodes.
        """
        bucket = self.buckets[bucket_idx]
        if not bucket:
            return _sha256(f"empty_bucket_{bucket_idx}")

        parts = []
        for key in sorted(bucket.keys()):
            entry = bucket[key]
            value_str = str(entry.get("value", ""))
            context_str = str(sorted(entry.get("context", {}).items()))
            parts.append(f"{key}:{value_str}:{context_str}")

        return _sha256("|".join(parts))

    def _build_tree(self, leaves: List[str]) -> List[str]:
        """
        Build a complete binary Merkle tree from leaf hashes.
        Tree stored as flat array: index 0 = root, children of i at 2i+1 and 2i+2.
        """
        n = len(leaves)
        # Pad to next power of 2
        padded_n = 1
        while padded_n < n:
            padded_n <<= 1
        padded = leaves + [_sha256(f"pad_{i}") for i in range(padded_n - n)]

        # Bottom-up construction
        level = padded
        tree_levels = [level]
        while len(level) > 1:
            next_level = []
            for i in range(0, len(level), 2):
                left = level[i]
                right = level[i + 1] if i + 1 < len(level) else left
                next_level.append(_sha256(left + right))
            level = next_level
            tree_levels.append(level)

        # Flatten: root first (from last level down to leaves)
        flat = []
        for lvl in reversed(tree_levels):
            flat.extend(lvl)
        return flat


# ── Key-level Sync Helpers ────────────────────────────────────────────────────

def should_adopt_remote(local_entry: Optional[Dict], remote_entry: Dict) -> bool:
    """
    Decide whether to adopt the remote version of a key.
    Rules:
      1. If we don't have the key locally → always adopt.
      2. If remote context strictly dominates local context → adopt.
      3. If concurrent (conflict) → adopt remote (last-write-wins fallback).
      4. If local dominates → keep local.
    """
    if local_entry is None:
        return True

    local_ctx = local_entry.get("context") or {}
    remote_ctx = remote_entry.get("context") or {}

    if not local_ctx and not remote_ctx:
        return False  # no version info, keep local

    all_nodes = set(local_ctx) | set(remote_ctx)
    local_ahead = any(local_ctx.get(n, 0) > remote_ctx.get(n, 0) for n in all_nodes)
    remote_ahead = any(remote_ctx.get(n, 0) > local_ctx.get(n, 0) for n in all_nodes)

    if remote_ahead and not local_ahead:
        return True   # remote strictly newer
    # concurrent or local wins → adopt remote anyway (anti-entropy is best-effort)
    return remote_ahead
