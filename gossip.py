"""
gossip.py — Dynamo Gossip Protocol Implementation

Each node maintains a membership table:
  { node_id: { "status": "alive"|"suspected"|"dead", "heartbeat": int, "updated_at": float } }

Every GOSSIP_INTERVAL seconds, the node:
  1. Increments its own heartbeat.
  2. Picks a random alive peer and sends it a POST /gossip with its full membership view.
  3. On receiving gossip, merges tables (max heartbeat wins).
  4. Marks nodes as "suspected" if heartbeat hasn't changed in FAILURE_TIMEOUT seconds.
  5. Marks nodes as "dead" if suspected for DEAD_TIMEOUT seconds.

This mirrors §4.8.2 of the Dynamo paper — decentralized failure detection via gossip.
"""

import asyncio
import random
import time
from typing import Dict, Any

import httpx

# ─── Configuration ──────────────────────────────────────────────────────────
GOSSIP_INTERVAL = 1.0       # seconds between gossip rounds
FAILURE_TIMEOUT = 5.0       # seconds of silence before "suspected"
DEAD_TIMEOUT    = 15.0      # seconds of silence before "dead"


class GossipManager:
    def __init__(self, node_id: str, seed_nodes: list[str]):
        self.node_id = node_id

        # membership table: node_id -> entry dict
        self._members: Dict[str, Dict[str, Any]] = {}

        # Seed ourselves + all seed nodes as initially alive
        now = time.time()
        for node in seed_nodes:
            self._members[node] = {
                "status": "alive",
                "heartbeat": 0,
                "updated_at": now,
            }
        # Make sure we exist with heartbeat 0 (will be incremented immediately)
        self._members[node_id] = {
            "status": "alive",
            "heartbeat": 0,
            "updated_at": now,
        }

        self.rounds = 0  # total gossip rounds fired (for metrics)
        self._client = httpx.AsyncClient(timeout=2.0)

    # ── Public API ────────────────────────────────────────────────────────────

    def get_members(self) -> Dict[str, Dict[str, Any]]:
        """Return a snapshot of the current membership table."""
        return dict(self._members)

    def get_alive_nodes(self) -> list[str]:
        """Return node IDs currently considered alive (excluding self)."""
        return [
            nid for nid, info in self._members.items()
            if info["status"] == "alive" and nid != self.node_id
        ]

    def get_all_reachable_nodes(self) -> list[str]:
        """Return alive + suspected nodes (suspected may still respond)."""
        return [
            nid for nid, info in self._members.items()
            if info["status"] in ("alive", "suspected") and nid != self.node_id
        ]

    def merge(self, incoming: Dict[str, Dict[str, Any]]):
        """
        Merge an incoming membership view with our own.
        For each node, take the entry with the highest heartbeat.
        Never override our own entry from remote data (we own our heartbeat).
        """
        now = time.time()
        for nid, remote_entry in incoming.items():
            if nid == self.node_id:
                continue  # we manage our own entry

            local = self._members.get(nid)
            if local is None or remote_entry["heartbeat"] > local["heartbeat"]:
                self._members[nid] = {
                    "status": remote_entry.get("status", "alive"),
                    "heartbeat": remote_entry["heartbeat"],
                    "updated_at": now,  # record when WE last heard about this node
                }

    # ── Background Loop ───────────────────────────────────────────────────────

    async def run(self):
        """Main gossip loop — runs forever as a background task."""
        while True:
            await asyncio.sleep(GOSSIP_INTERVAL)
            self._tick()
            await self._gossip_once()

    def _tick(self):
        """Increment our own heartbeat and update liveness of all members."""
        now = time.time()

        # Increment self
        self._members[self.node_id]["heartbeat"] += 1
        self._members[self.node_id]["updated_at"] = now
        self._members[self.node_id]["status"] = "alive"

        # Check all other members for timeouts
        for nid, info in self._members.items():
            if nid == self.node_id:
                continue
            silent_for = now - info["updated_at"]
            if silent_for > DEAD_TIMEOUT:
                info["status"] = "dead"
            elif silent_for > FAILURE_TIMEOUT:
                info["status"] = "suspected"
            else:
                info["status"] = "alive"

    async def _gossip_once(self):
        """Pick a random alive peer and send our membership view."""
        peers = self.get_alive_nodes()
        if not peers:
            # fall back to suspected nodes if everyone looks dead
            peers = [
                nid for nid, info in self._members.items()
                if nid != self.node_id and info["status"] != "dead"
            ]
        if not peers:
            return

        target = random.choice(peers)
        payload = {nid: info for nid, info in self._members.items()}

        try:
            await self._client.post(
                f"http://{target}/gossip",
                json=payload,
            )
            self.rounds += 1
        except Exception:
            # Peer is unreachable — the _tick() timeout will eventually mark it suspected/dead
            pass
