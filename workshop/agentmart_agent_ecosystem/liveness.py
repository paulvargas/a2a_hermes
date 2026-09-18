"""Agent liveness / dead-agent detection.

Tracks the last-seen timestamp (in ms) for each named agent and classifies
agents as "healthy" or "dead" against a TTL. Pure and deterministic: every
method accepts an injected `now_ms` so callers (and tests) never depend on
wall-clock sleeps.
"""

import time


def _now_ms() -> int:
    return int(time.time() * 1000)


class Liveness:
    """In-memory last-seen tracker for agent heartbeats."""

    def __init__(self):
        self._last_seen: dict[str, int] = {}

    def mark(self, agent: str, now_ms: int | None = None) -> None:
        """Record that `agent` was seen alive at `now_ms` (default: now)."""
        self._last_seen[agent] = now_ms if now_ms is not None else _now_ms()

    def dead(self, now_ms: int | None = None, ttl_ms: int = 6000) -> list[str]:
        """Return the names of known agents whose last-seen timestamp is older
        than `now_ms - ttl_ms` (i.e. they have gone silent for longer than the TTL)."""
        now = now_ms if now_ms is not None else _now_ms()
        cutoff = now - ttl_ms
        return [agent for agent, seen in self._last_seen.items() if seen < cutoff]

    def snapshot(self, now_ms: int | None = None, ttl_ms: int = 6000) -> dict[str, str]:
        """Return {agent: "healthy"|"dead"} for every agent ever marked."""
        dead_agents = set(self.dead(now_ms=now_ms, ttl_ms=ttl_ms))
        return {
            agent: ("dead" if agent in dead_agents else "healthy")
            for agent in self._last_seen
        }
