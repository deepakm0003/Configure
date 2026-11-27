"""
metrics.py – Rolling throughput counter and p50/p95 latency tracker.

Logs a JSON summary to stdout every 10 seconds.
Latency is derived from trace fields (emitted_ms → current stage _ms).
"""

from __future__ import annotations

import json
import math
import time
from collections import deque


class StageMetrics:
    """
    Per-stage rolling metrics.

    Args:
        role: Pipeline stage name (e.g. "normalize").
        window_s: Rolling window for latency percentiles (default 60 s).
        log_interval_s: How often to emit the JSON summary (default 10 s).
    """

    def __init__(self, role: str, window_s: float = 60.0, log_interval_s: float = 10.0) -> None:
        self._role = role
        self._window_s = window_s
        self._log_interval_s = log_interval_s

        # Throughput
        self._in_count = 0
        self._out_count = 0
        self._window_start = time.monotonic()

        # Latency samples: (monotonic_time, latency_ms)
        self._latency_samples: deque[tuple[float, float]] = deque()

        self._last_log = time.monotonic()

    # ── Public API ────────────────────────────────────────────────────────────

    def record_in(self) -> None:
        """Call once per message consumed."""
        self._in_count += 1

    def record_out(self) -> None:
        """Call once per message produced."""
        self._out_count += 1

    def record_latency(self, emitted_ms: int, stage_ms: int) -> None:
        """
        Record a latency sample.  emitted_ms is from trace.emitted_ms;
        stage_ms is the ms timestamp set by this stage (trace.<role>_ms).
        """
        if emitted_ms > 0 and stage_ms > 0:
            lat = stage_ms - emitted_ms
            if 0 <= lat < 60_000:   # sanity: ignore outliers > 1 min
                now = time.monotonic()
                self._latency_samples.append((now, lat))

    def maybe_log(self) -> None:
        """Call frequently (every message); logs at most every log_interval_s."""
        now = time.monotonic()
        if now - self._last_log < self._log_interval_s:
            return
        self._emit_log(now)
        self._last_log = now

    def flush_log(self) -> None:
        """Force-emit a final log line (call on shutdown)."""
        self._emit_log(time.monotonic())

    # ── Internals ─────────────────────────────────────────────────────────────

    def _prune_latency(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._latency_samples and self._latency_samples[0][0] < cutoff:
            self._latency_samples.popleft()

    def _percentile(self, p: float) -> float:
        """Return the p-th percentile (0–100) of recent latency samples."""
        if not self._latency_samples:
            return 0.0
        vals = sorted(s[1] for s in self._latency_samples)
        idx = (p / 100) * (len(vals) - 1)
        lo = int(idx)
        hi = min(lo + 1, len(vals) - 1)
        return vals[lo] + (vals[hi] - vals[lo]) * (idx - lo)

    def _emit_log(self, now: float) -> None:
        elapsed = now - self._window_start
        if elapsed <= 0:
            elapsed = 1.0

        self._prune_latency(now)
        p50 = self._percentile(50)
        p95 = self._percentile(95)

        line = {
            "ts": int(time.time() * 1000),
            "level": "INFO",
            "role": self._role,
            "msg": "metrics",
            "msgs_in_per_s": round(self._in_count / elapsed, 2),
            "msgs_out_per_s": round(self._out_count / elapsed, 2),
            "latency_p50_ms": round(p50, 1),
            "latency_p95_ms": round(p95, 1),
            "latency_samples": len(self._latency_samples),
        }
        print(json.dumps(line), flush=True)

        # Reset counters (sliding window)
        self._in_count = 0
        self._out_count = 0
        self._window_start = now


def log_json(role: str, level: str, msg: str, **extra: object) -> None:
    """Emit a single structured JSON log line to stdout."""
    line: dict[str, object] = {
        "ts": int(time.time() * 1000),
        "level": level,
        "role": role,
        "msg": msg,
    }
    line.update(extra)
    print(json.dumps(line), flush=True)
