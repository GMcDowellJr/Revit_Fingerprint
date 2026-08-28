"""Small stderr progress reporter for long Run A operations.

Heartbeats are best effort: Python scheduling, the GIL, and blocked I/O can delay
them. Opaque operations report elapsed time only, never invented completion.
"""
from __future__ import annotations
import math
import argparse
import sys
import threading
import time
from typing import Any, Callable, Optional, TextIO

DEFAULT_INTERVAL_SECONDS = 10.0

def positive_finite(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("progress interval must be a positive finite number")
    return number

class ProgressReporter:
    def __init__(self, interval: float = DEFAULT_INTERVAL_SECONDS, quiet: bool = False,
                 stream: TextIO = sys.stderr, clock: Callable[[], float] = time.perf_counter):
        self.interval, self.quiet, self.stream, self.clock = interval, quiet, stream, clock
        self.started = clock(); self._last = self.started; self._state: dict[str, Any] = {}
        self._stop = threading.Event(); self._thread: Optional[threading.Thread] = None

    def _line(self, message: str, **fields: Any) -> None:
        values = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
        print(f"[run-a] {message}" + (f" {values}" if values else ""), file=self.stream, flush=True)

    def event(self, message: str, **fields: Any) -> None:
        self._line(message, elapsed_seconds=f"{self.clock()-self.started:.3f}", **fields)

    def update(self, **fields: Any) -> None:
        self._state.update(fields)
        now = self.clock()
        if not self.quiet and now - self._last >= self.interval:
            self._last = now; self._line("progress", elapsed_seconds=f"{now-self.started:.3f}", **self._state)

    def start_heartbeat(self) -> None:
        if self.quiet or self._thread is not None: return
        def beat() -> None:
            while not self._stop.wait(self.interval):
                self._line("progress", elapsed_seconds=f"{self.clock()-self.started:.3f}", **self._state)
        self._thread = threading.Thread(target=beat, name="run-a-progress", daemon=True); self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread: self._thread.join(timeout=max(1.0, self.interval + 1.0)); self._thread = None
