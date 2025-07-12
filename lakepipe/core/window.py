"""
Windowing processor for stream DataParts.

Supports tumbling and sliding windows on event-time with a simple watermark
policy.  Falls back to processing-time (wall-clock) if the configured
`time_column` is missing.

NOTE: this is a first, in-memory implementation intended for small to
moderate windows.  Future versions may spill to disk or use external state
stores for very large / long windows.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Dict, List, Optional

import polars as pl

from lakepipe.core.processors import BaseProcessor, DataPart
from lakepipe.core.results import Result, Success, Failure, WindowError
from lakepipe.core.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _parse_duration(s: str) -> timedelta:
    """Parse simple duration strings like "10s", "5m", "2h" into timedelta."""
    s = s.strip().lower()
    if s.endswith("ms"):
        return timedelta(milliseconds=int(s[:-2]))
    if s.endswith("s"):
        return timedelta(seconds=int(s[:-1]))
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    raise WindowError(f"Unsupported duration format: {s}")


# ---------------------------------------------------------------------------
# WindowProcessor
# ---------------------------------------------------------------------------

class WindowProcessor(BaseProcessor[DataPart, DataPart]):
    """Assign incoming records to tumbling or sliding windows and emit them.

    Configuration dictionary keys (with defaults):
    {
        "type": "tumbling" | "sliding",
        "size": "1m",                  # window length (required)
        "slide": "1m",                 # only for sliding windows – defaults to size
        "allowed_lateness": "0s",      # watermark grace period
        "time_column": "timestamp"      # column with event-time
    }
    """

    def __init__(self, cfg: Dict[str, str]):
        super().__init__(cfg)

        self.window_type: str = cfg.get("type", "tumbling").lower()
        if self.window_type not in {"tumbling", "sliding"}:
            raise WindowError("Window type must be 'tumbling' or 'sliding'")

        self.size: timedelta = _parse_duration(cfg["size"])
        self.slide: timedelta = _parse_duration(cfg.get("slide", cfg["size"]))
        self.allowed_lateness: timedelta = _parse_duration(cfg.get("allowed_lateness", "0s"))
        self.time_col: str = cfg.get("time_column", "timestamp")

        # Internal state
        self._windows: Dict[datetime, List[DataPart]] = defaultdict(list)
        self._max_event_time: datetime | None = None  # watermark base

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    async def process(
        self, input_stream: AsyncGenerator[DataPart, None]
    ) -> AsyncGenerator[DataPart, None]:
        async for part in input_stream:
            evt_time = self._extract_event_time(part)
            self._update_watermark(evt_time)
            
            # Assign part to one or more windows
            for win_start in self._windows_for(evt_time):
                self._windows[win_start].append(part)
            
            # Flush finished windows
            for win_start in list(self._windows.keys()):
                win_end = win_start + self.size
                if self._watermark >= win_end + self.allowed_lateness:
                    parts = self._windows.pop(win_start)
                    yield self._emit_window(win_start, parts)
        
        # End of stream – emit all remaining windows
        for win_start, parts in list(self._windows.items()):
            yield self._emit_window(win_start, parts)
            self._windows.pop(win_start)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_event_time(self, part: DataPart) -> datetime:
        """Return event-time for DataPart or fallback to wall-clock now."""
        try:
            # Fast path: metadata may already include event_time
            if "event_time" in part["metadata"]:
                return part["metadata"]["event_time"]

            # Otherwise extract min timestamp of column (cheap for small batches)
            ts_series = part["data"].select(self.time_col).collect()[self.time_col]
            if len(ts_series):
                ts_val = ts_series[0]
                if isinstance(ts_val, datetime):
                    return ts_val
        except Exception:  # noqa: BLE001
            pass
        # Fallback
        return datetime.now(timezone.utc)

    # Watermark maintenance
    @property
    def _watermark(self) -> datetime:
        if self._max_event_time is None:
            return datetime.min.replace(tzinfo=timezone.utc)
        return self._max_event_time - self.allowed_lateness

    def _update_watermark(self, evt_time: datetime) -> None:
        if self._max_event_time is None or evt_time > self._max_event_time:
            self._max_event_time = evt_time

    # Window assignment
    def _windows_for(self, evt_time: datetime):
        if self.window_type == "tumbling":
            start = self._align_to_boundary(evt_time, self.size)
            yield start
        else:  # sliding
            first = self._align_to_boundary(evt_time, self.slide)
            # Generate previous starts within window size
            k = 0
            while True:
                win_start = first - k * self.slide
                if evt_time < win_start:
                    break
                if evt_time >= win_start + self.size:
                    break
                yield win_start
                k += 1

    @staticmethod
    def _align_to_boundary(ts: datetime, step: timedelta) -> datetime:
        epoch = ts.timestamp()
        step_sec = step.total_seconds()
        aligned = epoch - (epoch % step_sec)
        return datetime.fromtimestamp(aligned, tz=ts.tzinfo or timezone.utc)

    # Emit a windowed DataPart
    def _emit_window(self, win_start: datetime, parts: List[DataPart]) -> DataPart:
        # Concatenate LazyFrames efficiently
        lazy_frames = [p["data"] for p in parts]
        combined = pl.concat(lazy_frames).lazy()
        
        # Merge metadata (take first as base)
        base_meta = parts[0]["metadata"].copy()
        base_meta.update({
            "window": {
                "start": win_start,
                "end": win_start + self.size,
                "row_count": sum(p["metadata"].get("record_count", 0) for p in parts),
            }
        })
        
        return DataPart(
            data=combined,
            metadata=base_meta,
            source_info=parts[0]["source_info"],
            schema=parts[0]["schema"],
        ) 