#!/usr/bin/env python3
"""Basic unit tests for WindowProcessor.

Run with ``python test_windows.py`` or via ``pytest``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List

import polars as pl

from lakepipe.core.window import WindowProcessor
from lakepipe.core.processors import DataPart


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_part(idx: int, ts: datetime) -> DataPart:
    """Create a DataPart with single row containing id and ts."""
    df = pl.DataFrame({"id": [idx], "ts": [ts]})
    return DataPart(
        data=df.lazy(),
        metadata={"record_count": 1},
        source_info={"uri": "test://"},
        schema={"columns": ["id", "ts"]},
    )


async def collect_parts(proc: WindowProcessor, inp: List[DataPart]):
    async def gen():
        for part in inp:
            yield part
    out_parts = []
    async for out in proc.process(gen()):
        out_parts.append(out)
    return out_parts


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_tumbling():
    """Tumbling 2-second window groups rows correctly."""
    base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    events = [make_part(i, base + timedelta(seconds=i)) for i in range(5)]  # 0..4 s

    proc = WindowProcessor(
        {
            "type": "tumbling",
            "size": "2s",
            "allowed_lateness": "0s",
            "time_column": "ts",
        }
    )

    out_parts = asyncio.run(collect_parts(proc, events))

    # Expect 3 windows: [0-2), [2-4), [4-6)
    assert len(out_parts) == 3, f"expected 3 windows, got {len(out_parts)}"

    counts = [p["metadata"]["window"]["row_count"] for p in out_parts]
    assert counts == [2, 2, 1], f"row counts per window incorrect: {counts}"
    print("✅ tumbling window test passed")


def test_sliding():
    """Sliding window should create overlapping windows."""
    base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    events = [make_part(i, base + timedelta(seconds=i)) for i in range(4)]  # 0..3s

    proc = WindowProcessor(
        {
            "type": "sliding",
            "size": "3s",
            "slide": "1s",
            "allowed_lateness": "0s",
            "time_column": "ts",
        }
    )

    out_parts = asyncio.run(collect_parts(proc, events))

    # There should be 4 windows starting at t=0,1,2,3
    starts = [p["metadata"]["window"]["start"] for p in out_parts]
    starts_sec = [(s - base).total_seconds() for s in starts]
    assert starts_sec == [0, 1, 2, 3], f"unexpected window starts: {starts_sec}"

    # First window covers events 0-2, second 1-3, etc.
    counts = [p["metadata"]["window"]["row_count"] for p in out_parts]
    assert counts == [3, 3, 2, 1], f"unexpected counts {counts}"
    print("✅ sliding window test passed")


if __name__ == "__main__":
    test_tumbling()
    test_sliding()
    print("🎉 All WindowProcessor tests passed!") 