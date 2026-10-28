"""Split items into chunks that fit in the memory available."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import psutil

from stac_geoparquet.arrow._util import env_flag

logger = logging.getLogger(__name__)

# Converting and writing a chunk holds its items as Python objects, plus
# GEOMETRY_COPIES more copies of each geometry: the Arrow column and the Parquet
# writer's buffer. MARGIN leaves room for the Arrow arrays of the other fields.
GEOMETRY_COPIES = 2
MARGIN = 0.8
# Left for memory outside the chunk, like the rows streaming in from pgstac
RESERVED_BYTES = 100_000_000

# Estimate a chunk's size from every this many items
SAMPLE_EVERY = 256

PROC_CGROUP = Path("/proc/self/cgroup")
CGROUP_ROOT = Path("/sys/fs/cgroup")


def memory_headroom() -> int:
    """
    How many more bytes this process can use: the system's available memory, or
    what's left under its cgroup (container) memory limit if that is less.
    """
    available = psutil.virtual_memory().available
    cgroup = cgroup_headroom()
    return available if cgroup is None else min(available, cgroup)


def cgroup_headroom() -> int | None:
    """
    The least memory left under any of this process's cgroup limits, or None if
    there are none. Inactive file cache counts as free: the kernel can reclaim it.
    """
    try:
        lines = PROC_CGROUP.read_text().splitlines()
    except OSError:
        return None
    if (CGROUP_ROOT / "cgroup.controllers").exists():
        root = CGROUP_ROOT
        path = next((line[3:] for line in lines if line.startswith("0::")), "/")
        files = ("memory.max", "memory.current", "inactive_file")
    else:
        # cgroup v1, which hybrid hosts also use for memory
        root = CGROUP_ROOT / "memory"
        controllers = (line.split(":", 2) for line in lines)
        path = next((p for _, c, p in controllers if "memory" in c.split(",")), "/")
        files = (
            "memory.limit_in_bytes",
            "memory.usage_in_bytes",
            "total_inactive_file",
        )
    # Any parent can have a limit, and in a container the cgroup is usually the root
    cgroup = root / path.lstrip("/")
    directories = [d for d in [cgroup, *cgroup.parents] if d.is_relative_to(root)]
    headrooms = [_headroom(directory, *files) for directory in directories]
    return min((h for h in headrooms if h is not None), default=None)


def _headroom(
    path: Path, limit_file: str, usage_file: str, inactive_key: str
) -> int | None:
    try:
        text = (path / limit_file).read_text().strip()
        if text == "max":
            return None
        limit = int(text)
        usage = int((path / usage_file).read_text())
        stat = dict(
            line.split() for line in (path / "memory.stat").read_text().splitlines()
        )
    except (OSError, ValueError):
        return None
    # cgroup v1 shows no limit as a number larger than any machine's memory
    if limit >= psutil.virtual_memory().total:
        return None
    return limit - (usage - int(stat.get(inactive_key, 0)))


def deep_size(obj: Any) -> int:
    """Approximate bytes held by a JSON-like Python object."""
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        for key, value in obj.items():
            size += sys.getsizeof(key) + deep_size(value)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            size += deep_size(value)
    return size


def chunks_within_memory(
    items: Iterable[dict[str, Any]], chunk_size: int
) -> Iterator[list[dict[str, Any]]]:
    """
    Yield lists of up to `chunk_size` items, ending a list early if converting it
    would take more memory than is available when the first list is asked for,
    unless the `STAC_GEOPARQUET_MEMORY_CAP` environment variable is false.
    """
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least one")
    capped = env_flag("STAC_GEOPARQUET_MEMORY_CAP", True)
    headroom = memory_headroom()
    budget = int((headroom - RESERVED_BYTES) * MARGIN)
    chunk: list[dict[str, Any]] = []
    geometry_bytes = sampled_bytes = 0
    for item in items:
        chunk.append(item)
        # Count every geometry: one can be far larger than the rest of the items
        geometry_bytes += len(item["geometry"])
        if len(chunk) % SAMPLE_EVERY == 1:
            sampled_bytes += deep_size(item) - len(item["geometry"])
        full = len(chunk) >= chunk_size
        if capped and not full and len(chunk) % SAMPLE_EVERY == 0:
            estimate = (
                SAMPLE_EVERY * sampled_bytes + (1 + GEOMETRY_COPIES) * geometry_bytes
            )
            if estimate >= budget:
                logger.info(
                    f"Ending a chunk at {len(chunk)} items to stay within the"
                    f" {headroom / 1e6:.0f} MB of memory available"
                )
                full = True
        if full:
            yield chunk
            chunk = []
            geometry_bytes = sampled_bytes = 0
    if chunk:
        yield chunk
