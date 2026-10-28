import pytest

from stac_geoparquet.arrow import _memory


@pytest.fixture
def headroom(monkeypatch):
    def set_headroom(n):
        monkeypatch.setattr(_memory, "memory_headroom", lambda: n)

    return set_headroom


def _items(n, size=1000, geometry_size=0):
    return [
        {"id": str(i), "data": "x" * size, "geometry": b"x" * geometry_size}
        for i in range(n)
    ]


def _headroom_for(items):
    """The memory headroom at which a chunk ends once it has these items."""
    geometry = sum(len(item["geometry"]) for item in items)
    rest = sum(_memory.deep_size(item) for item in items) - geometry
    estimate = rest + (1 + _memory.GEOMETRY_COPIES) * geometry
    return int(estimate / _memory.MARGIN) + _memory.RESERVED_BYTES


def test_chunks_within_memory_ends_chunks_early(headroom):
    items = _items(2000)
    headroom(_headroom_for(items[:600]))

    chunks = list(_memory.chunks_within_memory(items, chunk_size=1000))

    # The size is checked every SAMPLE_EVERY items
    assert [len(c) for c in chunks] == [768, 768, 464]
    assert [item for chunk in chunks for item in chunk] == items


def test_chunks_within_memory_estimates_each_chunk(headroom):
    small = _items(1000, size=100)
    large = _items(1000, size=10_000)
    headroom(_headroom_for(large[:300]))

    chunks = list(_memory.chunks_within_memory(small + large, chunk_size=1000))

    assert [len(c) for c in chunks] == [1000, 512, 488]


def test_chunks_within_memory_counts_every_geometry(headroom):
    items = _items(2000)
    headroom(_headroom_for(items[:600]))
    # Not one of the items whose size is sampled
    items[300]["geometry"] = b"x" * 10_000_000

    chunks = list(_memory.chunks_within_memory(items, chunk_size=1000))

    assert len(chunks[0]) == 512


def test_chunks_within_memory_with_room(headroom):
    headroom(10**12)

    chunks = list(_memory.chunks_within_memory(_items(2500), chunk_size=1000))

    assert [len(c) for c in chunks] == [1000, 1000, 500]


def test_chunks_within_memory_needs_a_chunk_size():
    with pytest.raises(ValueError):
        next(_memory.chunks_within_memory([{}], chunk_size=0))


@pytest.fixture
def cgroup(monkeypatch, tmp_path):
    root = tmp_path / "cgroup"
    proc = tmp_path / "proc_cgroup"
    monkeypatch.setattr(_memory, "CGROUP_ROOT", root)
    monkeypatch.setattr(_memory, "PROC_CGROUP", proc)

    def make(proc_cgroup, directories):
        proc.write_text(proc_cgroup)
        for path, files in directories.items():
            (root / path).mkdir(parents=True, exist_ok=True)
            for name, text in files.items():
                (root / path / name).write_text(text)

    return make


def _cgroup_v2_files(limit, current, inactive):
    return {
        "memory.max": limit,
        "memory.current": current,
        "memory.stat": f"inactive_file {inactive}\n",
    }


def _cgroup_v1_files(limit, usage, inactive):
    return {
        "memory.limit_in_bytes": limit,
        "memory.usage_in_bytes": usage,
        "memory.stat": f"total_inactive_file {inactive}\n",
    }


def test_cgroup_v2_headroom(cgroup):
    cgroup(
        "0::/outer/inner\n",
        {
            ".": {"cgroup.controllers": "memory"},
            "outer": _cgroup_v2_files("1000", "300", "100"),
            "outer/inner": _cgroup_v2_files("max", "200", "0"),
        },
    )

    # Only the outer cgroup has a limit: 1000 - (300 - 100)
    assert _memory.cgroup_headroom() == 800


def test_cgroup_v2_without_a_limit(cgroup):
    cgroup("0::/\n", {".": {"cgroup.controllers": "memory"}})

    assert _memory.cgroup_headroom() is None


def test_cgroup_v1_headroom(cgroup):
    cgroup(
        "5:cpu,cpuacct:/job\n4:memory:/job\n",
        {
            "memory": _cgroup_v1_files("10000", "400", "0"),
            "memory/job": _cgroup_v1_files("1000", "400", "50"),
        },
    )

    assert _memory.cgroup_headroom() == 650


def test_cgroup_v1_in_a_container(cgroup):
    # The cgroup's own files are at the root, not at the path it's listed under
    cgroup("4:memory:/docker/abc\n", {"memory": _cgroup_v1_files("1000", "400", "50")})

    assert _memory.cgroup_headroom() == 650


def test_cgroup_v1_on_a_hybrid_host(cgroup):
    cgroup(
        "0::/unified\n4:memory:/\n", {"memory": _cgroup_v1_files("1000", "400", "50")}
    )

    assert _memory.cgroup_headroom() == 650


def test_cgroup_v1_without_a_limit(cgroup):
    cgroup(
        "4:memory:/\n", {"memory": _cgroup_v1_files("9223372036854771712", "400", "0")}
    )

    assert _memory.cgroup_headroom() is None


def test_memory_cap_turned_off(headroom, monkeypatch):
    monkeypatch.setenv("STAC_GEOPARQUET_MEMORY_CAP", "false")
    items = _items(2000)
    headroom(_headroom_for(items[:600]))

    chunks = list(_memory.chunks_within_memory(items, chunk_size=1000))

    assert [len(c) for c in chunks] == [1000, 1000]
