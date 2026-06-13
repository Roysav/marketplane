import pytest

from apiserver.records.memory import MemoryRecordStorage


@pytest.fixture
def storage() -> MemoryRecordStorage:
    return MemoryRecordStorage()


@pytest.mark.asyncio
async def test_set_and_get(storage: MemoryRecordStorage) -> None:
    await storage.set("k1", b"v1", [])
    assert await storage.get("k1") == b"v1"


@pytest.mark.asyncio
async def test_get_missing_key_raises(storage: MemoryRecordStorage) -> None:
    with pytest.raises(KeyError):
        await storage.get("missing")


@pytest.mark.asyncio
async def test_set_overwrites(storage: MemoryRecordStorage) -> None:
    await storage.set("k1", b"v1", [])
    await storage.set("k1", b"v2", [])
    assert await storage.get("k1") == b"v2"


@pytest.mark.asyncio
async def test_list_none_indices_no_filter(storage: MemoryRecordStorage) -> None:
    await storage.set("foo/1", b"a", [])
    await storage.set("foo/2", b"b", [])
    await storage.set("bar/1", b"c", [])
    result = await storage.list("foo/", None)
    assert sorted(result) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_list_empty_indices_returns_empty(storage: MemoryRecordStorage) -> None:
    await storage.set("foo/1", b"a", [])
    result = await storage.list("foo/", [])
    assert result == []


@pytest.mark.asyncio
async def test_list_by_index(storage: MemoryRecordStorage) -> None:
    await storage.set("k1", b"v1", ["env=prod"])
    await storage.set("k2", b"v2", ["env=staging"])
    await storage.set("k3", b"v3", ["env=prod"])
    result = await storage.list("", ["env=prod"])
    assert sorted(result) == [b"v1", b"v3"]


@pytest.mark.asyncio
async def test_list_by_prefix_and_index(storage: MemoryRecordStorage) -> None:
    await storage.set("foo/1", b"a", ["env=prod"])
    await storage.set("foo/2", b"b", ["env=staging"])
    await storage.set("bar/1", b"c", ["env=prod"])
    result = await storage.list("foo/", ["env=prod"])
    assert result == [b"a"]


@pytest.mark.asyncio
async def test_list_multiple_indices_intersection(storage: MemoryRecordStorage) -> None:
    await storage.set("k1", b"v1", ["env=prod", "region=us"])
    await storage.set("k2", b"v2", ["env=prod", "region=eu"])
    await storage.set("k3", b"v3", ["env=staging", "region=us"])
    result = await storage.list("", ["env=prod", "region=us"])
    assert result == [b"v1"]


@pytest.mark.asyncio
async def test_list_none_indices_empty_store_returns_empty(storage: MemoryRecordStorage) -> None:
    result = await storage.list("foo/", None)
    assert result == []


@pytest.mark.asyncio
async def test_list_index_no_match_returns_empty(storage: MemoryRecordStorage) -> None:
    await storage.set("k1", b"v1", ["env=prod"])
    result = await storage.list("", ["env=staging"])
    assert result == []
