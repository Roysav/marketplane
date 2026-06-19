import pytest

from apiserver.records.storage.exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch
from apiserver.records.storage.memory import MemoryRecordStorage
from apiserver.types import Subject


@pytest.fixture
def storage() -> MemoryRecordStorage:
    return MemoryRecordStorage()


def _subject(name: str, type_: str = "t", tradespace: str = "s") -> Subject:
    return Subject(type=type_, tradespace=tradespace, name=name)


@pytest.mark.asyncio
async def test_create_and_get(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", [])
    assert await storage.get(_subject("k1")) == b"v1"


@pytest.mark.asyncio
async def test_get_missing_key_raises(storage: MemoryRecordStorage) -> None:
    with pytest.raises(KeyNotFound):
        await storage.get(_subject("missing"))


@pytest.mark.asyncio
async def test_create_existing_key_raises(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", [])
    with pytest.raises(KeyAlreadyExists):
        await storage.create(_subject("k1"), b"v2", [])


@pytest.mark.asyncio
async def test_update_bumps_revision(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", [])
    await storage.update(_subject("k1"), b"v2", [], 1)
    assert await storage.get(_subject("k1")) == b"v2"


@pytest.mark.asyncio
async def test_update_wrong_revision_raises(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", [])
    with pytest.raises(RevisionMismatch):
        await storage.update(_subject("k1"), b"v2", [], 2)


@pytest.mark.asyncio
async def test_update_missing_key_raises(storage: MemoryRecordStorage) -> None:
    with pytest.raises(RevisionMismatch):
        await storage.update(_subject("missing"), b"v", [], 1)


@pytest.mark.asyncio
async def test_update_replaces_indices(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", ["env=prod"])
    await storage.update(_subject("k1"), b"v2", ["env=staging"], 1)
    assert await storage.list("", ["env=prod"]) == []
    assert await storage.list("", ["env=staging"]) == [b"v2"]


@pytest.mark.asyncio
async def test_set_inserts_then_overwrites_and_increments_revision(storage: MemoryRecordStorage) -> None:
    await storage.set(_subject("k1"), b"v1", ["env=prod"])
    assert await storage.get(_subject("k1")) == b"v1"
    assert storage._revisions[_subject("k1").key()] == 0
    await storage.set(_subject("k1"), b"v2", ["env=staging"])
    assert await storage.get(_subject("k1")) == b"v2"
    assert storage._revisions[_subject("k1").key()] == 1
    assert await storage.list("", ["env=prod"]) == []
    assert await storage.list("", ["env=staging"]) == [b"v2"]


@pytest.mark.asyncio
async def test_list_none_indices_no_filter(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("1", type_="foo"), b"a", [])
    await storage.create(_subject("2", type_="foo"), b"b", [])
    await storage.create(_subject("1", type_="bar"), b"c", [])
    result = await storage.list("foo/", None)
    assert sorted(result) == [b"a", b"b"]


@pytest.mark.asyncio
async def test_list_empty_indices_returns_empty(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("1", type_="foo"), b"a", [])
    result = await storage.list("foo/", [])
    assert result == []


@pytest.mark.asyncio
async def test_list_by_index(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", ["env=prod"])
    await storage.create(_subject("k2"), b"v2", ["env=staging"])
    await storage.create(_subject("k3"), b"v3", ["env=prod"])
    result = await storage.list("", ["env=prod"])
    assert sorted(result) == [b"v1", b"v3"]


@pytest.mark.asyncio
async def test_list_by_prefix_and_index(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("1", type_="foo"), b"a", ["env=prod"])
    await storage.create(_subject("2", type_="foo"), b"b", ["env=staging"])
    await storage.create(_subject("1", type_="bar"), b"c", ["env=prod"])
    result = await storage.list("foo/", ["env=prod"])
    assert result == [b"a"]


@pytest.mark.asyncio
async def test_list_multiple_indices_intersection(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", ["env=prod", "region=us"])
    await storage.create(_subject("k2"), b"v2", ["env=prod", "region=eu"])
    await storage.create(_subject("k3"), b"v3", ["env=staging", "region=us"])
    result = await storage.list("", ["env=prod", "region=us"])
    assert result == [b"v1"]


@pytest.mark.asyncio
async def test_list_none_indices_empty_store_returns_empty(storage: MemoryRecordStorage) -> None:
    result = await storage.list("foo/", None)
    assert result == []


@pytest.mark.asyncio
async def test_list_index_no_match_returns_empty(storage: MemoryRecordStorage) -> None:
    await storage.create(_subject("k1"), b"v1", ["env=prod"])
    result = await storage.list("", ["env=staging"])
    assert result == []
