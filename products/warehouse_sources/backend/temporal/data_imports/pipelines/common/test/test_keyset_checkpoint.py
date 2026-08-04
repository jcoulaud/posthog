from __future__ import annotations

from typing import Any

import pytest
from unittest.mock import AsyncMock

import pyarrow as pa

from products.warehouse_sources.backend.temporal.data_imports.pipelines.common.extract import (
    persist_keyset_resume_state,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumePlan
from products.warehouse_sources.backend.temporal.data_imports.sources.common.sql.keyset import KeysetResumeState


class _RecordingManager:
    def __init__(self):
        self.saved: list[Any] = []
        self.cleared = 0

    def save_state(self, data: KeysetResumeState) -> None:
        self.saved.append(data.last_key)

    def clear_state(self) -> None:
        self.cleared += 1


def _table(ids: list[int]) -> pa.Table:
    return pa.table({"id": ids, "body": [f"r{i}" for i in ids]})


def _plan(manager: _RecordingManager, keyset_column: str | None) -> Any:
    return ResumePlan(manager=manager, keyset_column=keyset_column)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_persists_max_key_of_committed_chunk():
    manager = _RecordingManager()
    await persist_keyset_resume_state(_plan(manager, "id"), _table([3, 1, 2]), AsyncMock())
    # Max, not last-appended, so an out-of-order chunk can't move the checkpoint backwards.
    assert manager.saved == [3]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "keyset_column,ids",
    [
        (None, [1, 2]),  # source checkpoints itself — the pipeline must not write over its state
        ("id", []),  # empty chunk carries no committed key
        ("other", [1, 2]),  # column projected out of the chunk
    ],
)
async def test_noop_cases(keyset_column, ids):
    manager = _RecordingManager()
    await persist_keyset_resume_state(_plan(manager, keyset_column), _table(ids), AsyncMock())
    assert manager.saved == []


@pytest.mark.asyncio
async def test_noop_without_plan():
    # No exception when the run isn't resumable at all.
    await persist_keyset_resume_state(None, _table([1, 2]), AsyncMock())


@pytest.mark.parametrize(
    "keyset_column,expected_clears",
    [("id", 1), (None, 0)],
)
def test_clear_pipeline_checkpoint_only_for_keyset_runs(keyset_column, expected_clears):
    # A source that checkpoints itself owns when its state is dropped; the pipeline must not guess.
    manager = _RecordingManager()
    _plan(manager, keyset_column).clear_pipeline_checkpoint()
    assert manager.cleared == expected_clears
