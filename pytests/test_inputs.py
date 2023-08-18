import asyncio
from datetime import timedelta

from bytewax.inputs import AsyncBatcher, Batcher
from pytest import raises


def test_batcher():
    batcher = Batcher(range(5), 3)
    assert batcher.next_batch() == [0, 1, 2]
    assert batcher.next_batch() == [3, 4]
    with raises(StopIteration):
        batcher.next_batch()
    with raises(StopIteration):
        batcher.next_batch()


def test_batcher_skip():
    batcher = Batcher(range(5), 3)
    assert batcher.next_batch() == [0, 1, 2]
    batcher.skip(1)
    assert batcher.next_batch() == [4]
    with raises(StopIteration):
        batcher.next_batch()


async def _gen():
    for i in range(5):
        await asyncio.sleep(0)
        yield i


def test_async_batcher():
    batcher = AsyncBatcher(_gen(), timeout=timedelta(seconds=10), batch_size=2)
    assert batcher.next_batch() == [0, 1]
    assert batcher.next_batch() == [2, 3]
    assert batcher.next_batch() == [4]
    with raises(StopIteration):
        batcher.next_batch()
