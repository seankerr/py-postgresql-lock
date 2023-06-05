from postgres_lock.asyncpg import acquire
from postgres_lock.asyncpg import acquire_async
from postgres_lock.asyncpg import handle_error
from postgres_lock.asyncpg import handle_error_async
from postgres_lock.asyncpg import release
from postgres_lock.asyncpg import release_async

from pytest import mark
from pytest import raises

from unittest.mock import AsyncMock
from unittest.mock import Mock


def test_acquire():
    with raises(NotImplementedError) as exc:
        acquire(None)

    assert str(exc.value) == "ascynpg interface does not support acquire()"


@mark.asyncio
@mark.parametrize("result", [None, True, False])
async def test_acquire_async__defaults(result):
    lock = Mock()
    lock.conn.fetchval = AsyncMock(return_value=result)

    lock_func = lock.blocking_lock_func

    if result is True:
        assert await acquire_async(lock)

    elif result is None:
        assert await acquire_async(lock)

    elif result is False:
        assert not await acquire_async(lock)

    lock.conn.fetchval.assert_called_with(
        f"SELECT pg_catalog.{lock_func}({lock.lock_id})"
    )


@mark.asyncio
@mark.parametrize("result", [None, True, False])
async def test_acquire_async__block_false(result):
    lock = Mock()
    lock.conn.fetchval = AsyncMock(return_value=result)

    lock_func = lock.nonblocking_lock_func

    if result is True:
        assert await acquire_async(lock, block=False)

    elif result is None:
        assert await acquire_async(lock, block=False)

    elif result is False:
        assert not await acquire_async(lock, block=False)

    lock.conn.fetchval.assert_called_with(
        f"SELECT pg_catalog.{lock_func}({lock.lock_id})"
    )


@mark.asyncio
@mark.parametrize("result", [None, True, False])
async def test_acquire_async__block_true(result):
    lock = Mock()
    lock.conn.fetchval = AsyncMock(return_value=result)

    lock_func = lock.blocking_lock_func

    if result is True:
        assert await acquire_async(lock, block=True)

    elif result is None:
        assert await acquire_async(lock, block=True)

    elif result is False:
        assert not await acquire_async(lock, block=True)

    lock.conn.fetchval.assert_called_with(
        f"SELECT pg_catalog.{lock_func}({lock.lock_id})"
    )


def test_handle_error():
    with raises(NotImplementedError) as exc:
        handle_error(None)

    assert str(exc.value) == "ascynpg interface does not support handle_error()"


@mark.asyncio
async def test_handle_error_async():
    lock = Mock()
    lock.conn.execute = AsyncMock()

    await handle_error_async(lock)


@mark.asyncio
async def test_handle_error_async__rollback_disabled():
    lock = Mock(rollback_on_error=False)

    await handle_error_async(lock)


def test_release():
    with raises(NotImplementedError) as exc:
        release(None)

    assert str(exc.value) == "ascynpg interface does not support release()"


@mark.asyncio
async def test_release_async():
    lock = Mock()
    lock.conn.fetchval = AsyncMock()

    assert lock.conn.fetchval.return_value == await release_async(lock)

    lock.conn.fetchval.assert_called_with(
        f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"
    )
