# postgresql-lock imports
from postgresql_lock.asyncpg import acquire
from postgresql_lock.asyncpg import acquire_async
from postgresql_lock.asyncpg import handle_error
from postgresql_lock.asyncpg import handle_error_async
from postgresql_lock.asyncpg import release
from postgresql_lock.asyncpg import release_async

# system imports
from typing import Any

from unittest.mock import AsyncMock
from unittest.mock import Mock

# dependency imports
from pytest import mark
from pytest import raises


def test_acquire() -> None:
    with raises(NotImplementedError) as exc:
        acquire(Mock())

    assert str(exc.value) == "ascynpg interface does not support acquire()"


@mark.asyncio
@mark.parametrize("result", [True, False])
async def test_acquire_async__defaults(result: Any) -> None:
    lock = Mock()
    lock.conn.fetchval = AsyncMock(return_value=result)

    lock_func = lock.blocking_lock_func

    if result:
        assert await acquire_async(lock)

    else:
        assert not await acquire_async(lock)

    lock.conn.fetchval.assert_called_with(
        f"SELECT COALESCE(pg_catalog.{lock_func}({lock.lock_id}), true)"
    )


@mark.asyncio
@mark.parametrize("result", [True, False])
async def test_acquire_async__block_false(result: Any) -> None:
    lock = Mock()
    lock.conn.fetchval = AsyncMock(return_value=result)

    lock_func = lock.nonblocking_lock_func

    if result:
        assert await acquire_async(lock, block=False)

    else:
        assert not await acquire_async(lock, block=False)

    lock.conn.fetchval.assert_called_with(
        f"SELECT COALESCE(pg_catalog.{lock_func}({lock.lock_id}), true)"
    )


@mark.asyncio
@mark.parametrize("result", [True, False])
async def test_acquire_async__block_true(result: Any) -> None:
    lock = Mock()
    lock.conn.fetchval = AsyncMock(return_value=result)

    lock_func = lock.blocking_lock_func

    if result:
        assert await acquire_async(lock, block=True)

    else:
        assert not await acquire_async(lock, block=True)

    lock.conn.fetchval.assert_called_with(
        f"SELECT COALESCE(pg_catalog.{lock_func}({lock.lock_id}), true)"
    )


def test_handle_error() -> None:
    with raises(NotImplementedError) as exc:
        handle_error(Mock(), Mock())

    assert str(exc.value) == "ascynpg interface does not support handle_error()"


@mark.asyncio
async def test_handle_error_async() -> None:
    lock = Mock()
    lock.conn.execute = AsyncMock()

    await handle_error_async(lock, Mock())


@mark.asyncio
async def test_handle_error_async__rollback_disabled() -> None:
    lock = Mock(rollback_on_error=False)

    await handle_error_async(lock, Mock())


def test_release() -> None:
    with raises(NotImplementedError) as exc:
        release(Mock())

    assert str(exc.value) == "ascynpg interface does not support release()"


@mark.asyncio
async def test_release_async() -> None:
    lock = Mock()
    lock.conn.fetchval = AsyncMock()

    assert lock.conn.fetchval.return_value == await release_async(lock)

    lock.conn.fetchval.assert_called_with(
        f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"
    )
