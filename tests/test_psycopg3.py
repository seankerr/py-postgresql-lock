from postgres_lock.psycopg3 import acquire
from postgres_lock.psycopg3 import acquire_async
from postgres_lock.psycopg3 import handle_error
from postgres_lock.psycopg3 import handle_error_async
from postgres_lock.psycopg3 import release
from postgres_lock.psycopg3 import release_async

from pytest import mark

from unittest.mock import AsyncMock
from unittest.mock import Mock


@mark.parametrize("result", [None, True, False])
def test_acquire__defaults(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.fetchone = Mock(return_value=[result])

    lock_func = lock.blocking_lock_func

    if result is True:
        assert acquire(lock)

    elif result is None:
        assert acquire(lock)

    elif result is False:
        assert not acquire(lock)

    cursor.execute.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    cursor.close.assert_called_once()


@mark.parametrize("result", [None, True, False])
def test_acquire__block_false(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.fetchone = Mock(return_value=[result])

    lock_func = lock.nonblocking_lock_func

    if result is True:
        assert acquire(lock, block=False)

    elif result is None:
        assert acquire(lock, block=False)

    elif result is False:
        assert not acquire(lock, block=False)

    cursor.execute.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    cursor.close.assert_called_once()


@mark.parametrize("result", [None, True, False])
def test_acquire__block_true(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.fetchone = Mock(return_value=[result])

    lock_func = lock.blocking_lock_func

    if result is True:
        assert acquire(lock, block=True)

    elif result is None:
        assert acquire(lock, block=True)

    elif result is False:
        assert not acquire(lock, block=True)

    cursor.execute.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    cursor.close.assert_called_once()


@mark.asyncio
@mark.parametrize("result", [None, True, False])
async def test_acquire_async__defaults(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=[result])

    lock_func = lock.blocking_lock_func

    if result is True:
        assert await acquire_async(lock)

    elif result is None:
        assert await acquire_async(lock)

    elif result is False:
        assert not await acquire_async(lock)

    cursor.execute.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    cursor.close.assert_called_once()


@mark.asyncio
@mark.parametrize("result", [None, True, False])
async def test_acquire_async__block_false(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=[result])

    lock_func = lock.nonblocking_lock_func

    if result is True:
        assert await acquire_async(lock, block=False)

    elif result is None:
        assert await acquire_async(lock, block=False)

    elif result is False:
        assert not await acquire_async(lock, block=False)

    cursor.execute.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    cursor.close.assert_called_once()


@mark.asyncio
@mark.parametrize("result", [None, True, False])
async def test_acquire_async__block_true(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=[result])

    lock_func = lock.blocking_lock_func

    if result is True:
        assert await acquire_async(lock, block=True)

    elif result is None:
        assert await acquire_async(lock, block=True)

    elif result is False:
        assert not await acquire_async(lock, block=True)

    cursor.execute.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    cursor.close.assert_called_once()


def test_handle_error():
    lock = Mock()

    handle_error(lock, None)

    lock.conn.rollback.assert_called_once()


def test_handle_error__rollback_disabled():
    lock = Mock(rollback_on_error=False)

    handle_error(lock, None)


@mark.asyncio
async def test_handle_error_async():
    lock = Mock()
    lock.conn.rollback = AsyncMock()

    await handle_error_async(lock, None)

    lock.conn.rollback.assert_called_once()


@mark.asyncio
async def test_handle_error_async__rollback_disabled():
    lock = Mock(rollback_on_error=False)

    await handle_error_async(lock, None)


@mark.parametrize("result", [True, False])
def test_release(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    result = Mock()
    cursor.fetchone = Mock(return_value=[result])

    assert result == release(lock)

    cursor.execute.assert_called_with(
        f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"
    )
    cursor.close.assert_called_once()


@mark.asyncio
@mark.parametrize("result", [True, False])
async def test_release_async(result):
    cursor = Mock()
    lock = Mock()
    lock.conn.cursor.return_value = cursor
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=[result])

    assert result == await release_async(lock)

    cursor.execute.assert_called_with(
        f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"
    )
    cursor.close.assert_called_once()
