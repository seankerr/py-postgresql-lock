# postgresql-lock imports
from postgresql_lock.sqlalchemy import acquire
from postgresql_lock.sqlalchemy import acquire_async
from postgresql_lock.sqlalchemy import handle_error
from postgresql_lock.sqlalchemy import handle_error_async
from postgresql_lock.sqlalchemy import release
from postgresql_lock.sqlalchemy import release_async

# system imports
from typing import Any

from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

# dependency imports
from pytest import mark

PATH = "postgresql_lock.sqlalchemy"


@mark.parametrize("result", [None, True, False])
@patch(f"{PATH}.text")
def test_acquire__defaults(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute.return_value = Mock(scalar=scalar)

    lock_func = lock.blocking_lock_func

    if result is True:
        assert acquire(lock)

    elif result is None:
        assert acquire(lock)

    elif result is False:
        assert not acquire(lock)

    text.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


@mark.parametrize("result", [None, True, False])
@patch(f"{PATH}.text")
def test_acquire__block_false(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute.return_value = Mock(scalar=scalar)

    lock_func = lock.nonblocking_lock_func

    if result is True:
        assert acquire(lock, block=False)

    elif result is None:
        assert acquire(lock, block=False)

    elif result is False:
        assert not acquire(lock, block=False)

    text.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


@mark.parametrize("result", [None, True, False])
@patch(f"{PATH}.text")
def test_acquire__block_true(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute.return_value = Mock(scalar=scalar)

    lock_func = lock.blocking_lock_func

    if result is True:
        assert acquire(lock, block=True)

    elif result is None:
        assert acquire(lock, block=True)

    elif result is False:
        assert not acquire(lock, block=True)

    text.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


@mark.asyncio
@mark.parametrize("result", [None, True, False])
@patch(f"{PATH}.text")
async def test_acquire_async__defaults(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute = AsyncMock(return_value=Mock(scalar=scalar))

    lock_func = lock.blocking_lock_func

    if result is True:
        assert await acquire_async(lock)

    elif result is None:
        assert await acquire_async(lock)

    elif result is False:
        assert not await acquire_async(lock)

    text.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


@mark.asyncio
@mark.parametrize("result", [None, True, False])
@patch(f"{PATH}.text")
async def test_acquire_async__block_false(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute = AsyncMock(return_value=Mock(scalar=scalar))

    lock_func = lock.nonblocking_lock_func

    if result is True:
        assert await acquire_async(lock, block=False)

    elif result is None:
        assert await acquire_async(lock, block=False)

    elif result is False:
        assert not await acquire_async(lock, block=False)

    text.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


@mark.asyncio
@mark.parametrize("result", [None, True, False])
@patch(f"{PATH}.text")
async def test_acquire_async__block_true(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute = AsyncMock(return_value=Mock(scalar=scalar))

    lock_func = lock.blocking_lock_func

    if result is True:
        assert await acquire_async(lock, block=True)

    elif result is None:
        assert await acquire_async(lock, block=True)

    elif result is False:
        assert not await acquire_async(lock, block=True)

    text.assert_called_with(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


def test_handle_error() -> None:
    lock = Mock()

    handle_error(lock, Mock())

    lock.conn.rollback.assert_called_once()


def test_handle_error__rollback_disabled() -> None:
    lock = Mock(rollback_on_error=False)

    handle_error(lock, Mock())


@mark.asyncio
async def test_handle_error_async() -> None:
    lock = Mock()
    lock.conn.rollback = AsyncMock()

    await handle_error_async(lock, Mock())

    lock.conn.rollback.assert_called_once()


@mark.asyncio
async def test_handle_error_async__rollback_disabled() -> None:
    lock = Mock(rollback_on_error=False)

    await handle_error_async(lock, Mock())


@mark.parametrize("result", [True, False])
@patch(f"{PATH}.text")
def test_release(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute.return_value = Mock(scalar=scalar)

    assert result == release(lock)

    text.assert_called_with(f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())


@mark.asyncio
@mark.parametrize("result", [True, False])
@patch(f"{PATH}.text")
async def test_release_async(text: Mock, result: Any) -> None:
    lock = Mock()
    scalar = Mock(return_value=result)
    lock.conn.execute = AsyncMock(return_value=Mock(scalar=scalar))

    assert result == await release_async(lock)

    text.assert_called_with(f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})")
    lock.conn.execute.assert_called_with(text())
