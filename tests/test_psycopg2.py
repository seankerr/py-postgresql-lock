# postgresql-lock imports
from postgresql_lock.psycopg2 import acquire
from postgresql_lock.psycopg2 import acquire_async
from postgresql_lock.psycopg2 import handle_error
from postgresql_lock.psycopg2 import handle_error_async
from postgresql_lock.psycopg2 import release
from postgresql_lock.psycopg2 import release_async

# system imports
from typing import Any

from unittest.mock import Mock

# dependency imports
from pytest import mark
from pytest import raises


@mark.parametrize("result", [None, True, False])
def test_acquire__defaults(result: Any) -> None:
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
def test_acquire__block_false(result: Any) -> None:
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


@mark.parametrize("result", [None, True, False])
def test_acquire__block_true(result: Any) -> None:
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
async def test_acquire_async() -> None:
    with raises(NotImplementedError) as exc:
        await acquire_async(Mock())

    assert str(exc.value) == "psycopg2 interface does not support acquire_async()"


def test_handle_error() -> None:
    lock = Mock()

    handle_error(lock, Mock())

    lock.conn.rollback.assert_called_once()


def test_handle_error__rollback_disabled() -> None:
    lock = Mock(rollback_on_error=False)

    handle_error(lock, Mock())


@mark.asyncio
async def test_handle_error_async() -> None:
    with raises(NotImplementedError) as exc:
        await handle_error_async(Mock(), Mock())

    assert str(exc.value) == "psycopg2 interface does not support handle_error_async()"


@mark.parametrize("result", [True, False])
def test_release(result: Any) -> None:
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
async def test_release_async() -> None:
    with raises(NotImplementedError) as exc:
        await release_async(Mock())

    assert str(exc.value) == "psycopg2 interface does not support release_async()"
