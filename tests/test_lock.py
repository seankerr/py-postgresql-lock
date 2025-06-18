# postgresql-lock imports
from postgresql_lock import Lock
from postgresql_lock import errors

# system imports
from typing import Any

from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import call
from unittest.mock import patch

# dependency imports
from pytest import mark
from pytest import raises

PATH = "postgresql_lock.lock"


@patch(f"{PATH}.str")
@patch(f"{PATH}.int")
@patch(f"{PATH}.hashlib")
@patch(f"{PATH}.Lock._load_impl")
def test___init___defaults(
    _load_impl: Mock, hashlib: Mock, int: Mock, str: Mock
) -> None:
    conn = Mock()
    key = Mock()
    lock = Lock(conn, key)

    assert str.call_args_list[0][0][0] == key

    call_args = str.call_args_list

    assert call_args[0] == call(key)

    str().encode.assert_called_with("utf-8")
    hashlib.sha1.assert_called_with(str().encode())
    hashlib.sha1().digest.assert_called_once()

    int.from_bytes.assert_called_with(
        hashlib.sha1().digest()[:8],
        byteorder="big",
        signed=True,
    )

    assert lock._conn == conn
    assert lock._interface == "auto"
    assert lock.impl == _load_impl()
    assert lock._key == key
    assert lock._lock_id == int.from_bytes()
    assert lock._rollback_on_error
    assert lock._scope == "session"
    assert not lock._locked
    assert lock._ref_count == 0
    assert not lock._shared

    assert lock._blocking_lock_func == "pg_advisory_lock"
    assert lock._nonblocking_lock_func == "pg_try_advisory_lock"
    assert lock._unlock_func == "pg_advisory_unlock"


@patch(f"{PATH}.Lock._load_impl")
def test___init___rollback_on_error_false(_load_impl: Mock) -> None:
    lock = Lock(None, "key", rollback_on_error=False)

    assert not lock._rollback_on_error


@patch(f"{PATH}.Lock._load_impl")
def test___init___rollback_on_error_true(_load_impl: Mock) -> None:
    lock = Lock(None, "key", rollback_on_error=True)

    assert lock._rollback_on_error


@patch(f"{PATH}.Lock._load_impl")
def test___init___session(_load_impl: Mock) -> None:
    lock = Lock(None, "key", scope="session")

    assert lock._blocking_lock_func == "pg_advisory_lock"
    assert lock._nonblocking_lock_func == "pg_try_advisory_lock"
    assert lock._unlock_func == "pg_advisory_unlock"


@patch(f"{PATH}.Lock._load_impl")
def test___init___session_shared(_load_impl: Mock) -> None:
    lock = Lock(None, "key", scope="session", shared=True)

    assert lock._shared

    assert lock._blocking_lock_func == "pg_advisory_lock_shared"
    assert lock._nonblocking_lock_func == "pg_try_advisory_lock_shared"
    assert lock._unlock_func == "pg_advisory_unlock_shared"


@patch(f"{PATH}.Lock._load_impl")
def test___init___transaction(_load_impl: Mock) -> None:
    lock = Lock(None, "key", scope="transaction")

    assert lock._blocking_lock_func == "pg_advisory_xact_lock"
    assert lock._nonblocking_lock_func == "pg_try_advisory_xact_lock"
    assert lock._unlock_func == "pg_advisory_unlock"


@patch(f"{PATH}.Lock._load_impl")
def test___init___transaction_shared(_load_impl: Mock) -> None:
    lock = Lock(None, "key", scope="transaction", shared=True)

    assert lock._shared

    assert lock._blocking_lock_func == "pg_advisory_xact_lock_shared"
    assert lock._nonblocking_lock_func == "pg_try_advisory_xact_lock_shared"
    assert lock._unlock_func == "pg_advisory_unlock_shared"


@patch(f"{PATH}.Lock._load_impl")
def test_property__blocking_lock_func(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.blocking_lock_func == lock._blocking_lock_func


@patch(f"{PATH}.Lock._load_impl")
def test_property__conn(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.conn == lock._conn


@patch(f"{PATH}.Lock._load_impl")
def test_property__impl(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.impl == lock._impl


@patch(f"{PATH}.Lock._load_impl")
def test_property__interface(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.interface == lock._interface


@patch(f"{PATH}.Lock._load_impl")
def test_property__key(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.key == lock._key


@patch(f"{PATH}.Lock._load_impl")
def test_property__lock_id(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.lock_id == lock._lock_id


@patch(f"{PATH}.Lock._load_impl")
def test_property__locked(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.locked == lock._locked


@patch(f"{PATH}.Lock._load_impl")
def test_property__nonblocking_lock_func(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.nonblocking_lock_func == lock._nonblocking_lock_func


@patch(f"{PATH}.Lock._load_impl")
def test_property__ref_count(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.ref_count == lock._ref_count


@patch(f"{PATH}.Lock._load_impl")
def test_property__rollback_on_error(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.rollback_on_error == lock._rollback_on_error


@patch(f"{PATH}.Lock._load_impl")
def test_property__scope(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.scope == lock._scope


@patch(f"{PATH}.Lock._load_impl")
def test_property__shared(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.shared == lock._shared


@patch(f"{PATH}.Lock._load_impl")
def test_property__unlock_func(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.unlock_func == lock._unlock_func


@patch(f"{PATH}.Lock._load_impl")
def test_acquire__defaults(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.impl.acquire.return_value == lock.acquire()

    lock.impl.acquire.assert_called_with(lock, block=True)

    assert lock._locked == lock.impl.acquire.return_value
    assert lock._ref_count == 1


@mark.parametrize("block", [True, False])
@patch(f"{PATH}.Lock._load_impl")
def test_acquire__block(_load_impl: Mock, block: Any) -> None:
    lock = Lock(None, "key")

    assert lock.impl.acquire.return_value == lock.acquire(block=block)

    lock.impl.acquire.assert_called_with(lock, block=block)

    assert lock._locked == lock.impl.acquire.return_value
    assert lock._ref_count == 1


@patch(f"{PATH}.Lock._load_impl")
def test_acquire__locked_not_shared(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock.acquire()

    with raises(errors.AcquireError) as exc:
        lock.acquire()

    assert (
        str(exc.value)
        == f"Lock for '{lock._key}' is already held by this {lock._scope} scope"
    )


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_acquire_async__defaults(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()  # type: ignore

    assert lock.impl.acquire_async.return_value == await lock.acquire_async()

    lock.impl.acquire_async.assert_called_with(lock, block=True)

    assert lock._locked == lock.impl.acquire_async.return_value
    assert lock._ref_count == 1


@mark.asyncio
@mark.parametrize("block", [True, False])
@patch(f"{PATH}.Lock._load_impl")
async def test_acquire_async__block(_load_impl: Mock, block: Any) -> None:
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()  # type: ignore

    assert lock.impl.acquire_async.return_value == await lock.acquire_async(block=block)

    lock.impl.acquire_async.assert_called_with(lock, block=block)

    assert lock._locked == lock.impl.acquire_async.return_value
    assert lock._ref_count == 1


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_acquire_async__locked_not_shared(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()  # type: ignore

    await lock.acquire_async()

    with raises(errors.AcquireError) as exc:
        await lock.acquire_async()

    assert (
        str(exc.value)
        == f"Lock for '{lock._key}' is already held by this {lock._scope} scope"
    )


@patch(f"{PATH}.Lock._load_impl")
def test_context_manager(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    with lock:
        pass

    lock.impl.acquire.assert_called_with(lock, block=True)
    lock.impl.release.assert_called_with(lock)


@patch(f"{PATH}.Lock._load_impl")
def test_context_manager__raises_exception(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    with raises(Exception) as exc:
        with lock:
            raise Exception("Inner exception")

    assert str(exc.value) == "Inner exception"

    lock.impl.acquire.assert_called_with(lock, block=True)
    lock.impl.handle_error.assert_called_with(lock, exc.value)
    lock.impl.release.assert_called_with(lock)


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_context_manager_async(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()  # type: ignore
    lock.impl.release_async = AsyncMock()  # type: ignore

    async with lock:
        pass

    lock.impl.acquire_async.assert_called_with(lock, block=True)
    lock.impl.release_async.assert_called_with(lock)


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_context_manager_async__raises_exception(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()  # type: ignore
    lock.impl.handle_error_async = AsyncMock()  # type: ignore
    lock.impl.release_async = AsyncMock()  # type: ignore

    with raises(Exception) as exc:
        async with lock:
            raise Exception("Inner exception")

    assert str(exc.value) == "Inner exception"

    lock.impl.acquire_async.assert_called_with(lock, block=True)
    lock.impl.handle_error_async.assert_called_with(lock, exc.value)
    lock.impl.release_async.assert_called_with(lock)


@patch(f"{PATH}.Lock._load_impl")
def test_handle_error(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    exc = Exception()

    lock.handle_error(exc)

    lock.impl.handle_error.assert_called_with(lock, exc)


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_handle_error_async(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock.impl.handle_error_async = AsyncMock()  # type: ignore
    exc = Exception()

    await lock.handle_error_async(exc)

    lock.impl.handle_error_async.assert_called_with(lock, exc)


@patch(f"{PATH}.Lock._load_impl")
def test_locked(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert not lock.locked

    lock._locked = True

    assert lock.locked


@patch(f"{PATH}.Lock._load_impl")
def test_lock_query__false(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert (
        lock.lock_query(False)
        == f"SELECT pg_catalog.{lock.nonblocking_lock_func}({lock.lock_id})"
    )


@patch(f"{PATH}.Lock._load_impl")
def test_lock_query__true(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert (
        lock.lock_query(True)
        == f"SELECT pg_catalog.{lock.blocking_lock_func}({lock.lock_id})"
    )


@patch(f"{PATH}.Lock._load_impl")
def test_ref_count(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert lock.ref_count == 0

    lock._ref_count += 1

    assert lock.ref_count == 1


@patch(f"{PATH}.Lock._load_impl")
def test_release(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock._locked = True
    lock._ref_count = 1

    assert lock.release()
    assert lock._ref_count == 0
    assert not lock._locked


@patch(f"{PATH}.Lock._load_impl")
def test_release__not_locked(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert not lock.release()


@patch(f"{PATH}.Lock._load_impl")
def test_release__not_released(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock._locked = True
    lock._ref_count = 1
    lock.impl.release.return_value = False

    with raises(errors.ReleaseError) as exc:
        lock.release()

    assert lock._ref_count == 1
    assert (
        str(exc.value)
        == f"Lock for '{lock._key}' was not held by this {lock._scope} scope"
    )


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_release_async(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock._locked = True
    lock._ref_count = 1
    lock.impl.release_async = AsyncMock(return_value=True)  # type: ignore

    assert await lock.release_async()
    assert lock._ref_count == 0
    assert not lock._locked


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_release_async__not_locked(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert not await lock.release_async()


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_release_async__not_released(_load_impl: Mock) -> None:
    lock = Lock(None, "key")
    lock._locked = True
    lock._ref_count = 1
    lock.impl.release_async = AsyncMock(return_value=False)  # type: ignore

    with raises(errors.ReleaseError) as exc:
        await lock.release_async()

    assert lock._ref_count == 1
    assert (
        str(exc.value)
        == f"Lock for '{lock._key}' was not held by this {lock._scope} scope"
    )


@patch(f"{PATH}.Lock._load_impl")
def test_shared(_load_impl: Mock) -> None:
    assert not Lock(None, "key").shared
    assert Lock(None, "key", shared=True).shared


@patch(f"{PATH}.Lock._load_impl")
def test_unlock_query(_load_impl: Mock) -> None:
    lock = Lock(None, "key")

    assert (
        lock.unlock_query() == f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"
    )


@patch(f"{PATH}.import_module")
def test__load_impl__detect_interface__asyncpg(import_module: Mock) -> None:
    _conn = Mock()
    _conn.__class__.__module__ = "asyncpg."
    lock = Mock(_conn=_conn, _interface="auto")

    assert import_module.return_value == Lock._load_impl(lock)

    import_module.assert_called_with(".asyncpg", package="postgresql_lock")


@patch(f"{PATH}.import_module")
def test__load_impl__detect_interface__psycopg3(import_module: Mock) -> None:
    _conn = Mock()
    _conn.__class__.__module__ = "psycopg"
    lock = Mock(_conn=_conn, _interface="auto")

    assert import_module.return_value == Lock._load_impl(lock)

    import_module.assert_called_with(".psycopg3", package="postgresql_lock")


@patch(f"{PATH}.import_module")
def test__load_impl__detect_interface__psycopg2(import_module: Mock) -> None:
    _conn = Mock()
    _conn.__class__.__module__ = "psycopg2."
    lock = Mock(_conn=_conn, _interface="auto")

    assert import_module.return_value == Lock._load_impl(lock)

    import_module.assert_called_with(".psycopg2", package="postgresql_lock")


@patch(f"{PATH}.import_module")
def test__load_impl__detect_interface__sqlalchemy(import_module: Mock) -> None:
    _conn = Mock()
    _conn.__class__.__module__ = "sqlalchemy"
    lock = Mock(_conn=_conn, _interface="auto")

    assert import_module.return_value == Lock._load_impl(lock)

    import_module.assert_called_with(".sqlalchemy", package="postgresql_lock")


@patch(f"{PATH}.import_module")
def test__load_impl__detect_interface__unsupported(import_module: Mock) -> None:
    conn = Mock()
    lock = Mock(conn=conn, _interface="auto")

    with raises(errors.UnsupportedInterfaceError) as exc:
        assert import_module.return_value == Lock._load_impl(lock)

    assert (
        str(exc.value)
        == "Cannot determine database interface, "
        + "try specifying it with the interface keyword"
    )


@patch(f"{PATH}.import_module")
def test__load_impl__specify_interface(import_module: Mock) -> None:
    _conn = Mock()
    _interface = Mock()
    lock = Mock(_conn=_conn, interface=_interface)

    assert import_module.return_value == Lock._load_impl(lock)

    import_module.assert_called_with(f".{lock._interface}", package="postgresql_lock")


@patch(f"{PATH}.import_module")
def test__load_impl__specify_interface__module_not_found(import_module: Mock) -> None:
    _conn = Mock()
    _interface = Mock()
    lock = Mock(_conn=_conn, _interface=_interface)

    import_module.side_effect = ModuleNotFoundError()

    with raises(errors.UnsupportedInterfaceError) as exc:
        Lock._load_impl(lock)

    assert str(exc.value) == f"Unsupported database interface '{lock._interface}'"
