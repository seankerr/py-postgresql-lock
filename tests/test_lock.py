from postgres_lock import Lock
from postgres_lock import errors

from pytest import mark
from pytest import raises

from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

PATH = "postgres_lock.lock"


@patch(f"{PATH}.str")
@patch(f"{PATH}.int")
@patch(f"{PATH}.hashlib")
@patch(f"{PATH}.Lock._load_impl")
def test___init___defaults(_load_impl, hashlib, int, str):
    conn = Mock()
    key = Mock()
    lock = Lock(conn, key)

    key.encode.assert_called_with("utf-8")
    hashlib.sha1.assert_called_with(key.encode())
    hashlib.sha1().hexdigest.assert_called_once()
    int.assert_called_with(hashlib.sha1().hexdigest(), 16)

    assert lock.conn == conn
    assert lock.interface == "auto"
    assert lock.impl == _load_impl()
    assert lock.key == key
    assert lock.lock_id == str()[:18]
    assert lock.scope == "session"
    assert not lock._locked
    assert lock._ref_count == 0
    assert not lock._shared

    assert lock.blocking_lock_func == "pg_advisory_lock"
    assert lock.nonblocking_lock_func == "pg_try_advisory_lock"
    assert lock.unlock_func == "pg_advisory_unlock"


@patch(f"{PATH}.Lock._load_impl")
def test___init___session(_load_impl):
    key = "key"
    lock = Lock(None, key, scope="session")

    assert lock.blocking_lock_func == "pg_advisory_lock"
    assert lock.nonblocking_lock_func == "pg_try_advisory_lock"
    assert lock.unlock_func == "pg_advisory_unlock"


@patch(f"{PATH}.Lock._load_impl")
def test___init___session_shared(_load_impl):
    key = "key"
    lock = Lock(None, key, scope="session", shared=True)

    assert lock._shared

    assert lock.blocking_lock_func == "pg_advisory_lock_shared"
    assert lock.nonblocking_lock_func == "pg_try_advisory_lock_shared"
    assert lock.unlock_func == "pg_advisory_unlock_shared"


@patch(f"{PATH}.Lock._load_impl")
def test___init___transaction(_load_impl):
    key = "key"
    lock = Lock(None, key, scope="transaction")

    assert lock.blocking_lock_func == "pg_advisory_xact_lock"
    assert lock.nonblocking_lock_func == "pg_try_advisory_xact_lock"
    assert lock.unlock_func == "pg_advisory_unlock"


@patch(f"{PATH}.Lock._load_impl")
def test___init___transaction_shared(_load_impl):
    key = "key"
    lock = Lock(None, key, scope="transaction", shared=True)

    assert lock._shared

    assert lock.blocking_lock_func == "pg_advisory_xact_lock_shared"
    assert lock.nonblocking_lock_func == "pg_try_advisory_xact_lock_shared"
    assert lock.unlock_func == "pg_advisory_unlock_shared"


@patch(f"{PATH}.Lock._load_impl")
def test_acquire__defaults(_load_impl):
    key = "key"
    lock = Lock(None, key)

    assert lock.impl.acquire.return_value == lock.acquire()

    lock.impl.acquire.assert_called_with(lock, block=True)

    assert lock._locked == lock.impl.acquire.return_value
    assert lock._ref_count == 1


@mark.parametrize("block", [True, False])
@patch(f"{PATH}.Lock._load_impl")
def test_acquire__block(_load_impl, block):
    key = "key"
    lock = Lock(None, key)

    assert lock.impl.acquire.return_value == lock.acquire(block=block)

    lock.impl.acquire.assert_called_with(lock, block=block)

    assert lock._locked == lock.impl.acquire.return_value
    assert lock._ref_count == 1


@patch(f"{PATH}.Lock._load_impl")
def test_acquire__locked_not_shared(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock.acquire()

    with raises(errors.AcquireError) as exc:
        lock.acquire()

    assert (
        str(exc.value)
        == f"Lock for '{lock.key}' is already held by this {lock.scope} scope"
    )


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_acquire_async__defaults(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock.impl.acquire_async = AsyncMock()

    assert lock.impl.acquire_async.return_value == await lock.acquire_async()

    lock.impl.acquire_async.assert_called_with(lock, block=True)

    assert lock._locked == lock.impl.acquire_async.return_value
    assert lock._ref_count == 1


@mark.asyncio
@mark.parametrize("block", [True, False])
@patch(f"{PATH}.Lock._load_impl")
async def test_acquire_async__block(_load_impl, block):
    key = "key"
    lock = Lock(None, key)
    lock.impl.acquire_async = AsyncMock()

    assert lock.impl.acquire_async.return_value == await lock.acquire_async(block=block)

    lock.impl.acquire_async.assert_called_with(lock, block=block)

    assert lock._locked == lock.impl.acquire_async.return_value
    assert lock._ref_count == 1


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_acquire_async__locked_not_shared(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock.impl.acquire_async = AsyncMock()

    await lock.acquire_async()

    with raises(errors.AcquireError) as exc:
        await lock.acquire_async()

    assert (
        str(exc.value)
        == f"Lock for '{lock.key}' is already held by this {lock.scope} scope"
    )


@patch(f"{PATH}.Lock._load_impl")
def test_context_manager(_load_impl):
    lock = Lock(None, "key")

    with lock:
        pass

    lock.impl.acquire.assert_called_with(lock, block=True)
    lock.impl.release.assert_called_with(lock)


@patch(f"{PATH}.Lock._load_impl")
def test_context_manager__raises_exception(_load_impl):
    lock = Lock(None, "key")

    with raises(Exception) as exc:
        with lock:
            raise Exception("Inner exception")

    assert str(exc.value) == "Inner exception"

    lock.impl.acquire.assert_called_with(lock, block=True)
    lock.impl.release.assert_called_with(lock)


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_context_manager_async(_load_impl):
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()
    lock.impl.release_async = AsyncMock()

    async with lock:
        pass

    lock.impl.acquire_async.assert_called_with(lock, block=True)
    lock.impl.release_async.assert_called_with(lock)


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_context_manager_async__raises_exception(_load_impl):
    lock = Lock(None, "key")
    lock.impl.acquire_async = AsyncMock()
    lock.impl.release_async = AsyncMock()

    with raises(Exception) as exc:
        async with lock:
            raise Exception("Inner exception")

    assert str(exc.value) == "Inner exception"

    lock.impl.acquire_async.assert_called_with(lock, block=True)
    lock.impl.release_async.assert_called_with(lock)


@patch(f"{PATH}.Lock._load_impl")
def test_locked(_load_impl):
    key = "key"
    lock = Lock(None, key)

    assert not lock.locked

    lock._locked = True

    assert lock.locked


@patch(f"{PATH}.Lock._load_impl")
def test_ref_count(_load_impl):
    key = "key"
    lock = Lock(None, key)

    assert lock.ref_count == 0

    lock._ref_count += 1

    assert lock.ref_count == 1


@patch(f"{PATH}.Lock._load_impl")
def test_release(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock._locked = True
    lock._ref_count = 1

    assert lock.release()
    assert lock._ref_count == 0
    assert not lock._locked


@patch(f"{PATH}.Lock._load_impl")
def test_release__not_locked(_load_impl):
    key = "key"
    lock = Lock(None, key)

    assert not lock.release()


@patch(f"{PATH}.Lock._load_impl")
def test_release__not_released(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock._locked = True
    lock._ref_count = 1
    lock.impl.release.return_value = False

    with raises(errors.ReleaseError) as exc:
        lock.release()

    assert lock._ref_count == 1
    assert (
        str(exc.value)
        == f"Lock for '{lock.key}' was not held by this {lock.scope} scope"
    )


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_release_async(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock._locked = True
    lock._ref_count = 1
    lock.impl.release_async = AsyncMock(return_value=True)

    assert await lock.release_async()
    assert lock._ref_count == 0
    assert not lock._locked


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_release_async__not_locked(_load_impl):
    key = "key"
    lock = Lock(None, key)

    assert not await lock.release_async()


@mark.asyncio
@patch(f"{PATH}.Lock._load_impl")
async def test_release_async__not_released(_load_impl):
    key = "key"
    lock = Lock(None, key)
    lock._locked = True
    lock._ref_count = 1
    lock.impl.release_async = AsyncMock(return_value=False)

    with raises(errors.ReleaseError) as exc:
        await lock.release_async()

    assert lock._ref_count == 1
    assert (
        str(exc.value)
        == f"Lock for '{lock.key}' was not held by this {lock.scope} scope"
    )


@patch(f"{PATH}.Lock._load_impl")
def test_shared(_load_impl):
    key = "key"

    assert not Lock(None, key).shared
    assert Lock(None, key, shared=True).shared
