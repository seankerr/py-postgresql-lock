"""
Lock support for asyncpg database interface.
"""

# postgresql-lock imports
from .lock import Lock
from .lock import logger


def acquire(lock: Lock, block: bool = True) -> bool:
    """
    This function is not implemented.

    Parameters:
        lock (Lock): Lock.
        block (bool): Return only once the lock has been acquired.

    Raises:
        NotImplementedError: This function is not implemented.
    """
    raise NotImplementedError("ascynpg interface does not support acquire()")


async def acquire_async(lock: Lock, block: bool = True) -> bool:
    """
    Acquire the lock asynchronously.

    Parameters:
        lock (Lock): Lock.
        block (bool): Return only once the lock has been acquired.

    Returns:
        bool: True, if the lock was acquired, otherwise False.
    """
    lock_query = lock.lock_query(block)

    logger().debug("Acquire statement for key: %s, %s", lock.key, lock_query)

    result = await lock.conn.fetchval(lock_query)

    # lock function returns True/False in nonblocking mode, and always None in blocking
    # mode
    return False if result is False else True


def handle_error(lock: Lock, exc: BaseException) -> None:
    """
    Handle an error.

    Parameters:
        exc (Exception): Exception.
    """
    raise NotImplementedError("ascynpg interface does not support handle_error()")


async def handle_error_async(lock: Lock, exc: BaseException) -> None:
    """
    Handle an error asynchronously.

    Parameters:
        exc (Exception): Exception.
    """
    if not lock.rollback_on_error:
        return

    await lock.conn.execute("ROLLBACK")


def release(lock: Lock) -> bool:
    """
    This function is not implemented.

    Parameters:
        lock (Lock): Lock.

    Raises:
        NotImplementedError: This function is not implemented.
    """
    raise NotImplementedError("ascynpg interface does not support release()")


async def release_async(lock: Lock) -> bool:
    """
    Release the lock asynchronously.

    Parameters:
        lock (Lock): Lock.

    Returns:
        bool: True, if the lock was released, otherwise False.
    """
    unlock_query = lock.unlock_query()

    logger().debug("Release statement for key: %s, %s", lock.key, unlock_query)

    return await lock.conn.fetchval(unlock_query)
