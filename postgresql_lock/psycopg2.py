"""
Lock support for psycopg2 database interface.
"""

# postgresql-lock imports
from .lock import Lock
from .lock import logger


def acquire(lock: Lock, block: bool = True) -> bool:
    """
    Acquire the lock.

    Parameters:
        lock (Lock): Lock.
        block (bool): Return only once the lock has been acquired.

    Returns:
        bool: True, if the lock was acquired, otherwise False.
    """
    lock_query = lock.lock_query(block)

    logger().debug("Acquire statement for key: %s, %s", lock.key, lock_query)

    cursor = lock.conn.cursor()
    cursor.execute(lock_query)
    result, *_ = cursor.fetchone()
    cursor.close()

    # lock function returns True/False in nonblocking mode, and always None in blocking
    # mode
    return False if result is False else True


async def acquire_async(lock: Lock, block: bool = True) -> bool:
    """
    This function is not implemented.

    Parameters:
        lock (Lock): Lock.
        block (bool): Return only once the lock has been acquired.

    Raises:
        NotImplementedError: This function is not implemented.
    """
    raise NotImplementedError("psycopg2 interface does not support acquire_async()")


def handle_error(lock: Lock, exc: BaseException) -> None:
    """
    Handle an error.

    Parameters:
        exc (Exception): Exception.
    """
    if not lock.rollback_on_error:
        return

    lock.conn.rollback()


async def handle_error_async(lock: Lock, exc: BaseException) -> None:
    """
    Handle an error asynchronously.

    Parameters:
        exc (Exception): Exception.
    """
    raise NotImplementedError(
        "psycopg2 interface does not support handle_error_async()"
    )


def release(lock: Lock) -> bool:
    """
    Release the lock.

    Parameters:
        lock (Lock): Lock.

    Returns:
        bool: True, if the lock was released, otherwise False.
    """
    unlock_query = lock.unlock_query()

    logger().debug("Release statement for key: %s, %s", lock.key, unlock_query)

    cursor = lock.conn.cursor()
    cursor.execute(unlock_query)
    result, *_ = cursor.fetchone()
    cursor.close()

    return result


async def release_async(lock: Lock) -> None:
    """
    This function is not implemented.

    Parameters:
        lock (Lock): Lock.

    Raises:
        NotImplementedError: This function is not implemented.
    """
    raise NotImplementedError("psycopg2 interface does not support release_async()")
