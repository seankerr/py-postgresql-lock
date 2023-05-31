"""
Lock support for psycopg3 database interface.
"""

from .lock import Lock


def acquire(lock: Lock, block: bool = True) -> bool:
    """
    Acquire the lock.

    Parameters:
        lock (Lock): Lock.
        block (bool): Return only once the lock has been acquired.

    Returns:
        bool: True, if the lock was acquired, otherwise False.
    """
    lock_func = lock.blocking_lock_func

    if not block:
        lock_func = lock.nonblocking_lock_func

    cursor = lock.conn.cursor()
    cursor.execute(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")
    result, *_ = cursor.fetchone()

    # lock function returns True/False in unblocking mode, and always None in blocking mode
    return False if result is False else True


async def acquire_async(lock: Lock, block: bool = True) -> bool:
    """
    Acquire the lock asynchronously.

    Parameters:
        lock (Lock): Lock.
        block (bool): Return only once the lock has been acquired.

    Returns:
        bool: True, if the lock was acquired, otherwise False.
    """
    lock_func = lock.blocking_lock_func

    if not block:
        lock_func = lock.nonblocking_lock_func

    cursor = lock.conn.cursor()

    await cursor.execute(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")

    result, *_ = await cursor.fetchone()

    # lock function returns True/False in unblocking mode, and always None in blocking mode
    return False if result is False else True


def release(lock: Lock) -> bool:
    """
    Release the lock.

    Parameters:
        lock (Lock): Lock.

    Returns:
        bool: True, if the lock was released, otherwise False.
    """
    cursor = lock.conn.cursor()
    cursor.execute(f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})")
    result, *_ = cursor.fetchone()

    return result


async def release_async(lock: Lock) -> bool:
    """
    Release the lock asynchronously.

    Parameters:
        lock (Lock): Lock.

    Returns:
        bool: True, if the lock was released, otherwise False.
    """
    cursor = lock.conn.cursor()

    await cursor.execute(f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})")

    result, *_ = await cursor.fetchone()

    return result
