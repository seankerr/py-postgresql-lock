# --------------------------------------------------------------------------------------
# Copyright (c) 2023 Sean Kerr
# --------------------------------------------------------------------------------------

"""
Lock support for psycopg3 database interface.
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
    lock_func = lock.blocking_lock_func

    if not block:
        lock_func = lock.nonblocking_lock_func

    lock_stmt = f"SELECT pg_catalog.{lock_func}({lock.lock_id})"

    logger().debug("Acquire statement for key: %s, %s", lock.key, lock_stmt)

    cursor = lock.conn.cursor()
    cursor.execute(lock_stmt)
    result, *_ = cursor.fetchone()
    cursor.close()

    # lock function returns True/False in unblocking mode, and always None in blocking
    # mode
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

    lock_stmt = f"SELECT pg_catalog.{lock_func}({lock.lock_id})"

    logger().debug("Acquire statement for key: %s, %s", lock.key, lock_stmt)

    cursor = lock.conn.cursor()

    await cursor.execute(f"SELECT pg_catalog.{lock_func}({lock.lock_id})")

    result, *_ = await cursor.fetchone()

    await cursor.close()

    # lock function returns True/False in unblocking mode, and always None in blocking
    # mode
    return False if result is False else True


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
    if not lock.rollback_on_error:
        return

    await lock.conn.rollback()


def release(lock: Lock) -> bool:
    """
    Release the lock.

    Parameters:
        lock (Lock): Lock.

    Returns:
        bool: True, if the lock was released, otherwise False.
    """
    unlock_stmt = f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"

    logger().debug("Release statement for key: %s, %s", lock.key, unlock_stmt)

    cursor = lock.conn.cursor()
    cursor.execute(unlock_stmt)
    result, *_ = cursor.fetchone()
    cursor.close()

    return result


async def release_async(lock: Lock) -> bool:
    """
    Release the lock asynchronously.

    Parameters:
        lock (Lock): Lock.

    Returns:
        bool: True, if the lock was released, otherwise False.
    """
    unlock_stmt = f"SELECT pg_catalog.{lock.unlock_func}({lock.lock_id})"

    logger().debug("Release statement for key: %s, %s", lock.key, unlock_stmt)

    cursor = lock.conn.cursor()

    await cursor.execute(unlock_stmt)

    result, *_ = await cursor.fetchone()

    await cursor.close()

    return result
