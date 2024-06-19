# --------------------------------------------------------------------------------------
# Copyright (c) 2023 Sean Kerr
# --------------------------------------------------------------------------------------

# postgresql-lock imports
from . import errors

# system imports
from importlib import import_module

from types import ModuleType
from types import TracebackType

from typing import Any
from typing import Literal
from typing import Optional
from typing import Type

import hashlib
import logging


def logger() -> logging.Logger:
    return logging.getLogger("postgresql_lock")


class Lock:
    """
    Lock mechanism implemented with PostgreSQL advisory locks.

    Default operation is session lock scope and blocking mode and is sufficient for
    distributed locks. The database interface will be detected automatically.

    Database interfaces:
        - asyncpg
            - asynchronous
        - psycopg2
            - synchronous
        - psycopg3
            - asynchronous
            - synchronous
        - sqlalchemy (supports version 1 & 2; can use any underlying database interface)
            - asynchronous
            - synchronous

    Lock scopes:
        - session: only a single session can hold the lock
        - transaction: only a single transaction within a single session can hold the
          lock

    Modes:
        - blocked: do not return until the lock is acquired
        - nonblocking: return immediately with or without the lock being acquired

    Sharing (locks can or cannot be reacquired from the same scope)
    """

    conn: Any
    interface: str
    key: Any
    lock_id: int
    rollback_on_error: bool
    scope: str
    _locked: bool
    _ref_count: int
    _shared: bool

    blocking_lock_func: str
    nonblocking_lock_func: str
    unlock_func: str

    def __init__(
        self,
        conn: Any,
        key: Any,
        interface: Literal[
            "auto",
            "asyncpg",
            "psycopg2",
            "psycopg3",
            "sqlalchemy",
        ] = "auto",
        rollback_on_error: bool = True,
        scope: Literal["session", "transaction"] = "session",
        shared: bool = False,
    ):
        """
        Create a new Lock instance.

        Parameters:
            conn (object): Database connection.
            key (object): Unique lock key.
            interface (str): Database interface.
            rollback_on_error (bool): Rollback if an error occurs while in a `with`
                statement.
            scope (str): Lock scope.
            shared (bool): Use a shared lock.
        """
        self.conn = conn
        self.interface = interface
        self.impl = self._load_impl()
        self.key = key
        self.lock_id = int.from_bytes(
            hashlib.sha1(str(key).encode("utf-8")).digest()[:8],
            byteorder="big",
            signed=True,
        )
        self.rollback_on_error = rollback_on_error
        self.scope = scope
        self._locked = False
        self._ref_count = 0
        self._shared = shared

        infix = "_xact"
        suffix = ""

        if scope == "session":
            infix = ""

        if shared:
            suffix = "_shared"

        self.blocking_lock_func = f"pg_advisory{infix}_lock{suffix}"
        self.nonblocking_lock_func = f"pg_try_advisory{infix}_lock{suffix}"
        self.unlock_func = f"pg_advisory_unlock{suffix}"

    def acquire(self, block: bool = True) -> bool:
        """
        Acquire the lock.

        Parameters:
            block (bool): Return only once the lock has been acquired.

        Returns:
            bool: True, if the lock was acquired, otherwise False.
        """
        if self._locked and not self._shared:
            raise errors.AcquireError(
                f"Lock for '{self.key}' is already held by this {self.scope} scope"
            )

        logger().info("Acquire lock for key: %s", self.key)

        self._locked = self.impl.acquire(self, block=block)
        self._ref_count += 1

        logger().debug("Acquire ref count for key: %s, %d", self.key, self._ref_count)

        return self._locked

    async def acquire_async(self, block: bool = True) -> bool:
        """
        Acquire the lock asynchronously.

        Parameters:
            block (bool): Return only once the lock has been acquired.

        Returns:
            bool: True, if the lock was acquired, otherwise False.
        """
        if self._locked and not self._shared:
            raise errors.AcquireError(
                f"Lock for '{self.key}' is already held by this {self.scope} scope"
            )

        logger().info("Acquire lock for key: %s", self.key)

        self._locked = await self.impl.acquire_async(self, block=block)
        self._ref_count += 1

        logger().debug("Ref count for key: %s, %d", self.key, self._ref_count)

        return self._locked

    def handle_error(self, exc: BaseException) -> None:
        """
        Handle an error.

        Parameters:
            exc (Exception): Exception.
        """
        return self.impl.handle_error(self, exc)

    async def handle_error_async(self, exc: BaseException) -> None:
        """
        Handle an error asynchronously.

        Parameters:
            exc (Exception): Exception.
        """
        return await self.impl.handle_error_async(self, exc)

    @property
    def locked(self) -> bool:
        """
        Returns the lock status.

        Returns:
            bool: Locked status.
        """
        return self._locked

    @property
    def ref_count(self) -> int:
        """
        Returns the reference count.

        Returns:
            int: Reference count.
        """
        return self._ref_count

    def release(self) -> bool:
        """
        Release the lock.

        When using shared locks, all references to the lock within the current scope
        must be released before this method will return True.

        Raises:
            errors.ReleaseError: An attempt to release the lock failed.

        Returns:
            bool: True, if the lock was released, otherwise False.
        """
        if not self._locked:
            logger().debug("Lock not held for key: %s", self.key)

            return False

        logger().info("Release lock for key: %s", self.key)

        if not self.impl.release(self):
            raise errors.ReleaseError(
                f"Lock for '{self.key}' was not held by this {self.scope} scope"
            )

        self._ref_count -= 1
        self._locked = self._ref_count > 0

        logger().debug("Release ref count for key: %s, %d", self.key, self._ref_count)

        return not self._locked

    async def release_async(self) -> bool:
        """
        Release the lock asynchronously.

        When using shared locks, all references to the lock within the current scope
        must be released before this method will return True.

        Raises:
            errors.ReleaseError: An attempt to release the lock failed.

        Returns:
            bool: True, if the lock was released, otherwise False.
        """
        if not self._locked:
            logger().debug("Lock not held for key: %s", self.key)

            return False

        logger().info("Release lock for key: %s", self.key)

        if not await self.impl.release_async(self):
            raise errors.ReleaseError(
                f"Lock for '{self.key}' was not held by this {self.scope} scope"
            )

        self._ref_count -= 1
        self._locked = self._ref_count > 0

        logger().debug("Release ref count for key: %s, %d", self.key, self._ref_count)

        return not self._locked

    @property
    def shared(self) -> bool:
        """
        Returns the shared status.

        Returns:
            bool: Shared status.
        """
        return self._shared

    async def __aenter__(self) -> bool:
        """
        Enter the context manager.
        """
        logger().debug("Enter context manager for key: %s", self.key)

        return await self.impl.acquire_async(self, block=True)

    def _load_impl(self) -> ModuleType:
        """
        Load the implementation.

        Raises:
            errors.UnsupportedInterfaceError: The database interface is unsupported.
        """
        module = self.conn.__class__.__module__
        interface = self.interface

        if interface == "auto":
            if module == "psycopg":
                interface = "psycopg3"

            elif module.startswith("psycopg2"):
                interface = "psycopg2"

            elif module.startswith("asyncpg"):
                interface = "asyncpg"

            elif module.startswith("sqlalchemy"):
                interface = "sqlalchemy"

            else:
                raise errors.UnsupportedInterfaceError(
                    "Cannot determine database interface, "
                    + "try specifying it with the interface keyword"
                )

        self.interface = interface

        try:
            return import_module(f".{interface}", package="postgresql_lock")

        except ModuleNotFoundError:
            raise errors.UnsupportedInterfaceError(
                f"Unsupported database interface '{interface}'"
            )

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Exit the context manager.

        Parameters:
            Exception type (Type[BaseException]): Type of exception that was raised.
            Exception (BaseException): Exception that was raised.
            Traceback (TracebackType): Traceback.
        """
        logger().debug("Exit context manager for key: %s", self.key)

        if exc:
            await self.impl.handle_error_async(self, exc)

        await self.impl.release_async(self)

        if exc:
            raise exc

    def __enter__(self) -> bool:
        """
        Enter the context manager.
        """
        logger().debug("Enter context manager for key: %s", self.key)

        return self.impl.acquire(self, block=True)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Exit the context manager.

        Parameters:
            Exception type (Type[BaseException]): Type of exception that was raised.
            Exception (BaseException): Exception that was raised.
            Traceback (TracebackType): Traceback.
        """
        logger().debug("Exit context manager for key: %s", self.key)

        if exc:
            self.impl.handle_error(self, exc)

        self.impl.release(self)

        if exc:
            raise exc
