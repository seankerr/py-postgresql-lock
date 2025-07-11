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


type Interface = Literal["auto", "asyncpg", "psycopg2", "psycopg3", "sqlalchemy"]
type Scope = Literal["session", "transaction"]


class Lock:
    """
    Lock mechanism implemented with PostgreSQL advisory locks.

    Default operation is session lock scope and blocking mode and is sufficient for
    distributed locks. The database interface will be detected automatically.

    Transaction level locks cannot be manually released. Because of this,
    transaction level Lock instances cannot be reused.

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

    _blocking_lock_func: str
    _conn: Any
    _impl: ModuleType
    _interface: Interface
    _key: Any
    _lock_id: int
    _locked: bool
    _nonblocking_lock_func: str
    _ref_count: int
    _rollback_on_error: bool
    _scope: Scope
    _shared: bool
    _unlock_func: str

    @property
    def blocking_lock_func(self) -> str:
        """
        Returns the blocking lock function name.

        Returns:
            str: Blocking lock function name.
        """
        return self._blocking_lock_func

    @property
    def conn(self) -> Any:
        """
        Returns the database connection.

        Returns:
            object: Database connection.
        """
        return self._conn

    @property
    def impl(self) -> ModuleType:
        """
        Returns the implementation module.

        Returns:
            ModuleType: Implementation module.
        """
        return self._impl

    @property
    def interface(self) -> Interface:
        """
        Returns the database interface.

        Returns:
            str: Database interface.
        """
        return self._interface

    @property
    def key(self) -> Any:
        """
        Returns the unique lock key.

        Returns:
            object: Unique lock key.
        """
        return self._key

    @property
    def lock_id(self) -> int:
        """
        Returns the lock ID.

        Returns:
            int: Lock ID.
        """
        return self._lock_id

    @property
    def locked(self) -> bool:
        """
        Returns the lock status.

        Returns:
            bool: Locked status.
        """
        return self._locked

    @property
    def nonblocking_lock_func(self) -> str:
        """
        Returns the non-blocking lock function name.

        Returns:
            str: Non-blocking lock function name.
        """
        return self._nonblocking_lock_func

    @property
    def ref_count(self) -> int:
        """
        Returns the reference count.

        Returns:
            int: Reference count.
        """
        return self._ref_count

    @property
    def rollback_on_error(self) -> bool:
        """
        Returns the rollback on error status.

        Returns:
            bool: Rollback on error status.
        """
        return self._rollback_on_error

    @property
    def scope(self) -> Scope:
        """
        Returns the lock scope.

        Returns:
            str: Lock scope.
        """
        return self._scope

    @property
    def shared(self) -> bool:
        """
        Returns the shared status.

        Returns:
            bool: Shared status.
        """
        return self._shared

    @property
    def unlock_func(self) -> str:
        """
        Returns the unlock function name.

        Returns:
            str: Unlock function name.
        """
        return self._unlock_func

    def __init__(
        self,
        conn: Any,
        key: Any,
        interface: Interface = "auto",
        rollback_on_error: bool = True,
        scope: Scope = "session",
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
        self._conn = conn
        self._interface = interface
        self._key = key
        self._lock_id = int.from_bytes(
            hashlib.sha1(str(key).encode("utf-8")).digest()[:8],
            byteorder="big",
            signed=True,
        )
        self._locked = False
        self._ref_count = 0
        self._rollback_on_error = rollback_on_error
        self._scope = scope
        self._shared = shared

        infix = "" if scope == "session" else "_xact"
        suffix = "_shared" if shared else ""

        self._blocking_lock_func = f"pg_advisory{infix}_lock{suffix}"
        self._nonblocking_lock_func = f"pg_try_advisory{infix}_lock{suffix}"
        self._unlock_func = f"pg_advisory_unlock{suffix}"

        self._impl = self._load_impl()

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
                f"Lock for '{self._key}' is already held by this {self._scope} scope"
            )

        logger().info("Acquire lock for key: %s", self._key)

        self._locked = self._impl.acquire(self, block=block)
        self._ref_count += 1

        logger().debug("Acquire ref count for key: %s, %d", self._key, self._ref_count)

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
                f"Lock for '{self._key}' is already held by this {self._scope} scope"
            )

        logger().info("Acquire lock for key: %s", self._key)

        self._locked = await self._impl.acquire_async(self, block=block)
        self._ref_count += 1

        logger().debug("Ref count for key: %s, %d", self._key, self._ref_count)

        return self._locked

    def handle_error(self, exc: BaseException) -> None:
        """
        Handle an error.

        Parameters:
            exc (Exception): Exception.
        """
        return self._impl.handle_error(self, exc)

    async def handle_error_async(self, exc: BaseException) -> None:
        """
        Handle an error asynchronously.

        Parameters:
            exc (Exception): Exception.
        """
        return await self._impl.handle_error_async(self, exc)

    def lock_query(self, block: bool) -> str:
        """
        Return the SQL query to acquire the lock.

        Parameters:
            block (bool): Return only once the lock has been acquired.

        Returns:
            str: SQL query.
        """
        lock_func = self._blocking_lock_func if block else self._nonblocking_lock_func

        return f"SELECT pg_catalog.{lock_func}({self._lock_id})"

    def release(self) -> bool:
        """
        Release the lock.

        Transaction level locks do not need to be released manually.

        When using shared locks, all references to the lock within the current scope
        must be released before this method will return True.

        Raises:
            errors.ReleaseError: An attempt to release the lock failed.

        Returns:
            bool: True, if the lock was released, otherwise False.
        """
        if not self._locked:
            logger().debug("Lock not held for key: %s", self._key)

            return False

        elif self._scope == "transaction":
            logger().info(
                "Transaction level lock will automatically be released by PostgreSQL for key: %s",
                self._key,
            )

            return False

        logger().info("Release lock for key: %s", self._key)

        if not self._impl.release(self):
            raise errors.ReleaseError(
                f"Lock for '{self._key}' was not held by this {self._scope} scope"
            )

        self._ref_count -= 1
        self._locked = self._ref_count > 0

        logger().debug("Release ref count for key: %s, %d", self._key, self._ref_count)

        return not self._locked

    async def release_async(self) -> bool:
        """
        Release the lock asynchronously.

        Transaction level locks do not need to be released manually.

        When using shared locks, all references to the lock within the current scope
        must be released before this method will return True.

        Raises:
            errors.ReleaseError: An attempt to release the lock failed.

        Returns:
            bool: True, if the lock was released, otherwise False.
        """
        if not self._locked:
            logger().debug("Lock not held for key: %s", self._key)

            return False

        elif self._scope == "transaction":
            logger().info(
                "Transaction level lock will automatically be released by PostgreSQL for key: %s",
                self._key,
            )

            return False

        logger().info("Release lock for key: %s", self._key)

        if not await self._impl.release_async(self):
            raise errors.ReleaseError(
                f"Lock for '{self._key}' was not held by this {self._scope} scope"
            )

        self._ref_count -= 1
        self._locked = self._ref_count > 0

        logger().debug("Release ref count for key: %s, %d", self._key, self._ref_count)

        return not self._locked

    def unlock_query(self, block: bool = True) -> str:
        """
        Return the SQL query to release the lock.

        Returns:
            str: SQL query.
        """
        return f"SELECT pg_catalog.{self._unlock_func}({self._lock_id})"

    def _load_impl(self) -> ModuleType:
        """
        Load the implementation.

        Raises:
            errors.UnsupportedInterfaceError: The database interface is unsupported.
        """
        module = self._conn.__class__.__module__
        interface = self._interface

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

        self._interface = interface

        try:
            return import_module(f".{interface}", package="postgresql_lock")

        except ModuleNotFoundError:
            raise errors.UnsupportedInterfaceError(
                f"Unsupported database interface '{interface}'"
            )

    async def __aenter__(self) -> bool:
        """
        Enter the context manager.
        """
        logger().debug("Enter context manager for key: %s", self._key)

        return await self._impl.acquire_async(self, block=True)

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
        logger().debug("Exit context manager for key: %s", self._key)

        if exc:
            await self._impl.handle_error_async(self, exc)

        if self._scope != "transaction":
            await self._impl.release_async(self)

        if exc:
            raise exc

    def __enter__(self) -> bool:
        """
        Enter the context manager.
        """
        logger().debug("Enter context manager for key: %s", self._key)

        return self._impl.acquire(self, block=True)

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
        logger().debug("Exit context manager for key: %s", self._key)

        if exc:
            self._impl.handle_error(self, exc)

        if self._scope != "transaction":
            self._impl.release(self)

        if exc:
            raise exc
