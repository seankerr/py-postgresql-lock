"""
Lock mechanism implemented with Postgres advisory locks.
"""

from importlib import import_module

from types import ModuleType
from types import TracebackType

from typing import Any
from typing import Literal
from typing import Optional
from typing import Type

import hashlib

from . import errors


class Lock:
    """ """

    conn: Any
    interface: str
    key: str
    lock_id: int
    scope: str
    shared: bool

    blocking_lock_func: str
    nonblocking_lock_func: str
    unlock_func: str

    def __init__(
        self,
        conn: Any,
        key: str,
        interface: Literal[
            "auto",
            "asyncpg",
            "psycopg2",
            "psycopg3",
            "sqlalchemy",
        ] = "auto",
        scope: Literal["session", "transaction"] = "session",
        shared: bool = False,
    ):
        """
        Create a new `Lock` instance.

        Parameters:
            conn (object): Database connection.
            key (str): Unique lock key.
            interface (str): Database interface.
            scope (str): Lock scope.
            shared (bool): Use a shared lock.
        """
        self.conn = conn
        self.interface = interface
        self.impl = self._load_impl()
        self.key = key
        self.lock_id = str(int(hashlib.sha1(key.encode("utf-8")).hexdigest(), 16))[:18]
        self.scope = scope
        self.shared = shared
        self._locked = False

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
        self._locked = self.impl.acquire(self, block=block)

        return self._locked

    async def acquire_async(self, block: bool = True) -> bool:
        """
        Acquire the lock asynchronously.

        Parameters:
            block (bool): Return only once the lock has been acquired.

        Returns:
            bool: True, if the lock was acquired, otherwise False.
        """
        self._locked = await self.impl.acquire_async(self, block=block)

        return self._locked

    @property
    def locked(self) -> bool:
        """
        Returns the lock status.
        """
        return self._locked

    def release(self) -> bool:
        """
        Release the lock.

        Raises:
            errors.LockReleaseError: An attempt to release the lock failed.

        Returns:
            bool: True, if the lock was released, otherwise False.
        """
        if not self._locked:
            return False

        if not self.impl.release(self):
            raise errors.LockReleaseError()

        self._locked = False

        return True

    async def release_async(self) -> bool:
        """
        Release the lock asynchronously.

        Raises:
            errors.LockReleaseError: An attempt to release the lock failed.

        Returns:
            bool: True, if the lock was released, otherwise False.
        """
        if not self._locked:
            return False

        if not await self.impl.release_async(self):
            raise errors.LockReleaseError()

        self._locked = False

        return True

    async def __aenter__(self) -> bool:
        """
        Enter the context manager.
        """
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
            return import_module(f".{interface}", package="postgres_lock")

        except ModuleNotFoundError:
            raise errors.UnsupportedInterfaceError(
                f"Unsupported database interface '{interface}' or python module not installed"
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
            Traceback (TracebackType): Exception traceback.
        """
        await self.impl.release_async(self)

        if exc:
            raise exc

    def __enter__(self) -> bool:
        """
        Enter the context manager.
        """
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
            Traceback (TracebackType): Exception traceback.
        """
        self.impl.release(self)

        if exc:
            raise exc
