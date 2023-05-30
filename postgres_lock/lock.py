"""
"""

from importlib import import_module
from typing import Any
from typing import Literal

from . import errors


class Lock:
    """ """

    def __init__(
        self,
        conn: Any,
        interface: Literal["asyncpg", "psycopg2", "psycopg3"],
        scope: Literal["session", "transaction"],
    ):
        """
        Create a new `Lock` instance.

        Parameters:
        conn (object): Database connection
        interface (str): Database connection interface
        scope (str): Lock scope
        """
        self.conn = conn
        self.scope = scope

        try:
            self.impl = import_module(f".{interface}", package="postgres_lock")
        except ModuleNotFoundError:
            raise errors.UnsupportedInterfaceError(
                f"Unsupported interface '{interface}'"
            )

    def acquire(self, block=True) -> bool:
        """
        Acquire the lock.

        Parameters:
        block (bool): Return only once the lock has been acquired

        Returns:
        bool: True, if the lock was acquired, otherwise False.
        """
        return self.impl.acquire(self.conn, self.scope, block=block)

    async def acquire_async(self, block=True) -> bool:
        """
        Acquire the lock asynchronously.

        Parameters:
        block (bool): Return only once the lock has been acquired

        Returns:
        bool: True, if the lock was acquired, otherwise False.
        """
        return await self.impl.acquire_async(self.conn, self.scope, block=block)

    def release(self) -> None:
        """
        Release the lock.
        """
        return self.impl.release(self.conn)

    async def release_async(self) -> None:
        """
        Release the lock asynchronously.
        """
        return await self.impl.release_async(self.conn)
