"""
Lock mechanism implemented with Postgres advisory locks.
"""

from .lock import Lock

__all__ = [Lock]
