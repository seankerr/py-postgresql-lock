"""
Lock mechanism implemented with PostgreSQL advisory locks.
"""

# postgresql-lock imports
from .lock import Lock

__all__ = ["Lock"]
