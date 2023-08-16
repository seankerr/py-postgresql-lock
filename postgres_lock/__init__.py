# --------------------------------------------------------------------------------------
# Copyright (c) 2023 Sean Kerr
# --------------------------------------------------------------------------------------

"""
Lock mechanism implemented with Postgres advisory locks.
"""

# postgres-lock imports
from .lock import Lock

__all__ = ["Lock"]
