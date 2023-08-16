# --------------------------------------------------------------------------------------
# Copyright (c) 2023 Sean Kerr
# --------------------------------------------------------------------------------------

# postgres-lock imports
import postgres_lock


def test__all__():
    assert postgres_lock.__all__ == ["Lock"]
