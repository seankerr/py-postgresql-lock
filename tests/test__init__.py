# postgresql-lock imports
import postgresql_lock


def test__all__() -> None:
    assert postgresql_lock.__all__ == ["Lock"]
