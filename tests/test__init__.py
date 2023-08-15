import postgres_lock


def test__all__():
    assert postgres_lock.__all__ == ["Lock"]
