class AcquireError(Exception):
    """An attempt to reacquire a non-shared lock failed."""

    pass


class ReleaseError(Exception):
    """An attempt to release a lock failed due to the current scope not holding it."""

    pass


class UnsupportedInterfaceError(Exception):
    """Database interface specified or detected is unsupported."""

    pass
