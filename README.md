## postgresql-lock

Lock mechanism implemented with PostgreSQL advisory locks.

Easily implement distributed database locking.

### Install

```sh
pip install postgresql-lock
```

### Supported database interfaces

-   **asyncpg**
    -   asynchronous
-   **psycopg2**
    -   synchronous
-   **psycopg3**
    -   asynchronous
    -   synchronous
-   **sqlalchemy** (supports version 1 & 2; can use any underlying database interface)
    -   asynchronous
    -   synchronous

### Why would I use this?

-   PostgreSQL table locks aren't sufficient for your use-case
-   PostgreSQL row locks don't work on `INSERT`
-   You want to prevent race conditions between `INSERT` and `UPDATE` on the same primary key
-   None of the aforementioned details fit your use-case, but you have PostgreSQL installed and need to prevent race conditions in a distributed system

### Default operation

By default `postgresql-lock` will use `session` lock scope in `blocking` mode with
`rollback_on_error` enabled. The `session` lock scope means only a single database connection can
acquire the lock at a time.

### Usage

All work revolves around the `Lock` class.

The easiest way to use `Lock` is with `with` or `async with` statements. The lock will be
released automatically. If `rollback_on_error` is enabled (default), rollbacks are automatically
handled prior to release.

_Using `with` and `async with` implies blocking mode._

```python
from postgresql_lock import Lock

# setup connection
conn = ...

# create and use lock
with Lock(conn, "shared-identifier"):
    print("Acquired lock!")

    # do something here
```

Now compare the above example to the equivalent try/finally example below:

```python
from postgresql_lock import Lock

# setup connection
conn = ...

# create lock
lock = Lock(conn, "shared-identifier")

try:
    # acquire lock
    lock.acquire()

    print("Acquired lock!")

    try:
        # do something here
        pass

    except Exception as exc:
        # handle_error() will rollback the transaction by default
        lock.handle_error(exc)

        raise exc
finally:
    # release lock (this is safe to run even if the lock has not been acquired)
    lock.release()
```

### Asynchronous usage (without `async with`)

```python
from postgresql_lock import Lock

# setup connection
conn = ...

# create lock
lock = Lock(conn, "shared-identifier")

try:
    # acquire lock
    await lock.acquire_async()

    print("Acquired lock!")

    try:
        # do something here
        pass

    except Exception as exc:
        # handle_error_async() will rollback the transaction by default
        await lock.handle_error_async(exc)

        raise exc
finally:
    # release lock (this is safe to run even if the lock has not been acquired)
    await lock.release_async()
```

### Non-blocking mode (supports async as well)

```python
from postgresql_lock import Lock

# setup connection
conn = ...

# create lock
lock = Lock(conn, "shared-identifier")

# acquire lock
if lock.acquire(block=False):
    # do something here
    pass

else:
    # could not acquire lock
    pass

# release lock (this is safe to run even if the lock has not been acquired)
lock.release()
```

### Specify the database interface manually

```python
from postgresql_lock import Lock

# setup connection
conn = ...

# create and use lock
lock = Lock(conn, "shared-identifier", interface="asyncpg")

# do things with the lock
```

### Handle rollbacks manually

```python
from postgresql_lock import Lock

# setup connection
conn = ...

# create and use lock
lock = Lock(conn, "shared-identifier", rollback_on_error=False)

# do things with the lock
```

### Changelog

-   **0.2.2**
    -   Fix: error raised when releasing transaction level locks
-   **0.2.1**
    -   Moved public Lock fields to properties
-   **0.1.9**
    -   Fix: release_async() bug for sqlalchemy connections
-   **0.1.8**
    -   Add logger() function
    -   Use "postgresql_lock" logger name
-   **0.1.7**
    -   Add logging statements
-   **0.1.6**
    -   Use int.from_bytes() to convert lock key into integer
    -   Fix: psycopg3 close() not being awaited bug
-   **0.1.5**
    -   Rename package from postgres-lock to postgresql-lock
-   **0.1.4**
    -   Add py.typed for mypy
-   **0.1.3**
    -   Key can be any object
-   **0.1.2**
    -   Add Lock.rollback_on_error (default true)
    -   Add Lock.handle_error() & Lock.handle_error_async()
-   **0.1.1**
    -   Key can be str or int
