## postgres-lock

Lock mechanism implemented with Postgres advisory locks.

Easily implement distributed database locking.

### Install

```sh
pip install postgres-lock
```

### Supported database interfaces

- **asyncpg**
  - asynchronous
- **psycopg2**
  - synchronous
- **psycopg3**
  - asynchronous
  - synchronous
- **sqlalchemy** (supports version 1 & 2; can use any underlying database interface)
  - asynchronous
  - synchronous

### Why would I use this?

- Postgres table locks aren't sufficient for your use-case
- Postgres row locks don't work on `INSERT`
- You want to prevent race conditions between `INSERT` and `UPDATE` on the same primary key
- None of the aforementioned details fit your use-case, but you have Postgres installed and need to prevent race conditions in a distributed system

### Default operation

By default `postgres-lock` will use `session` lock scope in `blocking` mode. The `session` lock scope
means only a single database connection can acquire the lock at a time.

### Usage

All work revolves around the `Lock` class.

The easiest way to use `Lock` is with `with` or `async with` statements. The lock will be
released automatically.

_Using `with` and `async with` implies blocking mode._

```python
from postgres_lock import Lock

# setup connection
conn = ...

# create and use lock
with Lock(conn, "shared-identifier"):
    print("Acquired lock!")

    # do something here
```

Now compare the above example to the equivalent try/finally example below:

```python
from postgres_lock import Lock

# setup connection
conn = ...

# create lock
lock = Lock(conn, "shared-identifier")

try:
    # acquire lock
    lock.acquire()

    print("Acquired lock!")

    # do something here
finally:
    # release lock (this is safe to run even if the lock has not been acquired)
    lock.release()
```

### Asynchronous usage (without `async with`)

```python
from postgres_lock import Lock

# setup connection
conn = ...

# create lock
lock = Lock(conn, "shared-identifier")

try:
    # acquire lock
    await lock.acquire_async()

    print("Acquired lock!")

    # do something here
finally:
    # release lock (this is safe to run even if the lock has not been acquired)
    await lock.release_async()
```

### Non-blocking mode (supports async as well)

```python
from postgres_lock import Lock

# setup connection
conn = ...

# create lock
lock = Lock(conn, "shared-identifier")

try:
    # acquire lock
    if lock.acquire(block=False):
        print("Acquired lock!")

        # do something here

    else:
        print("Could not acquire lock!")

finally:
    # release lock (this is safe to run even if the lock has not been acquired)
    lock.release()
```

### Specify the database interface manually

```python
from postgres_lock import Lock

# setup connection
conn = ...

# create and use lock
with Lock(conn, "shared-identifier", interface="asyncpg"):
    print("Acquired lock!")

    # do something here
```

### Changelog

- **0.1.1**
  - Key can be str or int
