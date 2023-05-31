## postgres-lock

Lock mechanism implemented with Postgres advisory locks.

### Scopes, modes, & connections

- `session` and `transaction` lock scopes
- `blocking` and `nonblocking` modes
- `synchronous` and `asynchronous` connections
- `shared` locks

### Database interfaces

- asyncpg
  - asynchronous
- psycopg2
  - synchronous
- psycopg3
  - asynchronous
  - synchronous
- sqlalchemy
  - asynchronous
  - synchronous

### Why would I use this?

- Postgres table locks aren't sufficient for your use-case
- Postgres row locks don't work on `INSERT`
- You want to prevent race conditions between `INSERT` and `UPDATE` on the same primary key

### Default operation

By default `postgres-lock` will use `session` lock scope in `blocking` mode. The `session` lock scope
means only a single database connection can acquire the lock at a time.

### Usage

All work revolves around the `Lock` class which holds the database connection, lock key, and
specifies the interface your connection is using.

The easiest way to use `Lock` is with python's `with` or `async with` statements. The lock will be
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
    # release lock
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
    # release lock
    await lock.release_async()
```

### Non-blocking mode

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
    # release lock
    lock.release()
```
