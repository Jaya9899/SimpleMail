# SimpleMail – Backend (smserver, smspooler, common)

Backend half of the SimpleMail lab project: the TCP server, the background
spooler, and the shared code. Written in C++17 for Linux/WSL.

## Files

| File | Purpose |
|------|---------|
| `smserver.cpp` | `poll()`-based TCP server. Speaks SMTP2 (send) and SMP (retrieve). |
| `smspooler.cpp` | Creates the shared-memory ring, drains it, writes mail files. |
| `smcommon.cpp` | DJB2 hash, timestamped logging, user-file loading. |
| `smcommon.h` | Shared structs and constants (provided, unmodified). |
| `smutil.h` | Extra backend helper declarations (keeps `smcommon.h` untouched). |
| `users.txt` | Sample users, one `username password` pair per line. |

## Build and run

```bash
make backend                 # builds smserver and smspooler

./smspooler users.txt        # terminal 1 – start first
./smserver 2525 users.txt    # terminal 2 – start second, same directory
```

Both programs must run from the same directory because mailboxes live in a
relative `mailboxes/` folder. If you restart the spooler, restart the server
too (the spooler recreates the shared-memory segment). Stop with Ctrl+C; the
spooler drains the queue and removes the segment on exit.

## Shared memory layout

One System V segment (`SM_SPOOL_KEY`) holding an `SMSPool`: a ring of 16
`SMMailMessage` slots (sender, recipients, subject, up to 65536 body bytes,
ticket id), plus `head`, `tail`, `next_ticket_id` and three process-shared
semaphores. The spooler creates and initialises it and sets the magic number
last; the server attaches and refuses to start if it is missing.

## Synchronisation

- `empty_slots` (starts at 16) and `full_slots` (starts at 0) count free and
  filled slots.
- `ring_mutex` (binary) protects `head`, `tail` and the ticket counter.
- Server (producer): `sem_trywait(empty)` → lock → reserve slot and ticket →
  unlock → fill slot → `sem_post(full)`. It never blocks; if the ring is full
  the client waits in an `ST_ENQUEUE` state and is retried each loop.
- Spooler (consumer): `sem_timedwait(full)` → lock → take `tail` → unlock →
  write files → `sem_post(empty)`.

## Concurrency

The server is single-threaded. All sockets are non-blocking, `poll()` waits on
the listener and every client, and each client is a small state machine with
its own input and output buffers, so a slow client never blocks others.
Clients that send no `MODE` within 30 seconds are disconnected.

## Flow control (BODY_STREAM)

- The window starts at 2048 bytes and drops by each `CHUNK`'s size; the server
  replies `OK Window <remaining>`.
- A `CHUNK` larger than the remaining window gets `ERR Window overflow` and
  the connection is dropped.
- Credit is returned with an unsolicited `WINDOW_UPDATE <n>` when the window
  reaches 0 or at least half a window is owed, and only while the ring has a
  free slot, so a slow spooler throttles senders.
- `END_BODY` queues the message; the reply is `OK Queued <ticket> to N mailboxes`.
- Legacy `BODY` uses dot-termination with byte-stuffing. Bodies over 65536
  bytes get `ERR Body too large` and the connection is closed.

## Mailboxes

Only the spooler writes mail. Each mail is `mailboxes/<user>/<id>.txt`,
written to a temp file and renamed into place. IDs increase monotonically per
user and are persisted in `.next_id`, so they are never reused after deletion.
The server only reads and deletes mailbox files (for `LIST`, `READ`,
`DELETE`, `COUNT`).

## Protocol notes agreed with the client

- `MODE RECV` → server sends `OK`, then `AUTH REQUIRED <nonce>`.
- Auth hash = DJB2 of `password + nonce`, sent as an unsigned decimal.
- Third failed login → `ERR Too many failures`, then close.
- `READ` → `OK`, the full mail (dot-stuffed), then a line with a single `.`.

## Tested

Verified with a scripted client: both send modes, window updates, window
overflow, body limit, authentication (good, bad, 3 failures), LIST / READ /
DELETE / COUNT, and ID increments. Not yet tested: many simultaneous clients,
a full ring (16 queued messages), and the real `smclient`.
