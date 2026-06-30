# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_amqp is a PostgreSQL C extension that lets SQL statements publish messages to an
AMQP 0-9-1 broker (RabbitMQ-compatible). Two parts ship together:

- **C module** (`src/pg_amqp.c`) — the backend functions, compiled into `pg_amqp.so`.
- **SQL schema** (`sql/`, `updates/`) — installed via `CREATE EXTENSION amqp`, creating the
  `amqp` schema with the `amqp.broker` config table, the `publish` / `autonomous_publish` /
  `exchange_declare` / `disconnect` functions, and the transactional-outbox feature
  (`amqp.outbox` table + `publish_outbox()` enqueue / `publish_outbox_batch()` relay) for
  at-least-once delivery that keeps the broker off the business transaction's commit path.

Note the naming split: the shared library is **`pg_amqp.so`** but the extension/control/schema are
all named **`amqp`** (`amqp.control`, `module_pathname = '$libdir/pg_amqp'`). SQL function bodies
reference `'pg_amqp.so'`; the extension is loaded as `amqp`.

## Current state: rabbitmq-c migration (branch `feature/rabbitmq-c-migration`)

The repo is mid-migration to **0.5.0**, which replaces the bundled librabbitmq fork with the
maintained system [rabbitmq-c](https://github.com/alanxz/rabbitmq-c) library. This affects how you work here:

- `src/pg_amqp.c` already includes the system headers (`<rabbitmq-c/amqp.h>` or legacy `<amqp.h>`),
  links via `pkg-config librabbitmq`, and uses rabbitmq-c's TLS sockets. **This is the file to edit.**
- **`src/librabbitmq/` is the old vendored fork and is NO LONGER COMPILED** (the Makefile's `OBJS`
  is `src/pg_amqp.o` only; `MODULE_big` globs `src/*.c`, not the subdir). Treat it as dead/legacy —
  do not make functional changes there.
- Version is now **0.5.0** across `amqp.control`, `META.json`, and `CHANGELOG`. The 0.5.0 release also
  carries a high-load reliability hardening pass (see the CHANGELOG 0.5.0 entry): bounded commit-path
  broker I/O, the `IMMUTABLE → VOLATILE` volatility fix, loud (no-longer-silent) message-loss WARNINGs,
  SubXact/2PC safety, broker flow-control awareness, and GUC-tunable timeouts. The only SQL change vs
  0.4.6 is `updates/amqp--0.4.6--0.5.0.sql`, which `ALTER FUNCTION ... VOLATILE`s the four functions
  (grants preserved); everything else is in `src/pg_amqp.c`.
- **Validating C changes without a full PG dev install:** this repo can be object-compiled even where
  PGXS isn't present —
  `cc -c -fPIC -O2 -Wall -Wdeclaration-after-statement -I"$(pg_config --includedir-server)" -I/opt/homebrew/opt/rabbitmq-c/include src/pg_amqp.c -o /tmp/x.o`.
  A clean compile here is a good pre-flight before a real `make`.

## Build & install

Prerequisites: `pg_config` on PATH (PostgreSQL `-devel`/`-dev` package) and rabbitmq-c dev headers
(`librabbitmq-dev` on Ubuntu/Debian — the primary target; `brew install rabbitmq-c` on macOS).

```bash
make                 # generate sql/amqp--<ver>.sql, then compile pg_amqp.so
sudo make install    # install .so + control + generated SQL + updates/ into the PG tree
make clean
```

- The build needs GNU make (use `gmake` if `make` isn't GNU). Requires PostgreSQL 9.1+; the Makefile
  hard-errors on older versions via the `PG_GE91` check.
- Point `pkg-config` at a non-standard prefix when needed (common on Apple Silicon):
  `env PKG_CONFIG_PATH=/opt/homebrew/opt/rabbitmq-c/lib/pkgconfig make`
- Override the server with `env PG_CONFIG=/path/to/pg_config make`.
- In-database: requires `shared_preload_libraries = 'pg_amqp.so'`, then `CREATE EXTENSION amqp;`
  (new install) or `ALTER EXTENSION amqp UPDATE;` (upgrade).

There is **no automated test suite** — the PGXS `REGRESS`/`TESTS` lines are commented out in the
Makefile. Verification is manual against a live RabbitMQ broker.

## Architecture

**Transactional vs autonomous publish is the core design.** Each broker connection opens two AMQP
channels, and the function you call selects which one (`pg_amqp_publish_opt(fcinfo, channel)`):

- **Channel 2 = transactional**, used by `amqp.publish()`. The channel runs in AMQP tx mode
  (`amqp_tx_select`). Published messages are *buffered* at the broker and only released when the
  surrounding **PostgreSQL transaction commits**. This is wired through `RegisterXactCallback` in
  `_PG_init`: `amqp_local_phase2` issues `amqp_tx_commit` on `XACT_EVENT_COMMIT` and `amqp_tx_rollback`
  on `XACT_EVENT_ABORT`. So a PG `ROLLBACK` also discards the AMQP messages.
- **Channel 1 = non-transactional**, used by `amqp.autonomous_publish()`. Messages are sent
  immediately and are unaffected by later PG commit/rollback.

**Per-backend connection state.** `brokerstate` structs form a linked list rooted at the static
`HEAD_BS`, allocated in `TopMemoryContext`, keyed by `broker_id`. Connections are lazy (opened on
first publish) and live until the backend exits or `amqp.disconnect(broker_id)` is called. There is
no shared/cross-backend state.

**Broker config & failover.** `amqp.broker` rows are loaded once per connection via SPI
(`local_amqp_load_hosts`) and cached for the connection's lifetime — `amqp.disconnect()` is what
invalidates the cache, so that's how rotated credentials or new hosts take effect without a backend
restart. **Multiple `amqp.broker` rows sharing one `broker_id` are failover hosts**, tried
round-robin (`bs->idx`, seeded per-backend by `MyProcPid` to spread load). Resilience tuning is split
across **GUCs** registered in `_PG_init` (`pg_amqp.connect_timeout_ms`, `handshake_timeout_ms`,
`operation_timeout_ms`, `commit_timeout_ms`, `connect_budget_ms`, `max_body_bytes`) plus the `AMQP_RECONNECT_BACKOFF_*`
constants (exponential backoff + jitter) and tuned `SO_KEEPALIVE`/`TCP_NODELAY`/keepalive intervals.
`local_amqp_set_timeout` re-applies the right budget per phase (handshake vs steady-state vs commit).

**Reliability invariants to preserve when editing the publish/commit paths** (these are load-bearing —
see the CHANGELOG 0.5.0 entry and the README "Delivery semantics"):
- The commit/abort callback (`amqp_local_phase2`) runs *after* PG is durably committed with locks
  held, so its broker I/O is bounded by `commit_timeout_ms` and never does a blocking graceful close.
  `local_amqp_disconnect_bs(bs, graceful)` takes `graceful=false` on all error/commit/redo paths.
- Discarding buffered messages must stay **loud** (`elog(WARNING, ... count ...)`), never silent.
- `XACT_EVENT_PRE_PREPARE` errors if anything is buffered (no 2PC); the SubXact callback discards the
  buffer on savepoint/`EXCEPTION` rollback (no phantom delivery).
- Publish trusts only the `amqp_basic_publish` return value; it does **not** consult
  `amqp_get_rpc_reply` after an async publish (that reads stale state). Broker-pushed frames
  (returns, `connection.blocked`, channel/connection close) are surfaced by draining before each publish.

**TLS** is per-broker via the `ssl*` columns on `amqp.broker` (handled inside rabbitmq-c's ssl socket;
TLSv1.2+). Note the security caveat in the README: `ssl_cacert`/`ssl_cert`/`ssl_key` are filesystem
paths opened by the postgres OS user, so write access to `amqp.broker` should be restricted.

**`mandatory` / unroutable returns.** With `mandatory => true`, the broker returns unroutable messages
via `basic.return`. rabbitmq-c has no return callback, so `local_amqp_drain_returns` polls for return
frames (`amqp_simple_wait_frame_noblock`) and emits a `WARNING`. For transactional publish this is
surfaced at commit time; on the autonomous channel detection is best-effort.

## SQL build & versioning conventions

- The install script is **generated, not hand-written**: `make` concatenates
  `sql/tables/*.sql` + `sql/functions/*.sql` into `sql/amqp--<EXTVERSION>.sql` (a simple `cat`).
  `EXTVERSION` is parsed out of `default_version` in `amqp.control`. Edit the sources under
  `sql/tables/` and `sql/functions/`, never the generated file.
- Schema sources use the `@extschema@` placeholder (resolves to `amqp`).
- Upgrades are incremental `updates/amqp--<from>--<to>.sql` scripts applied by `ALTER EXTENSION amqp
  UPDATE`. When a function signature changes you must **drop and recreate** it; the established
  pattern (see `updates/amqp--0.4.5--0.4.6.sql`) snapshots existing `EXECUTE` grants into a temp table
  and re-applies them after recreation so privileges survive the upgrade.
- **C and SQL signatures must stay in lockstep.** A new publish argument means: add the SQL param in
  `sql/functions/functions.sql`, read it by index in `pg_amqp_publish_opt` (current order:
  0 broker_id, 1 exchange, 2 routing_key, 3 message, 4 delivery_mode, 5 content_type, 6 reply_to,
  7 correlation_id, 8 mandatory), AND write a drop/recreate update script.
- When releasing, bump the version in **`amqp.control`, `META.json`, and `CHANGELOG`** together, and
  add the `updates/amqp--<prev>--<new>.sql` script.
