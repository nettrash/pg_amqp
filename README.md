pg_amqp
=============

The pg_amqp package provides the ability for postgres statements to directly
publish messages to an [AMQP](http://www.amqp.org/) broker.

This repository is a maintained continuation of the original
[OmniTI pg_amqp](https://github.com/omniti-labs/pg_amqp), carried forward by
[nettrash](https://github.com/nettrash). All bug reports, feature requests and
general questions can be directed to the [Issues
section](https://github.com/nettrash/pg_amqp/issues) of this repository. See
[Credits](#credits) below for the original authors and the history of changes.

Compatibility
-------------

Version 0.5.0 migrates the transport from the bundled librabbitmq fork to the
maintained [rabbitmq-c](https://github.com/alanxz/rabbitmq-c) library (0.9 or
newer; Ubuntu 20.04+/Debian 11+ `librabbitmq-dev` is supported, and the build is
verified on Ubuntu 22.04 and 24.04). This brings native TLS, IPv6, and ongoing
security maintenance. See Building for the new dependency.

Version 0.4.4 includes compatibility fixes for PostgreSQL 18 toolchains and
modern RabbitMQ deployments (including IPv6 resolution and TLS/SSL transport).

The extension speaks AMQP 0-9-1 and is intended for RabbitMQ-compatible
brokers.


Building
--------

As of 0.5.0 pg_amqp links against the maintained
[rabbitmq-c](https://github.com/alanxz/rabbitmq-c) client library instead of a
bundled copy. Install its development package first:

    # Ubuntu/Debian (primary target)
    sudo apt-get install librabbitmq-dev

    # RHEL/Rocky/Fedora
    sudo dnf install librabbitmq-devel

    # macOS (Homebrew)
    brew install rabbitmq-c

The build locates the library via `pkg-config librabbitmq` (falling back to a
plain `-lrabbitmq`). TLS/SSL is handled inside rabbitmq-c, so pg_amqp no longer
links OpenSSL directly. If `pkg-config` cannot find the library, point it at the
install prefix, e.g.:

    env PKG_CONFIG_PATH=/opt/homebrew/opt/rabbitmq-c/lib/pkgconfig make

To build pg_amqp, just do this:

    make
    make install

If you encounter an error such as:

    "Makefile", line 8: Need an operator

You need to use GNU make, which may well be installed on your system as
`gmake`:

    gmake
    gmake install

If you encounter an error such as:

    make: pg_config: Command not found

Be sure that you have `pg_config` installed and in your path. If you used a
package management system such as RPM to install PostgreSQL, be sure that the
`-devel` package is also installed. If necessary tell the build process where
to find it:

    env PG_CONFIG=/path/to/pg_config make && make install

Some prepackaged Mac installs of postgres might need a little coaxing with
modern XCodes.  If you encounter an error such as:

    make: /Applications/Xcode.app/Contents/Developer/Toolchains/OSX10.8.xctoolchain/usr/bin/cc: No such file or directory

Then you'll need to link the toolchain

    sudo ln -s /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain /Applications/Xcode.app/Contents/Developer/Toolchains/OSX10.8.xctoolchain

And if you encounter an error about a missing `/usr/bin/postgres`:

    ld: file not found: /usr/bin/postgres

You might need to link in your real postgres:

    sudo ln -s /usr/bin/postgres_real /usr/bin/postgres

Loading
-------

Once amqp is installed, you can add it to a database. Add this line to your
postgresql config

    shared_preload_libraries = 'pg_amqp.so'

This extension requires PostgreSQL 9.1.0 or greater, so loading amqp is as simple
as connecting to a database as a super user and running 

    CREATE EXTENSION amqp;

If you've upgraded your cluster to PostgreSQL 9.1 and already had amqp
installed, you can upgrade it to a properly packaged extension with:

    CREATE EXTENSION amqp FROM unpackaged;

This is required to update to any versions >= 0.4.0.

To update to the latest version, run the following command after running "make install" again:

    ALTER EXTENSION amqp UPDATE;

Basic Usage
-----------

Insert AMQP broker information (host/port/user/pass) into the
`amqp.broker` table.

A process starts and connects to PostgreSQL and runs:

    SELECT amqp.publish(broker_id, 'amqp.direct', 'foo', 'message');

Upon process termination, all broker connections will be torn down.
If there is a need to disconnect from a specific broker, one can call:

    select amqp.disconnect(broker_id);

which will disconnect from the broker if it is connected and do nothing
if it is already disconnected.

TLS/SSL Broker Configuration
----------------------------

The `amqp.broker` table supports optional TLS/SSL settings:

- `ssl` (boolean, default `false`): enable TLS for this broker connection
- `ssl_verify` (boolean, default `true`): verify the server certificate
- `ssl_cacert` (text): path to a PEM CA bundle for trust validation
- `ssl_cert` (text): path to PEM client certificate for mutual TLS
- `ssl_key` (text): path to PEM client private key for mutual TLS

Example (server verification enabled):

        INSERT INTO amqp.broker (
            host, port, username, password,
            vhost, ssl, ssl_verify, ssl_cacert
        ) VALUES (
            'rabbitmq.example.internal', 5671, 'app_user', 'secret',
            '/', true, true, '/etc/ssl/certs/ca-bundle.pem'
        );

For mutual TLS, also set `ssl_cert` and `ssl_key`.

Note: certificate/key paths must be readable by the PostgreSQL server process.

Security: `ssl_cacert`, `ssl_cert`, and `ssl_key` are filesystem paths opened
by the PostgreSQL backend. A role with INSERT/UPDATE on `amqp.broker` can
therefore cause the server process to attempt to open arbitrary files
readable by the postgres OS user. Restrict write privileges on
`amqp.broker` accordingly (typically: superuser/owner only). When `ssl` is
enabled with `ssl_verify = false` the server certificate is **not** validated
(a WARNING is logged); avoid it outside trusted networks.

Delivery semantics
------------------

Publishing is **synchronous and best-effort**, performed inside the calling
PostgreSQL backend:

- `amqp.publish()` buffers on a transactional AMQP channel and the messages are
  flushed to the broker when the surrounding **PostgreSQL transaction commits**
  (and discarded if it rolls back). The flush happens *after* PostgreSQL is
  already durably committed, so a broker failure at that point cannot abort the
  database transaction — it is reported as a `WARNING` (with the broker id and
  the count of lost messages) and the messages are lost. A `true` return from
  `publish()` means "buffered locally", **not** "delivered": there are no
  publisher confirms. Always check the boolean return and treat `false` as a
  reason to `ROLLBACK` if the message matters.
- `amqp.autonomous_publish()` sends immediately and is unaffected by the
  transaction outcome.
- Messages published inside a savepoint or PL/pgSQL `EXCEPTION` block that later
  rolls back are **not** delivered: to avoid emitting rolled-back work, the whole
  transaction's buffered messages are discarded (with a `WARNING`) at commit.
- `amqp.publish()` is rejected (`ERROR`) in a transaction that is then
  `PREPARE`d (two-phase commit). Use `autonomous_publish()` or an outbox for 2PC.

**If you cannot tolerate message loss, do not rely on inline publishing — use the
built-in transactional outbox.** Enqueue inside your business transaction with
`amqp.publish_outbox()` (a plain `INSERT` into `amqp.outbox`, atomic with your
data and entirely off the broker's commit path), then ship to the broker out of
band with `amqp.publish_outbox_batch()`:

    -- in your application transaction (same signature as amqp.publish):
    SELECT amqp.publish_outbox(broker_id, 'amq.direct', 'foo', 'message');

    -- in a relay worker / pg_cron job, as its own transaction:
    SELECT amqp.publish_outbox_batch();            -- all brokers, up to 100 rows
    SELECT amqp.publish_outbox_batch(broker_id, 500);  -- one broker, larger batch

`publish_outbox_batch()` claims rows with `FOR UPDATE SKIP LOCKED`, so you can run
several relay workers concurrently. Delivery is **at-least-once** (the relay
re-sends on retry and, absent publisher confirms, cannot detect a broker that
accepts-then-drops a message), so make consumers idempotent. Undelivered rows are
preserved across `pg_dump`/restore; published history and failures stay in
`amqp.outbox` (`published_at`, `attempts`, `last_error`) for inspection — prune it
yourself.

Note also that there is one broker connection (two channels) per PostgreSQL
backend with no pooling, so the broker connection count scales with your backend
count.

Tuning
------

The publish path will never block a backend longer than these (per broker host),
all set via GUCs (`SET`, `postgresql.conf`, or `ALTER SYSTEM`):

| GUC | Default | Controls |
|-----|---------|----------|
| `pg_amqp.connect_timeout_ms`   | `2000`  | TCP/TLS connect handshake |
| `pg_amqp.handshake_timeout_ms` | `2000`  | AMQP login / channel.open / tx.select |
| `pg_amqp.operation_timeout_ms` | `30000` | steady-state publish recv/send |
| `pg_amqp.commit_timeout_ms`    | `2000`  | tx.commit / tx.rollback on the PostgreSQL commit/abort path |
| `pg_amqp.connect_budget_ms`    | `0`     | total budget across all failover hosts in one connect attempt (`0` = unlimited) |
| `pg_amqp.max_body_bytes`       | `0`     | reject bodies larger than this many bytes (`0` = no limit) |

`commit_timeout_ms` is deliberately small because that operation runs inside
PostgreSQL's commit while locks are held; lower it further if your broker SLA is
tight. Under a degraded broker the failover/reconnect path also applies an
exponential backoff (2s base, 30s cap) with per-backend jitter to avoid a
cluster-wide reconnect storm. Prefer IP addresses over hostnames in
`amqp.broker` — DNS resolution during connect is not covered by
`connect_timeout_ms`.

Credits
-------

pg_amqp was originally written by [Theo Schlossnagle](http://lethargy.org/~jesus/)
at OmniTI Computer Consulting, Inc. (2009) and subsequently maintained by
David E. Wheeler and [Keith Fiske](http://www.keithf4.com), with contributions
from Vasilis Ventirozos, Eric Satterwhite, Mark Fowler, Patrick Molgaard, Phil
Sorber, Robert Treat and others. The original project lives at
[omniti-labs/pg_amqp](https://github.com/omniti-labs/pg_amqp).

Since 0.4.3 this repository is maintained by
[nettrash](https://github.com/nettrash), who added:

- PostgreSQL 16 and 18 compatibility (ANSI C prototypes, PG 18 `XactEvent`
  handling).
- Per-broker TLS/SSL connections and IPv6 support (`getaddrinfo`).
- Connection-handling optimizations: reconnect backoff, TCP keepalive/`NODELAY`,
  split connect/operation timeouts, and cached broker configuration.
- The optional `mandatory` argument with unroutable-return (`basic.return`)
  reporting.
- The **0.5.0** migration from the bundled ~2010 librabbitmq fork to the
  maintained [rabbitmq-c](https://github.com/alanxz/rabbitmq-c) client, plus a
  high-load / unreliable-broker hardening pass (bounded commit-path I/O,
  GUC-tunable timeouts, loud message-loss `WARNING`s, savepoint/2PC safety,
  broker flow-control awareness) and a transactional outbox for at-least-once
  delivery.

See the [CHANGELOG](CHANGELOG) for the full, per-release history.

License
-------

pg_amqp is distributed under the BSD license; see the copyright header at the
top of [`src/pg_amqp.c`](src/pg_amqp.c). Copyright (c) 2009 OmniTI Computer
Consulting, Inc. All rights reserved. Contributions from subsequent maintainers
are provided under the same terms.
