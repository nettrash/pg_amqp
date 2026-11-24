-- 0.5.0 -> 0.5.1
-- C: PostgreSQL 19 support, and on PostgreSQL 18+ the library reports its name and
-- version in pg_get_loaded_modules().  "make install" (and a restart, since
-- pg_amqp.so is preloaded) is required for that part.
--
-- SQL: bring every upgraded installation in line with a fresh 0.5.1 install, whatever
-- version it was originally created at.  Every statement below is idempotent.

-- Installations created at 0.4.2 or earlier never registered amqp.broker and its
-- sequence with pg_extension_config_dump (only the fresh-install script ever did),
-- so pg_dump silently left the broker configuration out.  Registering a table that
-- is already registered just re-sets its (empty) filter.
SELECT pg_catalog.pg_extension_config_dump('@extschema@.broker', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.broker_broker_id_seq', '');

-- The comments on the TLS columns were only ever added by the 0.4.2 -> 0.4.3 update,
-- never by the fresh-install script; they are now in both.
COMMENT ON COLUMN @extschema@.broker.ssl        IS 'Enable TLS/SSL for this broker connection';
COMMENT ON COLUMN @extschema@.broker.ssl_verify IS 'Verify the broker TLS certificate (recommended: true)';
COMMENT ON COLUMN @extschema@.broker.ssl_cacert IS 'Path to PEM CA certificate bundle (NULL = system defaults)';
COMMENT ON COLUMN @extschema@.broker.ssl_cert   IS 'Path to PEM client certificate for mutual TLS (optional)';
COMMENT ON COLUMN @extschema@.broker.ssl_key    IS 'Path to PEM client private key for mutual TLS (optional)';

-- The 0.4.6 -> 0.5.0 update created the outbox functions with abbreviated comments;
-- use the full text of the fresh-install script.
COMMENT ON FUNCTION @extschema@.publish_outbox(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) IS
'Enqueue a message into the transactional outbox (amqp.outbox) instead of publishing it
directly.  This is a plain INSERT, so it commits or rolls back atomically with the
surrounding transaction: the message is never lost merely because the business
transaction committed, and never sent if it rolled back.  Ship enqueued messages to the
broker with amqp.publish_outbox_batch().  Returns the new outbox row id.';

COMMENT ON FUNCTION @extschema@.publish_outbox_batch(integer, integer) IS
'Relay up to batch_limit unpublished amqp.outbox rows (optionally only for one broker_id)
to the broker via amqp.autonomous_publish(), marking each published.  Uses FOR UPDATE SKIP
LOCKED so several relay workers can run concurrently.  Call it on a schedule (e.g. pg_cron)
or from a polling worker; returns the number of messages published.  Delivery is
at-least-once: a relay crash mid-batch re-sends on the next run, and -- because pg_amqp has
no publisher confirms -- a broker that accepts then drops a message cannot be detected, so
consumers should be idempotent.';
