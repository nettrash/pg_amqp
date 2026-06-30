CREATE TABLE @extschema@.broker (
  broker_id   serial  NOT NULL,
  host        text    NOT NULL,
  port        integer NOT NULL DEFAULT 5672,
  vhost       text,
  username    text,
  password    text,
  ssl         boolean NOT NULL DEFAULT false,
  ssl_verify  boolean NOT NULL DEFAULT true,
  ssl_cacert  text,
  ssl_cert    text,
  ssl_key     text,
  PRIMARY KEY (broker_id, host, port)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.broker', '');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.broker_broker_id_seq', '');

-- Transactional outbox.  Enqueue with amqp.publish_outbox() inside your business
-- transaction (a plain INSERT, so it commits/rolls back atomically with your data),
-- then ship to the broker out of band with amqp.publish_outbox_batch().  This keeps
-- the broker entirely off the business transaction's commit path and guarantees a
-- message is never lost merely because the transaction committed.  Delivery is
-- at-least-once -- make consumers idempotent (see the functions' COMMENTs).
CREATE TABLE @extschema@.outbox (
  id             bigserial   PRIMARY KEY,
  broker_id      integer     NOT NULL,
  exchange       varchar     NOT NULL,
  routing_key    varchar     NOT NULL DEFAULT '',
  message        varchar     NOT NULL,
  delivery_mode  integer,
  content_type   varchar,
  reply_to       varchar,
  correlation_id varchar,
  mandatory      boolean     NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now(),
  published_at   timestamptz,
  attempts       integer     NOT NULL DEFAULT 0,
  last_error     text
);

-- The relay's hot path: as-yet-unpublished rows, in enqueue order, per broker.
CREATE INDEX outbox_unpublished_idx
  ON @extschema@.outbox (broker_id, id) WHERE published_at IS NULL;

-- pg_dump carries forward pending (undelivered) messages across dump/restore, but
-- not the already-published history; plus the sequence value.
SELECT pg_catalog.pg_extension_config_dump('@extschema@.outbox', 'WHERE published_at IS NULL');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.outbox_id_seq', '');

