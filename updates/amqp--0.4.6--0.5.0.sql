-- 0.5.0 replaces the bundled librabbitmq fork with the maintained rabbitmq-c
-- client library and hardens the extension for high-load / unreliable-broker
-- conditions.  Most of the change is in C/build; the only SQL change is the
-- function volatility correction below.
--
-- A "make install" against a build linked with rabbitmq-c (librabbitmq-dev on
-- Ubuntu/Debian) is required, followed by "ALTER EXTENSION amqp UPDATE".
--
-- Volatility fix: publish(), autonomous_publish(), exchange_declare() and
-- disconnect() perform side-effecting network I/O, so they MUST be VOLATILE.
-- They were previously (incorrectly) declared IMMUTABLE, which let the planner
-- constant-fold a call with all-constant arguments at plan time and reuse the
-- cached result -- silently publishing once (or not at all) instead of per
-- execution under prepared statements / PL/pgSQL.  ALTER FUNCTION preserves
-- existing privileges, so no drop/recreate is needed.

ALTER FUNCTION @extschema@.publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) VOLATILE;
ALTER FUNCTION @extschema@.autonomous_publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) VOLATILE;
ALTER FUNCTION @extschema@.exchange_declare(integer, varchar, varchar, boolean, boolean, boolean) VOLATILE;
ALTER FUNCTION @extschema@.disconnect(integer) VOLATILE;

-- Transactional outbox (new in 0.5.0): durably enqueue messages inside the business
-- transaction, then relay them to the broker out of band, off the commit path.
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

CREATE INDEX outbox_unpublished_idx
  ON @extschema@.outbox (broker_id, id) WHERE published_at IS NULL;

SELECT pg_catalog.pg_extension_config_dump('@extschema@.outbox', 'WHERE published_at IS NULL');
SELECT pg_catalog.pg_extension_config_dump('@extschema@.outbox_id_seq', '');

CREATE FUNCTION @extschema@.publish_outbox(
    broker_id integer
    , exchange varchar
    , routing_key varchar
    , message varchar
    , delivery_mode integer default null
    , content_type varchar default null
    , reply_to varchar default null
    , correlation_id varchar default null
    , mandatory boolean default false
)
RETURNS bigint AS $$
  INSERT INTO @extschema@.outbox (broker_id, exchange, routing_key, message,
                                  delivery_mode, content_type, reply_to,
                                  correlation_id, mandatory)
  VALUES (broker_id, exchange, routing_key, message,
          delivery_mode, content_type, reply_to, correlation_id, mandatory)
  RETURNING id;
$$ LANGUAGE sql VOLATILE;

COMMENT ON FUNCTION @extschema@.publish_outbox(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) IS
'Enqueue a message into the transactional outbox (amqp.outbox) instead of publishing it
directly.  This is a plain INSERT, so it commits or rolls back atomically with the
surrounding transaction.  Ship enqueued messages with amqp.publish_outbox_batch().';

CREATE FUNCTION @extschema@.publish_outbox_batch(
    broker_id integer default null
    , batch_limit integer default 100
)
RETURNS integer AS $$
DECLARE
  v_row   record;
  v_ok    boolean;
  v_count integer := 0;
BEGIN
  FOR v_row IN
    SELECT o.id, o.broker_id AS bid, o.exchange, o.routing_key, o.message,
           o.delivery_mode, o.content_type, o.reply_to, o.correlation_id, o.mandatory
      FROM @extschema@.outbox o
     WHERE o.published_at IS NULL
       AND (publish_outbox_batch.broker_id IS NULL
            OR o.broker_id = publish_outbox_batch.broker_id)
     ORDER BY o.id
     FOR UPDATE SKIP LOCKED
     LIMIT batch_limit
  LOOP
    v_ok := @extschema@.autonomous_publish(v_row.bid, v_row.exchange, v_row.routing_key,
                                           v_row.message, v_row.delivery_mode,
                                           v_row.content_type, v_row.reply_to,
                                           v_row.correlation_id, v_row.mandatory);
    IF v_ok THEN
      UPDATE @extschema@.outbox
         SET published_at = now(), attempts = attempts + 1, last_error = NULL
       WHERE id = v_row.id;
      v_count := v_count + 1;
    ELSE
      UPDATE @extschema@.outbox
         SET attempts = attempts + 1, last_error = 'autonomous_publish returned false'
       WHERE id = v_row.id;
    END IF;
  END LOOP;
  RETURN v_count;
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION @extschema@.publish_outbox_batch(integer, integer) IS
'Relay up to batch_limit unpublished amqp.outbox rows to the broker via
amqp.autonomous_publish(), marking each published.  FOR UPDATE SKIP LOCKED allows
concurrent relay workers.  At-least-once delivery; make consumers idempotent.';
