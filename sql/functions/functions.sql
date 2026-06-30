CREATE FUNCTION @extschema@.autonomous_publish(
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

RETURNS boolean AS 'pg_amqp.so', 'pg_amqp_autonomous_publish'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION @extschema@.autonomous_publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) IS
'Works as amqp.publish does, but the message is published immediately irrespective of the
current transaction state.  PostgreSQL commit and rollback at a later point will have no
effect on this message being sent to AMQP.
If mandatory is true, the broker returns unroutable messages and a WARNING is emitted; note
that on the autonomous (non-transactional) channel a return is only surfaced on a subsequent
operation that reads from the broker, so detection is best-effort.';


CREATE FUNCTION amqp.disconnect(broker_id integer)
RETURNS void AS 'pg_amqp.so', 'pg_amqp_disconnect'
LANGUAGE C VOLATILE STRICT;

COMMENT ON FUNCTION amqp.disconnect(integer) IS
'Explicitly disconnect the specified (broker_id) if it is current connected. Broker
connections, once established, live until the PostgreSQL backend terminated.  This
allows for more precise control over that.
select amqp.disconnect(broker_id) from amqp.broker
will disconnect any brokers that may be connected.';


CREATE FUNCTION amqp.exchange_declare(
    broker_id integer
    , exchange varchar 
    , exchange_type varchar
    , passive boolean
    , durable boolean
    , auto_delete boolean DEFAULT false
)
RETURNS boolean AS 'pg_amqp.so', 'pg_amqp_exchange_declare'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION amqp.exchange_declare(integer, varchar, varchar, boolean, boolean, boolean) IS
'Declares a exchange (broker_id, exchange_name, exchange_type, passive, durable, auto_delete)
auto_delete should be set to false (default) as unexpected errors can cause disconnect/reconnect which
would trigger the auto deletion of the exchange.';


CREATE FUNCTION @extschema@.publish(
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
RETURNS boolean AS 'pg_amqp.so', 'pg_amqp_publish'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION @extschema@.publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) IS
'Publishes a message (broker_id, exchange, routing_key, message).
The message will only be published if the containing PostgreSQL transaction successfully commits.
Under certain circumstances, the AMQP commit might fail.  In this case, a WARNING is emitted.
The optional parameters set the following message properties:
delivery_mode (either 1 or 2), content_type, reply_to and correlation_id.
If mandatory is true, the broker returns the message instead of dropping it when it is
unroutable, and a WARNING is emitted (surfaced at commit time for transactional publish).

Publish returns a boolean indicating if the publish command was successful.  Note that as
AMQP publish is asynchronous, you may find out later it was unsuccessful.';


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
surrounding transaction: the message is never lost merely because the business
transaction committed, and never sent if it rolled back.  Ship enqueued messages to the
broker with amqp.publish_outbox_batch().  Returns the new outbox row id.';


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
'Relay up to batch_limit unpublished amqp.outbox rows (optionally only for one broker_id)
to the broker via amqp.autonomous_publish(), marking each published.  Uses FOR UPDATE SKIP
LOCKED so several relay workers can run concurrently.  Call it on a schedule (e.g. pg_cron)
or from a polling worker; returns the number of messages published.  Delivery is
at-least-once: a relay crash mid-batch re-sends on the next run, and -- because pg_amqp has
no publisher confirms -- a broker that accepts then drops a message cannot be detected, so
consumers should be idempotent.';

