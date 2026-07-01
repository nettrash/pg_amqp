-- Add an optional "mandatory" parameter to publish() and autonomous_publish().
-- When true the broker returns unroutable messages (via basic.return) instead of
-- silently dropping them, and a WARNING is emitted.  A "make install" is also
-- required for the accompanying C change.
--
-- Adding the parameter changes the function signatures, so the old four-property
-- forms are dropped and recreated.  Existing EXECUTE privileges are preserved.

CREATE TEMP TABLE amqp_preserve_privs_temp (statement text);

INSERT INTO amqp_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.autonomous_publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';'
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'autonomous_publish';

INSERT INTO amqp_preserve_privs_temp
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';'
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'publish';

DROP FUNCTION @extschema@.autonomous_publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar);
DROP FUNCTION @extschema@.publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar);

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
LANGUAGE C IMMUTABLE;

COMMENT ON FUNCTION @extschema@.autonomous_publish(integer, varchar, varchar, varchar, integer, varchar, varchar, varchar, boolean) IS
'Works as amqp.publish does, but the message is published immediately irrespective of the
current transaction state.  PostgreSQL commit and rollback at a later point will have no
effect on this message being sent to AMQP.
If mandatory is true, the broker returns unroutable messages and a WARNING is emitted; note
that on the autonomous (non-transactional) channel a return is only surfaced on a subsequent
operation that reads from the broker, so detection is best-effort.';

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
LANGUAGE C IMMUTABLE;

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

-- Restore dropped object privileges
DO $$
DECLARE
v_row   record;
BEGIN
    FOR v_row IN SELECT statement FROM amqp_preserve_privs_temp LOOP
        IF v_row.statement IS NOT NULL THEN
            EXECUTE v_row.statement;
        END IF;
    END LOOP;
END
$$;

DROP TABLE IF EXISTS amqp_preserve_privs_temp;
