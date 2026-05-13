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

