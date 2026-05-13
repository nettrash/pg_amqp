-- 0.4.2 -> 0.4.3
-- C source fixes (no existing SQL objects changed):
--   * K&R-style function declarations updated for PostgreSQL 16+/18 compatibility
--   * Replaced deprecated gethostbyname() with getaddrinfo() (IPv6 support)
--   * Added TLS/SSL support via OpenSSL
--
-- New columns on amqp.broker for TLS/SSL configuration:
ALTER TABLE @extschema@.broker
  ADD COLUMN ssl         boolean NOT NULL DEFAULT false,
  ADD COLUMN ssl_verify  boolean NOT NULL DEFAULT true,
  ADD COLUMN ssl_cacert  text,
  ADD COLUMN ssl_cert    text,
  ADD COLUMN ssl_key     text;

COMMENT ON COLUMN @extschema@.broker.ssl        IS 'Enable TLS/SSL for this broker connection';
COMMENT ON COLUMN @extschema@.broker.ssl_verify IS 'Verify the broker TLS certificate (recommended: true)';
COMMENT ON COLUMN @extschema@.broker.ssl_cacert IS 'Path to PEM CA certificate bundle (NULL = system defaults)';
COMMENT ON COLUMN @extschema@.broker.ssl_cert   IS 'Path to PEM client certificate for mutual TLS (optional)';
COMMENT ON COLUMN @extschema@.broker.ssl_key    IS 'Path to PEM client private key for mutual TLS (optional)';
