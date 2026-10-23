-- Documentation, not a migration: the application never issues CREATE (ruling D8).
-- Source: docs/lld/03-writer-persistence.md section 10. Column lists are provisional
-- (OQ-13); the forced choices in section 10.4 are not.
-- NOT NULL only on the key and updated_at: a column absent from a partial mask is
-- NULL on insert. No foreign keys, no secondary indexes.

CREATE TABLE md_market_status (
  market_type NUMBER(10) NOT NULL,
  transcode NUMBER(5),
  message_len NUMBER(5),
  message_text VARCHAR2(128),
  bcast_destination VARCHAR2(2),
  token NUMBER(10),
  effective_ts TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_md_market_status PRIMARY KEY (market_type)
);

-- FeedValve needs INSERT and UPDATE only; SELECT is not required.
-- GRANT INSERT, UPDATE ON md_market_status TO <writer_account>;
