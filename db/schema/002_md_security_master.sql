-- Documentation, not a migration: the application never issues CREATE (ruling D8).
-- Source: docs/lld/03-writer-persistence.md section 10. Column lists are provisional
-- (OQ-13); the forced choices in section 10.4 are not.
-- NOT NULL only on the key and updated_at: a column absent from a partial mask is
-- NULL on insert. No foreign keys, no secondary indexes.

CREATE TABLE md_security_master (
  token NUMBER(10) NOT NULL,
  symbol VARCHAR2(10),
  series VARCHAR2(2),
  expiry_date TIMESTAMP,
  strike_price NUMBER(20,4),
  board_lot_quantity NUMBER(10),
  tick_size NUMBER(20,4),
  base_price NUMBER(20,4),
  delete_flag VARCHAR2(1),
  effective_ts TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_md_security_master PRIMARY KEY (token)
);

-- FeedValve needs INSERT and UPDATE only; SELECT is not required.
-- GRANT INSERT, UPDATE ON md_security_master TO <writer_account>;
