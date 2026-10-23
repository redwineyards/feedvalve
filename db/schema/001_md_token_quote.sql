-- Documentation, not a migration: the application never issues CREATE (ruling D8).
-- Source: docs/lld/03-writer-persistence.md section 10. Column lists are provisional
-- (OQ-13); the forced choices in section 10.4 are not.
-- NOT NULL only on the key and updated_at: a column absent from a partial mask is
-- NULL on insert. No foreign keys, no secondary indexes.

CREATE TABLE md_token_quote (
  token NUMBER(10) NOT NULL,
  last_traded_price NUMBER(20,4),
  volume_traded_today NUMBER(10),
  open_price NUMBER(20,4),
  high_price NUMBER(20,4),
  low_price NUMBER(20,4),
  closing_price NUMBER(20,4),
  trading_status NUMBER(5),
  net_change_indicator VARCHAR2(1),
  last_trade_quantity NUMBER(10),
  last_trade_time TIMESTAMP,
  average_trade_price NUMBER(20,4),
  total_buy_quantity NUMBER(38,10),
  total_sell_quantity NUMBER(38,10),
  book_type NUMBER(5),
  bid_qty_1 NUMBER(10),
  bid_qty_2 NUMBER(10),
  bid_qty_3 NUMBER(10),
  bid_qty_4 NUMBER(10),
  bid_qty_5 NUMBER(10),
  bid_px_1 NUMBER(20,4),
  bid_px_2 NUMBER(20,4),
  bid_px_3 NUMBER(20,4),
  bid_px_4 NUMBER(20,4),
  bid_px_5 NUMBER(20,4),
  bid_ord_1 NUMBER(5),
  bid_ord_2 NUMBER(5),
  bid_ord_3 NUMBER(5),
  bid_ord_4 NUMBER(5),
  bid_ord_5 NUMBER(5),
  ask_qty_1 NUMBER(10),
  ask_qty_2 NUMBER(10),
  ask_qty_3 NUMBER(10),
  ask_qty_4 NUMBER(10),
  ask_qty_5 NUMBER(10),
  ask_px_1 NUMBER(20,4),
  ask_px_2 NUMBER(20,4),
  ask_px_3 NUMBER(20,4),
  ask_px_4 NUMBER(20,4),
  ask_px_5 NUMBER(20,4),
  ask_ord_1 NUMBER(5),
  ask_ord_2 NUMBER(5),
  ask_ord_3 NUMBER(5),
  ask_ord_4 NUMBER(5),
  ask_ord_5 NUMBER(5),
  effective_ts TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_md_token_quote PRIMARY KEY (token)
);

-- FeedValve needs INSERT and UPDATE only; SELECT is not required.
-- GRANT INSERT, UPDATE ON md_token_quote TO <writer_account>;
