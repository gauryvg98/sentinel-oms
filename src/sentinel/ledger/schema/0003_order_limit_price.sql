-- Persist an order's requested limit price. NULL for market orders. This is
-- order metadata (set once at creation, never changed by a state transition),
-- so it lives on the projection alongside side/authority rather than in the
-- transition()-folded OrderCore. Needed to display a resting limit order's
-- price and to let a peg-to-touch strategy decide when to re-price.
ALTER TABLE orders ADD COLUMN limit_price NUMERIC;
