-- The pipeline of the end-to-end test: orders followed from a MetaLake table, totals by customer kept in another. The
-- aggregate is updating, so a new order retracts its customer's total and inserts the next one, and the last order of
-- a customer deleted retracts its total for good: the sink deletes those rows from the table.
--
-- Standard SQL over the lake's names, with no table declared: Arroyo finds `shop.orders` and `shop.totals` in the catalog
-- METALAKE_POSTGRES names, the first as a source and the second as a sink, as a `metalake` query reads and writes them.

INSERT INTO shop.totals
SELECT customer, count(*) AS orders, sum(amount) AS amount
FROM shop.orders
GROUP BY customer;
