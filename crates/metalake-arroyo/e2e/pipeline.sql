-- The pipeline of the end-to-end test: orders followed from a MetaLake table, totals by customer kept in another. The
-- aggregate is updating, so a new order retracts its customer's total and inserts the next one, and the last order of
-- a customer deleted retracts its total for good: the sink deletes those rows from the table.

CREATE TABLE orders (
    id BIGINT,
    customer TEXT,
    amount DOUBLE
) WITH (
    connector = 'metalake',
    'catalog.postgres' = 'host=metalake-e2e-pg user=postgres dbname=postgres',
    'catalog.schema' = 'lake',
    namespace = 'shop',
    table = 'orders',
    type = 'source',
    'source.poll_interval' = '500 milliseconds'
);

CREATE TABLE totals (
    customer TEXT,
    orders BIGINT,
    amount DOUBLE
) WITH (
    connector = 'metalake',
    'catalog.postgres' = 'host=metalake-e2e-pg user=postgres dbname=postgres',
    'catalog.schema' = 'lake',
    namespace = 'shop',
    table = 'totals',
    type = 'sink'
);

INSERT INTO totals
SELECT customer, count(*) AS orders, sum(amount) AS amount
FROM orders
GROUP BY customer;
