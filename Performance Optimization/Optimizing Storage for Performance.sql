-- Automatic Clustering, Search Optimization Service ,Materialized view

----------##########Prototypical queries ##########----------

--Prototypical Query for Clustering
-- Automatic Clustering provides a performance boost for range queries with large table scans. For example, the following query will execute faster if the shipdate column is the table’s cluster key because the WHERE clause scans a lot of data.

SELECT
  SUM(quantity) AS sum_qty,
  SUM(extendedprice) AS sum_base_price,
  AVG(quantity) AS avg_qty,
  AVG(extendedprice) AS avg_price,
  COUNT(*) AS count_order
FROM lineitem
WHERE shipdate >= DATEADD(day, -90, to_date('2024-01-01')



----------##########Prototypical Query for Search Optimization ##########----------
-- The Search Optimization Service can provide a performance boost for point lookup queries that scan a large table to return a small subset of records. For example, the following query will execute faster with the Search Optimization Service if the sender_ip column has a large number of distinct values.

SELECT error_message, receiver_ip
FROM logs
WHERE sender_ip IN ('198.2.2.1', '198.2.2.2');


----------##########Prototypical Query for Materialized View ##########----------
-- A materialized view can provide a performance boost for queries that access a small subset of data using expensive operations like aggregation. As an example, suppose that an administrator aggregated the totalprice column when creating a materialized view mv_view1. The following query against the materialized view will execute faster than it would against the base table.

SELECT
  orderdate,
  SUM(totalprice)
FROM mv_view1
GROUP BY 1;