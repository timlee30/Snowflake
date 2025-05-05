------##########Reducing queues ##########------
--This query lists the warehouses that had a queue in the last month, sorted by date.--

SELECT TO_DATE(start_time) AS date
  ,warehouse_name
  ,SUM(avg_running) AS sum_running
  ,SUM(avg_queued_load) AS sum_queued
FROM snowflake.account_usage.warehouse_load_history
WHERE TO_DATE(start_time) >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2
HAVING SUM(avg_queued_load) > 0;


-- Options for reducing queues
-- You have several options to stop warehouse queuing:

-- For a regular warehouse (i.e. not a multi-cluster warehouse), consider creating additional warehouses, and then distribute the queries among them. If specific queries are causing usage spikes, focus on moving those queries.

-- Consider converting a warehouse to a multi-cluster warehouse so the warehouse can elastically provision additional compute resources when demand spikes. Multi-cluster warehouses require the Enterprise Edition of Snowflake.

-- If you are already using a multi-cluster warehouse, increase the maximum number of clusters.



------##########Resolving Memory Spillage ##########------
SELECT query_id, SUBSTR(query_text, 1, 50) partial_query_text, user_name, warehouse_name,
  bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage
FROM  snowflake.account_usage.query_history
WHERE (bytes_spilled_to_local_storage > 0
  OR  bytes_spilled_to_remote_storage > 0 )
  AND start_time::date > dateadd('days', -45, current_date)
ORDER BY bytes_spilled_to_remote_storage, bytes_spilled_to_local_storage DESC
LIMIT 10;


-- Data spilling to storage can have a negative impact on query performance (especially if the query has to spill to remote storage). To alleviate this, Snowflake recommends:

-- Using a larger warehouse (effectively increasing the available memory/local storage space for the operation)

-- Processing data in smaller batches.


------##########Determining the load of the warehouse ##########------
SELECT TO_DATE(start_time) AS date,
  warehouse_name,
  SUM(avg_running) AS sum_running,
  SUM(avg_queued_load) AS sum_queued
FROM snowflake.account_usage.warehouse_load_history
WHERE TO_DATE(start_time) >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2
HAVING SUM(avg_queued_load) >0;



------##########Finding candidates for query acceleration ##########------
--Function: Determine if a specific query might benefit
SELECT PARSE_JSON(system$estimate_query_acceleration('8cd54bf0-1651-5b1c-ac9c-6a9582ebd20f'));

----will return eligivle of not on status

--Query: Best query candidates across warehouses
SELECT query_id, eligible_query_acceleration_time
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
  WHERE start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
  ORDER BY eligible_query_acceleration_time DESC;

--Query: Best warehouse candidates by execution time
SELECT warehouse_name, SUM(eligible_query_acceleration_time) AS total_eligible_time
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
  WHERE start_time > DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY warehouse_name
  ORDER BY total_eligible_time DESC;

--enable Query Acceleration Service
ALTER WAREHOUSE my_wh SET
  ENABLE_QUERY_ACCELERATION = true
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 0;



------##########Optimizing the warehouse cache ##########------

--Finding data scanned from cache
SELECT warehouse_name
  ,COUNT(*) AS query_count
  ,SUM(bytes_scanned) AS bytes_scanned
  ,SUM(bytes_scanned*percentage_scanned_from_cache) AS bytes_scanned_from_cache
  ,SUM(bytes_scanned*percentage_scanned_from_cache) / SUM(bytes_scanned) AS percent_scanned_from_cache
FROM snowflake.account_usage.query_history
WHERE start_time >= dateadd(month,-1,current_timestamp())
  AND bytes_scanned > 0
GROUP BY 1
ORDER BY 5;

--auto-suspension
ALTER WAREHOUSE my_wh SET AUTO_SUSPEND = 600;


------##########Limiting concurrently running queries ##########------
ALTER WAREHOUSE my_wh SET MAX_CONCURRENCY_LEVEL = 4;

