-- ################Query performance################ --

--Top n longest-running queries
SELECT query_id,
  ROW_NUMBER() OVER(ORDER BY partitions_scanned DESC) AS query_id_int,
  query_text,
  total_elapsed_time/1000 AS query_execution_time_seconds,
  partitions_scanned,
  partitions_total,
FROM snowflake.account_usage.query_history Q
WHERE warehouse_name = 'my_warehouse' AND TO_DATE(Q.start_time) > DATEADD(day,-1,TO_DATE(CURRENT_TIMESTAMP()))
  AND total_elapsed_time > 0 --only get queries that actually used compute
  AND error_code IS NULL
  AND partitions_scanned IS NOT NULL
ORDER BY total_elapsed_time desc
LIMIT 50;


--Queries organized by execution time over past month

SELECT
  CASE
    WHEN Q.total_elapsed_time <= 60000 THEN 'Less than 60 seconds'
    WHEN Q.total_elapsed_time <= 300000 THEN '60 seconds to 5 minutes'
    WHEN Q.total_elapsed_time <= 1800000 THEN '5 minutes to 30 minutes'
    ELSE 'more than 30 minutes'
  END AS BUCKETS,
  COUNT(query_id) AS number_of_queries
FROM snowflake.account_usage.query_history Q
WHERE  TO_DATE(Q.START_TIME) >  DATEADD(month,-1,TO_DATE(CURRENT_TIMESTAMP()))
  AND total_elapsed_time > 0
  AND warehouse_name = 'my_warehouse'
GROUP BY 1;

--Find long running repeated queries
SELECT
    query_hash,
    COUNT(*),
    SUM(total_elapsed_time),
    ANY_VALUE(query_id)
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE warehouse_name = 'MY_WAREHOUSE'
    AND DATE_TRUNC('day', start_time) >= CURRENT_DATE() - 7
  GROUP BY query_hash
  ORDER BY SUM(total_elapsed_time) DESC
  LIMIT 100;

--Track the average performance of a query over time

SELECT
    DATE_TRUNC('day', start_time),
    SUM(total_elapsed_time),
    ANY_VALUE(query_id)
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE query_parameterized_hash = 'cbd58379a88c37ed6cc0ecfebb053b03'
    AND DATE_TRUNC('day', start_time) >= CURRENT_DATE() - 30
  GROUP BY DATE_TRUNC('day', start_time);



-- ################Warehouse load################ --

--Total warehouse load

SELECT TO_DATE(start_time) AS date,
  warehouse_name,
  SUM(avg_running) AS sum_running,
  SUM(avg_queued_load) AS sum_queued
FROM snowflake.account_usage.warehouse_load_history
WHERE TO_DATE(start_time) >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2
HAVING SUM(avg_queued_load) >0;


-- ################Task performance################ --

--Longest running tasks
SELECT DATEDIFF(seconds, query_start_time,completed_time) AS duration_seconds,*
FROM snowflake.account_usage.task_history
WHERE state = 'SUCCEEDED'
  AND query_start_time >= DATEADD (day, -1, CURRENT_TIMESTAMP())
ORDER BY duration_seconds DESC;












