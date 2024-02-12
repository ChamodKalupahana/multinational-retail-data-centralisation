SELECT
    year,
    AVG(EXTRACT(EPOCH FROM (LEAD(timestamp) OVER (PARTITION BY year ORDER BY timestamp) - timestamp))) AS average_time_between_sales
FROM
    orders_table
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    year;

SELECT * FROM dim_date_times

SELECT
    year,
    AVG(time_diff) AS average_time_between_sales
FROM (
    SELECT
        year,
        timestamp,
        LEAD(timestamp) OVER (PARTITION BY year ORDER BY timestamp) AS next_timestamp,
        EXTRACT(EPOCH FROM (LEAD(timestamp) OVER (PARTITION BY year ORDER BY timestamp) - timestamp)) AS time_diff
    FROM
        dim_date_times
) AS subquery
GROUP BY
    year;


SELECT
    year,
    CONCAT(
        '{"hours": ', AVG(EXTRACT(HOUR FROM time_diff)),
        ', "minutes": ', AVG(EXTRACT(MINUTE FROM time_diff)),
        ', "seconds": ', AVG(EXTRACT(SECOND FROM time_diff)),
        ', "milliseconds": ', AVG(EXTRACT(MILLISECOND FROM time_diff)), '}'
    ) AS actual_time_taken
FROM (
    SELECT
        year,
        LEAD(timestamp) OVER (PARTITION BY year ORDER BY timestamp) - timestamp AS time_diff
    FROM
        dim_date_times
) AS subquery
GROUP BY
    year
ORDER BY
    actual_time_taken DESC;

