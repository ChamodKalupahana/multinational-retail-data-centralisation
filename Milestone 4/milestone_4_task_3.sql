SELECT
    COUNT(date_uuid),
    month
FROM
    dim_date_times
GROUP BY
    month
ORDER BY
    COUNT(date_uuid) DESC