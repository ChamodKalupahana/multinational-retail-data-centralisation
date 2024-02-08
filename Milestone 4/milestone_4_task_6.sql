SELECT
    SUM(ot.product_quantity),
    dt.year,
    dt.month
FROM
    orders_table ot
INNER JOIN
    dim_date_times dt ON ot.date_uuid = dt.date_uuid
GROUP BY
    dt.year, dt.month
ORDER BY
    SUM(ot.product_quantity) DESC


SELECT
    *
FROM
    dim_date_times