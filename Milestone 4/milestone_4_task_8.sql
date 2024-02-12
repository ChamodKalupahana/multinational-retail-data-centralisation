SELECT
    dim_store_details.store_type,
    dim_store_details.country_code,
    SUM(orders_table.product_quantity)
FROM
    dim_store_details
INNER JOIN
    orders_table ON dim_store_details.store_code = orders_table.store_code
GROUP BY
    dim_store_details.store_type,
    dim_store_details.country_code

SELECT
*
FROM
    orders_table