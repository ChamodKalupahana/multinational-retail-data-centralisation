SELECT     
    *
FROM
    dim_products

SELECT
    store_type,
    COUNT(address) AS address_count,
    COUNT(address) * 100.0 / total_address_count AS percentage
FROM
    dim_store_details
CROSS JOIN
    (SELECT COUNT(address) AS total_address_count FROM dim_store_details) AS total
GROUP BY
    store_type, total_address_count
ORDER BY
    COUNT(address) DESC;


