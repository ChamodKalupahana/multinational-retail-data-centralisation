SELECT
    locality,
    COUNT(locality)
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    COUNT(locality) DESC