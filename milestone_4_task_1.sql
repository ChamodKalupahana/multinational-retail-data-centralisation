SELECT
    country_code,
    COUNT(country_code)
FROM
    dim_store_details
GROUP BY
    country_code