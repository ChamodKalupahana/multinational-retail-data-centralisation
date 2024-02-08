SELECT
    SUM(staff_numbers),
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    SUM(staff_numbers) DESC