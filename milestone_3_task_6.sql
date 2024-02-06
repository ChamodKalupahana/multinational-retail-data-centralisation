SELECT * FROM dim_date_times

SELECT
    MAX(LENGTH(month)) AS max_month_length,
    MAX(LENGTH(year)) AS max_year_length,
    MAX(LENGTH(day)) AS max_day_length,
    MAX(LENGTH(time_period)) AS max_time_period_length
FROM
    dim_date_times;


/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_date_times';