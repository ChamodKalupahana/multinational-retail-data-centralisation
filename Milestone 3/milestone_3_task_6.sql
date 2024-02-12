SELECT * FROM dim_date_times

-- Convert timestamp to time without time zone
ALTER TABLE dim_date_times
ALTER COLUMN timestamp TYPE TIME WITHOUT TIME ZONE USING timestamp::TIME WITHOUT TIME ZONE;

-- Convert month, year, and day to integer
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE INTEGER USING month::INTEGER,
ALTER COLUMN year TYPE INTEGER USING year::INTEGER,
ALTER COLUMN day TYPE INTEGER USING day::INTEGER;

-- Convert time_period and date_uuid to text
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE TEXT,
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;



/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_date_times';