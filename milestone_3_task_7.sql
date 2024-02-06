SELECT * FROM dim_card_details


-- Convert card_number and expiry_date to VARCHAR
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(255),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
-- Convert date_payment_confirmed to DATE
ALTER COLUMN date_payment_confirmed TYPE DATE;

/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_card_details';