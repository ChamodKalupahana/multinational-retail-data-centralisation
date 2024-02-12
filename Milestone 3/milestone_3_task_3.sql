SELECT * FROM dim_store_details

/* latitude columns already combined*/

UPDATE dim_store_details
SET staff_numbers = NULL
WHERE staff_numbers !~ '^\d+$';

-- Alter staff_numbers data type to SMALLINT
ALTER TABLE dim_store_details
    ALTER COLUMN staff_numbers TYPE SMALLINT
    USING CASE WHEN staff_numbers ~ '^\d+$' THEN staff_numbers::SMALLINT ELSE NULL END;

/* Change data types*/
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(11),  -- Replace '?' with the desired length
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),  -- Replace '?' with the desired length
    ALTER COLUMN continent TYPE VARCHAR(255);

SELECT
    MAX(LENGTH(store_code)) AS max_store_code_length,
    MAX(LENGTH(country_code)) AS max_country_code_length
FROM
    dim_store_details;

/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_store_details';