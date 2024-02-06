SELECT * FROM dim_products

-- Rename removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Update still_available column to replace "Removed" with FALSE and everything else with TRUE
UPDATE dim_products
SET still_available = (still_available != 'Removed');


-- Convert product_price, weight, and EAN to their respective data types
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(255),
ALTER COLUMN product_code TYPE VARCHAR(255),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
ALTER COLUMN weight_class TYPE VARCHAR(255);


/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_products';