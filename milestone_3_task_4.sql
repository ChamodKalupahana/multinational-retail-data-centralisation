SELECT * FROM dim_products

SELECT
    weight
FROM
    dim_products

-- Add new column weight_class
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- Update weight_class based on weight range
UPDATE dim_products
SET weight_class = 'Light'
WHERE weight < 2;

UPDATE dim_products
SET weight_class = 'Mid_Sized'
WHERE weight >= 2 AND weight < 40;

UPDATE dim_products
SET weight_class = 'Heavy'
WHERE weight >= 40 AND weight < 140;

UPDATE dim_products
SET weight_class = 'Truck_Required'
WHERE weight >= 140;


SELECT
    weight_class
FROM
    dim_products

/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_products';