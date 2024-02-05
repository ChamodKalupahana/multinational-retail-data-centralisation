SELECT * FROM dim_users

/* Change data types*/
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN join_date TYPE DATE;

/* For user_uuid column*/
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Add a new column for UUID
ALTER TABLE dim_users
ADD COLUMN user_uuid_new UUID;

-- Update the new column with converted values
UPDATE dim_users
SET user_uuid_new = uuid_generate_v4(); -- Assuming you're using a UUID generation function

-- Verify the data in the new column
SELECT user_uuid_new FROM dim_users;

-- Drop the old column
ALTER TABLE dim_users DROP COLUMN user_uuid;

-- Rename the new column to the original name
ALTER TABLE dim_users RENAME COLUMN user_uuid_new TO user_uuid;


/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_users';