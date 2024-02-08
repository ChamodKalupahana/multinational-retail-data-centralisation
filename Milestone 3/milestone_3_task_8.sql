SELECT * FROM dim_card_details


/* Display data types*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'dim_card_details';

/* For dim_users, user_uuid*/

ALTER TABLE dim_users
ADD CONSTRAINT pk_dim_users PRIMARY KEY (user_uuid);

/* For dim_store_details, store_code*/

ALTER TABLE dim_store_details
ADD CONSTRAINT pk_dim_store_details PRIMARY KEY (store_code);


/* For dim_products, product_code*/

ALTER TABLE dim_products
ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_code);

/* For dim_date_times, date_uuid*/

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid);

/* For dim_card_details, card_number*/

/* See if there's dupliates rows */

SELECT card_number, COUNT(*)
FROM dim_card_details
GROUP BY card_number
HAVING COUNT(*) > 1;

/* Delete duplicate rows from card details*/

DELETE FROM dim_card_details
WHERE card_number IN (
    SELECT card_number
    FROM dim_card_details
    GROUP BY card_number
    HAVING COUNT(*) > 1
);

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number);

-- Foreign key constraint for referencing dim_date_times table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid)

/* Remove mismatching data */
SELECT DISTINCT user_uuid
FROM dim_users
WHERE user_uuid NOT IN (SELECT user_uuid FROM orders_table);

SELECT DISTINCT user_uuid
FROM dim_users

SELECT DISTINCT user_uuid
FROM orders_table

DELETE FROM dim_users
WHERE user_uuid NOT IN (
    SELECT user_uuid FROM dim_users
);

-- Foreign key constraint for referencing dim_users table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid)

-- Foreign key constraint for referencing dim_store_details table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code)

-- Foreign key constraint for referencing dim_products table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code)

-- Foreign key constraint for referencing dim_products table
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

/* Display foreign keys */
SELECT
    constraint_name,
    table_schema,
    table_name,
    column_name
FROM
    information_schema.key_column_usage
WHERE
    table_name = 'orders_table'
    AND constraint_name LIKE 'fk_%';
