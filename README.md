# 🚀 AiCore Multinational Retail Data Centralisation

<img src="aicore_logo.jpg" width="100"/>

Welcome to the GitHub repository for the Multinational Retail Data Centralisation project!

In this project, we address the challenge of consolidating sales data from various sources within a multinational company. Currently, the sales data is scattered across different data sources, making it difficult for team members to access and analyze efficiently.

To tackle this issue and promote a more data-driven approach, our organization aims to centralize its sales data into a single, accessible location. Our primary objective is to develop a system that stores the company's current data in a database, serving as the authoritative source for sales data.

By centralizing our sales data, we aim to streamline access and analysis processes, enabling team members to obtain up-to-date metrics for the business efficiently.

This repository contains the code and ressources for implementing this data centralization system and querying the database for business metrics. Feel free to explore and contribute to our efforts in enhancing data accessibility and analysis within our organization.

# Scripts Overview

## 1. data_extractor.py

This script contains a class `DataExtractor` with methods to extract and manipulate data from various sources. Below is a brief overview of each method:

### Methods:

1. `read_rds_table(DatabaseConnector_instance, table_name = 'legacy_store_details')`:
   - Reads a table from a relational database using the specified `DatabaseConnector_instance`.
   - Parameters:
     - `DatabaseConnector_instance`: Instance of `DatabaseConnector` class for database connection.
     - `table_name`: Name of the table to read from the database.
   - Returns: Pandas DataFrame containing the table data.

2. `retrieve_pdf_data()`:
   - Reads data from a PDF file using the `tabula` library.
   - Returns: Pandas DataFrame containing the extracted data from the PDF.

3. `list_number_of_stores(number_of_stores_endpoint, header_dict)`:
   - Retrieves the number of stores in the business from an API endpoint.
   - Parameters:
     - `number_of_stores_endpoint`: API endpoint URL for getting the number of stores.
     - `header_dict`: Dictionary containing headers for API request.
   - Returns: Number of stores.

4. `retrieve_stores_data(retrieve_a_store_endpoint, header_dict)`:
   - Retrieves data for all stores from an API endpoint.
   - Parameters:
     - `retrieve_a_store_endpoint`: API endpoint URL for retrieving store data (with `{store_number}` as a placeholder).
     - `header_dict`: Dictionary containing headers for API request.
   - Returns: Pandas DataFrame containing data for all stores.

5. `extract_from_s3(s3_address)`:
   - Extracts data from an S3 bucket.
   - Parameters:
     - `s3_address`: Address of the file in the S3 bucket (e.g., `'s3://bucket_name/file.csv'`).
   - Returns: Pandas DataFrame containing the extracted data from the CSV file.

### Main Section:
The script includes a main section to demonstrate the usage of the `DataExtractor` class methods.

## Usage:
To utilize the functionality provided by the `DataExtractor` class, instantiate the class and call the desired methods with appropriate arguments.

Example:
```python
test = DataExtractor()
DatabaseConnector_instance = DatabaseConnector()

# Read data from RDS table
table = test.read_rds_table(DatabaseConnector_instance, table_name='legacy_store_details')

# Retrieve data from S3 bucket
address = 's3://data-handling-public/products.csv'
df_from_s3 = test.extract_from_s3(address)

```
# Scripts Overview

## 2. database_connector.py

This script contains a class `DatabaseConnector` responsible for connecting to and interacting with a PostgreSQL database. Below is a summary of the methods and functionalities provided by this script:

### Methods:

1. `read_db_creds()`:
   - Reads the database credentials from a YAML file (`db_creds.yaml`).
   - Returns: Dictionary containing database credentials.

2. `init_db_engine()`:
   - Initializes a SQLAlchemy engine using the database credentials.
   - Returns: SQLAlchemy engine object.

3. `list_db_tables()`:
   - Lists all tables present in the connected database.
   - Returns: Pandas DataFrame containing table names.

4. `upload_to_db(table_to_upload, name_of_database)`:
   - Uploads a DataFrame to the specified table in the connected database.
   - Parameters:
     - `table_to_upload`: Pandas DataFrame to be uploaded.
     - `name_of_database`: Name of the table to upload the DataFrame.
   - Returns: None.

### Main Section:
The script includes a main section demonstrating the usage of the `DatabaseConnector` class methods for interacting with the database.

## Usage:
Instantiate the `DatabaseConnector` class and call the desired methods with appropriate arguments to connect to the database, list tables, and upload data.

Example:
```python
test = DatabaseConnector()

# List database tables
table_names = test.list_db_tables()
print(table_names)

# Upload DataFrame to database
df_to_upload = pd.DataFrame(...)  # DataFrame to upload
test.upload_to_db(df_to_upload, 'name_of_table')

```

# Scripts Overview

## 3. data_cleaning.py

This script provides functionalities for cleaning various types of data using the `DataCleaning` class. Below is a summary of the methods and functionalities offered by this script:

### Methods:  

1. `clean_user_data_legacy_store(DataExtractor_instance)`:
   - Cleans legacy store details data extracted using `DataExtractor`.
   - Cleans various columns such as `address`, `longitude`, `store_code`, etc.
   - Returns: Cleaned DataFrame.

2. `clean_user_data(DataExtractor_instance, DatabaseConnector_instance)`:
   - Cleans legacy user data extracted using `DataExtractor`.
   - Cleans columns such as `first_name`, `last_name`, `date_of_birth`, etc.
   - Returns: Cleaned DataFrame.

3. `clean_orders_data(DataExtractor_instance, DatabaseConnector_instance)`:
   - Cleans orders data extracted using `DataExtractor`.
   - Cleans columns such as `first_name`, `last_name`, `card_number`, etc.
   - Returns: Cleaned DataFrame.

4. `clean_card_data(card_details)`:
   - Cleans card details data.
   - Cleans columns such as `card_number`, `expiry_date`, etc.
   - Returns: None.

5. `called_clean_store_data(DataExtractor_instance)`:
   - Cleans store details data.
   - Cleans columns such as `address`, `longitude`, `store_code`, etc.
   - Returns: Cleaned DataFrame.

6. `convert_product_weights(DataExtractor_instance)`:
   - Converts product weights from various units to kilograms.
   - Returns: Cleaned DataFrame.

7. `clean_products_data(DataExtractor_instance)`:
   - Cleans products data extracted using `DataExtractor`.
   - Cleans columns such as `product_name`, `product_price`, `category`, etc.
   - Returns: Cleaned DataFrame.

8. `clean_date_times(DataExtractor_instance)`:
   - Cleans date-time data extracted using `DataExtractor`.
   - Cleans columns such as `product_name`, `product_price`, `category`, etc.
   - Returns: Cleaned DataFrame.

### Main Section:
The script includes a main section demonstrating the usage of the `DataCleaning` class methods for data cleaning.

## Usage:
Instantiate the `DataCleaning` class and call the desired methods with appropriate arguments to clean different types of data.

Example:
```python
test = DataCleaning()
DataExtractor_instance = DataExtractor()
DatabaseConnector_instance = DatabaseConnector()

# Clean legacy user data
user_data = test.clean_user_data(DataExtractor_instance, DatabaseConnector_instance)
print(user_data.head())

# Clean store details data
store_data = test.called_clean_store_data(DataExtractor_instance)
print(store_data.head())

# Clean orders data
orders_data = test.clean_orders_data(DataExtractor_instance, DatabaseConnector_instance)
print(orders_data.head())

# Clean products data
products_data = test.clean_products_data(DataExtractor_instance)
print(products_data.head())

# Clean date-time data
date_time_data = test.clean_date_times(DataExtractor_instance)
print(date_time_data.head())
