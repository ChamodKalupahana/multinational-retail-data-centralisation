# Import modules
import pandas as pd
import numpy as np

class DataCleaning:
    """
    A class for cleaning various types of data.

    Methods:
    - clean_user_data_legacy_store(DataExtractor_instance): Cleans legacy user data related to store details.
    - clean_user_data(DataExtractor_instance, DatabaseConnector_instance): Cleans user data.
    - clean_orders_data(DataExtractor_instance, DatabaseConnector_instance): Cleans orders data.
    - clean_card_data(card_details): Cleans card details data.
    - called_clean_store_data(DataExtractor_instance): Cleans store data.
    - convert_product_weights(DataExtractor_instance): Converts product weights to a consistent unit.
    - clean_products_data(DataExtractor_instance): Cleans product data.
    - clean_date_times(DataExtractor_instance): Cleans date and time data.
    """
    def __init__(self) -> None:
        pass

    def clean_user_data_legacy_store(self, DataExtractor_instance):
        """
        Cleans legacy user data related to store details.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned user data related to store details.
        """
        table = DataExtractor_instance.read_rds_table(DatabaseConnector_instance, table_name = 'legacy_store_details')

        # Clean address
        table['address'] = table['address'].str.replace('\n', ' ')
        table['address'] = table['address'].astype(str)

        # Clean longitude
        table['longitude'] = pd.to_numeric(table['longitude'], errors='coerce')
        table.dropna(subset=['longitude'], inplace=True) # Drop rows with NaN values (approx. 11)

        # Drop lat column, contains no columns
        table.drop(columns=['lat'], inplace=True)

        # Clean locality
        table['locality'] = table['locality'].astype(str)

        # Clean store_code
        table['store_code'] = table['store_code'].astype(str)

        # Clean staff_numbers
        table['staff_numbers'] = table['staff_numbers'].astype(str)

        # Clean opening_date
        table['opening_date'] = pd.to_datetime(table['opening_date'], format='mixed')

        # Clean store_type
        table['store_type'] = table['store_type'].astype(str)

        # Clean latitude
        table['latitude'] = table['latitude'].astype(float)

        # Clean country_code
        table['country_code'] = table['country_code'].astype(str)

        # Clean continent
        table['continent'] = table['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        table['continent'] = table['continent'].astype(str)

        table.info()
        return table
    
    def clean_user_data(self, DataExtractor_instance, DatabaseConnector_instance):
        """
        Cleans user data.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.
        DatabaseConnector_instance (DatabaseConnector): An instance of the DatabaseConnector class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned user data.
        """
        table = DataExtractor_instance.read_rds_table(DatabaseConnector_instance, table_name = 'legacy_users')

        # Clean first_name
        table['first_name'] = table['first_name'].astype(str)

        table['last_name'] = table['last_name'].astype(str)

        # CLean date of birth
        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'], errors='coerce',format='mixed')
        table.dropna(subset=['date_of_birth'], inplace=True)

        # Clean company
        table['company'] = table['company'].astype(str)

        # Clean email
        table['email_address'] = table['email_address'].astype(str)

        # Clean address 
        table['address'] = table['address'].str.replace('\n', ' ')
        table['address'] = table['address'].astype(str)

        # Clean country
        table['country'] = table['country'].astype(str)

        # Clean country_code
        table['country_code'] = table['country_code'].astype(str)

        # Clean phone_num
        table['phone_number'] = table['phone_number'].astype(str)

        # Clean join_date
        table['join_date'] = pd.to_datetime(table['join_date'], errors='coerce',format='mixed')
        table.dropna(subset=['join_date'], inplace=True)

        # Clean user_uuid
        table['user_uuid'] = table['user_uuid'].astype(str)
        
        table.info()
        return table

    def clean_orders_data(self, DataExtractor_instance, DatabaseConnector_instance):
        """ 
        Cleans orders data.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.
        DatabaseConnector_instance (DatabaseConnector): An instance of the DatabaseConnector class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned orders data.
        """
        table = DataExtractor_instance.read_rds_table(DatabaseConnector_instance, table_name = 'orders_table')  

        # Clean first_name
        table.dropna(subset=['first_name'], inplace=True)

        # Clean user_uuid
        table['date_uuid'] = table['date_uuid'].astype(str)

        # Clean names
        table['first_name'] = table['first_name'].astype(str)
        table['last_name'] = table['last_name'].astype(str)

        # Clean card_num
        table['card_number'] = table['card_number'].astype(int)

        # Clean store_code
        table['store_code'] = table['store_code'].astype(str)
        
        # Clean product_code
        table['product_code'] = table['product_code'].astype(str)

        # Drop 1, first_name, last_name column, contains no columns
        table.drop(columns=['1', 'first_name', 'last_name'], inplace=True)

                
        table.info()
        return table
    
    def clean_card_data(self, DataExtractor_instance):
        """
        Cleans card details data.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned card details data.
        """
        # Get database
        card_details = DataExtractor_instance.retrieve_pdf_data()

        # Clean card_number
        card_details['card_number'] = card_details['card_number'].astype(str)

        # Clean expiry_date
        card_details['expiry_date'] = card_details['expiry_date'].astype(str)

        # Clean card_provider
        card_details['card_provider'] = card_details['card_provider'].astype(str)

        # Clean date_payment_confirmed
        # Replace non-date strings with NaN
        card_details['date_payment_confirmed'] = pd.to_datetime(card_details['date_payment_confirmed'], errors='coerce')

        # Drop rows with null values in 'date_payment_confirmed' column
        card_details.dropna(subset=['date_payment_confirmed'], inplace=True)

        card_details.info()

        return card_details
    
    def called_clean_store_data(self, DataExtractor_instance):
        """
        Cleans store data.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned store data.
        """
        retrieve_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        header_dict ={'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

        table = DataExtractor_instance.retrieve_stores_data(retrieve_store_url, header_dict)

        # Clean address 
        table['address'] = table['address'].str.replace('\n', ' ')
        table['address'] = table['address'].astype(str)

        # Clean longitude
        table['longitude'] = pd.to_numeric(table['longitude'], errors='coerce')
        table.dropna(subset=['longitude'], inplace=True) # Drop rows with NaN values (approx. 11)

        # Drop lat column, contains no columns
        table.drop(columns=['lat'], inplace=True)

        # Clean locality
        table['locality'] = table['locality'].astype(str)

        # Clean store_code
        table['store_code'] = table['store_code'].astype(str)

        # Clean staff_numbers
        table['staff_numbers'] = table['staff_numbers'].astype(str)

        # Clean opening_date
        table['opening_date'] = pd.to_datetime(table['opening_date'], format='mixed')

        # Clean store_type
        table['store_type'] = table['store_type'].astype(str)

        # Clean latitude
        table['latitude'] = table['latitude'].astype(float)

        # Clean country_code
        table['country_code'] = table['country_code'].astype(str)

        # Clean continent
        table['continent'] = table['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
        table['continent'] = table['continent'].astype(str)

        table.info()

        return table
    
    def convert_product_weights(self, DataExtractor_instance):
        """
        Converts product weights to a consistent unit.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing converted product weights.
        """
        address = 's3://data-handling-public/products.csv'
        table = DataExtractor_instance.extract_from_s3(address)

        # Convert ml to g using a 1:1 ratio
        table['weight'] = table['weight'].str.replace('ml', 'g')

        # Function to clean and convert weight to kg
        def clean_and_convert_weight(x):
            x = str(x).strip()  # Remove leading/trailing whitespaces
            if 'g' in x:
                weight = x.replace('g', '')
            elif 'kg' in x:
                weight = x.replace('kg', '')
            elif 'k' in x:
                weight = x.replace('kg', '')
            elif 'ml' in x:  # Convert ml to g
                weight = float(x.replace('ml', '')) * 1  # Use a 1:1 ratio for ml to g
            elif 'oz' in x:  # Convert oz to g
                weight = float(x.replace('oz', '')) * 28.35  # Approximate 1 oz to 28.35 g
            else:
                return np.nan
            
            try:
                return float(weight) / 1000  # Convert to kg
            except ValueError:
                return np.nan  # Return NaN for non-numeric values

        # Apply function to clean and convert weight column
        table['weight'] = table['weight'].apply(clean_and_convert_weight)

        table.dropna(subset=['weight'], inplace=True) # Drop rows with NaN values (approx. 11)

        table.info()

        return table
    
    def clean_products_data(self, DataExtractor_instance):
        """
        Cleans product data.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned product data.
        """
        table = self.convert_product_weights(DataExtractor_instance)

        # Drop unnamed column, contains no columns
        table.drop(columns=['Unnamed: 0'], inplace=True)

        # Clean product_name
        table['product_name'] = table['product_name'].astype(str)

        # Remove the currency symbol and convert to float
        table['product_price'] = table['product_price'].str.replace('£', '').astype(float)

        # Clean category
        table['category'] = table['category'].astype(str)

        # Clean EAN
        table['EAN'] = table['EAN'].astype(str)

        # Clean date_added
        table['date_added'] = pd.to_datetime(table['date_added'], format='mixed')

        # Clean user_uuid
        table['uuid'] = table['uuid'].astype(str)

        # Clean removed
        table['removed'] = table['removed'].astype(str)

        # Clean product_code
        table['product_code'] = table['product_code'].astype(str)

        table.info()

        return table
    
    def clean_date_times(self, DataExtractor_instance):
        """
        Cleans date and time data.

        Args:
        DataExtractor_instance (DataExtractor): An instance of the DataExtractor class.

        Returns:
        pandas.core.frame.DataFrame: DataFrame containing cleaned date and time data.
        """

        import json

        # load data from downloaded json file, loading file from aws took far too long
        with open('date_details.json') as f:
            table = json.load(f)

        table = pd.DataFrame.from_dict(table)

        # Replace invalid timestamp values with NaN
        table['timestamp'] = pd.to_datetime(table['timestamp'], format='%H:%M:%S', errors='coerce')

        # Drop rows with NaN values in the timestamp column
        table = table.dropna(subset=['timestamp'])

        # Convert month, year, and day to integers
        table['month'] = table['month'].astype(int)
        table['year'] = table['year'].astype(int)
        table['day'] = table['day'].astype(int)

        # Convert time_period to title case
        table['time_period'] = table['time_period'].str.title()

        table.info()

        return table


if __name__ == '__main__':
    from data_extraction import DataExtractor
    from database_utils import DatabaseConnector

    test = DataCleaning()
    DataExtractor_instance = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    #table = test.clean_orders_data(DataExtractor_instance, DatabaseConnector_instance)
    # print(table.head())

    # card_details = DataExtractor_instance.retrieve_pdf_data()
    # test.clean_card_data(card_details)

    # table = test.called_clean_store_data()

    table = test.clean_date_times(DataExtractor_instance)