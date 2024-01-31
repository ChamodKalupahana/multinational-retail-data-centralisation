# Import modules
import pandas as pd

class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data_legacy_store(self, DataExtractor_instance):
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

    def clean_user_data_orders_table(self, DataExtractor_instance):
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

        # Drop 1 column, contains no columns
        table.drop(columns=['1'], inplace=True)

                
        table.info()
        return table

if __name__ == '__main__':
    from data_extraction import DataExtractor
    from database_utils import DatabaseConnector

    test = DataCleaning()
    DataExtractor_instance = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    table = test.clean_user_data_orders_table(DataExtractor_instance, DatabaseConnector_instance)
    print(table.head())