# Import modules
import pandas as pd

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data(self, DataExtractor_instance):
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
    
if __name__ == '__main__':
    test = DataCleaning()
    DataExtractor_instance = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    table = test.clean_user_data(DataExtractor_instance)
    print(table.head())