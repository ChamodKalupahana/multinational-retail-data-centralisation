# Import modules
import pandas as pd
import tabula as ta
import requests

from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, DatabaseConnector_instance, table_name = 'legacy_store_details'):
        engine = DatabaseConnector_instance.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            table = pd.read_sql_table(table_name, engine)
        return table

    def retrieve_pdf_data(self):
        pdf_path = 'card_details.pdf'

        card_details = ta.read_pdf(pdf_path, pages='all')

        card_details_df = card_details[0]

        for i in card_details:
            card_details_df = pd.concat([card_details_df, i], ignore_index=True)
        
        return card_details_df
    
    def list_number_of_stores(self, number_of_stores_endpoint, header_dict):

        # Return the number of stores in the business
        response = requests.get(number_of_stores_endpoint, headers=header_dict)
        num_stores = response.json()
        
        return num_stores


    def retrieve_stores_data(self, retrieve_a_store_endpoint, header_dict):

        #store_number = '450'  # Example store number, stores only go up to 450 from 0

        num_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header_dict=header_dict)
        num_stores = num_stores['number_stores']

        # Define the column names and initialize an empty DataFrame
        columns = ['index', 'address', 'longitude', 'lat', 'locality', 'store_code', 
                'staff_numbers', 'opening_date', 'store_type', 'latitude', 
                'country_code', 'continent']
        
        stores_dataset = pd.DataFrame(columns=columns)

        from tqdm import tqdm

        for i in tqdm(range(num_stores)):
            response = requests.get(retrieve_a_store_endpoint.format(store_number=str(i)), headers=header_dict)
            store_data = response.json()

            temp_dataset = pd.DataFrame([store_data])
            stores_dataset = pd.concat([stores_dataset, temp_dataset])

        return stores_dataset

# only run if this file is run
if __name__ == "__main__":
    test = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    # table = test.read_rds_table(DatabaseConnector_instance, table_name = 'legacy_store_details')
    # print(table)

    #pdf = test.retrieve_pdf_data()
    #print(pdf)
    
    num_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    retrieve_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    header_dict ={'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    num_stores = test.list_number_of_stores(num_stores_url, header_dict)
    store_data = test.retrieve_stores_data(retrieve_store_url, header_dict)
    print(num_stores)
    print(store_data)


