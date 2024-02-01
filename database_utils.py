# Import modules
import yaml
from sqlalchemy import create_engine, MetaData
import pandas as pd

from data_cleaning import DataCleaning

# Define class
class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        with open("db_creds.yaml", "r") as stream:
            try:
                creds = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        
        return creds

    def init_db_engine(self):
        credentials = self.read_db_creds()
        engine_connection = f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(engine_connection)

        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)
    
        # Extract table names
        table_names = pd.DataFrame(metadata.tables.keys())

        return table_names
    
    def upload_to_db(self, table_to_upload, name_of_database):

        # Define the database connection URL
        db_url = 'postgresql://postgres:Coolx12378@localhost/sales_data'

        # Create a SQLAlchemy engine
        engine = create_engine(db_url)

        # Upload the cleaned DataFrame to the dim_users table in the sales_data database
        table_to_upload.to_sql(name_of_database, engine, if_exists='replace', index=False)
        pass

if __name__ == "__main__":
    # Test class
    test = DatabaseConnector()
    # table_names = test.list_db_tables()
    # print(table_names)

    from data_extraction import DataExtractor
    DataExtractor_instance = DataExtractor()
    DataCleaning_instance = DataCleaning()
    # table = DataCleaning_instance.clean_user_data(DataExtractor_instance, test)

    # test.upload_to_db(table, 'dim_users')

    # upload store data from api
    
    # store_data = DataCleaning_instance.called_clean_store_data(DataExtractor_instance)
    # test.upload_to_db(store_data, 'dim_store_details')

    # upload product data from s3

    # product_data = DataCleaning_instance.clean_products_data(DataExtractor_instance)
    # test.upload_to_db(product_data, 'dim_products')

    # upload user_order

    # user_order_data = DataCleaning_instance.clean_orders_data(DataExtractor_instance, test)
    # test.upload_to_db(user_order_data, 'orders_table')

    # upload date_time_data

    date_time_data = DataCleaning_instance.clean_orders_data(DataExtractor_instance, test)
    test.upload_to_db(date_time_data, 'dim_date_times')

# For testing engine connection to RDS dataset
""" 


with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    # Reflect the metadata of the database tables
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    # Extract table names
    table_names = metadata.tables.keys()
    
    #return table_names

print('Help')

"""