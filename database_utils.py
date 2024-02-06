# Import modules
import yaml
from sqlalchemy import create_engine, MetaData
import pandas as pd

class DatabaseConnector:
    """
    A class for connecting to and interacting with a database.

    Methods:
    - read_db_creds(): Reads the database credentials from a YAML file.
    - init_db_engine(): Initializes a database engine using the provided credentials.
    - list_db_tables(): Retrieves a list of table names from the connected database.
    - upload_to_db(table_to_upload, name_of_database): Uploads a DataFrame to a specified database table.
    """
    
    def __init__(self) -> None:
        pass

    def read_db_creds(self):
        """
        Reads the database credentials from a YAML file.

        Returns:
        dict: A dictionary containing database credentials.
        """
        with open("db_creds.yaml", "r") as stream:
            try:
                creds = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        
        return creds

    def init_db_engine(self):
        """
        Initializes a database engine using the provided credentials.

        Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy engine object.
        """
        credentials = self.read_db_creds()
        engine_connection = f"postgresql+psycopg2://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(engine_connection)

        return engine
    
    def list_db_tables(self):
        """
        Retrieves a list of table names from the connected database.

        Returns:
        pandas.core.frame.DataFrame: A DataFrame containing the names of database tables.
        """
        engine = self.init_db_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)
    
        # Extract table names
        table_names = pd.DataFrame(metadata.tables.keys())

        return table_names
    
    def upload_to_db(self, table_to_upload, name_of_database):
        """
        Uploads a DataFrame to a specified database table.

        Args:
        table_to_upload (pandas.core.frame.DataFrame): DataFrame to upload to the database.
        name_of_database (str): Name of the database table to upload the DataFrame to.
        """
        # Define the database connection URL
        db_url = 'postgresql://postgres:Coolx12378@localhost/sales_data'

        # Create a SQLAlchemy engine
        engine = create_engine(db_url)

        # Upload the cleaned DataFrame to the specified database table
        table_to_upload.to_sql(name_of_database, engine, if_exists='replace', index=False)
        pass

    def init_local_database(self):
        # Define the database connection URL
        db_url = 'postgresql://postgres:Coolx12378@localhost/sales_data'

        # Create a SQLAlchemy engine
        engine = create_engine(db_url)

        return engine
    

if __name__ == "__main__":
    # Test class
    test = DatabaseConnector()
    # table_names = test.list_db_tables()
    # print(table_names)

    from data_extraction import DataExtractor
    from data_cleaning import DataCleaning
    DataExtractor_instance = DataExtractor()
    DataCleaning_instance = DataCleaning()


    # table = DataCleaning_instance.clean_user_data(DataExtractor_instance, test)

    # test.upload_to_db(table, 'dim_users')

    ### upload store data from api
    
    # store_data = DataCleaning_instance.called_clean_store_data(DataExtractor_instance)
    # test.upload_to_db(store_data, 'dim_store_details')

    ### upload card_data

    # card_data = DataCleaning_instance.clean_card_data(DataExtractor_instance)
    # test.upload_to_db(card_data, 'dim_card_details')

    ### upload product data from s3

    # product_data = DataCleaning_instance.clean_products_data(DataExtractor_instance)
    # test.upload_to_db(product_data, 'dim_products')

    ### upload user_order

    # user_order_data = DataCleaning_instance.clean_orders_data(DataExtractor_instance, test)
    # test.upload_to_db(user_order_data, 'orders_table')

    ### upload date_time_data

    date_time_data = DataCleaning_instance.clean_date_times(DataExtractor_instance)
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