# Import modules
import yaml
from sqlalchemy import create_engine, MetaData
import pandas as pd

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

if __name__ == "__main__":
    # Test class
    test = DatabaseConnector()
    table_names = test.list_db_tables()
    print(table_names)
    
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