# Import modules
import pandas as pd

from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_rds_table(self, DatabaseConnector_instance, table_name = 'legacy_store_details'):
        engine = DatabaseConnector_instance.init_db_engine()

        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            table = pd.read_sql_table(table_name, engine)
        return table

# only run if this file is run
if __name__ == "__main__":
    test = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    table = test.read_rds_table(DatabaseConnector_instance, table_name = 'legacy_store_details')
    print(table)