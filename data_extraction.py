# Import modules
import pandas as pd
import tabula as ta

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

        return pd.DataFrame(card_details)


# only run if this file is run
if __name__ == "__main__":
    test = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    # table = test.read_rds_table(DatabaseConnector_instance, table_name = 'legacy_store_details')
    # print(table)

    pdf = test.retrieve_pdf_data()
    print(pdf)

