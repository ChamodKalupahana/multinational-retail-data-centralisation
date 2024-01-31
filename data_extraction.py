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

        card_details_df = card_details[0]

        for i in card_details:
            card_details_df = pd.concat([card_details_df, i], ignore_index=True)
        
        return card_details_df

"""columns = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
card_details_df = pd.DataFrame(columns=columns)
card_details_df = card_details_df.append(i, ignore_index=True)"""
# only run if this file is run
if __name__ == "__main__":
    test = DataExtractor()
    DatabaseConnector_instance = DatabaseConnector()

    # table = test.read_rds_table(DatabaseConnector_instance, table_name = 'legacy_store_details')
    # print(table)

    pdf = test.retrieve_pdf_data()
    print(pdf)


for i in range(card_details):
    print(i)

