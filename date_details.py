import pandas as pd
import json

with open('date_details.json') as f:
    table = json.load(f)

table = pd.DataFrame.from_dict(table)

print(table.head())