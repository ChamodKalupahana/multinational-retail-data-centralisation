# Import modules
import yaml

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

# Test class

test = DatabaseConnector()
creds = test.read_db_creds()
print(creds)