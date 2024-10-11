from pymongo import MongoClient
import os
from dotenv import load_dotenv

class DbConnector:
    """
    Connects to the MongoDB server.
    Connector needs HOST, USER, PASSWORD, and DATABASE to connect.
    """

    def __init__(self):
        load_dotenv()

        # Retrieve environment variables
        DATABASE = os.getenv('MONGO_DATABASE')
        HOST = "localhost"
        USER = os.getenv('MONGO_INITDB_ROOT_USERNAME')
        PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')

        uri = f"mongodb://{USER}:{PASSWORD}@{HOST}:27017/{DATABASE}?authSource=admin"

        # Connect to the database
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)
            self.db = None

        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)


''' 
if __name__ == "__main__":
    try:
        db_connector = DbConnector()
    except ValueError as ve:
        print("ERROR:", ve)
    else:
        if db_connector.db is not None:
            db_connector.close_connection()

'''
