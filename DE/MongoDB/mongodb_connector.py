#XingSzen
from pymongo import MongoClient

class MongoDBConnector:
    # Initializes the connection using the URI and Database Name from the config
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    # Helper method to get a specific collection object
    def get_collection(self, collection_name):
        return self.db[collection_name]

    # Always good practice to close the connection when done
    def close(self):
        self.client.close()