# Xing Szen
from pymongo import MongoClient

class MongoDBConnector:
    
    def __init__(self, uri, db_name):
        # Initialize the client and bind to the specific database
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        # Retrieve the collection object
        return self.db[collection_name]

    def close(self):
        # Clean up the connection
        self.client.close()