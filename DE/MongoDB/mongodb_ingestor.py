#XingSzen

class MongoDBIngestor:
    # Requires a database connection object and the Spark Session to read HDFS
    def __init__(self, db_connector, spark_session):
        self.db_connector = db_connector
        self.spark = spark_session

    # Ingests the real-time potential offenders data
    def ingest_realtime_data(self, hdfs_path, collection_name):
        # Read the JSON data produced by the RealTime.py script
        df = self.spark.read.json(hdfs_path)
        # Convert Spark DataFrame to a list of Python dictionaries for MongoDB
        records = df.toPandas().to_dict('records')
        
        if records:
            collection = self.db_connector.get_collection(collection_name)
            collection.delete_many({}) # Clear old data to avoid duplicates during testing
            collection.insert_many(records) # Insert the new batch

    # Ingests the batch-processed fraud profiles
    def ingest_batch_data(self, hdfs_path, collection_name):
        # Read the Parquet data produced by the batch_processor.py script
        df = self.spark.read.parquet(hdfs_path)
        records = df.toPandas().to_dict('records')
        
        if records:
            collection = self.db_connector.get_collection(collection_name)
            collection.delete_many({}) 
            collection.insert_many(records)