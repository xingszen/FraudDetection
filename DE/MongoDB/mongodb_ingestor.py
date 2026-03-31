# Xing Szen

class MongoDBIngestor:
    
    def __init__(self, db_connector, spark_session):
        self.db_connector = db_connector
        self.spark = spark_session

    def ingest_realtime_data(self, hdfs_path, collection_name):
        # Real-time data is saved as JSON in HDFS
        df = self.spark.read.json(hdfs_path)
        records = df.toPandas().to_dict('records')
        
        if records:
            collection = self.db_connector.get_collection(collection_name)
            collection.delete_many({}) # Clear old data to avoid duplicates during testing
            collection.insert_many(records) 

    def ingest_batch_data(self, hdfs_path, collection_name):
        # Batch data is saved as Parquet in HDFS
        df = self.spark.read.parquet(hdfs_path)
        records = df.toPandas().to_dict('records')
        
        if records:
            collection = self.db_connector.get_collection(collection_name)
            collection.delete_many({}) 
            collection.insert_many(records)