# XingSzen
import json
import os
import pandas as pd
from pyspark.sql import SparkSession

from mongodb_connector import MongoDBConnector
from mongodb_ingestor import MongoDBIngestor
from mongodb_query import MongoDBQuerier

class ServingLayerRunner:
    def __init__(self, config_file_path):
        with open(config_file_path, 'r') as file:
            self.config = json.load(file)
            
        self.spark = SparkSession.builder \
            .appName("MongoDB_Serving_Layer") \
            .master("local[*]") \
            .getOrCreate()
            
        self.connector = MongoDBConnector(self.config['mongodb_uri'], self.config['mongodb_db'])
        self.ingestor = MongoDBIngestor(self.connector, self.spark)
        self.querier = MongoDBQuerier(self.connector)
        
        self.offenders_col = self.config['mongodb_offenders_collection']
        self.profiles_col = self.config['mongodb_profiles_collection']

    def execute_ingestion(self):
        print("Ingesting curated data into MongoDB...")
        self.ingestor.ingest_realtime_data(self.config['potential_offenders_hdfs_path'], self.offenders_col)
        self.ingestor.ingest_batch_data(self.config['batch_output_fraud'], self.profiles_col)

    def execute_queries_and_display(self):
        print("Running complex analytical aggregations...")
        
        # Get data from MongoDB
        # Use 1000 instead of 5000. This will show about 15-20 top offenders.
        offenders_data = self.querier.get_aggregated_repeat_offenders(self.offenders_col, 1000)

        # Use 1000 for volume. This is safe since your batch data showed ~25,000 per type.
        risk_profiles_data = self.querier.get_dynamic_risk_profiles(self.profiles_col, 1000)

        # Query 1: Write to an output file, then read as Spark DataFrame
        print("\n--- Query 1: Aggregated Repeat Offenders ---")
        if offenders_data:
            # 1. Write the output file
            with open("output_offenders.json", "w") as f:
                for record in offenders_data:
                    f.write(json.dumps(record) + "\n")
            
            # 2. Read natively into a Spark DataFrame using the absolute local path
            local_offenders_path = f"file://{os.path.abspath('output_offenders.json')}"
            offenders_df = self.spark.read.json(local_offenders_path)
            offenders_df.show(truncate=False) # <--- ADD THIS LINE!
        else:
            print("No offenders found matching the criteria in MongoDB.")

        # Query 2: Write to an output file, then read as Spark DataFrame
        print("\n--- Query 2: Dynamic Risk Profiling ---")
        if risk_profiles_data:
            # 1. Write the output file
            with open("output_profiles.json", "w") as f:
                for record in risk_profiles_data:
                    f.write(json.dumps(record) + "\n")

            # 2. Read natively into a Spark DataFrame using the absolute local path
            local_profiles_path = f"file://{os.path.abspath('output_profiles.json')}"
            profiles_df = self.spark.read.json(local_profiles_path)
            profiles_df.show(truncate=False) # <--- ADD THIS LINE!
        else:
            print("No risk profiles found matching the criteria in MongoDB.")

    def shutdown(self):
        self.connector.close()
        self.spark.stop()

if __name__ == "__main__":
    runner = ServingLayerRunner('../BatchProcessing/config.json')
    
    try:
        runner.execute_ingestion()
        runner.execute_queries_and_display()
    finally:
        runner.shutdown()