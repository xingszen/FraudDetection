# Author: Xing Szen
import json
import os
from pyspark.sql import SparkSession

from mongodb_connector import MongoDBConnector
from mongodb_ingestor import MongoDBIngestor
from mongodb_query import MongoDBQuerier

class ServingLayerRunner:
    def __init__(self, config_file_path):
        # Load the JSON configuration file containing our HDFS paths and MongoDB URI
        with open(config_file_path, 'r') as file:
            self.config = json.load(file)
            
        # Initialize a local Spark Session to handle data reading and DataFrame display
        self.spark = SparkSession.builder \
            .appName("MongoDB_Serving_Layer") \
            .master("local[*]") \
            .getOrCreate()
            
        # Initialize our custom connector, ingestor, and querier classes
        self.connector = MongoDBConnector(self.config['mongodb_uri'], self.config['mongodb_db'])
        self.ingestor = MongoDBIngestor(self.connector, self.spark)
        self.querier = MongoDBQuerier(self.connector)
        
        # Store the collection names locally in the class for easier access later
        self.offenders_col = self.config['mongodb_offenders_collection']
        self.profiles_col = self.config['mongodb_profiles_collection']
        self.receivers_col = self.config['mongodb_receivers_collection']
        self.iptakeover_col = self.config['mongodb_iptakeover_collection']

    def execute_ingestion(self):
        print("Ingesting curated data into MongoDB...")
        # Read the processed JSON/Parquet files from HDFS and write them directly into MongoDB
        # Note: The ingest_batch_data expects Parquet, while ingest_realtime_data expects JSON
        self.ingestor.ingest_realtime_data(self.config['potential_offenders_hdfs_path'], self.offenders_col)
        self.ingestor.ingest_batch_data(self.config['batch_output_fraud'], self.profiles_col)
        self.ingestor.ingest_realtime_data(self.config['suspicious_receivers_hdfs_path'], self.receivers_col)
        self.ingestor.ingest_realtime_data(self.config['ip_takeover_hdfs_path'], self.iptakeover_col)

    def execute_queries_and_display(self):
        print("Running complex analytical aggregations...")
        
        # Execute Query 1: The cross-reference query that uses $lookup across 3 collections
        cross_reference_data = self.querier.get_comprehensive_risk_analysis(
            self.offenders_col, 
            self.receivers_col, 
            self.iptakeover_col
        )

        # Execute Query 2: The dynamic risk profiling based on the batch data
        risk_profiles_data = self.querier.get_dynamic_risk_profiles(self.profiles_col, 1000)

        # Format and display Query 1
        print("\n--- Query 1: Cross-Referenced Real-Time Threats ---")
        if cross_reference_data:
            # We dump the MongoDB dictionary results into a temporary local JSON file
            with open("output_cross_referenced.json", "w") as f:
                for record in cross_reference_data:
                    f.write(json.dumps(record) + "\n")
            
            # We use PySpark to read that temporary JSON file so we can use the .show() method 
            # to render a nice ASCII table for the assignment screenshots[cite: 71, 75].
            local_cross_path = f"file://{os.path.abspath('output_cross_referenced.json')}"
            cross_df = self.spark.read.json(local_cross_path)
            cross_df.show(truncate=False) 
        else:
            print("No cross-referenced threats found matching the criteria.")

        # Format and display Query 2
        print("\n--- Query 2: Dynamic Batch Risk Profiling ---")
        if risk_profiles_data:
            # Same strategy: write results to a temp JSON file, read with Spark, and display
            with open("output_profiles.json", "w") as f:
                for record in risk_profiles_data:
                    f.write(json.dumps(record) + "\n")

            local_profiles_path = f"file://{os.path.abspath('output_profiles.json')}"
            profiles_df = self.spark.read.json(local_profiles_path)
            profiles_df.show(truncate=False) 
        else:
            print("No risk profiles found matching the criteria in MongoDB.")

    def shutdown(self):
        # Safely close the database connection and shut down the local Spark cluster
        self.connector.close()
        self.spark.stop()

if __name__ == "__main__":
    # Ensure this path correctly points to where your config.json is stored
    runner = ServingLayerRunner('../BatchProcessing/config.json')
    
    try:
        runner.execute_ingestion()
        runner.execute_queries_and_display()
    finally:
        # The finally block ensures that even if the code crashes, the connections are closed
        runner.shutdown()