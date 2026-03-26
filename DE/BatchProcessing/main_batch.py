# Author: Zhi Xuan
from pyspark.sql import SparkSession
from batch_config import BatchConfig
from batch_processor import BatchProcessor

class BatchPipeline:
    def __init__(self):
        self.spark = SparkSession.builder.appName("FinancialBatchProcessing").getOrCreate()
        self.config = BatchConfig()
        self.processor = BatchProcessor(self.spark)

    def execute_pipeline(self):
        
        raw_df = self.processor.load_data(self.config.get_raw_input_path())
       
        curated_df = self.processor.curate_data(raw_df)
        self.processor.save_data(curated_df, self.config.get_curated_output_path())

        volume_df = self.processor.aggregate_volume(curated_df)
        fraud_df = self.processor.aggregate_fraud(curated_df)

        self.processor.save_data(volume_df, self.config.get_batch_output_volume())
        self.processor.save_data(fraud_df, self.config.get_batch_output_fraud())

        self.spark.stop()

if __name__ == "__main__":
    pipeline = BatchPipeline()
    pipeline.execute_pipeline()
