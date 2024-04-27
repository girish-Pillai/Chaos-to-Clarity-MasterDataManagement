#!/usr/bin/ python

import os
import sys
from pyspark.sql import SparkSession
from function import DataProcessor

class MainApplication:
    def __init__(self, spark):
        self.spark = spark
        self.data_processor = DataProcessor(spark)
        
    def main(self):

        # Define the path name of storage bucket
        STORAGE_BUCKET = "mdm_data_bucket"

        file1_path = f"gs://{STORAGE_BUCKET}/input_data/file1.csv"
        file2_path = f"gs://{STORAGE_BUCKET}/input_data/file2.csv"
        intermediate_result_path = f"gs://{STORAGE_BUCKET}/stage_data/parquet_file"
        output_file = f"gs://{STORAGE_BUCKET}/stage_data/intermediateCSV"
        final_df_path = f"gs://{STORAGE_BUCKET}/final_df/Final_result.csv"

        merged_df = self.data_processor.process_and_merge_data(file1_path, file2_path)
        merged_df = self.data_processor.basic_cleaning(merged_df)
        merged_df = self.data_processor.standardize_address(merged_df)
        merged_df = self.data_processor.clean_entity(merged_df)
        self.data_processor.write_to_file(merged_df, intermediate_result_path, file_format="parquet", write_mode="overwrite")
        print("Intermediate result stored as Parquet File")

        merged_df = self.data_processor.read_intermediate_result(spark, intermediate_result_path)

        df_with_null_entity = self.data_processor.filter_df_by_null_column(merged_df, 'ENTITY')
        df_with_null_address = self.data_processor.filter_df_by_null_column(merged_df, 'CLEAN_ADDRESS')
        df_with_null_city = self.data_processor.filter_df_by_null_column(merged_df, 'CITY')
        df_with_null_zipcode = self.data_processor.filter_df_by_null_column(merged_df, 'ZIPCODE')

        merged_df_noNull = self.data_processor.filter_non_null_rows(merged_df)
        single_acz_single_entity = self.data_processor.get_single_acz_single_entity(merged_df_noNull)
        self.data_processor.write_to_file(single_acz_single_entity, output_file, file_format="csv", write_mode="overwrite")
        print("1st batch of data pushed to CSV")

        single_acz_multiple_entity = self.data_processor.get_single_acz_multiple_entity(merged_df_noNull)
        all_unique_acze = self.data_processor.get_all_unique_acze(single_acz_multiple_entity)
        self.data_processor.write_to_file(all_unique_acze, output_file, file_format="csv", write_mode="append")
        print("2nd batch of data pushed to CSV")

        single_acz_possible_multi_entity = self.data_processor.get_single_acz_possible_multi_entity(single_acz_multiple_entity, all_unique_acze)
        fuzzy_result_part_1 = self.data_processor.main_processing_pipeline(single_acz_possible_multi_entity)
        self.data_processor.write_to_file(fuzzy_result_part_1, output_file, file_format="csv", write_mode="append")
        print("3rd batch of data pushed to CSV")

        single_ecz_multiple_address = self.data_processor.get_single_ecz_multiple_address(merged_df_noNull)
        all_unique_ecza = self.data_processor.get_all_unique_ecza(single_ecz_multiple_address)
        single_ecz_possible_multi_address = self.data_processor.get_single_ecz_possible_multi_address(single_ecz_multiple_address, all_unique_ecza)
        fuzzy_result_part_2 = self.data_processor.main_processing_pipeline(single_ecz_possible_multi_address)
        self.data_processor.write_to_file(fuzzy_result_part_2, output_file, file_format="csv", write_mode="append")
        print("4th batch of data pushed to CSV")

        final_result = self.data_processor.process_output_file(output_file)
        self.data_processor.write_to_file(final_result, final_df_path, file_format="csv", write_mode="overwrite")

        print("FinalResult Stored in CSV")
        print("DATA Loading Complete")

        spark.stop()
    

# Call the main function
if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Chaos-to-clarity-pyspark-job") \
            .getOrCreate()
    app = MainApplication(spark)
    app.main()
