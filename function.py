#!/usr/bin/ python

import os
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col
from pyspark.sql.functions import when, length
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, lower, upper, regexp_replace, lpad, expr
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from fuzzywuzzy import fuzz

class DataProcessor:
    
    def __init__(self, spark):
        self.spark = spark
        
        
    def read_and_clean_csv(self, file_path, custom_schema, file_name):

        df = (
              self.spark.read.format("csv")
                  .option("header", "true")
                  .option("inferSchema", "false")     
                  .schema(custom_schema)
                  .load(file_path)
        )

        ################################################################################################################
        # Dataset contains values as "NAN", "nan" as strings or Empty strings so replacing those with nullValues
        ################################################################################################################

        for column in df.columns:
            df = df.withColumn(column, when((df[column] == "NAN") | (df[column] == "nan") | (df[column] == "") | (df[column] == " "), None).otherwise(df[column]))

        df = df.withColumn("FILE_NAME", lit(file_name))

        df = df.withColumnRenamed(f"name{file_name}", "ENTITY") \
                .withColumnRenamed(f"address{file_name}", "ADDRESS") \
                .withColumnRenamed(f"city{file_name}", "CITY") \
                .withColumnRenamed(f"zip{file_name}", "ZIPCODE")

        return df

    def process_and_merge_data(self, file1_path, file2_path):

        # Defining custom schemas
        custom_schema_1 = StructType([
            StructField("name1", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("city1", StringType(), True),
            StructField("zip1", StringType(), True),
        ])

        custom_schema_2 = StructType([
            StructField("name2", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("city2", StringType(), True),
            StructField("zip2", StringType(), True),
        ])

        df1 = self.read_and_clean_csv(file1_path, custom_schema_1, 1)
        df2 = self.read_and_clean_csv(file2_path, custom_schema_2, 2)

        # Merge dataframes
        merged_df = self.merge_dataframes(df1, df2)

        merged_df.repartition(10)

        return merged_df

    def merge_dataframes(self, df1, df2):
        return df1.unionAll(df2)


    def basic_cleaning(self, merged_df):
        ################################################################################################################
        # Making sure everything is uppercase, removing extra spaces, white spaces, leading spaces
        # Removing special characters
        # Zipcode is not standardized in the dataset, Making it 5 digit with padding zeros to the left
        ################################################################################################################
        cleaned_df = (
            merged_df
            .withColumn("ADDRESS", trim(upper(regexp_replace(col("ADDRESS"), "\s+", " "))))
            .withColumn("ENTITY", trim(upper(regexp_replace(col("ENTITY"), "\s+", " "))))
            .withColumn("CITY", trim(upper(regexp_replace(col("CITY"), "\s+", " "))))
            .withColumn("ZIPCODE", trim(upper(regexp_replace(col("ZIPCODE"), "\s+", " "))))
            .withColumn("ADDRESS", trim(upper(regexp_replace(col("ADDRESS"), r"^\s+|\s+$", ""))))
            .withColumn("ENTITY", trim(upper(regexp_replace(col("ENTITY"), r"^\s+|\s+$", ""))))
            .withColumn("CITY", trim(upper(regexp_replace(col("CITY"), r"^\s+|\s+$", ""))))
            .withColumn("ZIPCODE", trim(upper(regexp_replace(col("ZIPCODE"), r"^\s+|\s+$", ""))))
            .withColumn("ADDRESS", trim(upper(regexp_replace(col("ADDRESS"), "[^a-zA-Z0-9 ]", ""))))
            .withColumn("ENTITY", trim(upper(regexp_replace(col("ENTITY"), "[^a-zA-Z0-9 ]", ""))))
            .withColumn("CITY", trim(upper(regexp_replace(col("CITY"), "[^a-zA-Z0-9 ]", ""))))
            .withColumn("ZIPCODE", trim(upper(regexp_replace(col("ZIPCODE"), "[^a-zA-Z0-9 ]", ""))))
            .withColumn("ZIPCODE", lpad(col("ZIPCODE").cast("string"), 5, "0"))
        )

        return cleaned_df




    def standardize_address(self, merged_df):

        ################################################################################################################
        # Making address standard. For this I have used the Standard USPS-Suffix-Abbreviations and tried to harmonize 
        # as much as poossible. To apply any similiraty Metrics or Egde Distance calculation further, Data needs to  
        # be as uniform as possible to get a better confidence score for 'Near duplicates' or 'Almost Duplicates'
        ################################################################################################################
        cleaned_df = (
            merged_df
            .withColumn("CLEAN_ADDRESS", regexp_replace(col("ADDRESS"), r"\bRD\b", "ROAD"))
            .withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), r"\bSTE\b", "SUITE"))
            .withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), r'\bSUITE\s*(\d+)\b', 'SUITE $1'))
            .withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), r"\bDR\b", "DRIVE")) 
            .withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), r"\b(ST|STE|STR)\b", "STREET"))
            .withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), r"\b(AV|AVE|AVENU)\b",  "AVENUE"))
        )

        replacements_boulevard = {
            r'\bBLVD\b': 'BOULEVARD',
            r'\bBLVD(\d+)\b': 'BOULEVARD $1',
            r'\bBLVD(\S+)\b': 'BOULEVARD $1',
            r'(\S*?)BLVD': '$1 BOULEVARD',
        }

        # Apply replacements using map and a for loop
        for pattern, replacement in replacements_boulevard.items():
            cleaned_df = cleaned_df.withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), pattern, replacement))

        replacements_highway = {
            r'\b(HWY|HIGHWY|HGHWY|HWYUS|HWYY|HWYS|USHWY|HWYHWY|KHWY)\b': 'HIGHWAY',
            r'\bHWY(\d+)\b': 'HIGHWAY $1',
            r'\bNHWY\b': 'N HIGHWAY',
            r'\bSHWY\b': 'S HIGHWAY',
            r'\bWHWY\b': 'W HIGHWAY',
            r'\bEHWY\b': 'E HIGHWAY',
            r'\bTHWY\b': 'T HIGHWAY',
            r'\bMILITARYHWY\b': 'MILITARY HIGHWAY',
            r'\bFEDERALHWY\b': 'FEDERAL HIGHWAY',
            r'\bNCHWY\b': 'NC HIGHWAY',
            r'(\d+)\s*HWY\b': '$1 HIGHWAY',
            r'\bHWY(\d+[A-Za-z]?)\b': '$1 HIGHWAY',
            r'\bHWYUNIT\b': 'HIGHWAY UNIT',
        }

        for pattern, replacement in replacements_highway.items():
            cleaned_df = cleaned_df.withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), pattern, replacement))


        dir_replacements = {
            r'\bN\b': 'NORTH',
            r'\bS\b': 'SOUTH',
            r'\bE\b': 'EAST',
            r'\bW\b': 'WEST',
            r'\bNE\b': 'NORTH EAST',
            r'\bSE\b': 'SOUTH EAST',
            r'\bNW\b': 'NORTH WEST',
            r'\bSW\b': 'SOUTH WEST',
        }

        # Apply replacements using map and a for loop
        for pattern, replacement in dir_replacements.items():
            cleaned_df = cleaned_df.withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), pattern, replacement))


        replacements_misc = {
            r'\bCIR\b': 'CIRCLE',
            r'\bEST\b': 'ESTATE',
            r'\b(PKWY|PKY|PKWAY|PWY)\b': 'PARKWAY',
            r'\b(FWY|FRWY)\b': 'FREEWAY',
            r'\bLK\b': 'LAKE',
            r'\bLN\b': 'LANE',
            r'\bMT\b': 'MOUNT',
            r'\bPT\b': 'POINT',
            r'\bWY\b': 'WAY',
        }

        for pattern, replacement in replacements_misc.items():
            cleaned_df = cleaned_df.withColumn("CLEAN_ADDRESS", regexp_replace(col("CLEAN_ADDRESS"), pattern, replacement))

        return cleaned_df




    def clean_entity(self, merged_df):
        ###############################################################################################################
        # Removing High frequesnt words in business like INC, LLC, CORP, CORPORATION, LTD, CO
        # Need a 3 pass scan because after the first regex replace a lot of word were together
        # So after the first pass many values have INC, LLC, CORP, etc together
        # EG: "CENTURY KANG INC CO", "DANILOS INC LLC", "KRISTYS FOOD MART LLC CORP"
        # So when "CO", "CORP" is removed, we still have new entity with "INC", "LLC"
        ###############################################################################################################
        cleaned_df = (
            merged_df
            .withColumn("CLEAN_ENTITY", 
                        when((length(col("ENTITY")) > 4) & (col("ENTITY").endswith("INC")), 
                             regexp_replace(col("ENTITY"), r'INC$', '')
                            )
                        .otherwise(col("ENTITY"))
                       )
            .withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), r'\s+LLC\s*$', ''))
            .withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), r'\s+CORPORATION\s*$', ''))
            .withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), r'\s+INCORPORATED\s*$', ''))
            .withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), r'\s+COMPANY LTD\s*$', ''))
            .withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), r'\s+CO LTD\s*$', ''))
            .withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), r'\s+LTD\s*$', '')  
            )
        )

        words_to_remove_second = ["INC", "LLC", "CORPORATION", "CORP", "INCORPORATED", "COMPANY LTD", "CO LTD", "LTD"]

        for word in words_to_remove_second:
            cleaned_df = cleaned_df.withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), fr'\s+{word}\s*$', ''))

        words_to_remove_third = ["INC", "LLC"]

        for word in words_to_remove_third:
            cleaned_df = cleaned_df.withColumn("CLEAN_ENTITY", regexp_replace(col("CLEAN_ENTITY"), fr'\s+{word}\s*$', ''))



        ##############################################################################################################
        # This part of cleaning is about places that are well known, and their names have been repeated over 200 times. 
        # Eamples include : [DUNKIN DONUTS, DOMINOS, SUBWAY, MCDONALDS,etc]
        # However, there are variations of these names. If we make them same, we can group them together. 
        # I have only selected the top few establishments with the highest repetitions
        # Doing this manually will help the EDIT DISTINACE Algo to perform better and give better Similarity metrics
        ##############################################################################################################

        cleaned_df = (
            cleaned_df
            .withColumn("CLEAN_ENTITY", 
                        when(col("CLEAN_ENTITY").startswith("SUBWAY"), "SUBWAY")
                        .when((col("CLEAN_ENTITY").startswith("WALMART")) | (col("CLEAN_ENTITY").startswith("WAL MART")), "WALMART")
                        .when(col("CLEAN_ENTITY").startswith("HOLIDAY INN EXPRESS"), "HOLIDAY INN EXPRESS")
                        .when(col("CLEAN_ENTITY").startswith("HOLIDAY INN"), "HOLIDAY INN")
                        .otherwise(col("CLEAN_ENTITY"))
            )
        )

        cleaned_df = (
            cleaned_df
            .withColumn("CLEAN_ENTITY", 
                        when((col("CLEAN_ENTITY").like("%DUNKIN DO%")) | (col("CLEAN_ENTITY").like("%DUNKIN DU%")) | (col("CLEAN_ENTITY").like("DUNKIN%")), "DUNKIN DONUTS")
                        .when(col("CLEAN_ENTITY").like("%PIZZA HUT%"), "PIZZA HUT")
                        .when((col("CLEAN_ENTITY").like("%MCDONALDS%")) | (col("CLEAN_ENTITY").startswith("%MC DONALDS%")), "MCDONALDS")
                        .when(col("CLEAN_ENTITY").like("%DOMINOS%"), "DOMINOS PIZZA")
                        .when(col("CLEAN_ENTITY").like("%BURGER KING%"), "BURGER KING")
                        .when((col("CLEAN_ENTITY").like("%WENDYS%")) & (col("CLEAN_ENTITY").like("%DBA%")), "WENDYS")
                        .when((col("CLEAN_ENTITY").like("%KENTUCKY FRIED CHICKEN%")) | (col("CLEAN_ENTITY").like("%KFC%")), "KENTUCKY FRIED CHICKEN")
                        .when(col("CLEAN_ENTITY").like("%PAPA JOHNS PIZZA%"), "PAPA JOHNS PIZZA")
                        .when(col("CLEAN_ENTITY").like("%DAIRY QUEEN%"), "DAIRY QUEEN")
                        .when(col("CLEAN_ENTITY").like("%CHICK FIL A%"), "CHICK FIL A")
                        .when(col("CLEAN_ENTITY").like("%ARBYS%"), "ARBYS")
                        .when(col("CLEAN_ENTITY").like("%APPLEBEES NEIGHBORHOOD%"), "APPLEBEES NEIGHBORHOOD GRILL AND BAR")
                        .when(col("CLEAN_ENTITY").like("%JIMMY JOHNS%"), "JIMMY JOHNS")
                        .when((col("CLEAN_ENTITY").like("%US PROTECT%")) | (col("CLEAN_ENTITY").like("%USPROTECT%")) | (col("CLEAN_ENTITY").like("U S PROTECT%")), "US PROTECT")
                        .when((col("CLEAN_ENTITY").like("%UNITED STATES POSTAL SERVICE%")) | (col("CLEAN_ENTITY").like("%US POSTAL SERVICE%")) | (col("CLEAN_ENTITY").like("USPS%")), "UNITED STATES POSTAL SERVICE")
                        .when(col("CLEAN_ENTITY").like("%RIO FRESH%"), "RIO FRESH PRODUCE")
                        .when(col("CLEAN_ENTITY").like("%FRONTERA PRODUCE%"), "FRONTERA PRODUCEE")
                        .when(col("CLEAN_ENTITY").like("%SLM TRANS%"), "SLM TRANS")
                        .when(col("CLEAN_ENTITY").like("%UNITED AIRLINES%"), "UNITED AIRLINES")
                        .when(col("CLEAN_ENTITY").like("%GENERAL MOTORS%"), "GENERAL MOTORS")
                        .when(col("CLEAN_ENTITY").like("%CVS%"), "CVS PHARMACY")
                        .when(col("CLEAN_ENTITY").like("%WALGREENS%"), "WALGREENS PHARMACY")
                        .when(col("CLEAN_ENTITY").like("%7 ELEVEN%"), "7 ELEVEN")
                        .when(col("CLEAN_ENTITY").like("%QUALITY INN%"), "QUALITY INN")
                        .when(col("CLEAN_ENTITY").like("%OUTBACK STEAKHOUSE%"), "OUTBACK STEAKHOUSE")
                        .when(col("CLEAN_ENTITY").like("%SUPER 8 MOTEL%"), "SUPER 8 MOTEL")
                        .when((col("CLEAN_ENTITY").like("%IHOP%")) | (col("CLEAN_ENTITY").like("%INTERNATIONAL HOUSE OF PANCAKES%")), "INTERNATIONAL HOUSE OF PANCAKES")
                        .when(col("CLEAN_ENTITY").like("%SONIC DRIVE IN%"), "SONIC DRIVE IN")
                        .when(col("CLEAN_ENTITY").like("%DENNYS%"), "DENNYS")
                        .when(col("CLEAN_ENTITY").like("%JERSEY MIKES SUBS%"), "JERSEY MIKES SUBS")
                        .when(col("CLEAN_ENTITY").like("%LITTLE CAESARS%"), "LITTLE CAESARS")
                        .when(col("CLEAN_ENTITY").like("%ZAXBYS%"), "ZAXBYS")
                        .when(col("CLEAN_ENTITY").like("%MOES SOUTHWEST GRILL%"), "MOES SOUTHWEST GRILL")
                        .when(col("CLEAN_ENTITY").like("%BEEF O BRADYS%"), "BEEF O'BRADYS")
                        .when(col("CLEAN_ENTITY").like("%HUDDLE HOUSE%"), "HUDDLE HOUSE")
                        .when(col("CLEAN_ENTITY").like("%BLAZE PIZZA%"), "BLAZE PIZZA")
                        .otherwise(col("CLEAN_ENTITY"))
            )
        )
        return cleaned_df


    def store_intermediate_result(self, merged_df, output_path):
        ##############################################################################################################
        # Writing the Cleaned DataFrame to a Parquet format so that further porcessing can be faster
        # Selecting ALL columns, butafter this we will be using only Clean COlumns
        ##############################################################################################################
        rearranged_df = merged_df.select(
            "ENTITY",
            "CLEAN_ENTITY",
            "ADDRESS",
            "CLEAN_ADDRESS",
            "CITY",
            "ZIPCODE",
            "FILE_NAME"
        )

        rearranged_df.write.mode("overwrite").parquet(output_path)


    def read_intermediate_result(self, spark, intermediate_result_path):
        # Reading the Parquet file into a DataFrame
        merged_df = self.spark.read.parquet(intermediate_result_path)

        # Repartitioning the DataFrame into 1 partition as data size is less tha 128MB
        merged_df = merged_df.repartition(1)

        return merged_df


    def filter_df_by_null_column(self, merged_df, column_name):
        ###############################################################################################################
        # Seperating out all values containing Null based on each column
        # At the end when wehave some confidence in the data, we can try to replace the the Null values
        # if we find an exact match from other three columns in the MASTER DATA
        ###############################################################################################################
        filtered_df = merged_df.select(
            F.col('CLEAN_ENTITY').alias('ENTITY'),
            F.col('CLEAN_ADDRESS').alias('ADDRESS'),
            F.col('CITY'),
            F.col('ZIPCODE'),
            F.col('FILE_NAME')
        )

        # Filtering rows where the specified column is null
        filtered_df = filtered_df.filter(F.col(column_name).isNull())

        return filtered_df


    def filter_non_null_rows(self, merged_df):
        # Selecting specific columns with alias names
        selected_columns = [
            F.col('CLEAN_ENTITY').alias('ENTITY'),
            F.col('CLEAN_ADDRESS').alias('ADDRESS'),
            F.col('CITY'),
            F.col('ZIPCODE'),
            F.col('FILE_NAME')
        ]

        # Filtering rows where all selected columns are not null
        merged_df_noNull = merged_df.select(*selected_columns).filter(
            (F.col('CLEAN_ENTITY').isNotNull()) &
            (F.col('CLEAN_ADDRESS').isNotNull()) &
            (F.col('CITY').isNotNull()) &
            (F.col('ZIPCODE').isNotNull())
        )

        return merged_df_noNull


    """
    #######################################################################################
    # From this step onwards, output will be appended to a CSV file
    # CSV file. Its a four step process, thats way every step will write
    # few chunks of data to the output File
    #######################################################################################
    """

    def write_to_file(self, df, output_file, file_format="csv", write_mode="overwrite"):
        """
        Write DataFrame to file with options for file format and write mode.

        Parameters:
            df (DataFrame): The DataFrame to write to file.
            output_file (str): The path to the output file.
            file_format (str): The file format, can be 'parquet' or 'csv'. Default is 'csv'.
            write_mode (str): The write mode, can be 'append' or 'overwrite'. Default is 'overwrite'.
        """
        # Check if the file exists
        file_exists = os.path.exists(output_file)

        # Write the DataFrame based on user options
        if file_format == "csv":
            if not file_exists or write_mode == "overwrite":
                df.coalesce(1).write.csv(output_file, header=True, mode="overwrite")
            else:
                df.write.csv(output_file, header=True, mode="append")
        elif file_format == "parquet":
            if not file_exists or write_mode == "overwrite":
                df.write.parquet(output_file, mode="overwrite")
            else:
                df.write.parquet(output_file, mode="append")
        else:
            raise ValueError("Unsupported file format. Please choose 'csv' or 'parquet'.")





    #######################################################################################
    # PART 1 ---> Unique on Address, City, Zipcode ---> Different Entities/Restaurants

    # Finding unique Address, City, Zipcode
    # That may correspond to different Entities/Restaurants
    # Attempting to Entity deduplication issue
    # With respect to Address as addresses were standardized
    #######################################################################################

    def get_single_acz_single_entity(self, merged_df_noNull):
        #################################################################################################################
        # Logic behind the DF name : single_address_city_zip_single_entity
        # Collecting Combination of unique (Address, City, Zipcode) with just single entity
        # GROUP BY ON Address, City, Zipcode WHERE COUNT(ENTITY) = 1
        # This data has a unique combination of (Address, City, Zipcode and Enitity), So we can push this to the output
        ##################################################################################################################
        single_acz_single_entity = (
            merged_df_noNull.alias('t1')
            .join(
                (
                    merged_df_noNull
                    .groupBy('ADDRESS', 'CITY', 'ZIPCODE', 'FILE_NAME')
                    .agg(F.count('*').alias('ENTITY_CNT'))
                    .where(F.col('ENTITY_CNT') == 1)
                ).alias('t2'),
                (F.col('t1.ADDRESS') == F.col('t2.ADDRESS')) &
                (F.col('t1.CITY') == F.col('t2.CITY')) &
                (F.col('t1.ZIPCODE') == F.col('t2.ZIPCODE')) &
                (F.col('t1.FILE_NAME') == F.col('t2.FILE_NAME')),
                'inner'
            )
            .select('t1.ENTITY', 't1.ADDRESS', 't1.CITY', 't1.ZIPCODE', 't1.FILE_NAME')
        )


        return single_acz_single_entity


    def get_single_acz_multiple_entity(self, merged_df_noNull):
        #################################################################################################################
        # Logic behind the DF name : single_address_city_zip_multiple_entity
        # Collecting Combination of unique (Address, City, Zipcode) with many Entities and Restaurant
        # GROUP BY ON Address, City, Zipcode WHERE COUNT(ENTITY) > 1
        # This data has a unique combination of (Address, City, Zipcode and Enitity), but has alot of repeated Data as well
        # This is thje kind of Data where FuzzyWuzzy will be applied
        ##################################################################################################################
        single_acz_multiple_entity = (
            merged_df_noNull.alias('t1')
            .join(
                merged_df_noNull.groupBy('ADDRESS', 'CITY', 'ZIPCODE', 'FILE_NAME')
                .agg(F.count('*').alias('ENTITY_CNT'))
                .where(F.col('ENTITY_CNT') > 1).alias('t2'),
                (F.col('t1.ADDRESS') == F.col('t2.ADDRESS'))
                & (F.col('t1.CITY') == F.col('t2.CITY'))
                & (F.col('t1.ZIPCODE') == F.col('t2.ZIPCODE'))
                & (F.col('t1.FILE_NAME') == F.col('t2.FILE_NAME')),
                'inner'
            )
            .select('t1.ENTITY', 't1.ADDRESS', 't1.CITY', 't1.ZIPCODE', 't1.FILE_NAME')
        )

        return single_acz_multiple_entity


    def get_all_unique_acze(self, single_acz_multiple_entity):
        ############################################################################################################################
        # acze: address_city_zip_entity
        # The DataFrame named single_address_city_zip_multiple_entity has duplicate entries
        # This code specifically investigates instances where the count of occurrences for (Address, City, Zipcode, and Entity) > 1
        # The data, which repeats multiple times, undergoes a Group BY operation to distill unique records
        # The resulting DataFrame encompasses around 21,664 distinct records
        # This records are directly pushed to the Output
        ############################################################################################################################

        all_unique_acze = (
            single_acz_multiple_entity
            .groupBy("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME")
            .agg(F.count("*").alias("count"))
            .filter(col("count") > 1)
            .select("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME")
        )

        return all_unique_acze

    def get_single_acz_possible_multi_entity(self, single_acz_multiple_entity, all_unique_acze):
        ############################################################################################################################
        # single_acz_possible_multi_entity: Every Combination of ADDRESS, CITY, ZIP is Unique, but ENTITY can be multiple
        # The DataFrame, single_acz_possible_multi_entity, is derived by excluding records present in all_unique_acze.
        # It performs a left anti-join on columns (ENTITY, ADDRESS, CITY, ZIPCODE, FILE_NAME).
        # The resulting DataFrame captures instances where the combination is present in single_acz_multiple_entity but not in all_unique_acze.
        # It includes columns: ENTITY, ADDRESS, CITY, ZIPCODE, and FILE_NAME.
        # Perform a left anti-join to exclude records present in all_unique_acze
        ############################################################################################################################

        single_acz_possible_multi_entity = (
            single_acz_multiple_entity
            .join(all_unique_acze, ["ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME"], "left_anti")
            .select("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME")
        )

        return single_acz_possible_multi_entity


    #######################################################################################
    # PART 2 ---> Unique on Enitity, City, Zipcode ---> Different Address

    # Finding unique Enity, City, Zipcode
    # That may correspond to different Address or repeated Address
    # Attempting to address deduplication issue
    # With respect to entity as addresses
    #######################################################################################

    def get_single_ecz_multiple_address(self, merged_df_noNull):
        #################################################################################################################
        # Logic behind the DF name : single_entity_city_zip_multiple_addrres
        # Collecting Combination of unique (Entity, City, Zipcode) with many Address and Restaurant
        # GROUP BY ON Enitty, City, Zipcode WHERE COUNT(Address) > 1
        # This data has a unique combination of (Address, City, Zipcode and Enitity), but has alot of repeated Data as well
        ##################################################################################################################
        single_ecz_multiple_address = (
            merged_df_noNull.alias('t1')
            .join(
                merged_df_noNull.groupBy('ENTITY', 'CITY', 'ZIPCODE', 'FILE_NAME')
                .agg(F.count('*').alias('ADDRESS_CNT'))
                .where(F.col('ADDRESS_CNT') > 1)
                .alias('t2'),
                (F.col('t1.ENTITY') == F.col('t2.ENTITY')) &
                (F.col('t1.CITY') == F.col('t2.CITY')) &
                (F.col('t1.ZIPCODE') == F.col('t2.ZIPCODE')) &
                (F.col('t1.FILE_NAME') == F.col('t2.FILE_NAME')),
                'inner'
            )
            .select('t1.ENTITY', 't1.ADDRESS', 't1.CITY', 't1.ZIPCODE', 't1.FILE_NAME')
        )

        return single_ecz_multiple_address



    def get_all_unique_ecza(self, single_ecz_multiple_address):
        ######################################################################################
        # Group by columns and count the occurrences
        # This function takes the DataFrame single_ecz_multiple_address as input, 
        # DataFrame containing the unique combinations of Address, City, Zipcode and Enitity
        # No need to write this data again as its already been pushed in the first step
        # This is just to check even in this Data set we get the same unique
        # Also this df is used to remove unique data from single_ecz_multiple_address
        #######################################################################################
        all_unique_ecza = (
            single_ecz_multiple_address
            .groupBy("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME")
            .agg(F.count("*").alias("count"))
            .filter(col("count") > 1)
            .select("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME")
        )

        return all_unique_ecza



    def get_single_ecz_possible_multi_address(self, single_ecz_multiple_address, all_unique_ecza):
        # Find rows in 'single_ecz_multiple_address' that are not in 'all_unique_ecza'
        single_ecz_possible_multi_address = (
            single_ecz_multiple_address
            .join(all_unique_ecza, ["ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME"], "left_anti")
            .select("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME")
        )

        return single_ecz_possible_multi_address




    def apply_edit_distance_algorithm(self, intermediate_df):
        ###########################################################################
        # The following code applies an edit distance algorithm to the DataFrame 
        # Duplicate dataframe which was notpossible to clean manually.
        ############################################################################
        window_spec = Window.orderBy("ADDRESS", "ENTITY", "CITY", "ZIPCODE").partitionBy("ADDRESS", "CITY", "ZIPCODE")
        df = intermediate_df.withColumn("ROW_NUM", F.row_number().over(window_spec))

        df_row_number_1 = df.filter('ROW_NUM = 1') \
            .withColumnRenamed('ENTITY', 'ENTITY_1') \
            .withColumnRenamed('ADDRESS', 'ADDRESS_1') \
            .withColumnRenamed('CITY', 'CITY_1') \
            .withColumnRenamed('ZIPCODE', 'ZIPCODE_1') \
            .withColumnRenamed('FILE_NAME', 'FILE_NAME_1') \
            .withColumnRenamed('ROW_NUM', 'ROW_NUM_1')

        df_row_number_2 = df.filter('ROW_NUM = 2') \
            .withColumnRenamed('ENTITY', 'ENTITY_2') \
            .withColumnRenamed('ADDRESS', 'ADDRESS_2') \
            .withColumnRenamed('CITY', 'CITY_2') \
            .withColumnRenamed('ZIPCODE', 'ZIPCODE_2') \
            .withColumnRenamed('FILE_NAME', 'FILE_NAME_2') \
            .withColumnRenamed('ROW_NUM', 'ROW_NUM_2')

        fuzzywuzzy_udf = udf(lambda x, y: fuzz.ratio(x, y), IntegerType())

        joined_df_fuzzy = df_row_number_1.alias("df1").join(
            df_row_number_2.alias("df2"),
            (df_row_number_1["ADDRESS_1"] == df_row_number_2["ADDRESS_2"]) &
            (df_row_number_1["CITY_1"] == df_row_number_2["CITY_2"]) &
            (df_row_number_1["ZIPCODE_1"] == df_row_number_2["ZIPCODE_2"]) &
            (df_row_number_1["FILE_NAME_1"] == df_row_number_2["FILE_NAME_2"]),
            "inner"
        )

        result_df_fuzzy = joined_df_fuzzy.withColumn("FUZZY_SCORE", fuzzywuzzy_udf(col("df1.ENTITY_1"), col("df2.ENTITY_2")))

        fuzzy_result = result_df_fuzzy.select("df1.*", "FUZZY_SCORE", "df2.*")

        return fuzzy_result


    # Combine and process DataFrames
    def combine_and_process_dataframes(self, intermediate_df):
        fuzzy_result = self.apply_edit_distance_algorithm(intermediate_df)

        columns_1 = [col for col in fuzzy_result.columns if col.endswith("_1")]
        df_1 = fuzzy_result.select(columns_1 + ["FUZZY_SCORE"])

        columns_2 = [col for col in fuzzy_result.columns if col.endswith("_2")]
        df_2 = fuzzy_result.select(columns_2 + ["FUZZY_SCORE"])

        df_1 = df_1.toDF(*[name[:-2] if name.endswith("_1") else name for name in df_1.columns])
        df_2 = df_2.toDF(*[name[:-2] if name.endswith("_2") else name for name in df_2.columns])

        result_combined = df_1.union(df_2).orderBy("ADDRESS", "ENTITY", "CITY", "ZIPCODE")

        return result_combined


    def process_df(self, df):
        df = df.withColumn("ENTITY_length", F.length("ENTITY"))
        df_filtered = df.filter(F.col("ROW_NUM").isin(1, 2))

        windowSpec = Window.partitionBy("ADDRESS", "CITY", "ZIPCODE").orderBy("ROW_NUM")

        df_processed = df_filtered.withColumn("max_entity_length", F.max("ENTITY_length").over(windowSpec)) \
                                  .withColumn("lag_fuzzy_score", F.lag("FUZZY_SCORE").over(windowSpec)) \
                                  .filter(((F.col("FUZZY_SCORE") >= 70) & (F.col("FUZZY_SCORE") == F.col("lag_fuzzy_score"))) |
                                          (F.col("FUZZY_SCORE") < 70)) \
                                  .drop("lag_fuzzy_score")

        df_processed = df_processed.drop("ROW_NUM", "FUZZY_SCORE", "ENTITY_length", "max_entity_length")

        return df_processed


    def main_processing_pipeline(self, intermediate_df):
        final_result_combined = self.combine_and_process_dataframes(intermediate_df)

        final_processed_result = self.process_df(final_result_combined)

        return final_processed_result


    def process_output_file(self, output_file):

        schema = StructType([
            StructField("ENTITY", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("ZIPCODE", StringType(), True),
            StructField("FILE_NAME", IntegerType(), True),
            ])



        outputDF = (
            self.spark.read.format("csv")
                      .option("header", "true")
                      .option("inferSchema", "false")  
                      .schema(schema)
                      .load(output_file)
              )

        # Create a temporary view for SQL queries
        outputDF.createOrReplaceTempView("outputDF")
                
        result_df = outputDF.groupBy("ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME").agg(F.count("*").alias("COUNT_OCCURRENCES"))

        result_df = result_df.withColumn("ID", monotonically_increasing_id() + 1)

        result_df = result_df.select("ID", "ENTITY", "ADDRESS", "CITY", "ZIPCODE", "FILE_NAME", "COUNT_OCCURRENCES")

        return result_df
    