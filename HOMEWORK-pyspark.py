from pyspark.sql.functions import to_date, col, min
from pyspark.sql.utils import AnalysisException


# BLOCK: load to bronze
input_path = "/Volumes/my_homework/default/test/raw_data/"

# Path for the bronze delta table storage
bronze_table_path = "/Volumes/my_homework/default/test/bronze_table/"

# AutoLoader setup
raw_df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")  
          .option("header", "true") 
          .option("cloudFiles.schemaLocation", bronze_table_path + "/schema_bronze")
          .load(input_path))

# Write data to bronze delta table
bronze_table = (raw_df.writeStream
                .option("checkpointLocation", bronze_table_path + "/_checkpoint_bronze")
                .option("mergeSchema", "true")
                .trigger(processingTime="5 minutes") 
                .table("bronze_table"))


# BLOCK: transform bronze data into silver
try:
    try:
        static_bronze_df = spark.read.table("bronze_table")
        min_last_review = static_bronze_df.agg(min(col("last_review"))).collect()[0][0]
    except AnalysisException as e:
            print(f"Error loading static bronze table or computing min_last_review: {e}")
            min_last_review = '2024-09-28'

    try:
        bronze_df = spark.readStream.table("bronze_table")
    except AnalysisException as e:
        raise RuntimeError(f"Error loading streaming bronze table: {e}")

    # Filter out rows with invalid price
    bronze_df = bronze_df.filter(col("price") > 0)

    # Convert last_review column to date, handling missing data, fill missing reviews_per_month values
    bronze_df = bronze_df.withColumn("last_review_date", to_date(col("last_review"), "yyyy-MM-dd"))
    bronze_df = bronze_df.fillna({"last_review_date": min_last_review, "reviews_per_month": 0}).drop('last_review')

    # Drop rows with missing latitude and longitude
    bronze_df = bronze_df.dropna(subset=['latitude', 'longitude'])

    silver_table_path = "/Volumes/my_homework/default/test/silver_table/"

    # Write the transformed data to silver delta table
    silver_query = (bronze_df.writeStream
                    .format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", silver_table_path + "/_checkpoint_silver")
                    .trigger(processingTime="5 minutes")
                    .table("silver_table"))
        
except Exception as e:
    print(f"An error occurred: {e}")
