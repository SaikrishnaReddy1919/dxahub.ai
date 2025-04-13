# Databricks notebook source
# COMMAND ----------
# %md
# # International Football Data ETL
# This notebook performs an ETL process on international football data, including data loading, cleaning, transformation, and writing to a Unity Catalog target table.

# COMMAND ----------
# %python
# Import necessary libraries
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, when, sum as _sum, expr, regexp_extract, row_number, col, lit
from pyspark.sql.window import Window

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Assume the Spark session is pre-initialized as 'spark'

# COMMAND ----------
# %python
# Load CSV files into DataFrames
def load_data():
    try:
        logger.info("Loading CSV files into DataFrames")
        segment_df = spark.read.csv("/path/to/Segment.csv", header=True, inferSchema=True)
        goalscorers_df = spark.read.csv("/path/to/goalscorers.csv", header=True, inferSchema=True)
        competitions_df = spark.read.csv("/path/to/International Competitions.csv", header=True, inferSchema=True)
        results_df = spark.read.csv("/path/to/results.csv", header=True, inferSchema=True)
        return segment_df, goalscorers_df, competitions_df, results_df
    except Exception as e:
        logger.error(f"Error loading CSV files: {e}")
        raise

segment_df, goalscorers_df, competitions_df, results_df = load_data()

# COMMAND ----------
# %python
# Data Cleaning and Standardization
def clean_data(results_df):
    try:
        # Removing unnecessary columns to streamline the dataset for analysis
        results_df = results_df.drop("date-1", "home_team-1", "away_team-1")
        return results_df
    except Exception as e:
        logger.error(f"Error during data cleaning: {e}")
        raise

results_df = clean_data(results_df)

# COMMAND ----------
# %python
# Split Segment Field
def split_segment_field(segment_df):
    try:
        # Splitting Segment field into Lower Bound and Upper Bound for better analysis granularity
        segment_df = segment_df.withColumn("Lower Bound", split(col("Segment"), "-").getItem(0).cast("int"))
        segment_df = segment_df.withColumn("Upper Bound", split(col("Segment"), "-").getItem(1).cast("int"))
        return segment_df
    except Exception as e:
        logger.error(f"Error splitting Segment field: {e}")
        raise

segment_df = split_segment_field(segment_df)

# COMMAND ----------
# %python
# Remap Null Values
def remap_null_values(segment_df):
    try:
        # Remapping null values to default bounds to ensure complete segment coverage
        segment_df = segment_df.withColumn("Lower Bound", when(segment_df["Lower Bound"].isNull(), 90).otherwise(segment_df["Lower Bound"]))
        segment_df = segment_df.withColumn("Upper Bound", when(segment_df["Upper Bound"].isNull(), 180).otherwise(segment_df["Upper Bound"]))
        return segment_df
    except Exception as e:
        logger.error(f"Error remapping null values: {e}")
        raise

segment_df = remap_null_values(segment_df)

# COMMAND ----------
# %python
# Join Datasets
def join_datasets(goalscorers_df, segment_df):
    try:
        # Joining datasets based on minute and segment bounds to consolidate scoring data
        join_condition = (col("minute") >= col("Lower Bound")) & (col("minute") < col("Upper Bound"))
        joined_df = goalscorers_df.join(segment_df, join_condition, "inner")
        return joined_df
    except Exception as e:
        logger.error(f"Error joining datasets: {e}")
        raise

joined_df = join_datasets(goalscorers_df, segment_df)

# COMMAND ----------
# %python
# Aggregate Data
def aggregate_data(joined_df):
    try:
        # Aggregating data to calculate total goals and expected goals per segment and competition
        aggregated_df = joined_df.groupBy("Competition", "Segment", "Decade").agg(
            _sum("Goals").alias("Total Goals"),
            _sum("Expected Goals").alias("Expected number of Goals")
        )
        return aggregated_df
    except Exception as e:
        logger.error(f"Error aggregating data: {e}")
        raise

aggregated_df = aggregate_data(joined_df)

# COMMAND ----------
# %python
# Custom Calculations
def custom_calculations(results_df, aggregated_df):
    try:
        # Extracting football association from tournament name for additional insights
        results_df = results_df.withColumn("Football Association", regexp_extract("tournament", "([A-Za-z]+)", 1))
        # Calculating expected goals per match for performance analysis
        aggregated_df = aggregated_df.withColumn("Expected number of Goals", (aggregated_df["Total Goals"] / aggregated_df["Matches in a Decade per Competition"]).cast("int"))
        return results_df, aggregated_df
    except Exception as e:
        logger.error(f"Error in custom calculations: {e}")
        raise

results_df, aggregated_df = custom_calculations(results_df, aggregated_df)

# COMMAND ----------
# %python
# Filter and Update Data
def filter_and_update_data(aggregated_df):
    try:
        # Filtering data to focus on relevant decades and updating competition names for consistency
        filtered_df = aggregated_df.filter(aggregated_df["Decade"] >= 1950)
        filtered_df = filtered_df.withColumn("Competition", when(filtered_df["Competition"] == "African", "Africa").otherwise(filtered_df["Competition"]))
        return filtered_df
    except Exception as e:
        logger.error(f"Error filtering and updating data: {e}")
        raise

filtered_df = filter_and_update_data(aggregated_df)

# COMMAND ----------
# %python
# Assign Unique Match IDs
def assign_unique_match_ids(results_df):
    try:
        # Assigning unique IDs to matches for better tracking and analysis
        window_spec = Window.partitionBy("date", "home_team").orderBy("date")
        results_df = results_df.withColumn("Match ID", row_number().over(window_spec))
        return results_df
    except Exception as e:
        logger.error(f"Error assigning Match IDs: {e}")
        raise

results_df = assign_unique_match_ids(results_df)

# COMMAND ----------
# %python
# Define the final schema using select
def define_final_schema(filtered_df):
    try:
        # Selecting relevant columns for the final output schema
        final_df = filtered_df.select("Competition", "Segment", "Decade", "Total Goals", "Expected number of Goals")
        return final_df
    except Exception as e:
        logger.error(f"Error defining final schema: {e}")
        raise

final_df = define_final_schema(filtered_df)

# COMMAND ----------
# %python
# Write to Unity Catalog Target Table
def write_to_unity_catalog(final_df):
    try:
        logger.info("Writing final DataFrame to Unity Catalog target table")
        final_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.international_football_output")
    except Exception as e:
        logger.error(f"Error writing to Unity Catalog: {e}")
        raise

write_to_unity_catalog(final_df)

logger.info("ETL process completed successfully")
