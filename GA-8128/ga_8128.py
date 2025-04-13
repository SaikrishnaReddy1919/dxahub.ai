# Databricks notebook source
# COMMAND ----------
# %md
# # ETL Workflow for International Football Data
# This notebook performs an ETL process on international football data using PySpark. The workflow includes data loading, cleaning, integration, aggregation, custom calculations, filtering, and output data generation.

# COMMAND ----------
# %python
# Import necessary libraries
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, round, regexp_replace, rank
from pyspark.sql.window import Window

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# %python
# Load data from Unity Catalog tables
def load_data():
    try:
        segment_df = spark.table("catalog.source_db.segment")
        goalscorers_df = spark.table("catalog.source_db.goalscorers")
        competitions_df = spark.table("catalog.source_db.international_competitions")
        results_df = spark.table("catalog.source_db.results")
        logger.info("Data loaded successfully from Unity Catalog tables.")
        return segment_df, goalscorers_df, competitions_df, results_df
    except Exception as e:
        logger.error(f"Error loading data from Unity Catalog tables: {e}")
        raise

segment_df, goalscorers_df, competitions_df, results_df = load_data()

# COMMAND ----------
# %python
# Data Cleaning and Standardization
def clean_and_standardize_data(segment_df, results_df):
    try:
        # Dropping unnecessary columns from results_df
        results_df = results_df.drop('date-1', 'home_team-1', 'away_team-1')
        
        # Standardizing segment_df by handling null values
        segment_df = segment_df.withColumn("Lower Bound", when(col("Segment").isNull(), 90).otherwise(col("Segment")))
        segment_df = segment_df.withColumn("Upper Bound", when(col("Segment").isNull(), 180).otherwise(col("Segment")))
        logger.info("Data cleaning and standardization completed.")
        return segment_df, results_df
    except Exception as e:
        logger.error(f"Error during data cleaning and standardization: {e}")
        raise

segment_df, results_df = clean_and_standardize_data(segment_df, results_df)

# COMMAND ----------
# %python
# Data Integration and Aggregation
def integrate_and_aggregate_data(segment_df, goalscorers_df):
    try:
        # Define join condition for clarity and maintainability
        join_condition = (goalscorers_df.minute >= segment_df["Lower Bound"]) & (goalscorers_df.minute < segment_df["Upper Bound"])
        joined_df = goalscorers_df.join(segment_df, join_condition, "inner")
        
        # Aggregating data to calculate total goals
        aggregated_df = joined_df.groupBy("Competition", "Segment", "Decade").agg(sum("Total Goals").alias("Total Goals"))
        aggregated_df = aggregated_df.select("Competition", "Segment", "Decade", "Total Goals")
        logger.info("Data integration and aggregation completed.")
        return aggregated_df
    except Exception as e:
        logger.error(f"Error during data integration and aggregation: {e}")
        raise

aggregated_df = integrate_and_aggregate_data(segment_df, goalscorers_df)

# COMMAND ----------
# %python
# Custom Calculations
def perform_custom_calculations(aggregated_df, results_df):
    try:
        # Calculating expected number of goals
        aggregated_df = aggregated_df.withColumn("Expected number of Goals", round(col("Total Goals") / col("Matches in a Decade per Competition"), 2))
        
        # Assigning unique Match ID using window function
        windowSpec = Window.partitionBy("date", "home_team").orderBy("date")
        results_df = results_df.withColumn("Match ID", rank().over(windowSpec))
        logger.info("Custom calculations completed.")
        return aggregated_df, results_df
    except Exception as e:
        logger.error(f"Error during custom calculations: {e}")
        raise

aggregated_df, results_df = perform_custom_calculations(aggregated_df, results_df)

# COMMAND ----------
# %python
# Filtering and Data Refinement
def filter_and_refine_data(aggregated_df, competitions_df):
    try:
        # Filtering data for decades from 1950 onwards
        filtered_df = aggregated_df.filter(col("Decade") >= 1950)
        
        # Updating competition names for consistency
        competitions_df = competitions_df.withColumn("Competition", regexp_replace(col("Competition"), "African", "Africa"))
        logger.info("Filtering and data refinement completed.")
        return filtered_df, competitions_df
    except Exception as e:
        logger.error(f"Error during filtering and data refinement: {e}")
        raise

filtered_df, competitions_df = filter_and_refine_data(aggregated_df, competitions_df)

# COMMAND ----------
# %python
# Output Data Generation
def generate_output_data(filtered_df):
    try:
        # Drop existing table if necessary to avoid conflicts
        spark.sql("DROP TABLE IF EXISTS catalog.target_db.international_football_output")
        
        # Write the final processed data to Unity Catalog table
        filtered_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.international_football_output")
        logger.info("Output data generation completed successfully.")
    except Exception as e:
        logger.error(f"Error during output data generation: {e}")
        raise

generate_output_data(filtered_df)

# COMMAND ----------
# %md
# ## Conclusion
# The ETL workflow for international football data has been completed successfully. The processed data is now available in the Unity Catalog for further analysis and reporting.
