# Databricks notebook source
# COMMAND ----------
# MAGIC %python
# Import necessary libraries
import logging
from pyspark.sql.functions import split, col, round, regexp_extract, year, countDistinct
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %python
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
# MAGIC %python
try:
    # Step 1: Data Ingestion
    # Load CSV files into DataFrames to prepare for transformation and analysis
    # This step ensures that all necessary data is available for processing
    logger.info("Loading CSV files into DataFrames")
    segment_df = spark.read.csv("/mnt/data/Segment.csv", header=True, inferSchema=True)
    goalscorers_df = spark.read.csv("/mnt/data/goalscorers.csv", header=True, inferSchema=True)
    competitions_df = spark.read.csv("/mnt/data/International Competitions.csv", header=True, inferSchema=True)
    results_df = spark.read.csv("/mnt/data/results.csv", header=True, inferSchema=True)

# COMMAND ----------
# MAGIC %python
    # Step 2: Data Cleaning and Standardization
    # Remove unnecessary fields to streamline the dataset for analysis
    logger.info("Cleaning and standardizing data")
    results_df = results_df.drop("date-1", "home_team-1", "away_team-1")

# COMMAND ----------
# MAGIC %python
    # Step 3: Data Integration
    # Integrate datasets by splitting segments and joining on relevant conditions
    logger.info("Integrating datasets")
    segment_df = segment_df.withColumn("Lower Bound", split(col("Segment"), "-")[0].cast("integer"))
    segment_df = segment_df.withColumn("Upper Bound", split(col("Segment"), "-")[1].cast("integer"))

    join_condition = (col("minute") >= col("Lower Bound")) & (col("minute") < col("Upper Bound"))
    joined_df = goalscorers_df.join(segment_df, join_condition, "inner")

# COMMAND ----------
# MAGIC %python
    # Step 4: Custom Calculations
    # Perform custom calculations to derive insights such as expected goals and football associations
    logger.info("Performing custom calculations")
    joined_df = joined_df.withColumn("Expected number of Goals", round(joined_df["Total Goals"] / joined_df["Matches in a Decade per Competition"], 2))
    joined_df = joined_df.withColumn("Football Association", regexp_extract(joined_df["tournament"], '([A-Z]{2,}).*', 1))
    joined_df = joined_df.withColumn("Decade", (year(joined_df["date"]) / 10).cast("integer") * 10)

# COMMAND ----------
# MAGIC %python
    # Step 5: Aggregation and Analysis
    # Aggregate data to count matches per decade and competition for trend analysis
    logger.info("Aggregating data for analysis")
    aggregated_df = joined_df.groupBy("Decade", "Competition").agg(countDistinct("Match ID").alias("Matches in a Decade per Competition"))

# COMMAND ----------
# MAGIC %python
    # Step 6: Business Rules Application
    # Apply business rules to filter out irrelevant data and focus on main tournament matches
    logger.info("Applying business rules")
    filtered_df = joined_df.filter(joined_df["tournament"] != "qualification") \
                           .filter((joined_df["Decade"] >= 1950) & (joined_df["Decade"] <= 2030)) \
                           .select("Decade", "Competition", "Matches in a Decade per Competition", "Expected number of Goals", "Football Association")

# COMMAND ----------
# MAGIC %python
    # Step 7: Output Generation
    # Save the final output as a Delta table for efficient querying and versioning
    logger.info("Generating final output")
    filtered_df.write.format("delta").mode("overwrite").saveAsTable("catalog.target_db.international_football_output")

except Exception as e:
    logger.error("An error occurred during the ETL process", exc_info=True)
