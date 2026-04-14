from datetime import datetime, timedelta, timezone
import logging
import time

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lower, trim, coalesce, lit

# --- PROFESSIONAL LOGGING CONFIGURATION ---
# Using standard logging to monitor the Spark execution flow and data retrieval.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION & CONSTANTS ---
# Centralizing database and collection names for consistent integration.
MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "hospital_db"
COLLECTION_NAME = "er_patients"

# Defined departments based on Project Syllabus requirements.
TARGET_DEPARTMENTS = ["Orthopedics", "Cardiology", "Neurology", "Psychiatry"]

def run_hospital_analytics():
    """
    Core Processing Logic: 
    1. Connects to MongoDB to fetch the most recent data.
    2. Filters for records strictly from the last 60 minutes (Real-time window).
    3. Leverages Spark to compute mandatory KPIs (Arrivals, Discharge Rate, Averages).
    """
    spark = None
    mongo_client = None

    try:
        # 1. INITIALIZE SPARK SESSION
        # Setting up the Spark engine for data processing.
        spark = SparkSession.builder.appName("ER_Last_Hour_Analytics").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark Analytics Engine Started.")

        # 2. CONNECT TO DATA ARCHIVE (MONGODB)
        mongo_client = MongoClient(MONGO_URI)
        collection = mongo_client[DB_NAME][COLLECTION_NAME]

        # Calculate the 1-hour time threshold for the analytics window.
        last_hour_threshold = datetime.now(timezone.utc) - timedelta(hours=1)

        # Pull only last-hour records from MongoDB (Optimized query).
        records = list(collection.find(
            {"stored_at": {"$gte": last_hour_threshold}},
            {"_id": 0}
        ))

        # Check if any data exists in the current window to avoid processing errors.
        if not records:
            logger.info("=" * 60)
            logger.info("HOSPITAL ER ANALYTICS REPORT - LAST HOUR")
            logger.info("=" * 60)
            logger.info("No patient records found for the last hour.")
            logger.info("=" * 60)
            return

        # 3. CREATE SPARK DATAFRAME
        # Converting raw MongoDB records into a distributed DataFrame for analysis.
        df = spark.createDataFrame(records)

        # DATA CLEANING: Strip hidden whitespaces from column names.
        for column_name in df.columns:
            clean_name = column_name.strip()
            if clean_name != column_name:
                df = df.withColumnRenamed(column_name, clean_name)

        # 4. TYPE CONVERSION & STANDARDIZATION
        # Ensuring numeric values and flags are in the correct format for calculation.
        df = df.withColumn(
            "wait_time",
            coalesce(col("Patient Waittime").cast("double"), lit(0.0))
        ).withColumn(
            "age",
            coalesce(col("Patient Age").cast("double"), lit(0.0))
        ).withColumn(
            "admission_flag",
            lower(trim(col("Patient Admission Flag")))
        ).withColumn(
            "department_referral_clean",
            trim(coalesce(col("Department Referral"), lit("Unknown")))
        )

        # 5. COMPUTE KPIs (Key Performance Indicators)
        # Calculating the specific metrics requested in Section D of the project outline.
        total_arrivals = df.count()
        discharged_count = df.filter(col("admission_flag") == "false").count()
        admitted_count = df.filter(col("admission_flag") == "true").count()

        avg_wait = df.agg(avg("wait_time").alias("avg_wait")).collect()[0]["avg_wait"] or 0.0
        avg_age = df.agg(avg("age").alias("avg_age")).collect()[0]["avg_age"] or 0.0

        # Department Workload Breakdown for specific target departments.
        dept_counts = (
            df.filter(col("department_referral_clean").isin(TARGET_DEPARTMENTS))
              .groupBy("department_referral_clean")
              .count()
              .collect()
        )

        # Map results to a dictionary for professional display.
        dept_map = {dept: 0 for dept in TARGET_DEPARTMENTS}
        for row in dept_counts:
            dept_map[row["department_referral_clean"]] = row["count"]

        # 6. OUTPUT RESULTS
        # Displaying the calculated metrics in the console/terminal as required.
        logger.info("=" * 60)
        logger.info("HOSPITAL ER ANALYTICS REPORT - LAST HOUR")
        logger.info("=" * 60)
        logger.info(f"Total arrivals (last hour):     {total_arrivals}")
        logger.info(f"Discharged patients:            {discharged_count}")
        logger.info(f"Admitted patients:              {admitted_count}")
        logger.info(f"Average wait time:              {avg_wait:.2f} minutes")
        logger.info(f"Average patient age:            {avg_age:.2f} years")
        logger.info("-" * 60)
        logger.info("Department referrals:")
        logger.info(f"Orthopedics:                    {dept_map['Orthopedics']}")
        logger.info(f"Cardiology:                     {dept_map['Cardiology']}")
        logger.info(f"Neurology:                      {dept_map['Neurology']}")
        logger.info(f"Psychiatry:                     {dept_map['Psychiatry']}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Spark Processing Error: {e}")
    finally:
        # 7. CLEANUP
        # Ensuring sessions and connections are closed properly.
        if mongo_client is not None:
            mongo_client.close()
        if spark is not None:
            spark.stop()
        logger.info("Spark Session Closed.")

if __name__ == "__main__":
    while True:
        run_hospital_analytics()
        time.sleep(60)