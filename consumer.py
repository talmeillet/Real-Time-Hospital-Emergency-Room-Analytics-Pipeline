import json
import redis
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from pymongo import MongoClient

# --- PROFESSIONAL LOGGING CONFIGURATION ---
# Using logging to track system flow and debug issues in the Docker environment.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION & CONSTANTS ---
# Centralizing connection details for easier maintenance and Docker networking.
KAFKA_SERVER = 'kafka:9092'
KAFKA_TOPIC = 'er_arrivals'
MONGO_URI = "mongodb://mongodb:27017/"
REDIS_HOST = 'redis'
REDIS_PORT = 6379

def run_consumer():
    """
    Main consumer loop: Reads patient records from Kafka, 
    archives them in MongoDB with timestamps, and updates Redis for the Live Dashboard.
    """
    mongo_client = None
    consumer = None

    try:
        # 1. INITIALIZE STORAGE CLIENTS
        # MongoDB: Used for long-term historical storage (NoSQL archive)
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["hospital_db"]
        collection = db["er_patients"]

        # Redis: Used for high-speed, real-time analytics for the Dashboard
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

        # 2. INITIALIZE KAFKA CONSUMER
        # 'earliest' ensures we process all messages in the stream from the start.
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        logger.info("Consumer is online. Waiting for patient data stream...")

        for message in consumer:
            patient_record = message.value

            # --- FEATURE ENHANCEMENT: BSON DATETIME ---
            # Storing real UTC datetime objects to allow Spark to perform 
            # efficient "Last Hour" time-window filtering.
            patient_record["stored_at"] = datetime.now(timezone.utc)

            # --- TASK 1: PERSISTENCE (MONGODB) ---
            # Save the raw event for permanent historical records.
            collection.insert_one(patient_record)

            # --- TASK 2: REAL-TIME ANALYTICS (REDIS) ---
            # Increment the total patient counter for the dashboard.
            r.incr("total_patients")

            # Standardize department names and handle empty/None values.
            department = (patient_record.get("Department Referral") or "Unknown").strip()
            if not department:
                department = "Unknown"
            r.hincrby("department_counts", department, 1)

            # Update running totals for real-time average calculation (Age & Wait Time).
            try:
                age = float(patient_record.get("Patient Age", 0) or 0)
                wait_time = float(patient_record.get("Patient Waittime", 0) or 0)
                r.incrbyfloat("total_age", age)
                r.incrbyfloat("total_wait", wait_time)
            except (ValueError, TypeError):
                logger.warning(
                    f"Data issue with Patient {patient_record.get('Patient Id')}: invalid numeric format."
                )

            # Track Admission vs Discharge status based on the Patient Admission Flag.
            admission_flag = str(patient_record.get("Patient Admission Flag", "")).strip().lower()
            if admission_flag == "true":
                r.incr("admitted_count")
            elif admission_flag == "false":
                r.incr("discharged_count")

            logger.info(
                f"Success: Patient {patient_record.get('Patient Id')} saved to Mongo and updated in Redis."
            )

    except Exception as e:
        logger.error(f"Critical System Failure: {e}")
    finally:
        # 3. CLEANUP
        # Ensuring all connections are closed properly to prevent resource leaks.
        if consumer is not None:
            consumer.close()
        if mongo_client is not None:
            mongo_client.close()
        logger.info("Consumer connections closed.")

if __name__ == "__main__":
    run_consumer()