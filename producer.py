import csv
import json
import time
import logging
from datetime import datetime  
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration for Professional Logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants/Settings
KAFKA_SERVER = 'kafka:9092'  # Kafka broker address (Docker service name)
TOPIC_NAME = 'er_arrivals'
CSV_FILE = 'Hospital_ER_Data.csv'
SLEEP_INTERVAL = 10  # Seconds between each patient arrival simulation

def initialize_producer():
    """
    Initializes the Kafka Producer with essential configurations.
    - bootstrap_servers: The address of the Kafka broker.
    - value_serializer: Converts Python dictionaries to JSON bytes for transmission.
    - retries: Number of times to retry sending if a transient error occurs.
    """
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
    except KafkaError as e:
        logger.error(f"Critical Error: Could not connect to Kafka: {e}")
        return None

def start_ingestion():
    """
    Main loop to read the CSV file and stream records to Kafka.
    Implements real-time simulation by sleeping between records.
    """
    producer = initialize_producer()
    if not producer:
        return

    logger.info(f"Ingestion Started: Reading data from {CSV_FILE}")

    try:
        while True:
            # Use utf-8-sig to handle potential Byte Order Mark (BOM) in CSV files
            with open(CSV_FILE, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # DATA CLEANING: Strip whitespace from keys and values to avoid Spark matching errors
                    clean_row = {str(k).strip(): str(v).strip() for k, v in row.items()}
                    
                    # --- ADDED LOGIC: Override historical date with current time for real-time simulation ---
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    # Assuming the column name in your CSV is 'Patient Admin Date' or similar. 
                    # If it has a slightly different name, change the string key below.
                    if 'Patient Admin Date' in clean_row:
                        clean_row['Patient Admin Date'] = current_time
                    elif 'Patient Admin Date ' in clean_row: # Just in case trailing space wasn't caught
                        clean_row['Patient Admin Date '] = current_time
                    # -------------------------------------------------------------------------------------

                    # ASYNCHRONOUS SEND: Push data to the Kafka Topic
                    producer.send(TOPIC_NAME, clean_row)
                    
                    patient_id = clean_row.get('Patient Id', 'Unknown')
                    logger.info(f"Streamed Patient Record: {patient_id} with time: {current_time}") # Added time to log for debugging
                    
                    # REAL-TIME SIMULATION: Wait before sending the next record
                    time.sleep(SLEEP_INTERVAL)
                
    except FileNotFoundError:
        logger.error(f"File System Error: {CSV_FILE} was not found in the directory.")
    except Exception as e:
        logger.error(f"Runtime Error during streaming: {e}")
    finally:
        # Ensure all messages in the buffer are sent before closing
        producer.flush()
        producer.close()
        logger.info("Producer shutdown complete.")

if __name__ == "__main__":
    start_ingestion()