import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import logging
import sys
import signal
import os

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Money Laundering Detection 1.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define Schema for trans
schema = StructType([
    StructField("Amount", FloatType(), True),
    StructField("Payment_currency", StringType(), True),
    StructField("Received_currency", StringType(), True),
    StructField("Sender_bank_location", StringType(), True),
    StructField("Receiver_bank_location", StringType(), True),
    StructField("Payment_type", StringType(), True),
    StructField("Same_Location", IntegerType(), True),
    StructField("Hour", IntegerType(), True),
    StructField("Sender_transaction_count", IntegerType(), True),
    StructField("Receiver_transaction_count", IntegerType(), True),
     StructField("currency_match", IntegerType(), True),
    StructField("Is_laundering", IntegerType(), True),
])


OUTPUT_TOPIC = 'suspiciousTransactions'

# Read Kafka Stream
try:
    trans = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions0") \
        .option("failOnDataLoss", "false") \
        .load()
except Exception as e:
    logger.error(f"Failed to read from Kafka: {e}")
    sys.exit(1)


# Parse the Data
parsed_trans = trans \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")


# Define Detection Logic
def detect_suspicious(df):
    return df.filter(
        (col("Is_laundering") == 1)
    )

# Function to log batch details
def log_batch(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id}")
    logger.info(f"Number of rows in batch: {batch_df.count()}")

def write_to_kafka(batch_df, batch_id):
    log_batch(batch_df, batch_id)
    logger.info(f"Writing batch {batch_id} to kafka.")

    try:
          # This needs to be done because, the value which is being pushed must be a string or bytes and not a DataFrame
          batch_df.select(to_json(struct("*")).alias("value")) \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", OUTPUT_TOPIC) \
                .save()

    except Exception as e:
        logger.error(f"Error writing to Kafka {e}")

# Apply Suspicious Transaction Detection
suspicious_trans = detect_suspicious(parsed_trans)

# Write Suspicious transactions to Parquet and log batches
checkpoint_path = os.path.join(os.path.dirname(__file__), "spark-checkpoints")
# This will create a directory called 'spark-checkpoints' in the same folder where the python script is located.

query = suspicious_trans \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_kafka) \
    .option("checkpointLocation", checkpoint_path) \
    .start()


# add graceful shutdown
def signal_handler(sig, frame):
    print('You pressed Ctrl+C! Shutting Down')
    query.stop()
    print("Query Stopped")
    print("Exiting Program")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# keep the main thread alive
query.awaitTermination()