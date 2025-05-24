"""
Kafka Producer for Subscription Management System
Sends processed/cleaned data to Kafka topics
"""
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
from typing import Dict, List, Any
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SubscriptionDataProducer:
    """Kafka producer for subscription management data"""

    def __init__(self, bootstrap_servers='localhost:9092', topic_prefix='subscription'):
        """Initialize Kafka producer"""
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix

        # Configure producer with proper serialization
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432,
            compression_type='gzip'
        )

        logger.info(f"Kafka producer initialized with servers: {bootstrap_servers}")

    def create_topic_name(self, table_name: str) -> str:
        """Create standardized topic name"""
        # Remove 'raw_' prefix and convert to lowercase
        clean_name = table_name.replace('raw_', '').lower()
        return f"{self.topic_prefix}_{clean_name}"

    def send_table_data(self, table_name: str, records: List[Dict[str, Any]]) -> bool:
        """Send all records from a table to its corresponding Kafka topic"""
        topic_name = self.create_topic_name(table_name)

        try:
            logger.info(f"Sending {len(records)} records to topic: {topic_name}")

            # Send each record with a unique key
            futures = []
            for i, record in enumerate(records):
                # Create a unique key for each record
                key = f"{table_name}_{record.get('raw_id', i)}"

                # Add metadata to the record
                enriched_record = {
                    'table_name': table_name,
                    'record_id': record.get('raw_id', i),
                    'timestamp': int(time.time() * 1000),  # Unix timestamp in milliseconds
                    'data': record
                }

                # Send to Kafka
                future = self.producer.send(topic_name, key=key, value=enriched_record)
                futures.append(future)

            # Wait for all messages to be sent
            for future in futures:
                try:
                    record_metadata = future.get(timeout=30)
                    logger.debug(
                        f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                except KafkaError as e:
                    logger.error(f"Failed to send message: {e}")
                    return False

            logger.info(f"Successfully sent all {len(records)} records to {topic_name}")
            return True

        except Exception as e:
            logger.error(f"Error sending data to topic {topic_name}: {e}")
            return False

    def send_all_tables(self, cleaned_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, bool]:
        """Send all tables to their respective Kafka topics"""
        results = {}

        for table_name, records in cleaned_data.items():
            if records:  # Only send non-empty tables
                success = self.send_table_data(table_name, records)
                results[table_name] = success
            else:
                logger.warning(f"Skipping empty table: {table_name}")
                results[table_name] = True

        return results

    def send_batch_data(self, cleaned_data: Dict[str, List[Dict[str, Any]]], batch_size: int = 100):
        """Send data in batches to manage memory and throughput"""
        for table_name, records in cleaned_data.items():
            if not records:
                continue

            logger.info(f"Processing {table_name} with {len(records)} records in batches of {batch_size}")

            # Split records into batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(records) + batch_size - 1) // batch_size

                logger.info(f"Sending batch {batch_num}/{total_batches} for {table_name}")

                success = self.send_table_data(f"{table_name}_batch_{batch_num}", batch)
                if not success:
                    logger.error(f"Failed to send batch {batch_num} for {table_name}")

                # Small delay between batches
                time.sleep(0.1)

    def close(self):
        """Close the producer and flush any remaining messages"""
        try:
            self.producer.flush(timeout=30)
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


def load_cleaned_data(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load cleaned data from JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"Loaded cleaned data from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {e}")
        return {}


def main():
    """Main function to send cleaned data to Kafka"""
    parser = argparse.ArgumentParser(description='Send cleaned subscription data to Kafka')
    parser.add_argument('--input-file', default='cleaned_data_output.json',
                        help='Path to cleaned data JSON file')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic-prefix', default='subscription',
                        help='Prefix for Kafka topics')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Batch size for sending data')
    parser.add_argument('--use-batches', action='store_true',
                        help='Send data in batches instead of all at once')

    args = parser.parse_args()

    # Load cleaned data
    cleaned_data = load_cleaned_data(args.input_file)
    if not cleaned_data:
        logger.error("No data to send. Exiting.")
        return

    # Initialize producer
    producer = SubscriptionDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic_prefix=args.topic_prefix
    )

    try:
        if args.use_batches:
            # Send data in batches
            producer.send_batch_data(cleaned_data, args.batch_size)
        else:
            # Send all data at once
            results = producer.send_all_tables(cleaned_data)

            # Print summary
            logger.info("\nSending Summary:")
            for table_name, success in results.items():
                status = "✓ SUCCESS" if success else "✗ FAILED"
                record_count = len(cleaned_data[table_name])
                logger.info(f"{table_name}: {record_count} records - {status}")

        logger.info("Data sending completed!")

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()