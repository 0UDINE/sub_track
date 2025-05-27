import json
import logging
from confluent_kafka import Producer
from typing import Dict, Any
import time
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SubscriptionDataProducer:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka producer with configuration

        Args:
            config: Kafka producer configuration dictionary
        """
        self.producer = Producer(config)
        self.topic = "subscriptions_processed_data"
        self.delivery_success = False
        self.delivery_error = None

    def delivery_report(self, err, msg):
        """
        Delivery report callback for producer
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
            self.delivery_error = err
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
            self.delivery_success = True

    def send_data_batch(self, data_batch: Dict[str, Any], batch_id: str):
        """
        Send a batch of data to Kafka topic

        Args:
            data_batch: Dictionary containing different data types
            batch_id: Unique identifier for this batch
        """
        try:
            # Reset delivery status
            self.delivery_success = False
            self.delivery_error = None

            # Create message with metadata
            message = {
                "batch_id": batch_id,
                "timestamp": time.time(),
                "data": data_batch
            }

            # Convert to JSON string
            message_json = json.dumps(message, ensure_ascii=False)

            # Send to Kafka
            self.producer.produce(
                topic=self.topic,
                key=batch_id,
                value=message_json,
                callback=self.delivery_report
            )

            logger.info(f"Sent batch {batch_id} with {len(data_batch)} data types")

            # Poll for delivery reports with timeout
            max_polls = 100  # Maximum number of polls
            poll_count = 0

            while not self.delivery_success and self.delivery_error is None and poll_count < max_polls:
                self.producer.poll(0.1)  # Poll with 100ms timeout
                poll_count += 1
                time.sleep(0.01)  # Small delay between polls

            if self.delivery_error:
                raise Exception(f"Message delivery failed: {self.delivery_error}")
            elif not self.delivery_success:
                logger.warning("Delivery confirmation not received within timeout, but continuing...")

        except Exception as e:
            logger.error(f"Error sending batch {batch_id}: {str(e)}")
            raise

    def process_json_file(self, file_path: str):
        """
        Read JSON file and send data to Kafka in batches

        Args:
            file_path: Path to the JSON file
        """
        try:
            logger.info(f"Reading JSON file: {file_path}")

            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Generate batch ID based on timestamp
            batch_id = f"batch_{int(time.time())}"

            # Send the entire data structure as one batch
            self.send_data_batch(data, batch_id)

            # Wait for all messages to be delivered with timeout
            logger.info("Flushing remaining messages...")
            remaining_messages = self.producer.flush(timeout=30)  # 30 second timeout

            if remaining_messages > 0:
                logger.warning(f"{remaining_messages} messages were not delivered within timeout")
            else:
                logger.info("All messages successfully delivered")

            logger.info("Successfully processed and sent all data to Kafka")

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise

    def close(self):
        """
        Close the producer connection
        """
        try:
            # Final flush with timeout
            remaining = self.producer.flush(timeout=10)
            if remaining > 0:
                logger.warning(f"{remaining} messages not delivered before closing")
            logger.info("Producer connection closed")
        except Exception as e:
            logger.error(f"Error closing producer: {str(e)}")


def main():
    # Kafka producer configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'subscription-data-producer',
        'acks': '1',  # Changed from 'all' to '1' for single broker setup
        'retries': 3,
        'retry.backoff.ms': 1000,
        'delivery.timeout.ms': 30000,
        'request.timeout.ms': 30000
        # Removed idempotence and other advanced configs
    }

    # For SASL/SSL authentication (uncomment and configure if needed)
    # kafka_config.update({
    #     'security.protocol': 'SASL_SSL',
    #     'sasl.mechanisms': 'PLAIN',
    #     'sasl.username': 'your-username',
    #     'sasl.password': 'your-password'
    # })

    # JSON file path
    json_file_path = "cleaned_data_output.json"  # Update with your file path

    try:
        # Create producer instance
        producer = SubscriptionDataProducer(kafka_config)

        # Process and send data
        producer.process_json_file(json_file_path)

        # Close producer
        producer.close()

        logger.info("Data pipeline completed successfully")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()