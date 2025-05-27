"""
Kafka Producer for Subscription Data
Reads the generated raw data and sends it to a Kafka topic
"""
import json
import time
import argparse
from pathlib import Path
from confluent_kafka import Producer
from typing import Dict, List, Any


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def read_data_from_json(filepath: str) -> Dict[str, List[Dict[str, Any]]]:
    """Read the raw data from the JSON file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Successfully loaded data from {filepath}")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return {}


def produce_messages(producer: Producer, topic: str, data: Dict[str, List[Dict[str, Any]]],
                     delay: float = 0.01):
    """Produce messages to the Kafka topic"""
    message_count = 0

    # Process each table
    for table_name, records in data.items():
        print(f"Processing {len(records)} records for {table_name}")

        # Send each record to Kafka with table name in the message
        for record in records:
            # Create a message with table name and record data
            message = {
                'table': table_name,
                'data': record
            }

            # Convert message to JSON
            message_json = json.dumps(message, ensure_ascii=False)

            # Send message to Kafka
            producer.produce(
                topic,
                key=table_name,  # Use table name as the key
                value=message_json.encode('utf-8'),  # Message value
                callback=delivery_report  # Callback function
            )

            # Increment message count
            message_count += 1

            # Small delay to avoid overwhelming the broker
            time.sleep(delay)

            # Trigger any available delivery report callbacks
            producer.poll(0)

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()

    print(f"Produced {message_count} messages to topic {topic}")
    return message_count


def main():
    parser = argparse.ArgumentParser(description='Kafka Producer for Subscription Data')
    parser.add_argument('--input', type=str, default=r'C:\Users\MaxHeap\PycharmProjects\subscription_pipeline\raw_data_output.json',
                        help='Input JSON file with raw data')
    parser.add_argument('--topic', type=str, default='subscriptions_raw',
                        help='Kafka topic to produce messages to')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--delay', type=float, default=0.01,
                        help='Delay between messages in seconds')

    args = parser.parse_args()

    # Verify the input file exists
    if not Path(args.input).is_file():
        print(f"Error: Input file {args.input} does not exist")
        return

    # Configure the producer
    producer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'subscription-producer'
    }

    # Create the Kafka producer
    producer = Producer(producer_config)

    # Read the data from JSON
    data = read_data_from_json(args.input)

    if not data:
        print("No data to produce")
        return

    # Start producing messages
    try:
        message_count = produce_messages(producer, args.topic, data, args.delay)
        print(f"Successfully produced {message_count} messages")
    except KeyboardInterrupt:
        print("Producer interrupted")
    except Exception as e:
        print(f"Error producing messages: {e}")
    finally:
        # Make sure all messages are sent before exiting
        producer.flush()


if __name__ == "__main__":
    main()