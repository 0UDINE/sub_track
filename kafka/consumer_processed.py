"""
Kafka Consumer for Subscription Management System
Consumes processed data from Kafka topics and loads into SQL Server
"""
import json
import logging
import pyodbc
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time
from typing import Dict, List, Any, Optional
import argparse
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SQLServerManager:
    """Manages SQL Server database connections and operations"""

    def __init__(self, server: str, database: str, username: str = None, password: str = None,
                 trusted_connection: bool = True):
        """Initialize SQL Server connection"""
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.connection = None

        # Table mapping from Kafka topics to SQL Server tables
        self.table_mapping = {
            'subscription_user': 'Users',
            'subscription_utilisateur_personnel': 'UtilisateurPersonnel',
            'subscription_admin': 'Admins',
            'subscription_utilisateur_fournisseur': 'UtilisateurFournisseur',
            'subscription_fournisseur': 'Fournisseurs',
            'subscription_notifications': 'Notifications',
            'subscription_journal': 'Journal',
            'subscription_abonnement': 'Abonnements',
            'subscription_paiement': 'Paiements',
            'subscription_methodepaiement': 'MethodesPaiement',
            'subscription_utilisateurnotif': 'UtilisateurNotifications',
            'subscription_utilisateur_abonement': 'UtilisateurAbonnements'
        }

    def connect(self) -> bool:
        """Establish connection to SQL Server"""
        try:
            if self.trusted_connection:
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"Trusted_Connection=yes;"
                )
            else:
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                )

            self.connection = pyodbc.connect(connection_string)
            self.connection.autocommit = False
            logger.info(f"Connected to SQL Server: {self.server}/{self.database}")
            return True

        except Exception as e:
            logger.error(f"Error connecting to SQL Server: {e}")
            return False

    def get_table_name(self, topic_name: str) -> str:
        """Get SQL Server table name from Kafka topic name"""
        return self.table_mapping.get(topic_name, topic_name.replace('subscription_', ''))

    def create_table_if_not_exists(self, table_name: str, sample_data: Dict[str, Any]) -> bool:
        """Create table if it doesn't exist based on sample data structure"""
        try:
            cursor = self.connection.cursor()

            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """, table_name)

            if cursor.fetchone()[0] > 0:
                logger.debug(f"Table {table_name} already exists")
                return True

            # Generate CREATE TABLE statement
            columns = []
            data_sample = sample_data.get('data', {})

            for column_name, value in data_sample.items():
                sql_type = self._get_sql_type(value)
                columns.append(f"[{column_name}] {sql_type}")

            # Add metadata columns
            columns.extend([
                "[kafka_topic] NVARCHAR(255)",
                "[kafka_partition] INT",
                "[kafka_offset] BIGINT",
                "[processed_timestamp] DATETIME2 DEFAULT GETDATE()"
            ])

            create_sql = f"CREATE TABLE [{table_name}] ({', '.join(columns)})"

            cursor.execute(create_sql)
            self.connection.commit()
            logger.info(f"Created table: {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            self.connection.rollback()
            return False

    def _get_sql_type(self, value) -> str:
        """Determine SQL Server data type based on Python value"""
        if value is None:
            return "NVARCHAR(MAX)"
        elif isinstance(value, bool):
            return "BIT"
        elif isinstance(value, int):
            return "BIGINT"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            if len(value) <= 255:
                return "NVARCHAR(255)"
            else:
                return "NVARCHAR(MAX)"
        else:
            return "NVARCHAR(MAX)"

    def insert_batch(self, table_name: str, records: List[Dict[str, Any]],
                     topic_info: Dict[str, Any]) -> bool:
        """Insert a batch of records into SQL Server table"""
        try:
            if not records:
                return True

            cursor = self.connection.cursor()
            sql_table_name = self.get_table_name(table_name)

            # Create table if it doesn't exist
            if not self.create_table_if_not_exists(sql_table_name, records[0]):
                return False

            # Prepare INSERT statement
            first_record = records[0].get('data', {})
            columns = list(first_record.keys()) + ['kafka_topic', 'kafka_partition', 'kafka_offset']
            placeholders = ', '.join(['?' for _ in columns])

            insert_sql = f"""
                INSERT INTO [{sql_table_name}] ({', '.join([f'[{col}]' for col in columns])})
                VALUES ({placeholders})
            """

            # Prepare data for batch insert
            batch_data = []
            for record in records:
                data = record.get('data', {})
                row_data = [data.get(col) for col in first_record.keys()]
                row_data.extend([
                    topic_info.get('topic'),
                    topic_info.get('partition'),
                    topic_info.get('offset')
                ])
                batch_data.append(tuple(row_data))

            # Execute batch insert
            cursor.executemany(insert_sql, batch_data)
            self.connection.commit()

            logger.info(f"Inserted {len(records)} records into {sql_table_name}")
            return True

        except Exception as e:
            logger.error(f"Error inserting batch into {sql_table_name}: {e}")
            self.connection.rollback()
            return False

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("SQL Server connection closed")


class SubscriptionDataConsumer:
    """Kafka consumer for subscription management data"""

    def __init__(self, bootstrap_servers='localhost:9092', group_id='subscription-consumer',
                 topics=None, sql_manager: SQLServerManager = None):
        """Initialize Kafka consumer"""
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.sql_manager = sql_manager
        self.running = False

        # Default topics to consume
        if topics is None:
            self.topics = [
                'subscription_user', 'subscription_utilisateur_personnel', 'subscription_admin',
                'subscription_utilisateur_fournisseur', 'subscription_fournisseur',
                'subscription_notifications', 'subscription_journal', 'subscription_abonnement',
                'subscription_paiement', 'subscription_methodepaiement', 'subscription_utilisateurnotif',
                'subscription_utilisateur_abonement'
            ]
        else:
            self.topics = topics

        # Configure consumer
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100,
            consumer_timeout_ms=10000
        )

        logger.info(f"Kafka consumer initialized for topics: {self.topics}")

        # Batch processing
        self.batch_size = 50
        self.batch_timeout = 5  # seconds
        self.message_batches = {}  # topic -> list of messages
        self.last_batch_time = {}  # topic -> timestamp

    def start_consuming(self):
        """Start consuming messages from Kafka topics"""
        self.running = True
        logger.info("Starting Kafka consumer...")

        try:
            while self.running:
                try:
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000)

                    if message_batch:
                        self.process_message_batch(message_batch)

                    # Process accumulated batches if timeout reached
                    self.process_timed_out_batches()

                except Exception as e:
                    logger.error(f"Error in consumer loop: {e}")
                    time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            self.stop_consuming()

    def process_message_batch(self, message_batch):
        """Process a batch of messages from Kafka"""
        for topic_partition, messages in message_batch.items():
            topic = topic_partition.topic

            # Initialize batch for topic if needed
            if topic not in self.message_batches:
                self.message_batches[topic] = []
                self.last_batch_time[topic] = time.time()

            # Add messages to batch
            for message in messages:
                try:
                    self.message_batches[topic].append({
                        'data': message.value,
                        'topic_info': {
                            'topic': topic,
                            'partition': message.partition,
                            'offset': message.offset,
                            'timestamp': message.timestamp
                        }
                    })
                except Exception as e:
                    logger.error(f"Error processing message from {topic}: {e}")

            # Process batch if it's full
            if len(self.message_batches[topic]) >= self.batch_size:
                self.process_topic_batch(topic)

    def process_timed_out_batches(self):
        """Process batches that have timed out"""
        current_time = time.time()

        for topic in list(self.message_batches.keys()):
            if (topic in self.last_batch_time and
                    current_time - self.last_batch_time[topic] > self.batch_timeout and
                    len(self.message_batches[topic]) > 0):
                self.process_topic_batch(topic)

    def process_topic_batch(self, topic: str):
        """Process accumulated messages for a specific topic"""
        try:
            batch = self.message_batches[topic]
            if not batch:
                return

            logger.info(f"Processing batch of {len(batch)} messages for topic: {topic}")

            # Extract records and topic info
            records = [msg['data'] for msg in batch]
            topic_info = batch[0]['topic_info']  # Use first message's info

            # Insert into SQL Server
            if self.sql_manager:
                success = self.sql_manager.insert_batch(topic, records, topic_info)

                if success:
                    # Commit Kafka offsets
                    self.consumer.commit()
                    logger.info(f"Successfully processed batch for {topic}")
                else:
                    logger.error(f"Failed to insert batch for {topic}")

            # Clear processed batch
            self.message_batches[topic] = []
            self.last_batch_time[topic] = time.time()

        except Exception as e:
            logger.error(f"Error processing batch for {topic}: {e}")

    def stop_consuming(self):
        """Stop the consumer gracefully"""
        self.running = False

        # Process any remaining batches
        for topic in list(self.message_batches.keys()):
            if len(self.message_batches[topic]) > 0:
                self.process_topic_batch(topic)

        self.consumer.close()
        logger.info("Kafka consumer stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal")
    sys.exit(0)


def main():
    """Main function to consume data from Kafka and load to SQL Server"""
    parser = argparse.ArgumentParser(description='Consume subscription data from Kafka and load to SQL Server')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--group-id', default='subscription-consumer',
                        help='Kafka consumer group ID')
    parser.add_argument('--sql-server', required=True,
                        help='SQL Server instance (e.g., localhost\\SQLEXPRESS)')
    parser.add_argument('--database', required=True,
                        help='SQL Server database name')
    parser.add_argument('--username', help='SQL Server username (optional if using trusted connection)')
    parser.add_argument('--password', help='SQL Server password (optional if using trusted connection)')
    parser.add_argument('--trusted-connection', action='store_true', default=True,
                        help='Use Windows authentication')

    args = parser.parse_args()

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initialize SQL Server manager
    sql_manager = SQLServerManager(
        server=args.sql_server,
        database=args.database,
        username=args.username,
        password=args.password,
        trusted_connection=args.trusted_connection
    )

    if not sql_manager.connect():
        logger.error("Failed to connect to SQL Server. Exiting.")
        return

    # Initialize consumer
    consumer = SubscriptionDataConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        sql_manager=sql_manager
    )

    try:
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        sql_manager.close()


if __name__ == "__main__":
    main()