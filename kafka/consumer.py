"""
Kafka Consumer for Subscription Data
Reads messages from Kafka topic and inserts them into SQL Server database
"""
import json
import time
import argparse
import pyodbc
from confluent_kafka import Consumer, KafkaError
from typing import Dict, Any, List

# Define SQL table names and their corresponding columns
TABLE_COLUMNS = {
    'raw_User': ['userId', 'email', 'motsDePasse', 'rôle', 'date_extraction', 'source_system', 'data_quality_flag'],
    'raw_Utilisateur_personnel': ['userId', 'prenom', 'nom', 'telephone', 'adresse', 'date_extraction', 'source_system',
                                  'data_quality_notes'],
    'raw_Admin': ['userId', 'date_extraction', 'source_system'],
    'raw_Utilisateur_fournisseur': ['userId', 'responsableContractActivity', 'operationRespoOccupation', 'chefFactorId',
                                    'date_extraction', 'source_system'],
    'raw_Fournisseur': ['idFournisseur', 'nomFournisseur', 'emailFournisseur', 'numTele', 'address',
                        'secteurFournisseur', 'date_extraction', 'source_system'],
    'raw_Notifications': ['idNotification', 'typeNotification', 'dateEnvoi', 'typeEnvoi', 'contenu', 'date_extraction',
                          'source_system'],
    'raw_Journal': ['idJournal', 'userId', 'idAbonnement', 'descriptionJournal', 'typeActivite', 'dateCreationJournal',
                    'valeurPrécedente', 'nouvelleValeur', 'StatusActivite', 'Timestamps', 'date_extraction',
                    'source_system'],
    'raw_Abonnement': ['idAbonnement', 'idFournisseur', 'titreAbonnement', 'descriptionAbonnement', 'prixAbonnement',
                       'categorieAbonnement', 'date_extraction', 'source_system'],
    'raw_Paiement': ['idPaiement', 'idAbonnement', 'userId', 'methodePaiement', 'datePaiement', 'montantPaiement',
                     'motifPaiement', 'statusPaiement', 'date_extraction', 'source_system'],
    'raw_MethodePaiement': ['idPaiement', 'userId', 'informationCarte', 'dateExpirementCarte', 'typeMethode',
                            'date_extraction', 'source_system'],
    'raw_UtilisateurNotif': ['userId', 'idNotification', 'estLu', 'estUrgent', 'date_extraction', 'source_system'],
    'raw_utilisateur_abonement': ['userId', 'idAbonnement', 'typeFacturation', 'statutAbonnement', 'dateRenouvelement',
                                  'dateDebutFacturation', 'prochaineDeFacturation', 'date_extraction', 'source_system']
}

# SQL table name mapping from Python names to SQL Server names
SQL_TABLE_MAPPING = {
    'raw_User': 'raw_User',
    'raw_Utilisateur_personnel': 'raw_Utilisateur_personnel',
    'raw_Admin': 'raw_Admin',
    'raw_Utilisateur_fournisseur': 'raw_Utilisateur_fournisseur',
    'raw_Fournisseur': 'raw_Fournisseur',
    'raw_Notifications': 'raw_Notifications',
    'raw_Journal': 'raw_Journal',
    'raw_Abonnement': 'raw_Abonnement',
    'raw_Paiement': 'raw_Paiement',
    'raw_MethodePaiement': 'raw_MethodePaiement',
    'raw_UtilisateurNotif': 'raw_UtilisateurNotif',
    'raw_utilisateur_abonement': 'raw_utilisateur_abonement'
}

# Column name mapping for specific tables
# This handles cases where column names in the generated data differ from SQL table
COLUMN_MAPPING = {
    'raw_Fournisseur': {
        'address': 'addressFournisseur'
    }
}


def create_db_connection(server: str, database: str, trusted_connection: bool = True) -> pyodbc.Connection:
    """Create a connection to the SQL Server database"""
    if trusted_connection:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        # If you need to use username/password
        # Add username and password parameters and use them here
        raise ValueError("Username/password auth not implemented")

    try:
        conn = pyodbc.connect(conn_str)
        print(f"Connected to database {database} on server {server}")
        return conn
    except pyodbc.Error as e:
        print(f"Database connection error: {e}")
        raise e


def prepare_insert_statement(table_name: str, data: Dict[str, Any]) -> tuple:
    """Prepare SQL INSERT statement based on table and data"""
    # Get the SQL table name
    sql_table_name = SQL_TABLE_MAPPING.get(table_name, table_name)

    # Get columns for this table
    if table_name not in TABLE_COLUMNS:
        print(f"Warning: Unknown table {table_name}")
        # Extract columns from data
        columns = list(data.keys())
        if 'raw_id' in columns:
            columns.remove('raw_id')  # Let the database handle this
    else:
        columns = TABLE_COLUMNS[table_name]

    # Check which columns actually exist in the data
    existing_columns = []
    values = []
    for col in columns:
        # Check for column mapping
        mapped_col = col
        if table_name in COLUMN_MAPPING and col in COLUMN_MAPPING[table_name]:
            mapped_col = COLUMN_MAPPING[table_name][col]

        if mapped_col in data:
            existing_columns.append(col)
            values.append(data[mapped_col])
        elif col in data:
            existing_columns.append(col)
            values.append(data[col])

    # Prepare the SQL statement
    placeholders = '?' + ', ?' * (len(existing_columns) - 1)
    sql = f"INSERT INTO {sql_table_name} ({', '.join(existing_columns)}) VALUES ({placeholders})"

    return sql, values


def process_message(conn: pyodbc.Connection, message: Dict[str, Any]) -> bool:
    """Process a message by inserting it into the database"""
    try:
        table_name = message.get('table')
        data = message.get('data')

        if not table_name or not data:
            print("Invalid message format: missing table or data")
            return False

        # Prepare the SQL statement
        sql, values = prepare_insert_statement(table_name, data)

        # Execute the SQL statement
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()

        return True
    except Exception as e:
        print(f"Error processing message: {e}")
        # Try to rollback the transaction
        try:
            conn.rollback()
        except:
            pass
        return False


def consume_messages(consumer: Consumer, conn: pyodbc.Connection, timeout: float = 1.0,
                     max_messages: int = None, batch_size: int = 100) -> int:
    """Consume messages from Kafka and insert them into the database"""
    message_count = 0
    batch_count = 0

    try:
        running = True
        while running:
            # Poll for a message
            msg = consumer.poll(timeout)

            if msg is None:
                # No message received
                continue

            if msg.error():
                # Error handling
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    print(f"Reached end of partition: {msg.topic()}, {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}")
                continue

            # Process the message
            try:
                # Parse the message value as JSON
                message_json = json.loads(msg.value().decode('utf-8'))

                # Process the message
                if process_message(conn, message_json):
                    message_count += 1
                    batch_count += 1

                    # Commit offsets every batch_size messages
                    if batch_count >= batch_size:
                        consumer.commit(asynchronous=False)
                        print(f"Processed {batch_count} messages, total: {message_count}")
                        batch_count = 0

                    # Check if we've reached the maximum number of messages
                    if max_messages and message_count >= max_messages:
                        print(f"Reached maximum messages ({max_messages}), stopping")
                        running = False
            except json.JSONDecodeError as e:
                print(f"Error decoding message: {e}")
                continue
            except Exception as e:
                print(f"Error processing message: {e}")
                continue

    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        # Commit any remaining offsets
        if batch_count > 0:
            consumer.commit(asynchronous=False)
            print(f"Final commit: processed {batch_count} messages, total: {message_count}")

    return message_count


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer for Subscription Data')
    parser.add_argument('--topic', type=str, default='subscriptions_raw',
                        help='Kafka topic to consume messages from')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--group-id', type=str, default='subscription-consumer',
                        help='Consumer group ID')
    parser.add_argument('--sql-server', type=str, default='localhost',
                        help='SQL Server instance name')
    parser.add_argument('--database', type=str, default='Raw_Subscription_Data_DB',
                        help='SQL Server database name')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of messages to process before committing')
    parser.add_argument('--max-messages', type=int, default=None,
                        help='Maximum number of messages to consume (None for unlimited)')

    args = parser.parse_args()

    # Configure the consumer
    consumer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group_id,
        'auto.offset.reset': 'earliest',  # Start reading from the beginning of the topic
        'enable.auto.commit': False  # Manually commit after processing
    }

    # Create the Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to the topic
    consumer.subscribe([args.topic])
    print(f"Subscribed to topic: {args.topic}")

    # Create a database connection
    try:
        conn = create_db_connection(args.sql_server, args.database)

        # Start consuming messages
        message_count = consume_messages(consumer, conn, batch_size=args.batch_size, max_messages=args.max_messages)
        print(f"Successfully processed {message_count} messages")

        # Close the database connection
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the consumer
        consumer.close()


if __name__ == "__main__":
    main()