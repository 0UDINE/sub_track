# Kafka Broker Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',  # Comma-separated for multiple brokers
    'group.id': 'subscription-group',
    'auto.offset.reset': 'earliest'
}

# Topic Names
TOPICS = {
    'raw': 'subscriptions_raw',
    'processed': 'subscriptions_processed_data',
    'errors': 'subscriptions_errors'
}

# Producer Configuration (additional settings)
PRODUCER_CONFIG = {
    'acks': 'all',
    'retries': 3,
    'compression.type': 'snappy'
}