# 📊 Subscription Data Pipeline

A beginner-friendly data engineering pipeline that demonstrates real-time data processing using Python, SQL Server, and Apache Kafka. This project ingests subscription data, processes it through a streaming pipeline, and stores results in a database.

## 🏗️ Architecture Overview

![Architecture Diagram](assets/images/architecture_diagram.png)

This project implements a comprehensive data engineering pipeline with the following components:

### Data Flow Architecture
```
Faker (Data Generation) → Kafka Producer → Kafka Topic → Kafka Consumer → SQL Server → Analytics
```

### Detailed Component Flow

1. **Data Generation Layer**
   - **Faker Library**: Generates realistic subscription data
   - **Kafka Producer**: Publishes raw data to Kafka topic "subscriptions_raw"

2. **Message Streaming Layer**
   - **Apache Kafka**: Handles real-time data streaming with topic partitioning
   - **Topic**: `subscriptions_raw` with 3 partitions for scalability

3. **Data Processing Layer**
   - **Kafka Consumer**: Reads data from Kafka topic
   - **SQLAlchemy + PyODBC**: Database connectivity and ORM operations
   - **Pandas**: Data transformation and processing

4. **Storage Layer**
   - **SQL Server Database**: Stores both raw and processed subscription data
   - **Tables**: 
     - `raw_subscriptions`: Raw ingested data
     - `processed_subscriptions`: Cleaned and transformed data

5. **Analytics Layer**
   - **SubTrack**: Business intelligence and analytics dashboard
   - **Data Consumption**: Reads processed data for reporting and insights

## 🛠️ Technology Stack

- **Data Generation**: Python Faker library
- **Message Streaming**: Apache Kafka
- **Database**: Microsoft SQL Server
- **Data Processing**: Pandas, SQLAlchemy
- **Database Connectivity**: PyODBC driver
- **Analytics**: SubTrack dashboard
- **Languages**: Python, SQL

## 🛠️ Prerequisites

Before getting started, ensure you have:
- Python 3.8 or higher installed
- SQL Server (or SQL Server Express) running
- Java 8 or higher (required for Kafka)
- Administrative privileges to install packages

## 🚀 Quick Start Guide

### Step 1: Environment Setup

Clone this repository and navigate to the project directory:
```bash
git clone <repository-url>cd subscription-pipeline
```

Create and activate a virtual environment (recommended):
```bash
python -m venv pipeline_env
# Windows
pipeline_env\Scripts\activate
# macOS/Linux
source pipeline_env/bin/activate
```

### Step 2: Install Dependencies

Install all required Python packages:
```bash
pip install -r requirements.txt
```

Or install individually:
```bash
pip install pandas faker numpy pathlib confluent-kafka pyodbc
```

### Step 3: Database Configuration

1. **Create the database** by running the SQL script:
   ```sql
   -- Run the script: Raw_Subscription_Data_DB.sql
   ```

2. **Update database connection settings** in your configuration file:
   ```python
   # Update connection string in database/config.py
   CONNECTION_STRING = "your_connection_string_here"
   ```

### Step 4: Apache Kafka Setup

1. **Download and extract Kafka**:
   - Download from: https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
   - Extract to your preferred directory

2. **Start Kafka services** (run each command in separate terminals):

   **Terminal 1 - Start ZooKeeper:**
   ```bash
   # Windows
   bin\windows\zookeeper-server-start.bat config\zookeeper.properties
   
   # macOS/Linux
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```

   **Terminal 2 - Start Kafka Server:**
   ```bash
   # Windows
   bin\windows\kafka-server-start.bat config\server.properties
   
   # macOS/Linux
   bin/kafka-server-start.sh config/server.properties
   ```

3. **Create the Kafka topic:**
   ```bash
   # Windows
   bin\windows\kafka-topics.bat --create --topic subscriptions_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   
   # macOS/Linux
   bin/kafka-topics.sh --create --topic subscriptions_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```

### Step 5: Run the Pipeline

Execute the following commands in separate terminals:

1. **Generate sample data:**
   ```bash
   python data_generation/faker_generator.py
   ```

2. **Start the Kafka producer:**
   ```bash
   python kafka/producer.py
   ```

3. **Start the Kafka consumer:**
   ```bash
   python kafka/consumer.py
   ```

## 📁 Project Structure

```
subscription_pipeline/
├── .venv/                     # Virtual environment
├── data_generation/
│   ├── __init__.py
│   └── faker_generator.py     # Generates fake subscription data
├── database/
│   ├── __init__.py
│   ├── config.py             # Database configuration
│   ├── db_utils.py           # Database utility functions
│   └── models.py             # Database models and schemas
├── kafka/
│   ├── __init__.py
│   ├── config.py             # Kafka configuration
│   ├── consumer.py           # Kafka message consumer
│   └── producer.py           # Kafka message producer
├── processing/
│   ├── __init__.py
│   ├── data_quality.py       # Data validation and quality checks
│   └── transformer.py        # Data transformation logic
├── .gitignore                # Git ignore file
├── Raw_Subscription_Data_DB.sql  # Database schema script
└── README.md                 # This file
```

## 🔧 Configuration

### Database Connection
Update the connection string in `database/config.py`:
```python
SERVER = 'localhost'
DATABASE = 'SubscriptionDB'
USERNAME = 'your_username'
PASSWORD = 'your_password'
```

### Kafka Configuration
Update Kafka settings in `kafka/config.py`:
- **Bootstrap Server:** localhost:9092
- **Topic Name:** subscriptions_raw
- **Partitions:** 3
- **Replication Factor:** 1

## 🐛 Troubleshooting

### Common Issues and Solutions

**Kafka Connection Errors:**
- Ensure ZooKeeper and Kafka server are running
- Check if ports 2181 (ZooKeeper) and 9092 (Kafka) are available
- Verify Java is installed and JAVA_HOME is set

**Database Connection Issues:**
- Verify SQL Server is running and accessible
- Check connection string parameters
- Ensure database exists and user has proper permissions

**Python Import Errors:**
- Activate your virtual environment
- Reinstall packages: `pip install -r requirements.txt`

## 📊 Monitoring Your Pipeline

Once running, you can monitor your pipeline by:

1. **Checking the database** for new records in `Raw_Subscription_Data_DB`
2. **Kafka topic inspection:**
   ```bash
   # Windows
   bin\windows\kafka-console-consumer.bat --topic subscriptions_raw --from-beginning --bootstrap-server localhost:9092
   
   # macOS/Linux
   bin/kafka-console-consumer.sh --topic subscriptions_raw --from-beginning --bootstrap-server localhost:9092
   ```

## 🎯 Next Steps

- **Data Quality:** Enhance validation rules in `processing/data_quality.py`
- **Transformations:** Add complex data transformations in `processing/transformer.py`
- **Database Models:** Extend database schemas in `database/models.py`
- **Error Handling:** Implement comprehensive error handling across all modules
- **Testing:** Add unit tests for each component
- **Monitoring:** Set up logging and monitoring dashboards
- **Containerization:** Deploy using Docker containers
- **CI/CD:** Implement automated testing and deployment pipelines

## 🤝 Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**🎉 Congratulations!** Your subscription data pipeline is now running. Check your `Raw_Subscription_Data_DB` database to see the processed data flowing through your pipeline.
