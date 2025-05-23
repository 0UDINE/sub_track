````markdown
sub_track

## Setup Instructions

### 1. Install Required Libraries
First, install all the necessary libraries for this project:

```bash
python -m pip install pandas faker numpy pathlib confluent_kafka pyodbc
````

### 2. Create Database

Create the database by running the SQL script provided in the following file:

* SQL script link or path: `[Insert your SQL script file link or path here]`

Run this script to set up your database schema and initial data.

### 3. Kafka Setup

This project uses Kafka for message streaming. You can install Kafka from the following link:

* Kafka download link: `https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz`

After installing Kafka, start the necessary services:

* **Start ZooKeeper:**

```bash
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

* **Start Kafka Server:**

```bash
bin\windows\kafka-server-start.bat config\server.properties
```

* **Create Kafka Topic:**

Create a topic named `"subscriptions_raw"` with this command:

```bash
bin\windows\kafka-topics.bat --create --topic subscriptions_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 4. Run the Python Faker Code

Run your Python code to generate fake data:

```bash
python .\data_generation\faker_generator.py
```

### 5. Run the Producer

Start the Kafka producer to send messages:

```bash
python .\kafka\producer.py 
```

### 6. Run the Consumer

Start the Kafka consumer to consume messages:

```bash
python .\kafka\consumer.py
```

---

**And Taraaaa! You're done! 🎉 You can go an check your Raw_Subscription_Data_DB**

```

