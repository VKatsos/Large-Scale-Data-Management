# Kafka, Spark, and Cassandra Streaming Project

## Overview

This project is part of the Programming Project II for the M.Sc. in Data Science at the Athens University of Economics and Business. It involves creating a streaming pipeline using Apache Kafka for message generation, Apache Spark for stream processing, and Apache Cassandra for persisting the processed data.

The project is divided into two parts:
1. **Kafka Producer**: Generates a stream of songs listened to by a group of people and sends them to Kafka.
2. **Spark Streaming with Cassandra**: Consumes the Kafka stream, processes the messages, and stores the results in a Cassandra database.

## Prerequisites

To set up and run the project, you will need the following tools and frameworks:
- [Vagrant](https://www.vagrantup.com/)
- [Apache Spark](https://spark.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Cassandra](https://cassandra.apache.org/)
- [Python 3.x](https://www.python.org/)
- [pyspark](https://spark.apache.org/docs/latest/api/python/)
- [Faker](https://faker.readthedocs.io/en/master/)

The provided `Vagrantfile` will set up the necessary environment with Apache Spark, Apache Kafka, and Apache Cassandra running in containers.

## Part I: Kafka Producer

In this part, a Python script generates a stream of songs listened to by a group of people, including your name (hardcoded). The script uses the Faker library to generate names and listens to a random song from the `spotify-songs.csv` file. The stream is sent to Kafka, where each message contains:
- Person's name
- Song name
- Timestamp

### Running the Producer

To start generating the Kafka stream, run the following command in the VM:

```bash
python3 examples/python-kafka-example.py
```

### Dependencies

You will need to install the following Python libraries:

```bash
pip install kafka-python Faker pandas
```

## Part II: Spark Streaming with Cassandra

The second part of the project involves consuming the Kafka stream using PySpark, processing the messages, and persisting them in Cassandra. The processed data includes the name of the person, the timestamp, and the details of the song from the `spotify-songs.csv` file.

### Cassandra Table

You will need to create a table in Cassandra to store the processed data. Use the following commands in Cassandra's `cqlsh`:

```bash
CREATE KEYSPACE spotify WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE spotify.records (
    id int PRIMARY KEY,
    name text,
    song text,
    timestamp timestamp
);
```

### Running the Consumer

To run the Spark Streaming script and process the Kafka messages, use the following command:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 cassandra-spark-streaming-example.py
```

### Configurable Persistence Interval

The persistence of messages to Cassandra occurs at a configurable interval (default: 30 seconds), which can be modified in the PySpark script.

## Data Model

The Cassandra table schema is designed to optimize query performance for specific aggregations, such as retrieving songs listened to by a person within a particular hour or calculating the average song properties (e.g., danceability, tempo).

## Queries and Results

After processing the data, you can execute the following CQL queries to retrieve information from the Cassandra database:

1. **Average Danceability**: Retrieve the average danceability of the songs listened to by a particular person during a specific hour.
   
2. **Songs List**: Retrieve the names of songs listened to by a person during a specific hour.

Example query:

```sql
SELECT AVG(danceability) FROM spotify.records WHERE name='Your Name' AND timestamp > '2024-02-03 10:00';
```

## Execution

To execute the scripts:

1. **Kafka Producer**: Run the Python script to generate Kafka messages.
2. **Spark Streaming**: Run the Spark streaming script to consume and process the Kafka messages and store them in Cassandra.

You may also start a PySpark session using:

```bash
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0
```

## Reporting

The final report includes:
1. The Python script for the Kafka producer.
2. The PySpark script for streaming and processing.
3. Details about the Cassandra data model.
4. A sample of 50 persisted rows from the Cassandra table.
5. Two queries that provide song details for your name and compute the average danceability.

---

Let me know if you'd like to adjust anything!