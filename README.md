# **Kafka-Spark Streaming Integration**

This project demonstrates an end-to-end data pipeline implementation with Apache Kafka and Apache Spark Streaming. It shows how to produce data to Kafka from a CSV file and consume/process that data using Spark Streaming.

## **Project Structure**

The project consists of two Maven modules:

1. **kafka**: Contains Kafka producer code to read from CSV and send to a Kafka topic

2. **spark**: Contains Spark Streaming consumer code to process data from Kafka

## **Prerequisites**

Before running this project, make sure you have:

1. Java 17 or later (configured in the project)

2. Apache Maven installed

3. Apache Kafka (3.0.0) running locally

4. A Kafka topic named "demo" created

## **Features**

- CSV file reading and streaming to Kafka

- Real-time data processing with Spark Streaming

- CSV data transformation and filtering

- Offset management in Kafka

- Writing processed data to output CSV files

## **Data Flow**

1. CSV data is read from the input file (test.csv)

2. The Kafka producer sends each row to the Kafka topic "demo"

3. Spark Streaming consumes messages from Kafka in micro-batches

4. The consumer processes each record, extracting relevant fields

5. Processed data is written to "Master\_dataset.csv"



