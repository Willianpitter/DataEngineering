# DataEngineering

# Table of Contents


# Trip Ingestion Pipeline
This project consists of a data ingestion pipeline that reads a CSV file containing trip data, stores it in a PostgreSQL database, and allows you to retrieve insights about the data through an API. Additionally, it provides real-time updates on the status of the ingestion process via WebSocket notifications.

# Project Overview
This project allows for:

- An automated process to ingest and store trip data.
- Grouping trips by similar origin, destination, and time of day.
- Obtaining the weekly average number of trips for a region or bounding box.
- Real-time updates on ingestion status via WebSocket notifications.
- Scalability to handle up to 100 million entries.
- Bonus Features
- Containerization with Docker.


Requirements
- Python 3.8+
- Docker


# Step 1: Setup Environment

git clone https://github.com/Willianpitter/DataEngineering.git

# Step 2. Install Dependencies

python -m venv venv
source venv/bin/activate   # For Windows: venv\Scripts\activate



# GCP Trip Data Ingestion and Processing API

This project ingests trip data from a CSV in Cloud Storage, processes it with Cloud Functions, and stores it in BigQuery for querying and analytics. It also provides real-time notifications of ingestion status using WebSockets hosted on Cloud Run.

## GCP Services Used
- **Cloud Storage**: Store trip CSV files.
- **BigQuery**: Store and query trip data.
- **Cloud Functions**: Ingest and process CSV data.
- **Pub/Sub**: Trigger ingestion and notify WebSocket service.
- **Cloud Run**: Host the WebSocket notification service.

## Setup Instructions

1. **Create Cloud Storage Bucket**: 
   - Create a bucket and upload CSV files.

2. **Set Up BigQuery**: 
   - Create a dataset and partitioned table for storing trip data.

3. **Deploy Cloud Function**: 
   - Use the provided `ingest_csv_to_bigquery` Cloud Function to ingest data.

4. **Deploy WebSocket Service to Cloud Run**:
   - Containerize the WebSocket service and deploy to Cloud Run using the `Dockerfile`.

5. **Run Queries**: 
   - Use BigQuery to group trips and calculate weekly averages with the provided SQL scripts.

## Features
- Automated ingestion from Cloud Storage to BigQuery.
- Grouping of trips by origin, destination, and time.
- Weekly average calculation for bounding boxes.
- Real-time status updates using WebSockets.
