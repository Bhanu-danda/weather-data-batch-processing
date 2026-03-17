# 🌦️ End-to-End Weather Data Engineering Pipeline

## 🚀 Overview

This project demonstrates a **production-style batch data pipeline** built using a **Bronze → Silver → Gold architecture**. It ingests real-time weather data, processes it into structured formats, and loads it into a PostgreSQL data warehouse for analytics.

The pipeline is designed to mimic real-world data engineering workflows, including **incremental loading, partitioned data lakes, and SQL-based analytics readiness**.

---

## 🏗️ Architecture

```
Weather API
   ↓
Python Ingestion Script
   ↓
S3 Bronze (Raw JSON - Immutable)
   ↓
Python Transformation (Pandas + PyArrow)
   ↓
S3 Silver (Partitioned Parquet)
   ↓
Python Incremental Loader
   ↓
PostgreSQL Gold (Analytics Layer)
```

---

## ⚙️ Tech Stack

* **Programming Language:** Python
* **Libraries:** Pandas, PyArrow, Requests, Boto3, SQLAlchemy, psycopg2, s3fs
* **Storage:** AWS S3 (Data Lake)
* **Database:** PostgreSQL (Local Data Warehouse)
* **Concepts:** ETL/ELT, Data Lake, Partitioning, Incremental Loading, SQL Analytics

---

## 📥 Data Source

* Weather data fetched from a public weather API (Open-Meteo)
* Data includes:

  * Temperature
  * Wind speed & direction
  * Weather codes
  * Observation timestamps

---

## 🥉 Bronze Layer — Raw Ingestion

* API data fetched using Python (`requests`)
* Metadata enrichment added:

  * `ingestion_timestamp`
  * `run_id` (UUID)
  * `source_system`
* Stored as **immutable JSON files** in S3

**Structure:**

```
s3://weather-data-raw-bhanu/
   raw/weather/{run_id}.json
```

---

## 🥈 Silver Layer — Transformation

* Raw JSON processed using Pandas
* Extracted structured schema:

  ```
  run_id
  ingestion_time
  temperature
  windspeed
  winddirection
  weathercode
  observation_time
  ```
* Converted to **Parquet format** using PyArrow
* Stored in S3 with **event-time partitioning**

**Partitioned Structure:**

```
s3://weather-data-processed-bhanuu/silver/weather/
   year=YYYY/
     month=MM/
       day=DD/
         weather_HHMMSS.parquet
```

---

## 🥇 Gold Layer — Data Warehouse

* PostgreSQL used as analytics layer
* Data loaded directly from S3 (no local storage)

### 📊 Table: `weather_observations`

| Column           | Type           |
| ---------------- | -------------- |
| observation_time | TIMESTAMP (PK) |
| temperature      | DOUBLE         |
| windspeed        | DOUBLE         |
| winddirection    | INTEGER        |
| weathercode      | INTEGER        |
| ingestion_time   | TIMESTAMP      |
| run_id           | TEXT           |

---

## 🔁 Incremental Loading (Key Feature)

Implemented **watermark-based incremental loading**:

1. Query latest record from PostgreSQL:

   ```sql
   SELECT MAX(observation_time)
   FROM weather_observations;
   ```

2. Filter new records in Python:

   ```python
   df = df[df["observation_time"] > last_time]
   ```

3. Insert only new data

### ✅ Benefits

* Prevents duplicate data
* Ensures idempotent pipeline runs
* Improves performance

---

## 📂 Project Structure

```
project-root/
│
├── ingestion/
│   └── ingestion.py
│
├── transformation/
│   └── transformation.py
│
├── gold/
│   └── load_to_postgre.py
│
├── logs/
│
└── README.md
```

---

## ▶️ How to Run

### 1. Install dependencies

```bash
pip install pandas pyarrow boto3 s3fs sqlalchemy psycopg2-binary
```

### 2. Configure AWS

```bash
aws configure
```

### 3. Run pipeline

```bash
python ingestion/ingestion.py
python transformation/transformation.py
python gold/load_to_postgre.py
```

---

## 📌 Key Highlights

* Built a **complete end-to-end data pipeline**
* Used **data lake + data warehouse architecture**
* Implemented **event-time partitioning**
* Enabled **direct S3 → PostgreSQL loading**
* Designed **incremental batch processing system**
* Ensured **idempotent and production-safe execution**

---

## 🔮 Future Improvements

* Add orchestration (Airflow / cron jobs)
* Build analytical tables (daily summaries, trends)
* Add data quality checks
* Dockerize the pipeline
* Integrate dashboarding (Power BI / Tableau)

---

## 💡 Learning Outcomes

This project demonstrates:

* Real-world data engineering workflows
* Handling semi-structured → structured data
* Working with cloud storage (S3)
* Designing scalable data pipelines
* Implementing incremental loading strategies
* Preparing data for analytics

---

## 👨‍💻 Author

**Bhanu Prasad**
Aspiring Data Engineer

---

## ⭐ If you found this useful

Give it a star ⭐ and feel free to fork or contribute!
