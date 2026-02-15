# ============================================================
# IMPORTS
# ============================================================

import pandas as pd
import boto3
import json
import io
from datetime import datetime


"""
Weather Data Transformation (Silver Layer)

Reads raw weather JSON from S3 Bronze layer,
extracts required fields,
converts to structured dataframe,
and uploads partitioned parquet to Silver layer.
"""


# ============================================================
# READ RAW JSON FROM S3 (BRONZE)
# ============================================================

def read_json_file():

    # Create S3 client
    s3 = boto3.client("s3")

    bucket_name = "weather-data-raw-bhanu"

    # List objects
    response = s3.list_objects_v2(Bucket=bucket_name)

    if "Contents" not in response:
        print("No raw files found")
        return None

    # Pick first file (current logic)
    first_file_key = response["Contents"][0]["Key"]
    print(f"Reading file: {first_file_key}")

    # Read object
    obj = s3.get_object(
        Bucket=bucket_name,
        Key=first_file_key
    )

    raw_data = json.loads(
        obj["Body"].read().decode("utf-8")
    )

    # Extract payload
    current_weather = raw_data["payload"].get("current_weather")

    # Build Silver schema record
    transformed_record = {
        "run_id": raw_data["metadata"]["run_id"],
        "ingestion_time": raw_data["metadata"]["ingestion_timestamp"],
        "temperature": current_weather["temperature"],
        "windspeed": current_weather["windspeed"],
        "winddirection": current_weather["winddirection"],
        "weathercode": current_weather["weathercode"],
        "observation_time": current_weather["time"]
    }

    print("Transformed Record:")
    print(transformed_record)

    return transformed_record


# ============================================================
# UPLOAD PARQUET TO S3 (SILVER)
# ============================================================

def upload_to_s3(df):

    print("\nUploading transformed data to S3 (Silver Layer)...")

    s3 = boto3.client("s3")
    bucket_name = "weather-data-processed-bhanuu"

    # Event-time partitioning
    event_time = df["observation_time"].iloc[0]

    year = event_time.strftime("%Y")
    month = event_time.strftime("%m")
    day = event_time.strftime("%d")

    # Create parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Partitioned path
    file_name = (
        f"silver/weather/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"weather_{event_time.strftime('%H%M%S')}.parquet"
    )

    # Upload
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=parquet_buffer.getvalue()
    )

    print(f"✅ Uploaded partitioned file: {file_name}")


# ============================================================
# PIPELINE RUNNER (USED BY ORCHESTRATOR)
# ============================================================

def run_transformation():

    print("\n===== TRANSFORMATION STARTED =====")

    record = read_json_file()

    if not record:
        print("No data found for transformation")
        return

    # Convert to dataframe
    df = pd.DataFrame([record])

    # Fix datatypes
    df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])
    df["observation_time"] = pd.to_datetime(df["observation_time"])

    # Upload to Silver
    upload_to_s3(df)

    print("===== TRANSFORMATION COMPLETED =====")


# ============================================================
# SCRIPT ENTRY POINT (Standalone Execution)
# ============================================================

if __name__ == "__main__":
    run_transformation()
