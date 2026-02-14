import pandas as pd      # transformation library
import requests          # (not used now, but ok to keep)
import boto3             # AWS SDK
import json              # JSON parsing
import logging
from datetime import datetime 
import io               # This lets us create an in-memory parquet file.

"""
Weather Data Transformation
Reads raw weather JSON data from S3 (bronze layer),
transforms it into structured format,
and writes processed data back to S3 (silver layer).
"""

""" 
def read_json_file():
    s3 = boto3.client("s3")

    bucket_name = "weather-data-raw-bhanu"

    response = s3.list_objects_v2(
        Bucket=bucket_name,
    )

    if "Contents" not in response:
        print("No raw files found")
        return
    
      # pick the first file
    first_file_key = response["Contents"][0]["Key"]
    print(f"Reading file: {first_file_key}")

    obj = s3.get_object(
        Bucket=bucket_name,
        Key=first_file_key
    )

    raw_data = json.loads(obj["Body"].read().decode("utf-8"))

    print("\nTop-level keys:")
    print(raw_data.keys())

    print("\nMetadata:")
    print(raw_data.get("metadata"))

    print("\nPayload keys:")
    print(raw_data.get("payload").keys())

    current_weather = raw_data["payload"].get("current_weather")
    print("\nCurrent weather data:")
    print(current_weather)


if __name__=="__main__":
    read_json_file()             

"""

##  MANY DAYS LATER 
# Now we move to:
# Raw JSON → Structured Table (DataFrame)                 This is the Silver layer beginning.
                                            #STEP 7 — Extract only required fields (VERY important)






def read_json_file():

    # ---------------------------------
    # Create S3 client
    # ---------------------------------
    s3 = boto3.client("s3")

    bucket_name = "weather-data-raw-bhanu"

    # ---------------------------------
    # List files in bucket
    # ---------------------------------
    response = s3.list_objects_v2(
        Bucket=bucket_name
    )

    if "Contents" not in response:
        print("No raw files found")
        return

    # ---------------------------------
    # Pick first file
    # ---------------------------------
    first_file_key = response["Contents"][0]["Key"]
    print(f"Reading file: {first_file_key}")

    # ---------------------------------
    # Read file from S3
    # ---------------------------------
    obj = s3.get_object(
        Bucket=bucket_name,
        Key=first_file_key
    )

    raw_data = json.loads(
        obj["Body"].read().decode("utf-8")
    )

    # ---------------------------------
    # Inspect structure
    # ---------------------------------
    print("\nTop-level keys:")
    print(raw_data.keys())

    print("\nMetadata:")
    print(raw_data.get("metadata"))

    print("\nPayload keys:")
    print(raw_data.get("payload").keys())

    # ---------------------------------
    # Extract Current Weather
    # ---------------------------------
    current_weather = raw_data["payload"].get("current_weather")

    # ---------------------------------
    # Select REQUIRED fields (Silver schema start)
    # ---------------------------------
    transformed_record = {
        "run_id": raw_data["metadata"]["run_id"],
        "ingestion_time": raw_data["metadata"]["ingestion_timestamp"],
        "temperature": current_weather["temperature"],
        "windspeed": current_weather["windspeed"],
        "winddirection": current_weather["winddirection"],
        "weathercode": current_weather["weathercode"],
        "observation_time": current_weather["time"]
    }

    print("\nTransformed Record:")
    print(transformed_record)
    
    # from silver schema code 
    return transformed_record

##############################################  UPLOADING TO NEW SILVER BUCKET BY PARTIONING BASED ON INGESTION TIME ###########################################################

# import io
# import boto3
# from datetime import datetime

def upload_to_s3(df):
    print("\nUploading transformed data to S3 (Silver Layer)...")

    s3 = boto3.client("s3")

    bucket_name = "weather-data-processed-bhanuu"

    # ---------------------------------
    # Extract event time for partitioning
    # ---------------------------------
    event_time = df["observation_time"].iloc[0]

    year = event_time.strftime("%Y")
    month = event_time.strftime("%m")
    day = event_time.strftime("%d")

    # ---------------------------------
    # Create parquet in memory
    # ---------------------------------
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # ---------------------------------
    # Partitioned S3 path (Silver layout)
    # ---------------------------------
    file_name = (
        f"silver/weather/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"weather_{event_time.strftime('%H%M%S')}.parquet"
    )

    # ---------------------------------
    # Upload to S3
    # ---------------------------------
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=parquet_buffer.getvalue()
    )

    print(f"✅ Uploaded partitioned file: {file_name}")




# ✅ What changed (clear understanding)
# You added a transformation layer:
#  Raw JSON
# Select useful fields
# Structured record (Silver schema)
# uploading to new silver bucket in s3


##########################################   NOW IT'S MY TIME ####################################################

if __name__ == "__main__":
    # We capture the returned dictionary into a variable named 'record'
    record = read_json_file()

    if record:
        print("\n--- TRANSFORMATION using Pandas ---")
        
        # Convert the single dictionary (wrapped in a list) to a DataFrame
        df = pd.DataFrame([record])
        
        print("\nDataframe preview:")
        print(df)
        
        # Optional: Check the data types
        print("\nColumn Data Types:")
        print(df.dtypes)

        # Convert columns to proper datatypes
        # ---------------------------------

        df["ingestion_time"] = pd.to_datetime(df["ingestion_time"])
        df["observation_time"] = pd.to_datetime(df["observation_time"])

        print("\nUpdated Data Types:")
        print(df.dtypes)

        upload_to_s3(df)  #  function calling from here 

    else:
        print("Transformation failed because no data was found.")










