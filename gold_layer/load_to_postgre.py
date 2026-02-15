
#READ Silver Data (No PostgreSQL Yet)
""" 
import pandas as pd
import pyarrow.dataset as ds
import s3fs

print("Reading parquet from S3...")

# create S3 filesystem
fs = s3fs.S3FileSystem()

# dataset root (PARTITION ROOT)
path = "weather-data-processed-bhanuu/silver/weather/"

# create arrow dataset
dataset = ds.dataset(
    path,
    filesystem=fs,
    format="parquet"
)

# convert to table → pandas
table = dataset.to_table()
df = table.to_pandas()

print("Data loaded successfully!")
print(df.head())
print(df.dtypes)


#####################################   CONNECTING TO POSTGRESQL ##############################

from sqlalchemy import create_engine

print("Connecting to PostgreSQL...")

engine = create_engine(
    "postgresql+psycopg2://postgres:Bhanu%401853083@localhost:5432/weather-Gold-layer-bhanu"  #Db-name //mask password later
)

conn = engine.connect()

print("PostgreSQL connected!")



### Ask PostgreSQL for Latest Record
print("Checking last loaded timestamp...")

query = """
# SELECT MAX(observation_time) AS last_time
# FROM weather_observations;
"""

result = pd.read_sql(query, engine)

last_time = result.iloc[0]["last_time"]

print("Last loaded timestamp:", last_time)

##############################        Add Incremental Decision Logic    
# IF table empty → load everything
# ELSE → load only new rows
print("Preparing data for load...")

if last_time is not None:
    df = df[df["observation_time"] > last_time]
    print(f"Filtered new rows count: {len(df)}")
else:
    print("Initial load detected — all rows will be inserted.")
    print(f"Rows ready for insert: {len(df)}")


## Insert Only If Data Exists
if not df.empty:
    print("Loading data into PostgreSQL...")

    df.to_sql(
        "weather_observations",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    print("Data inserted successfully!")
else:
    print("No new data to load.")

"""

#####################    For orchestration  again in neat function

# ============================================================
# IMPORTS
# ============================================================

import pandas as pd
import pyarrow.dataset as ds
import s3fs
from sqlalchemy import create_engine


"""
Gold Layer Loader

Reads partitioned parquet from Silver layer (S3),
applies incremental loading using watermark logic,
and loads curated data into PostgreSQL warehouse.
"""


# ============================================================
# GOLD LOAD FUNCTION
# ============================================================

def run_gold_load():

    print("\n===== GOLD LOAD STARTED =====")

    # ---------------------------------
    # Read Silver Dataset
    # ---------------------------------
    fs = s3fs.S3FileSystem()

    path = "weather-data-processed-bhanuu/silver/weather/"

    try:
        dataset = ds.dataset(
            path,
            filesystem=fs,
            format="parquet"
        )

        table = dataset.to_table()
        df = table.to_pandas()

    except Exception:
        print("No Silver data available yet.")
        return

    print(f"Rows read from Silver: {len(df)}")

    # ---------------------------------
    # Connect to PostgreSQL
    # ---------------------------------
    engine = create_engine(
        "postgresql+psycopg2://postgres:Bhanu%401853083@localhost:5432/weather-Gold-layer-bhanu"
    )

    print("Connected to PostgreSQL")

    # ---------------------------------
    # Get Watermark
    # ---------------------------------
    query = """
    SELECT MAX(observation_time) AS last_time
    FROM weather_observations;
    """

    result = pd.read_sql(query, engine)
    last_time = result.iloc[0]["last_time"]

    print("Last loaded timestamp:", last_time)

    # ---------------------------------
    # Incremental Filtering
    # ---------------------------------
    if last_time is not None:
        df = df[df["observation_time"] > last_time]
        print(f"New rows after filtering: {len(df)}")
    else:
        print("Initial load detected")

    # ---------------------------------
    # Insert Data
    # ---------------------------------
    if not df.empty:

        df.to_sql(
            "weather_observations",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

        print(f"{len(df)} rows inserted into Gold layer")

    else:
        print("No new data to load")

    # Close connections
    engine.dispose()

    print("===== GOLD LOAD COMPLETED =====")


# ============================================================
# ENTRY POINT (Standalone Run)
# ============================================================

if __name__ == "__main__":
    run_gold_load()
