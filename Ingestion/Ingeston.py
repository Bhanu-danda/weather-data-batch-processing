import pandas as pd
import requests 
import json
import logging
from datetime import datetime

print("All is Well")

# stage:-1
# # API endpoint
# url = "https://api.open-meteo.com/v1/forecast"

# # Query parameters
# params = {
#     "latitude": 12.97,
#     "longitude": 77.59,
#     "current_weather": True
# }

# # Make API request
# response = requests.get(url, params=params)

# # Convert response to JSON
# data = response.json()

# # Print JSON data
# print(data)


# stage:-2 STATUS CHECK + SAFE JSON
# API endpoint
# url = "https://api.open-meteo.com/v1/forecast"   

#❌ Failure test (DO THIS ON PURPOSE)
#Change URL slightly:
# url = "https://api.open-meteo.com/v1/forecasttt"       api fail with this error 404


# Query parameters
# params = {
#     "latitude":13.0,
#     "longitude": 77.59,
#     "current_weather": True
# }

# # Make API request
# response = requests.get(url, params=params)

# # -----------------------------
# # HTTP STATUS CHECK
# # -----------------------------
# if response.status_code == 200:
#     print("API call successful")

#     # -----------------------------
#     # SAFE JSON PARSING
#     # -----------------------------
#     data = response.json()
#     print(data)

# else:
#     print("API call failed")
#     print("Status code:", response.status_code)

#Script does not crash
#👉 You know why it failed
#📌 This is not error handling yet, just controlled flow.

##################################################################################3

# STAGE 3
#✅ Proper error handling using try / except
#❌ Still NO logging
#❌ Still NO timestamps

#Goal of this stage
#✔ Prevent script crashes
#✔ Catch known failure types
#✔ Fail gracefully, not violently

#  1️⃣ WHY ERROR HANDLING IS NEEDED (WRITE THIS)
# Even with status checks, these can still happen:
# Internet down
# API timeout
# DNS failure
# Response not valid JSON
# Without error handling → script crashes → pipeline stops.

import requests

# API endpoint
url = "https://api.open-meteo.com/v1/forecast"

# Query parameters
params = {
    "latitude": 12.97,
    "longitude": 77.59,
    "current_weather": True
}

try:
    # Make API request
    response = requests.get(url, params=params, timeout=10)

    # HTTP status check
    if response.status_code == 200:
        print("API call successful")

        # Safe JSON parsing
        data = response.json()
        print(data)

    else:
        print("API call failed")
        print("Status code:", response.status_code)

# -----------------------------
# ERROR HANDLING
# -----------------------------
except requests.exceptions.Timeout:
    print("Error: API request timed out")

except requests.exceptions.ConnectionError:
    print("Error: Network connection problem")

except ValueError:
    print("Error: Response is not valid JSON")

except requests.exceptions.RequestException as e:
    print("General request error:", e)



# WHAT EACH except MEANS (IMPORTANT)
# 🔹 Timeout
# except requests.exceptions.Timeout:

# 👉 API too slow
# 👉 Prevents infinite waiting

# 🔹 Connection error
# except requests.exceptions.ConnectionError:
# 👉 No internet / DNS issue / API down

# 🔹 Invalid JSON
# except ValueError:
# 👉 Response exists but isn’t JSON

# 🔹 Generic fallback
# except requests.exceptions.RequestException as e:

# 👉 Catches all other request-related errors
# 👉 Always keep this last

# 4️⃣ TEST ERROR HANDLING (DO THIS)
# 🔸 Timeout test
# Change timeout:
# timeout=0.001
# Expected:
# Error: API request timed out
# 🔸 Connection test
# Turn off internet and run.
# Expected:
# Error: Network connection problem
# 🔸 Invalid JSON test
# Change URL to a non-JSON page.
# Expected:
# Error: Response is not valid JSON



##### stage:4   logging ###########################################

# import requests
# import logging
# import os

# -------------------------------------------------
# LOG FILE PATH (FIXED FOR YOUR FOLDER STRUCTURE)
# -------------------------------------------------
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# LOG_FILE = os.path.join(BASE_DIR, "logs", "ingestion.log")

# -------------------------------------------------
# LOGGING CONFIGURATION
# -------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler(LOG_FILE),
#         logging.StreamHandler()
#     ]
# )

# -------------------------------------------------
# API CONFIGURATION
# -------------------------------------------------
# url = "https://api.open-meteo.com/v1/forecast"
# url = "https://www.youtube.com/"   # fake url for testing log
# params = {
#     "latitude": 12.97,
#     "longitude": 77.59,
#     "current_weather": True
# }

# # -------------------------------------------------
# # INGESTION LOGIC
# # -------------------------------------------------
# logging.info("Ingestion started")

# try:
#     logging.info("Calling weather API")

#     response = requests.get(url, params=params, timeout=10)

#     if response.status_code == 200:
#         logging.info("API call successful")

#         data = response.json()
#         logging.info("JSON parsed successfully")

#         print(data)

#     else:
#         logging.error(f"API call failed with status code {response.status_code}")

# except requests.exceptions.Timeout:
#     logging.error("API request timed out")

# except requests.exceptions.ConnectionError:
#     logging.error("Network connection error")

# except ValueError:
#     logging.error("Response is not valid JSON")

# except requests.exceptions.RequestException as e:
#     logging.error(f"Request exception occurred: {e}")

# logging.info("Ingestion finished")


########################################################################################

# 🔷                             STAGE 5 — TIMESTAMPING & METADATA ENRICHMENT
# What we are adding (ONLY this)
# We will attach metadata to the ingested data, not change the actual weather values.

# Metadata fields:
# ingestion_timestamp → when data was fetched
# source_system → which API
# run_id → unique ID for this run
# 📌 This is NOT transformation.
# 📌 This is ingestion responsibility.


import requests
import logging
import os
from datetime import datetime
import uuid

# -------------------------------------------------
# LOG FILE PATH
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(BASE_DIR, "logs", "ingestion.log")

# -------------------------------------------------
# LOGGING CONFIGURATION
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# -------------------------------------------------
# API CONFIGURATION
# -------------------------------------------------
API_URL = "https://api.open-meteo.com/v1/forecast"

PARAMS = {
    "latitude": 12.97,
    "longitude": 77.59,
    "current_weather": True
}

# -------------------------------------------------
# INGESTION FUNCTION
# -------------------------------------------------
def ingest_weather_data():
    logging.info("Ingestion started")

    try:
        logging.info("Calling weather API")

        response = requests.get(API_URL, params=PARAMS, timeout=10)

        if response.status_code != 200:
            logging.error(f"API call failed with status code {response.status_code}")
            return None

        data = response.json()
        logging.info("JSON parsed successfully")

        enriched_data = {
            "metadata": {
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "source_system": "open-meteo",
                "run_id": str(uuid.uuid4())
            },
            "payload": data
        }

        logging.info("Metadata added to ingested data")
        logging.info("Ingestion finished successfully")

        return enriched_data

    except requests.exceptions.Timeout:
        logging.error("API request timed out")

    except requests.exceptions.ConnectionError:
        logging.error("Network connection error")

    except ValueError:
        logging.error("Response is not valid JSON")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred: {e}")

    return None


# # -------------------------------------------------
# # SCRIPT ENTRY POINT
# # -------------------------------------------------
# if __name__ == "__main__":
#     result = ingest_weather_data()
#     print(result)


########### DONE ######## 

##############################################    LOADING RAW DATA ################################

import boto3
import json

def store_raw_data_s3(enriched_data):
    if enriched_data is None:
        logging.warning("No data to upload to S3")
        return

    s3 = boto3.client("s3")

    run_id = enriched_data["metadata"]["run_id"]
    file_name = f"{run_id}.json"

    s3.put_object(
        Bucket="weather-data-raw-bhanu",
        Key=f"raw/weather/{file_name}",
        Body=json.dumps(enriched_data),
        ContentType="application/json"
    )

    logging.info(f"Raw data uploaded to S3: raw/weather/{file_name}")


################################################   

# -------------------------------------------------
# SCRIPT ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    result = ingest_weather_data()
    store_raw_data_s3(result)


































































