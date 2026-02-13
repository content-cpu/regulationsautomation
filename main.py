import os
import json
import base64
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# --- CONFIGURATION ---
PROJECT_ID = "your-project-id"  # <--- CHECK THIS IS CORRECT
DATASET_ID = "stock_data"       # <--- CHECK THIS IS CORRECT
GCP_JSON_KEY = os.environ.get("GCP_JSON_KEY")

def test_connection():
    print("--- STARTING DIAGNOSTIC TEST ---")
    
    # 1. Decode Key
    try:
        key_json = json.loads(base64.b64decode(GCP_JSON_KEY).decode('utf-8'))
        credentials = service_account.Credentials.from_service_account_info(key_json)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        print("✅ step 1: Auth Successful.")
    except Exception as e:
        print(f"❌ Step 1 FAILED: Could not read JSON Key. Error: {e}")
        return

    # 2. Check Dataset
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Step 2: Dataset '{DATASET_ID}' found.")
    except Exception as e:
        print(f"❌ Step 2 FAILED: Dataset '{DATASET_ID}' NOT FOUND. Did you create it? Is the PROJECT_ID correct?")
        print(f"Error details: {e}")
        return

    # 3. Try to Create a Test Table
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.test_table_123"
    df = pd.DataFrame({"id": [1], "name": ["Test"]})
    
    print(f"Attempting to create table: {table_ref}...")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print("✅ Step 3: SUCCESS! Table created.")
        print("Go check BigQuery now.")
    except Exception as e:
        print("❌ Step 3 FAILED: Could not create table.")
        print("ERROR MESSAGE (Show this to support):")
        print(e)

if __name__ == "__main__":
    test_connection()
