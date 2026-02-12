import os
import smtplib
import pandas as pd
import json
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google.cloud import bigquery
from google.oauth2 import service_account

# --- CONFIGURATION (Loaded from GitHub Secrets) ---
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
TO_EMAIL = os.environ.get("TO_EMAIL")
GCP_JSON_KEY = os.environ.get("GCP_JSON_KEY")
PROJECT_ID = "your-project-id"       # <--- REPLACE WITH YOUR PROJECT ID
DATASET_ID = "your_dataset_id"       # <--- REPLACE WITH YOUR DATASET ID

# --- HELPER FUNCTIONS ---

def upload_to_bigquery(df, table_name):
    """Appends DataFrame to BigQuery. Creates table if it doesn't exist."""
    print(f"Uploading {table_name} to BigQuery...")
    
    # Decode credentials
    key_json = json.loads(base64.b64decode(GCP_JSON_KEY).decode('utf-8'))
    credentials = service_account.Credentials.from_service_account_info(key_json)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        # Create table if missing, otherwise append
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        autodetect=True, # Auto-detect schema from DataFrame
        source_format=bigquery.SourceFormat.CSV
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Wait for job to finish
    print(f"Success: Appended rows to {table_id}")

def send_email(csv_files):
    """Sends an email with multiple CSV attachments."""
    print("Sending email...")
    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL
    msg['Subject'] = f"Daily Report: {len(csv_files)} CSVs Processed"
    
    body = "The daily python script has run successfully. Attached are the generated CSV files."
    msg.attach(MIMEText(body, 'plain'))

    for filename in csv_files:
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {filename}")
        msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
    server.sendmail(GMAIL_USER, TO_EMAIL, msg.as_string())
    server.quit()
    print("Email sent!")

# --- YOUR 7 CODES GO HERE ---

def run_code_1():
    # PASTE YOUR CODE 1 HERE
    # Example:
    # df = pd.read_csv("url") or scrape logic
    # return df, "table_name_1"
    
    # Placeholder example:
    data = [{"col1": "A", "col2": 10}] 
    df = pd.DataFrame(data)
    return df, "table_01_users"  # <--- Change Table Name

def run_code_2():
    # PASTE YOUR CODE 2 HERE
    data = [{"col1": "B", "col2": 20}]
    df = pd.DataFrame(data)
    return df, "table_02_sales"

def run_code_3():
    # PASTE YOUR CODE 3 HERE
    data = [{"col1": "C", "col2": 30}]
    df = pd.DataFrame(data)
    return df, "table_03_inventory"

def run_code_4():
    # PASTE YOUR CODE 4 HERE
    data = [{"col1": "D", "col2": 40}]
    df = pd.DataFrame(data)
    return df, "table_04_logs"

def run_code_5():
    # PASTE YOUR CODE 5 HERE
    data = [{"col1": "E", "col2": 50}]
    df = pd.DataFrame(data)
    return df, "table_05_analytics"

def run_code_6():
    # PASTE YOUR CODE 6 HERE
    data = [{"col1": "F", "col2": 60}]
    df = pd.DataFrame(data)
    return df, "table_06_finance"

def run_code_7():
    # PASTE YOUR CODE 7 HERE
    data = [{"col1": "G", "col2": 70}]
    df = pd.DataFrame(data)
    return df, "table_07_marketing"

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    generated_files = []
    
    # List of your functions
    tasks = [
        run_code_1, run_code_2, run_code_3, 
        run_code_4, run_code_5, run_code_6, run_code_7
    ]

    for task in tasks:
        try:
            # 1. Run the code
            df, table_name = task()
            
            # 2. Save to CSV for Email
            csv_filename = f"{table_name}.csv"
            df.to_csv(csv_filename, index=False)
            generated_files.append(csv_filename)
            
            # 3. Upload to BigQuery
            upload_to_bigquery(df, table_name)
            
        except Exception as e:
            print(f"Error running task {task.__name__}: {e}")

    # 4. Send all CSVs in one email
    if generated_files:
        send_email(generated_files)
