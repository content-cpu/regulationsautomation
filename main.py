import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date
import smtplib
import json
import base64
import urllib3
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google.cloud import bigquery
from google.oauth2 import service_account

# --- 1. CONFIGURATION (CORRECTED) ---
# Based on the JSON key you provided:
PROJECT_ID = "stockautomator"
# Based on the URL you provided:
DATASET_ID = "Regulations_8pm_daily"

# Secrets
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
TO_EMAIL = os.environ.get("TO_EMAIL")
GCP_JSON_KEY = os.environ.get("GCP_JSON_KEY")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 2. BIGQUERY SETUP ---
def get_bq_client():
    if not GCP_JSON_KEY:
        print("❌ Error: GCP_JSON_KEY is missing.")
        return None
    try:
        key_json = json.loads(base64.b64decode(GCP_JSON_KEY).decode('utf-8'))
        credentials = service_account.Credentials.from_service_account_info(key_json)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        return None

def upload_to_bigquery(df, table_name):
    client = get_bq_client()
    if not client or df.empty: return

    # Full Table Address: stockautomator.Regulations_8pm_daily.table_name
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"🔄 Processing Table: {table_id}")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",       # Append new rows
        create_disposition="CREATE_IF_NEEDED",  # Create table if missing
        autodetect=True,                        # Auto-detect schema
        source_format=bigquery.SourceFormat.CSV
    )

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Success: Appended {len(df)} rows to {table_name}")
    except Exception as e:
        print(f"❌ Upload Failed for {table_name}: {e}")

# --- 3. SCRAPERS (7 CODES) ---

def run_scraper(name, func, table_name):
    print(f"\n--- Running {name} ---")
    try:
        df, filename = func()
        if df is not None and not df.empty:
            # Add a 'Run_Date' column to track when data was added
            df['Run_Date'] = str(date.today())
            upload_to_bigquery(df, table_name)
            return df, filename, f"✅ {name}: {len(df)} records."
        else:
            return None, None, f"⚠️ {name}: No data found today."
    except Exception as e:
        return None, None, f"❌ {name}: Error - {e}"

# [1] BSE Index
def scrape_bse_index():
    try:
        url = "https://www.bseindia.com/markets/MarketInfo/spbseindex_MediaRelease.aspx"
        target = date.today().strftime("%B %d, %Y")
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        tables = soup.find_all("table")
        if len(tables) < 2: return None, None
        data = []
        for row in tables[1].find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 3 and cols[0].text.strip() == target:
                link = cols[2].find("a")
                pdf = "https://www.bseindia.com" + link["href"] if link else "N/A"
                data.append({"Date": target, "Subject": cols[1].text.strip(), "PDF": pdf})
        if data: return pd.DataFrame(data), f"BSE_Index_{date.today()}.csv"
    except: pass
    return None, None

# [2] BSE Media
def scrape_bse_media():
    try:
        url = "https://www.bseindia.com/markets/MarketInfo/MediaRelease.aspx"
        target = date.today().strftime("%B %d, %Y")
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        tables = soup.find_all("table")
        if len(tables) < 2: return None, None
        data = []
        for row in tables[1].find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 3 and cols[0].text.strip() == target:
                data.append({"Date": target, "Subject": cols[1].text.strip(), "Category": cols[2].text.strip()})
        if data: return pd.DataFrame(data), f"BSE_Media_{date.today()}.csv"
    except: pass
    return None, None

# [3] BSE Notices
def scrape_bse_notices():
    try:
        url = "https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?id=0&txtscripcd=&pagecont=&subject="
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("table", {"id": "ContentPlaceHolder1_GridView1"})
        data = []
        if table:
            for row in table.find_all("tr")[1:20]: # Top 20
                cols = row.find_all("td")
                if len(cols) >= 6:
                    data.append({
                        "Notice": cols[0].text.strip(),
                        "Subject": cols[1].text.strip(),
                        "Segment": cols[2].text.strip(),
                        "Category": cols[3].text.strip()
                    })
        if data: return pd.DataFrame(data), f"BSE_Notices_{date.today()}.csv"
    except: pass
    return None, None

# [4] SEBI
def scrape_sebi():
    try:
        url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
        target = date.today().strftime("%b %d, %Y")
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.content, "html.parser")
        data = []
        for table in soup.find_all("table"):
            if "Date" in table.text:
                for row in table.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 3 and cols[0].text.strip() == target:
                        data.append({"Date": target, "Type": cols[1].text.strip(), "Title": cols[2].text.strip()})
                break
        if data: return pd.DataFrame(data), f"SEBI_{date.today()}.csv"
    except: pass
    return None, None

# [5] NSE Circulars
def scrape_nse_circ():
    try:
        target = date.today().strftime("%B %d, %Y")
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        s.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
        res = s.get("https://www.nseindia.com/api/circulars", timeout=10)
        data = []
        for i in res.json().get('data', []):
            if i.get('cirDisplayDate') == target:
                data.append({"Date": target, "Subject": i.get('sub'), "Dept": i.get('circDepartment')})
        if data: return pd.DataFrame(data), f"NSE_Circ_{date.today()}.csv"
    except: pass
    return None, None

# [6] NSE Press
def scrape_nse_press():
    try:
        target = date.today().strftime("%d-%m-%Y")
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        s.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
        res = s.get(f"https://www.nseindia.com/api/press-release-cms20?fromDate={target}&toDate={target}", timeout=10)
        data = []
        if res.status_code == 200:
            for i in res.json():
                c = i.get('content', {})
                data.append({"Date": c.get('field_date'), "Title": c.get('title')})
        if data: return pd.DataFrame(data), f"NSE_Press_{date.today()}.csv"
    except: pass
    return None, None

# [7] NSDL
def scrape_nsdl():
    try:
        target_match = date.today().strftime("%d %B")
        res = requests.get("https://nsdl.co.in/business/circular_stat.php", verify=False, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        data = []
        for row in soup.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 3 and target_match.lower() in cols[0].text.lower():
                data.append({"Date": cols[0].text.strip(), "Subject": cols[2].text.strip()})
        if data: return pd.DataFrame(data), f"NSDL_{date.today()}.csv"
    except: pass
    return None, None

# --- 4. EMAILER ---
def send_email(report, attachments):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD: return
    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL
    msg['Subject'] = f"Daily Scraping Report: {date.today()}"
    msg.attach(MIMEText("\n".join(report), 'plain'))
    
    for f in attachments:
        if os.path.exists(f):
            with open(f, "rb") as fil:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(fil.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename= {f}")
            msg.attach(part)
            
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, TO_EMAIL, msg.as_string())
        server.quit()
        print("✅ Email Sent")
    except Exception as e:
        print(f"❌ Email Failed: {e}")

# --- 5. MAIN EXECUTION ---
if __name__ == "__main__":
    print(f"🚀 Starting Run for Project: {PROJECT_ID}")
    
    # Define the 7 Tasks (Code Name, Function, BigQuery Table Name)
    tasks = [
        ("BSE Index", scrape_bse_index, "table_01_bse_index"),
        ("BSE Media", scrape_bse_media, "table_02_bse_media"),
        ("BSE Notices", scrape_bse_notices, "table_03_bse_notices"),
        ("SEBI", scrape_sebi, "table_04_sebi"),
        ("NSE Circ", scrape_nse_circ, "table_05_nse_circ"),
        ("NSE Press", scrape_nse_press, "table_06_nse_press"),
        ("NSDL", scrape_nsdl, "table_07_nsdl"),
    ]
    
    report_log = []
    generated_files = []
    
    for name, func, table_name in tasks:
        df, fname, status = run_scraper(name, func, table_name)
        report_log.append(status)
        if fname: generated_files.append(fname)
        print(status)
        
    send_email(report_log, generated_files)
