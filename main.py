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
import csv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google.cloud import bigquery
from google.oauth2 import service_account

# --- DISABLE SSL WARNINGS (For NSDL) ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION (Secrets) ---
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
TO_EMAIL = os.environ.get("TO_EMAIL")
GCP_JSON_KEY = os.environ.get("GCP_JSON_KEY")
PROJECT_ID = "your-project-id"       # <--- REPLACE IF NOT USING ENV VAR
DATASET_ID = "your_dataset_id"       # <--- REPLACE WITH YOUR BQ DATASET NAME

# --- HELPER FUNCTIONS ---

def get_bq_client():
    if not GCP_JSON_KEY:
        return None
    try:
        key_json = json.loads(base64.b64decode(GCP_JSON_KEY).decode('utf-8'))
        credentials = service_account.Credentials.from_service_account_info(key_json)
        return bigquery.Client(credentials=credentials, project=PROJECT_ID)
    except Exception as e:
        print(f"BigQuery Auth Error: {e}")
        return None

def upload_to_bigquery(df, table_name):
    client = get_bq_client()
    if not client or df.empty:
        return
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"Uploading to BigQuery: {table_id}...")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Append to existing
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True, # Auto-detect schema
        source_format=bigquery.SourceFormat.CSV
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"Success: Uploaded {len(df)} rows to {table_name}")
    except Exception as e:
        print(f"BigQuery Upload Failed for {table_name}: {e}")

def send_email(report_lines, attachments):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print("Skipping email (Credentials missing)")
        return

    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    msg['To'] = TO_EMAIL
    msg['Subject'] = f"Daily Scraping Report: {date.today()}"
    
    body = "Summary of today's run:\n\n" + "\n".join(report_lines)
    msg.attach(MIMEText(body, 'plain'))

    for filename in attachments:
        if os.path.exists(filename):
            with open(filename, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename= {filename}")
            msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, TO_EMAIL, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Email failed: {e}")

# ==========================================
# 1. BSE Index Media Releases
# ==========================================
def code_1_bse_index():
    target_date_str = date.today().strftime("%B %d, %Y")
    URL = "https://www.bseindia.com/markets/MarketInfo/spbseindex_MediaRelease.aspx"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

    try:
        response = requests.get(URL, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        if len(tables) < 2: return None, None

        records = []
        for row in tables[1].find_all("tr"):
            cells = row.find_all("td")
            if len(cells) != 3: continue
            if cells[0].get_text(strip=True) == target_date_str:
                link = cells[2].find("a")
                pdf = "https://www.bseindia.com" + link["href"] if link else "N/A"
                records.append({
                    "Date": cells[0].get_text(strip=True),
                    "Subject": cells[1].get_text(strip=True),
                    "PDF_Link": pdf
                })
        
        if records:
            df = pd.DataFrame(records)
            filename = f"BSEIndexMediaRelease_{date.today()}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 1: {e}")
    return None, None

# ==========================================
# 2. BSE Media Release
# ==========================================
def code_2_bse_media():
    target_date_str = date.today().strftime("%B %d, %Y")
    URL = "https://www.bseindia.com/markets/MarketInfo/MediaRelease.aspx"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}

    try:
        response = requests.get(URL, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        if len(tables) < 2: return None, None

        records = []
        for row in tables[1].find_all("tr"):
            cells = row.find_all("td")
            if len(cells) != 3: continue
            
            if cells[0].get_text(strip=True) == target_date_str:
                subj_cell = cells[1]
                link = subj_cell.find("a")
                subject = link.get_text(strip=True) if link else subj_cell.get_text(strip=True)
                pdf = "https://www.bseindia.com" + link["href"] if link else ""
                
                records.append({
                    "Date": cells[0].get_text(strip=True),
                    "Subject": subject,
                    "Category": cells[2].get_text(strip=True),
                    "PDF_Link": pdf
                })

        if records:
            df = pd.DataFrame(records)
            filename = f"BSEMediaRelease_{date.today()}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 2: {e}")
    return None, None

# ==========================================
# 3. BSE Notices
# ==========================================
def code_3_bse_notices():
    # Only scrapes first page, recent notices
    url = "https://www.bseindia.com/markets/MarketInfo/NoticesCirculars.aspx?id=0&txtscripcd=&pagecont=&subject="
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "ContentPlaceHolder1_GridView1"})
        if not table: return None, None

        records = []
        # Check date format used by BSE Notices (usually dd/MM/yyyy inside the table isn't shown, so we fetch all recent)
        # Assuming we just want today's run to capture latest. 
        # Logic: Scrape all on page 1, let BigQuery handle duplicates or just append.
        
        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 6: continue
            
            # Helper for links
            def get_link(cell):
                a = cell.find("a")
                if a and a.get("href"):
                    l = a["href"].replace("../", "")
                    return f"https://www.bseindia.com/markets/MarketInfo/{l}" if not l.startswith("http") else l
                return "N/A"

            records.append({
                "Notice_No": cols[0].get_text(strip=True),
                "Subject": cols[1].get_text(strip=True),
                "Link": get_link(cols[1]),
                "Segment": cols[2].get_text(strip=True),
                "Category": cols[3].get_text(strip=True),
                "Department": cols[4].get_text(strip=True),
                "PDF": get_link(cols[5])
            })

        if records:
            df = pd.DataFrame(records)
            filename = f"BSE_Notices_{date.today()}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 3: {e}")
    return None, None

# ==========================================
# 4. SEBI News
# ==========================================
def code_4_sebi():
    url = "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingAll=yes"
    target_date = date.today().strftime("%b %d, %Y")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36"}

    try:
        response = requests.get(url, headers=headers, timeout=20)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        records = []
        for table in soup.find_all("table"):
            if "Date" in table.text and "Title" in table.text:
                for row in table.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 3:
                        if cols[0].text.strip() == target_date:
                            records.append({
                                "Date": cols[0].text.strip(),
                                "Type": cols[1].text.strip(),
                                "Title": cols[2].text.strip()
                            })
                break 

        if records:
            df = pd.DataFrame(records)
            filename = f"SEBI_News_{date.today()}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 4: {e}")
    return None, None

# ==========================================
# 5. NSE Exchange Circulars
# ==========================================
def code_5_nse_circulars():
    target_date = date.today().strftime("%B %d, %Y")
    API_URL = "https://www.nseindia.com/api/circulars"
    BASE_URL = "https://www.nseindia.com"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.nseindia.com/resources/exchange-communication-circulars"
    }

    session = requests.Session()
    session.headers.update(headers)
    
    try:
        session.get(BASE_URL, timeout=10) # Authorize
        time.sleep(1)
        response = session.get(API_URL, timeout=10)
        data_list = response.json().get('data', [])
        
        records = []
        for item in data_list:
            if item.get('cirDisplayDate', '').strip() == target_date:
                records.append({
                    "DATE": item.get('cirDisplayDate', ''),
                    "SUBJECT": item.get('sub', ''),
                    "DEPARTMENT": item.get('circDepartment', ''),
                    "CIRCULAR_NO": item.get('circDisplayNo', ''),
                    "ATTACHMENT": item.get('circFilelink', ''),
                    "FILE_SIZE": item.get('circFileSize', '')
                })

        if records:
            df = pd.DataFrame(records)
            filename = f"NSE_Circulars_{date.today()}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 5: {e}")
    return None, None

# ==========================================
# 6. NSE Press Release
# ==========================================
def code_6_nse_press():
    target_str = date.today().strftime("%d-%m-%Y")
    # API requires d-m-Y format
    api_url = f"https://www.nseindia.com/api/press-release-cms20?fromDate={target_str}&toDate={target_str}"
    base_url = "https://www.nseindia.com"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.nseindia.com/resources/exchange-communication-press-releases',
    }

    session = requests.Session()
    session.headers.update(headers)

    try:
        session.get(base_url, timeout=15)
        time.sleep(1)
        response = session.get(api_url, timeout=15)
        
        records = []
        if response.status_code == 200:
            data = response.json()
            for item in data:
                content = item.get('content', {})
                # Clean subject html
                raw_sub = content.get('body', '')
                sub_clean = BeautifulSoup(raw_sub, 'html.parser').get_text(strip=True) if raw_sub else ""
                
                att = content.get('field_file_attachement', {})
                att_url = att.get('url', '') if isinstance(att, dict) else ''

                records.append({
                    'DATE': content.get('field_date', ''),
                    'SUBJECT': sub_clean,
                    'DEPARTMENT': content.get('title', ''),
                    'ATTACHMENT': att_url
                })

        if records:
            df = pd.DataFrame(records)
            filename = f"NSE_PressRelease_{target_str}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 6: {e}")
    return None, None

# ==========================================
# 7. NSDL Circulars
# ==========================================
def code_7_nsdl():
    NSDL_URL = "https://nsdl.co.in/business/circular_stat.php"
    target_date = date.today().strftime("%d %B , %Y") # Format: 12 December , 2025
    target_match = date.today().strftime("%d %B") # For loose matching

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        response = requests.get(NSDL_URL, headers=headers, verify=False, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        container = soup.find('div', class_='right-panel')
        if not container: return None, None

        records = []
        for row in container.find('table').find_all('tr'):
            cols = row.find_all('td')
            if len(cols) >= 3:
                raw_date = cols[0].text.strip()
                if "Select Year" in raw_date: continue
                
                # Check if today's date is in the text
                if target_match.lower() in raw_date.lower():
                    link_tag = cols[2].find('a')
                    link = f"https://nsdl.co.in{link_tag.get('href')}" if link_tag else "N/A"
                    
                    records.append({
                        "Date": " ".join(raw_date.split()),
                        "Status": " ".join(cols[1].text.strip().split()),
                        "Subject": " ".join(cols[2].text.strip().split()),
                        "Link": link
                    })

        if records:
            df = pd.DataFrame(records)
            filename = f"NSDL_Circulars_{date.today()}.csv"
            df.to_csv(filename, index=False)
            return df, filename
    except Exception as e:
        print(f"Error Code 7: {e}")
    return None, None


# ==========================================
# MAIN EXECUTION LIST
# ==========================================
if __name__ == "__main__":
    print(f"--- Starting Daily Run: {date.today()} ---")
    
    tasks = [
        ("BSE Index Media", code_1_bse_index, "bse_index_media"),
        ("BSE Media Release", code_2_bse_media, "bse_media_release"),
        ("BSE Notices", code_3_bse_notices, "bse_notices"),
        ("SEBI News", code_4_sebi, "sebi_news"),
        ("NSE Circulars", code_5_nse_circulars, "nse_circulars"),
        ("NSE Press", code_6_nse_press, "nse_press"),
        ("NSDL Circulars", code_7_nsdl, "nsdl_circulars"),
    ]

    report_log = []
    generated_files = []

    for name, func, table_id in tasks:
        print(f"\nRunning {name}...")
        try:
            df, filename = func()
            
            if df is not None and not df.empty:
                status = f"✅ {name}: Generated {len(df)} rows."
                report_log.append(status)
                generated_files.append(filename)
                
                # Upload
                upload_to_bigquery(df, table_id)
            else:
                status = f"❌ {name}: No output generated for today."
                report_log.append(status)
                
        except Exception as e:
            err = f"⚠️ {name}: Failed with error - {str(e)}"
            report_log.append(err)
            print(err)

    # Send Email
    send_email(report_log, generated_files)
