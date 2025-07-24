import os
import re
import io
import requests
import zipfile
import pandas as pd
from datetime import datetime
from urllib.parse import unquote, parse_qs, urlparse
import glob

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import gspread

# --- SETTINGS ---
GMAIL_QUERY = 'from:(Inventory@aspecwheels.com OR data@wheelpros.com) subject:"INVENTORY FEED IS READY"'
TARGET_CSV = 'wheelInvPriceData.csv'
SPREADSHEET_ID = '1lARcmMmdfrHePJk8bhOz2_ww1WZuY0cFW_-8HMJGSBo'
SHEET_NAME = 'Sheet1'
LOG_SHEET_NAME = 'Log'
DOWNLOAD_DIR = r'C:\pythonScripts\Wheelpros\Download'

SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/spreadsheets'
]

def gmail_authenticate():
    # --- Authenticate to Gmail API, store/refresh token.json ---
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def get_latest_zip_link_from_gmail(service):
    # --- Step 1: Get download link and email ID from latest email ---
    results = service.users().messages().list(userId='me', q=GMAIL_QUERY, maxResults=1).execute()
    messages = results.get('messages', [])
    if not messages:
        raise Exception("No matching emails found.")
    msg_id = messages[0]['id']
    msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
    parts = msg['payload'].get('parts', [])
    body = ""
    if parts:
        for part in parts:
            if part['mimeType'] == 'text/plain':
                body = part['body'].get('data', '')
                break
            elif part['mimeType'] == 'text/html':
                body = part['body'].get('data', '')
        import base64
        if body:
            body = base64.urlsafe_b64decode(body).decode('utf-8')
    else:
        import base64
        body = base64.urlsafe_b64decode(msg['payload']['body']['data']).decode('utf-8')
    match = re.search(r'https://backend\.api\.data\.wheelpros\.com/prod/feed/download[^"\'<\s]+', body)
    if not match:
        raise Exception("Download link not found in email.")
    return match.group(0), msg_id

def get_or_create_log_sheet(spreadsheet):
    try:
        log_sheet = spreadsheet.worksheet(LOG_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows="1000", cols="7")
        log_sheet.append_row([
            "Upload Time",
            "File Name",
            "Rows",
            "Sheet Range",
            "Status",
            "TotalQOH",
            "Change from Previous Day"
        ])
    return log_sheet

def log_upload(log_sheet, upload_time, file_name, row_count, range_str, status_msg, total_qoh):
    log_sheet.append_row([
        upload_time,
        file_name,
        row_count,
        range_str,
        status_msg,
        total_qoh,
        ""  # Change from previous day, computed below
    ])

def sort_log_sheet(log_sheet):
    values = log_sheet.get_all_values()
    if len(values) <= 1:
        return  # Only header
    data = values[1:]
    data.sort(key=lambda r: r[0], reverse=True)
    log_sheet.clear()
    log_sheet.append_row(values[0])
    for row in data:
        log_sheet.append_row(row)

def compute_daily_change(log_sheet):
    # --- Step 6: Compute 'Change from Previous Day' in log ---
    values = log_sheet.get_all_values()
    if len(values) < 3:
        return
    totalqoh_col = 5
    change_col = 6

    for i in range(1, len(values)):
        try:
            today_qoh = float(values[i][totalqoh_col])
        except:
            today_qoh = None

        prev_qoh = None
        for j in range(i + 1, len(values)):
            try:
                prev_val = float(values[j][totalqoh_col])
                prev_qoh = prev_val
                break
            except:
                continue

        diff = today_qoh - prev_qoh if (today_qoh is not None and prev_qoh is not None) else ""
        log_sheet.update_cell(i + 1, change_col + 1, diff)

def strip_leading_zeros(val):
    # Remove leading zeros for numeric-only PartNumbers
    if isinstance(val, str) and val.isdigit():
        return str(int(val))
    return val

def main():
    creds = gmail_authenticate()
    gmail_service = build('gmail', 'v1', credentials=creds)
    gs_client = gspread.authorize(creds)
    spreadsheet = gs_client.open_by_key(SPREADSHEET_ID)

    print("Looking for latest Wheel Pros inventory email...")
    # --- Step 1: Get download link and email ID from latest email ---
    zip_url, msg_id = get_latest_zip_link_from_gmail(gmail_service)
    print("Found ZIP link:", zip_url)

    print("Downloading ZIP...")
    resp = requests.get(zip_url)
    if resp.status_code != 200:
        raise Exception(f"Failed to download ZIP: {resp.status_code}")

    # --- Step 2: Download and save the ZIP with fallback filename ---
    now = datetime.now().strftime('%m-%d-%Y_%I-%M-%S-%p')
    file_name = f"wheelpros_{now}"
    zip_filename = f"{file_name}.zip"
    zip_path = os.path.join(DOWNLOAD_DIR, zip_filename)

    print(f"Saving ZIP as: {zip_filename}")

    with open(zip_path, 'wb') as f:
        f.write(resp.content)
    print(f"Saved ZIP to {zip_path}")

    # --- Step 3: Prune old ZIPs, keep only the latest 10 ---
    zip_files = sorted(
        glob.glob(os.path.join(DOWNLOAD_DIR, '*.zip')),
        key=os.path.getctime,
        reverse=True
    )
    for old_file in zip_files[10:]:
        try:
            os.remove(old_file)
            print(f"Deleted old ZIP: {old_file}")
        except Exception as e:
            print(f"Failed to delete {old_file}: {e}")

    print("Extracting CSV from ZIP...")
    # --- Step 4: Extract CSV from ZIP ---
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        filelist = z.namelist()
        print("Files in ZIP:", filelist)
        target_path = next((f for f in filelist if f.lower().endswith('/' + TARGET_CSV.lower()) or f.lower() == TARGET_CSV.lower()), None)
        if not target_path:
            raise Exception(f"{TARGET_CSV} not found in ZIP!")
        with z.open(target_path) as csvfile:
            df = pd.read_csv(csvfile)

    print("Columns in CSV:", df.columns.tolist())
    needed_columns = ['PartNumber', 'PartDescription', 'TotalQOH']
    missing = [col for col in needed_columns if col not in df.columns]
    if missing:
        raise Exception(f"Missing columns in CSV: {missing}")
    df_small = df[needed_columns].fillna('')
    df_small['PartNumber'] = df_small['PartNumber'].apply(strip_leading_zeros)

    print(f"Uploading {len(df_small)} rows and {len(df_small.columns)} columns to Google Sheets...")
    # --- Step 5: Upload to Google Sheets ---
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    worksheet.clear()
    worksheet.update([df_small.columns.values.tolist()] + df_small.values.tolist())
    print("Upload to Sheet1 done.")

    # --- Step 6: Log import to Log sheet ---
    log_sheet = get_or_create_log_sheet(spreadsheet)
    upload_time = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    file_name_with_date = f"WheelPros Email – {upload_time}"
    row_count = len(df_small)
    col_count = len(df_small.columns)
    last_row = row_count + 1
    col_letter = chr(64 + col_count) if col_count <= 26 else f"Z"
    range_str = f"A2:{col_letter}{last_row}"
    status_msg = "✅ wheelInvPriceData.csv imported successfully."
    total_qoh = float(df_small['TotalQOH'].apply(pd.to_numeric, errors='coerce').sum())

    log_upload(log_sheet, upload_time, file_name_with_date, row_count, range_str, status_msg, total_qoh)
    sort_log_sheet(log_sheet)
    compute_daily_change(log_sheet)
    print("Log updated.")

    # --- Step 7: Mark email as read and archive ---
    gmail_service.users().messages().modify(
        userId='me',
        id=msg_id,
        body={'removeLabelIds': ['INBOX', 'UNREAD']}
    ).execute()
    print("Archived and marked email as read.")

if __name__ == '__main__':
    main()
