import os
import re
import io
import requests
import zipfile
import pandas as pd
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import gspread

# --- SETTINGS ---
GMAIL_QUERY = 'from:data@wheelpros.com subject:"INVENTORY FEED IS READY"'
TARGET_CSV = 'wheelInvPriceData.csv'
SPREADSHEET_ID = '1lARcmMmdfrHePJk8bhOz2_ww1WZuY0cFW_-8HMJGSBo'  # <-- Your provided sheet
SHEET_NAME = 'Sheet1'
LOG_SHEET_NAME = 'Log'

SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/spreadsheets'
]

def gmail_authenticate():
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
    # Search for messages
    results = service.users().messages().list(userId='me', q=GMAIL_QUERY, maxResults=1).execute()
    messages = results.get('messages', [])
    if not messages:
        raise Exception("No matching emails found.")
    msg_id = messages[0]['id']

    # Get the message details
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
        # Fallback: raw body
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
        log_sheet = spreadsheet.add_worksheet(title=LOG_SHEET_NAME, rows="1000", cols="6")
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
        ""  # Change from previous day, computed later
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

def main():
    # --- Authenticate Gmail and Sheets ---
    creds = gmail_authenticate()
    gmail_service = build('gmail', 'v1', credentials=creds)
    gs_client = gspread.authorize(creds)
    spreadsheet = gs_client.open_by_key(SPREADSHEET_ID)

    # --- Step 1: Get download link and email ID from latest email ---
    print("Looking for latest Wheel Pros inventory email...")
    zip_url, msg_id = get_latest_zip_link_from_gmail(gmail_service)
    print("Found ZIP link:", zip_url)

    # --- Step 2: Download ZIP ---
    print("Downloading ZIP...")
    resp = requests.get(zip_url)
    if resp.status_code != 200:
        raise Exception(f"Failed to download ZIP: {resp.status_code}")

    # --- Step 3: Extract CSV from ZIP ---
    print("Extracting CSV from ZIP...")
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

    def strip_leading_zeros(val):
        if isinstance(val, str) and val.isdigit():
            return str(int(val))
        return val

    df_small['PartNumber'] = df_small['PartNumber'].apply(strip_leading_zeros)


    # --- Step 4: Upload to Sheet1 ---
    print(f"Uploading {len(df_small)} rows and {len(df_small.columns)} columns to Google Sheets...")
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    worksheet.clear()
    worksheet.update([df_small.columns.values.tolist()] + df_small.values.tolist())
    print("Upload to Sheet1 done.")

    # --- Step 5: Log the upload to Log sheet ---
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

    # --- Step 6: Archive and mark the email as read ---
    gmail_service.users().messages().modify(
        userId='me',
        id=msg_id,
        body={'removeLabelIds': ['INBOX', 'UNREAD']}
    ).execute()
    print("Archived and marked email as read.")

if __name__ == '__main__':
    main()
