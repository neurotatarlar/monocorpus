from utils import get_engine, get_in_workdir
import pandas as pd
from rich import print
from google.oauth2.credentials import Credentials
import gspread
import os
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
import csv
from gspread_dataframe import set_with_dataframe
import time
from rich.progress import track



# The OAuth 2.0 scopes we need.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
shared_folder_id = "1WFYCcbrtKGv3KTwyKdcKHKxXwmr9iFHE"
spread_sheet_id = "1qHkn0ZFObgUZtQbPXtdbXa1Bf0UWPKjsyuhOZCTyNGQ"

def dump():
    try:
        output_path = get_in_workdir(file = "monocorpus_backup.csv")
        
        print("Dumping Document table to CSV...")
        df = _dump_to_csv(output_path)
        print(f"✅ Exported {len(df)} rows to {output_path}")
        
        # Authorize gspread with service account credentials
        creds = _get_credentials()
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
        title = f"monocorpus_{timestamp}"
        
        _export_to_gdrive(output_path, creds, title) 
        print(f"✅ Uploaded to Google Drive")
        
        print("Exporting to Google Sheets...")
        _export_to_gsheets(output_path, creds, title)
        print(f"✅ Exported to Google Sheets with title '{title}'")
    finally:
        if os.path.exists(output_path):
            os.remove(output_path)

    
def _export_to_gsheets(csv_path, creds, title, chunk_size=500):
    gc = gspread.authorize(creds)  # same creds as above
    sh = gc.open_by_key(spread_sheet_id) 
    worksheet = sh.add_worksheet(title=title, rows="100000", cols="30")
    worksheet.update_index(0)

    # Read CSV as raw strings
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        data = list(reader)
        

    for start in track(range(0, len(data), chunk_size), description="Uploading to Google Sheets..."):
        end = start + chunk_size
        chunk = data[start:end]
        worksheet.update(f"A{start + 1}", chunk)



def _dump_to_csv(output_path):
    """Dump a PostgreSQL table to CSV."""
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM Document", engine)
    df.to_csv(output_path, index=False)
    return df
    
    
def _export_to_gdrive(csv_path, creds, title):
    # Create a new sheet with timestamp

    drive_service = build('drive', 'v3', credentials=creds)
    
    file_name = f"{title}.csv"
    file_metadata = {
        'name': file_name,
        'mimetype': 'text/csv',
        'parents': [shared_folder_id]  # the folder
    }   
    
    media = MediaFileUpload(csv_path, mimetype='text/csv', resumable=True)
    
    _ = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    
def _get_credentials():
    token_file = "personal_token.json"
    
    if os.path.exists(token_file):
        return Credentials.from_authorized_user_file(token_file, SCOPES)
    
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
    with open(token_file, 'w') as f:
        f.write(creds.to_json())
    return Credentials.from_authorized_user_file(token_file, SCOPES)




