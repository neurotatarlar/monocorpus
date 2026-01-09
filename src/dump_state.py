from utils import get_engine, get_in_workdir
import pandas as pd
from rich import print
from google.oauth2.credentials import Credentials
import gspread
from gspread.exceptions import WorksheetNotFound
import os
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
import csv
import zipfile
from rich.progress import track
from models import Document, DocumentCrh



# The OAuth 2.0 scopes we need.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
shared_folder_id = "1WFYCcbrtKGv3KTwyKdcKHKxXwmr9iFHE"
spread_sheet_id = "1qHkn0ZFObgUZtQbPXtdbXa1Bf0UWPKjsyuhOZCTyNGQ"
worksheet_monocorpus = "tt"
worksheet_monocorpus_crh = "crh"

def dump():
    csv_path = None
    csv_path_crh = None
    zip_path = None
    try:
        csv_path = get_in_workdir(file = "monocorpus_backup.csv")
        csv_path_crh = get_in_workdir(file = "monocorpus_crh_backup.csv")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
        title = f"monocorpus_{timestamp}"
        
        print("Dumping `document` table to CSV...")
        df = _dump_table_to_csv(csv_path, Document)
        print(f"✅ Exported {len(df)} rows to {csv_path}")

        print("Dumping `document_crh` table to CSV...")
        df_crh = _dump_table_to_csv(csv_path_crh, DocumentCrh)
        print(f"✅ Exported {len(df_crh)} rows to {csv_path_crh}")
        
        print("Creating ZIP archive...")
        zip_path = get_in_workdir(file = "monocorpus_backup.zip")
        zip(csv_path, zip_path, title)
        
        # Authorize gspread with service account credentials
        creds = _get_credentials()
        
        _export_to_gdrive(zip_path, creds, title) 
        print(f"✅ Uploaded to Google Drive")
        
        print("Exporting to Google Sheets (main corpus)...")
        _export_to_gsheets(csv_path, creds, worksheet_monocorpus)
        print(f"✅ Exported to Google Sheets: {worksheet_monocorpus}")

        print("Exporting to Google Sheets (Crimean Tatar corpus)...")
        _export_to_gsheets(csv_path_crh, creds, worksheet_monocorpus_crh)
        print(f"✅ Exported to Google Sheets: {worksheet_monocorpus_crh}")
    finally:
        for path in (csv_path, csv_path_crh, zip_path):
            if path and os.path.exists(path):
                os.remove(path)
            
            
def zip(csv_path, zip_path, title):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, arcname=f"{title}.csv")

    
def _export_to_gsheets(csv_path, creds, worksheet_name, chunk_size=1000):
    gc = gspread.authorize(creds)  # same creds as above
    sh = gc.open_by_key(spread_sheet_id) 
    worksheet = _get_or_create_worksheet(sh, worksheet_name)

    worksheet.clear()
    
    # Read CSV as raw strings
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        data = list(reader)

    for start in track(range(0, len(data), chunk_size), description="Uploading to Google Sheets..."):
        end = start + chunk_size
        chunk = data[start:end]
        worksheet.update(f"A{start + 1}", chunk)
        
    # set text wrapping and number format for all cells
    wrap_request = {
        "requests": [
            {
                "repeatCell": {

                    "range": {
                        "sheetId": worksheet.id
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "CLIP",  # Options: WRAP, OVERFLOW_CELL, CLIP
                            "numberFormat": {"type": "NUMBER", "pattern": "0"}
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,numberFormat)"
                }
            }
        ]
    }

    sh.batch_update(wrap_request)


def _get_or_create_worksheet(sh, title):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        return sh.add_worksheet(title=title, rows="40000", cols="25")


def _dump_table_to_csv(output_path, model):
    """Dump a PostgreSQL table to CSV."""
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {model.__tablename__} where language in ('crh-Latn', 'crh-Cyrl', 'crh-Latn-x-yanalif', 'crh-Arab') or meta is NULL ORDER BY ya_path", engine)
    df = df.convert_dtypes()
    df.to_csv(output_path, index=False)
    return df
    
    
def _export_to_gdrive(zip_path, creds, title):
    # Create a new sheet with timestamp

    drive_service = build('drive', 'v3', credentials=creds)
    
    file_name = f"{title}.zip"
    file_metadata = {
        'name': file_name,
        'mimetype': 'application/zip',
        'parents': [shared_folder_id]  # the folder
    }   
    
    media = MediaFileUpload(zip_path, mimetype='application/zip', resumable=True)
    
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



