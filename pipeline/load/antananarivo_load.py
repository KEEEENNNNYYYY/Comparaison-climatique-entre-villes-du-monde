import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_or_create_sheet(service, sheet_name, folder_id):
    query = f"'{folder_id}' in parents and name = '{sheet_name}' and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])

    if files:
        spreadsheet_id = files[0]["id"]
        print(f" Google Sheets trouvé : {spreadsheet_id}")
    else:
        file_metadata = {
            "name": sheet_name,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id]
        }
        file = service.files().create(body=file_metadata, fields="id").execute()
        spreadsheet_id = file.get("id")
        print(f" Nouveau Google Sheets créé : {spreadsheet_id}")

    return spreadsheet_id

def write_df_to_sheet(service, spreadsheet_id, df, sheet_name='Sheet1'):
    sheets_service = service.spreadsheets()
    sheets_service.values().clear(spreadsheetId=spreadsheet_id, range=sheet_name).execute()

    values = [df.columns.tolist()] + df.values.tolist()
    sheets_service.values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    print(f" {len(df)} lignes écrites dans la feuille.")

def upload_csv_to_drive(service_account_path, file_path, file_name, folder_id):
    # Authentification
    creds = service_account.Credentials.from_service_account_file(
        service_account_path,
        scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    )
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    # Lire le CSV en DataFrame
    df = pd.read_csv(file_path)

    # Envoi vers Google Sheets
    sheet_name = os.path.splitext(file_name)[0]
    spreadsheet_id = get_or_create_sheet(drive_service, sheet_name, folder_id)
    write_df_to_sheet(sheets_service, spreadsheet_id, df)
