from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def upload_csv_to_drive(service_account_path, file_path, file_name, folder_id):
    # Authentification
    creds = service_account.Credentials.from_service_account_file(
        service_account_path,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=creds)

    # Vérifie si le fichier existe déjà
    query = f"'{folder_id}' in parents and name='{file_name}' and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])

    media = MediaFileUpload(file_path, mimetype='text/csv')

    if files:
        file_id = files[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"✅ Fichier mis à jour sur Drive (ID: {file_id})")
    else:
        file_metadata = {
            'name': file_name,
            'parents': [folder_id],
            'mimeType': 'text/csv'
        }
        uploaded = service.files().create(body=file_metadata, media_body=media).execute()
        print(f"✅ Nouveau fichier uploadé (ID: {uploaded['id']})")

if __name__ == '__main__':
    current_dir = os.path.dirname(__file__)
    upload_csv_to_drive(
        service_account_path=os.path.join(current_dir, 'service-account.json'),
        file_path=os.path.join(current_dir, '../../data/data_pret/cape_town.csv'),
        file_name='cape_town.csv',
        folder_id='1RNPT0k2C2ySy8r1XS9g7d-ykFjmHhOuc'
    )

