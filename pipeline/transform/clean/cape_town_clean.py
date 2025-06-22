import os
import json

def clean_and_write():
    base_path = os.path.abspath(os.path.dirname(__file__))
    source_folder = os.path.join(base_path, '../../../data/data_brut/cape_town-23-25')
    destination_folder = os.path.join(base_path, '../../../data/data_propre/cape_town-23-25')
    os.makedirs(destination_folder, exist_ok=True)

    for filename in os.listdir(source_folder):
        if filename.endswith('.json'):
            source_path = os.path.join(source_folder, filename)
            destination_path = os.path.join(destination_folder, filename)

            with open(source_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            cleaned_data = {
                key: (0.0 if value is None else value)
                for key, value in data.items()
            }

            with open(destination_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

    print("Nettoyage terminé. Fichiers propres enregistrés dans :", destination_folder)
