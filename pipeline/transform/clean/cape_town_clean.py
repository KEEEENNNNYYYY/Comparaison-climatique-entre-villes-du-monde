import os
import json

# Dossiers source et destination
source_folder = '/home/unity/airflow/data/data_brut/cape_town-23-25'
destination_folder = '/home/unity/airflow/data/data_propre/cape_town-23-25'

# Parcours de tous les fichiers JSON dans le dossier source
for filename in os.listdir(source_folder):
    if filename.endswith('.json'):
        source_path = os.path.join(source_folder, filename)
        destination_path = os.path.join(destination_folder, filename)

        # Lecture du contenu JSON
        with open(source_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Remplacement des valeurs null par 0.0
        cleaned_data = {
            key: (0.0 if value is None else value)
            for key, value in data.items()
        }

        # Écriture du fichier nettoyé dans le dossier destination
        with open(destination_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

print("Nettoyage terminé. Fichiers propres enregistrés dans :", destination_folder)
