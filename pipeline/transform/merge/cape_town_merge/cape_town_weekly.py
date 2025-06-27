import os
import json
import pandas as pd

def merge_weekly_averages():
    # Chemin du dossier source contenant les fichiers journaliers JSON
    source_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../../data/data_propre/cape_town-20-25')
    )
    # Chemin de sortie pour le fichier de résultats hebdomadaires
    output_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../../data/merge/cape_town_weekly.json')
    )

    records = []

    # Lecture de tous les fichiers JSON du dossier
    for filename in os.listdir(source_folder):
        if filename.endswith('.json'):
            file_path = os.path.join(source_folder, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if 'date' not in data:
                    continue

                data['date'] = pd.to_datetime(data['date'])
                records.append(data)

            except Exception as e:
                print(f"Erreur lors de la lecture de {filename} : {e}")
                continue

    if not records:
        print("Aucune donnée trouvée.")
        return

    # Création du DataFrame
    df = pd.DataFrame(records)

    # Ajout de la semaine ISO pour regrouper
    df['week'] = df['date'].dt.isocalendar().week
    df['year'] = df['date'].dt.year

    # Groupement par année + semaine, puis moyenne
    grouped = df.groupby(['year', 'week']).agg({
        'temperature_avg': 'mean',
        'temperature_min': 'mean',
        'temperature_max': 'mean',
        'precipitation': 'mean',
        'snow': 'mean',
        'wind_speed': 'mean',
        'humidity': 'mean'
    }).reset_index()

    # Fusion des colonnes année + semaine en un champ lisible
    grouped['week_id'] = grouped['year'].astype(str) + '-W' + grouped['week'].astype(str).str.zfill(2)
    grouped = grouped.drop(columns=['year', 'week'])

    # Réorganiser les colonnes
    cols = ['week_id'] + [col for col in grouped.columns if col != 'week_id']
    grouped = grouped[cols]

    # Sauvegarde du résultat
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    grouped.to_json(output_path, orient='records', indent=2)

    print(f"Fichier des moyennes hebdomadaires sauvegardé ici : {output_path}")


# Pour exécution directe
if __name__ == '__main__':
    merge_weekly_averages()
