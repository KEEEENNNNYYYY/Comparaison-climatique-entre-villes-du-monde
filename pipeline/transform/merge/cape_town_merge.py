import os
import json
import pandas as pd

def merge_all_json_to_csv():
    source_folder = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../../data/data_propre/cape_town-20-25')
    )
    output_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../../../data/data_pret/cape_town.csv')
    )

    records = []

    # Lecture de tous les fichiers JSON
    for filename in os.listdir(source_folder):
        if filename.endswith('.json'):
            file_path = os.path.join(source_folder, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if 'date' not in data:
                    continue

                # Conversion de la date
                data['date'] = pd.to_datetime(data['date'])

                # Nettoyage des valeurs nulles
                for key in ['temperature_avg', 'temperature_min', 'temperature_max', 'precipitation', 'snow', 'wind_speed', 'humidity']:
                    if key not in data or data[key] is None:
                        data[key] = 0.0

                records.append(data)

            except Exception as e:
                print(f"Erreur lors de la lecture de {filename} : {e}")
                continue

    if not records:
        print("Aucune donnée trouvée.")
        return

    df = pd.DataFrame(records)

    # Conversion de la date en format ISO (YYYY-MM-DD)
    df['date'] = df['date'].dt.date

    # Sauvegarde directe sans groupement
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')

    print(f" Tous les fichiers JSON ont été fusionnés ici : {output_path}")

if __name__ == '__main__':
    merge_all_json_to_csv()
