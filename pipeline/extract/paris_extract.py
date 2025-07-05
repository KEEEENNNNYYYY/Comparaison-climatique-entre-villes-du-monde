from datetime import datetime
import os
import json
from meteostat import Point, Daily
import pandas as pd

# Coordonnées géographiques de Paris
paris_point = Point(48.8566, 2.3522)

# Chemin de sortie dynamique basé sur l'emplacement du script
base_path = os.path.abspath(os.path.dirname(__file__))
target_folder = os.path.normpath(os.path.join(base_path, '../../data/data_brut/paris-20-25'))

def fetch_and_save_history(start_date: str = "2020-01-01", output_dir: str = target_folder):
    """
    Récupère les données météorologiques historiques de Paris à partir de la date donnée.
    Enregistre chaque jour sous forme de fichier JSON.
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.now()

        os.makedirs(output_dir, exist_ok=True)

        # Récupération des données journalières
        data = Daily(paris_point, start, end).fetch()

        if data.empty:
            print(" Aucune donnée météo disponible pour cette période.")
            return

        for date, row in data.iterrows():
            filename = f"paris_{date.strftime('%Y-%m-%d')}.json"
            path = os.path.join(output_dir, filename)

            # Nettoyage des NaN pour JSON
            row_cleaned = row.where(pd.notnull(row), None)

            weather_info = {
                "city": "Paris",
                "date": date.strftime("%Y-%m-%d"),
                "temperature_avg": row_cleaned.get("tavg", None),
                "temperature_min": row_cleaned.get("tmin", None),
                "temperature_max": row_cleaned.get("tmax", None),
                "precipitation": row_cleaned.get("prcp", None),
                "snow": row_cleaned.get("snow", None),
                "wind_speed": row_cleaned.get("wspd", None),
                "humidity": row_cleaned.get("rhum", None),
            }

            with open(path, "w", encoding="utf-8") as f:
                json.dump(weather_info, f, indent=2, ensure_ascii=False)

            print(f" Données sauvegardées dans {path}")

    except Exception as e:
        print(f" Une erreur est survenue : {e}")
