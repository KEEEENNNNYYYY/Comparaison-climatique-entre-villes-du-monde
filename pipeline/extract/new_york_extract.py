from datetime import datetime
import os
import json
from meteostat import Point, Daily
import pandas as pd

# Coordonnées de New York City, USA
new_york = Point(40.7128, -74.0060)

# Définition du dossier de sortie
base_path = os.path.abspath(os.path.dirname(__file__))
target_folder = os.path.normpath(os.path.join(base_path, '../../data/data_brut/new_york-20-25'))


def fetch_and_save_history(start_date: str = "2020-01-01", output_dir: str = target_folder):
    """Récupère les données historiques Meteostat pour New York et les enregistre dans un fichier JSON par jour"""

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.now()

        os.makedirs(output_dir, exist_ok=True)

        # Récupération des données météo
        data = Daily(new_york, start, end).fetch()

        if data.empty:
            print(" Aucune donnée récupérée.")
            return

        for date, row in data.iterrows():
            filename = f"new_york_{date.strftime('%Y-%m-%d')}.json"
            path = os.path.join(output_dir, filename)

            # Convertit les valeurs manquantes en None (pour JSON)
            row_cleaned = row.where(pd.notnull(row), None)

            weather_info = {
                "city": "New York",
                "date": date.strftime("%Y-%m-%d"),
                "temperature_avg": row_cleaned.get("tavg"),
                "temperature_min": row_cleaned.get("tmin"),
                "temperature_max": row_cleaned.get("tmax"),
                "precipitation": row_cleaned.get("prcp"),
                "snow": row_cleaned.get("snow"),
                "wind_speed": row_cleaned.get("wspd"),
                "humidity": row_cleaned.get("rhum"),
            }

            with open(path, "w") as f:
                json.dump(weather_info, f, indent=2)

            print(f" Données sauvegardées dans {path}")

    except Exception as e:
        print(f" Une erreur est survenue : {e}")
