from datetime import datetime
import os
import json
from meteostat import Point, Daily
import pandas as pd


# Coordonnées pour Paris
paris = Point(48.8566, 2.3522)

def fetch_and_save_history(start_date: str = "2023-05-19", output_dir: str = "/home/unity/airflow/data"):
    """Récupère les données historiques Meteostat et les enregistre dans un fichier par jour"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.now()

    os.makedirs(output_dir, exist_ok=True)

    # Récupération des données
    data = Daily(paris, start, end).fetch()

    for date, row in data.iterrows():
        filename = f"paris_{date.strftime('%Y-%m-%d')}.json"
        path = os.path.join(output_dir, filename)

        # Convertit les NA en None pour que JSON les accepte
        row_cleaned = row.where(pd.notnull(row), None)

        weather_info = {
            "city": "Paris",
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

        print(f"[✔] Données sauvegardées dans {path}")
