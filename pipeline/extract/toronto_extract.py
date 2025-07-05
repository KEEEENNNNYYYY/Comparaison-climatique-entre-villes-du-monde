from datetime import datetime
import os
import json
from meteostat import Point, Daily
import pandas as pd

# Coordonnées pour toronto, Canada
toronto = Point(-33.9249, 18.4241)

def fetch_and_save_history(start_date: str = "2020-01-01", output_dir: str = "/home/unity/airflow/data/data_brut/toronto-20-25/"):
    """Récupère les données historiques Meteostat pour toronto et les enregistre dans un fichier par jour"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.now()

    os.makedirs(output_dir, exist_ok=True)

    # Récupération des données
    data = Daily(toronto, start, end).fetch()

    for date, row in data.iterrows():
        filename = f"toronto_{date.strftime('%Y-%m-%d')}.json"
        path = os.path.join(output_dir, filename)

        # Convertit les NA en None pour que JSON les accepte
        row_cleaned = row.where(pd.notnull(row), None)

        weather_info = {
            "city": "toronto",
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
