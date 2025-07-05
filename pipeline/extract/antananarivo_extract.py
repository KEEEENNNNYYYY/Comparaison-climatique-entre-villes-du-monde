from datetime import datetime
import os
import json
from meteostat import Point, Daily
import pandas as pd

# Coordonnées d'Antananarivo, Madagascar
antananarivo = Point(-18.8792, 47.5079)

def fetch_and_save_history(start_date: str = "2020-01-01", output_dir: str = "./data"):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.now()

        os.makedirs(output_dir, exist_ok=True)

        data = Daily(antananarivo, start, end).fetch()

        if data.empty:
            print(" Aucune donnée récupérée.")
            return

        for date, row in data.iterrows():
            filename = f"antananarivo_{date.strftime('%Y-%m-%d')}.json"
            path = os.path.join(output_dir, filename)
            row_cleaned = row.where(pd.notnull(row), None)

            weather_info = {
                "city": "Antananarivo",
                "date": date.strftime("%Y-%m-%d"),
                "temperature_avg": row_cleaned.get("tavg"),
                "temperature_min": row_cleaned.get("tmin"),
                "temperature_max": row_cleaned.get("tmax"),
                "precipitation": row_cleaned.get("prcp"),
                "snow": row_cleaned.get("snow"),
                "wind_speed": row_cleaned.get("wspd"),
                "humidity": row_cleaned.get("rhum"),
            }

            with open(path, "w", encoding="utf-8") as f:
                json.dump(weather_info, f, indent=2)

            print(f" Données sauvegardées dans {path}")

    except Exception as e:
        print(f" Une erreur est survenue : {e}")
