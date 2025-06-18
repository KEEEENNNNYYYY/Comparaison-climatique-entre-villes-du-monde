import requests
import os
from datetime import datetime
import json

# Clé API (à garder secrète dans la vraie vie !)
API_KEY = "96967120a2c94520738e8cd813a82b2a"

# URL de base pour les données météo actuelles
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def build_url(city_name: str, country_code: str = "FR") -> str:
    """Construit l'URL d'appel à l'API OpenWeather pour une ville donnée"""
    return f"{BASE_URL}?q={city_name},{country_code}&appid={API_KEY}&units=metric"

def fetch_weather_data(city_name: str, country_code: str = "FR") -> dict:
    """Fait une requête à l'API OpenWeather et retourne les données météo brutes"""
    url = build_url(city_name, country_code)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erreur API pour {city_name}: {response.status_code} - {response.text}")

def parse_weather_data(data: dict) -> dict:
    """Extrait et structure les infos météo principales depuis la réponse JSON"""
    return {
        "city": data.get("name"),
        "timestamp": datetime.utcfromtimestamp(data.get("dt")),
        "temperature": data["main"].get("temp"),
        "humidity": data["main"].get("humidity"),
        "pressure": data["main"].get("pressure"),
        "weather": data["weather"][0].get("description"),
        "wind_speed": data["wind"].get("speed"),
        "clouds": data["clouds"].get("all")
    }

def save_raw_data(city_name: str, parsed_data: dict, output_dir: str = "data/raw") -> None:
    """Sauvegarde les données météo extraites dans un fichier JSON"""
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{city_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path = os.path.join(output_dir, filename)
    with open(path, "w") as f:
        json.dump(parsed_data, f, indent=2)
    print(f" Données météo sauvegardées pour {city_name} dans {path}")
