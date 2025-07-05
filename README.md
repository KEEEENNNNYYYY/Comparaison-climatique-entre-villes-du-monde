# 🌍 Comparaison climatique entre villes du monde

## 🎯 Objectif

Ce projet vise à construire un pipeline de données complet pour **comparer le climat de plusieurs grandes villes** à travers le monde.  
L'objectif est de **suivre, analyser et visualiser** des indicateurs météorologiques clés afin de :

- Étudier la stabilité ou la variabilité climatique entre les villes
- Identifier les villes les plus “extrêmes” ou les plus “stables”
- Mettre en évidence les tendances sur le long terme

---

## 🛠️ Technologies utilisées

- **Python 3.11**
- **Apache Airflow** – Automatisation des tâches ETL
- **OpenWeather API** – Données météo en temps réel
- **Données historiques** – Sources CSV / Json publiques 
- **Pandas / Numpy** – Traitement des données
- **Looker Studio** – Visualisation interactive

---

## 🧪 Pipeline ETL automatisé

Le pipeline est structuré et orchestré à l’aide de **DAG Airflow** :

```text
[extract_historic_data] ---> 
[extract_openweather_data] ---> 
[clean_and_merge_data] ---> 
[save_data_to_csv_or_db] ---> 
[update_dashboard]
