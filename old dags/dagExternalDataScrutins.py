from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta  
import requests
import zipfile
import json
import os

# Paramètres du DAG
url = "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip"
zip_filename = "Scrutins.json.zip"

# Ajouter la date du jour au nom du dossier
today = datetime.today().strftime('%Y-%m-%d')  # Format de la date : '2025-02-12'
extracted_folder = f"scrutins_data/{today}"  # Le dossier 'scrutins_data/{date}'
json_folder = os.path.join(extracted_folder, "json")

# Fonction pour télécharger le fichier ZIP
def download_zip_file():
    print("Téléchargement du fichier ZIP via REST API...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(zip_filename, 'wb') as f:
            f.write(response.content)
        print(f"Fichier {zip_filename} téléchargé avec succès.")
    else:
        raise Exception(f"Erreur lors du téléchargement (code {response.status_code}).")

# Fonction pour extraire le fichier ZIP
def extract_zip_file():
    print(f"Extraction du fichier ZIP {zip_filename}...")
    os.makedirs(extracted_folder, exist_ok=True)  # Création du dossier avec la date du jour sous 'scrutins_data'
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)  # Extraction dans le dossier structuré par date
    print(f"Extraction du fichier ZIP dans {extracted_folder} réussie.")

# Fonction pour charger les données JSON
def load_json_data():
    print(f"Chargement des données depuis {json_folder}...")
    data = []
    for filename in os.listdir(json_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(json_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data.append(json.load(f))
    if isinstance(data, list):
        print(f"Nombre d'éléments dans les données JSON : {len(data)}")
    else:
        print(f"Les données JSON ne sont pas au format liste. Structure : {type(data)}")

# Fonction pour supprimer le fichier ZIP après extraction
def delete_zip_file():
    print(f"Suppression du fichier ZIP {zip_filename}...")
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"Le fichier ZIP {zip_filename} a été supprimé.")
    else:
        print(f"Le fichier ZIP {zip_filename} n'existe pas.")

# Définir le DAG Airflow
with DAG(
    dag_id="dataDownload_scrutins",
    default_args={
        'owner': 'romain',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='Un DAG pour télécharger, extraire et charger des données JSON à partir d\'un fichier ZIP',
    schedule_interval=None,  # Pas de planification régulière, déclenché manuellement
    start_date=datetime(2025, 2, 7),
    catchup=False,
) as dag:

    # Tâches du DAG
    start_task = DummyOperator(task_id="start_task")
    download_task = PythonOperator(
        task_id='download_zip_file',
        python_callable=download_zip_file
    )
    extract_task = PythonOperator(
        task_id='extract_zip_file',
        python_callable=extract_zip_file
    )
    load_data_task = PythonOperator(
        task_id='load_json_data',
        python_callable=load_json_data
    )
    delete_zip_task = PythonOperator(
        task_id='delete_zip_file',
        python_callable=delete_zip_file
    )

    # Ordre d'exécution des tâches
    start_task >> download_task >> extract_task >> load_data_task >> delete_zip_task