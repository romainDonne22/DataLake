from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import requests
import zipfile
import sys
import os

# Recuperer la date du jour
today = datetime.today().strftime('%Y-%m-%d')  # Format de la date : '2025-02-12'

# Fonction pour télécharger le fichier ZIP
def download_zip_file(url, zip_filename):
    print("Téléchargement du fichier ZIP via REST API...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(zip_filename, 'wb') as f:
            f.write(response.content)
        print(f"Fichier {zip_filename} téléchargé avec succès.")
    else:
        raise Exception(f"Erreur lors du téléchargement (code {response.status_code}).")

# Fonction pour extraire le fichier ZIP
def extract_zip_file(zip_filename, extracted_folder):
    print(f"Extraction du fichier ZIP {zip_filename}...")
    os.makedirs(extracted_folder, exist_ok=True)  # Création du dossier avec la date du jour sous 'deputes_data'
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)  # Extraction dans le dossier structuré par date
    print(f"Extraction du fichier ZIP dans {extracted_folder} réussie.")

# Fonction pour supprimer le fichier ZIP après extraction
def delete_zip_file(zip_filename):
    print(f"Suppression du fichier ZIP {zip_filename}...")
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"Le fichier ZIP {zip_filename} a été supprimé.")
    else:
        print(f"Le fichier ZIP {zip_filename} n'existe pas.")


def spark_json_to_parquet(deputes_folder, scrutins_folder):
    spark = SparkSession.builder \
        .appName("ConvertJSONToParquet") \
        .getOrCreate()

    # Chemins des fichiers JSON
    deputes_acteur_json_path = os.path.join(f"{deputes_folder}/json/acteur", "*.json")
    deputes_organe_json_path = os.path.join(f"{deputes_folder}/json/organe", "*.json")
    scrutins_json_path = os.path.join(f"{scrutins_folder}/json", "*.json")
    
    # Convertir deputes_acteur JSON en Parquet
    deputes_acteur_df = spark.read.json(deputes_acteur_json_path)
    deputes_acteur_parquet_path = os.path.join(f"{deputes_folder}/json/acteur", "parquet")
    deputes_acteur_df.write.parquet(deputes_acteur_parquet_path, mode="overwrite")
    deputes_acteur_df.unpersist() # Libérer la mémoire
    print("Conversion des fichiers JSON deputes_acteur en fichiers Parquet réussie.")

    # Convertir deputes_organe JSON en Parquet
    deputes_organe_df = spark.read.json(deputes_organe_json_path)
    deputes_organe_parquet_path = os.path.join(f"{deputes_folder}/json/organe", "parquet")
    deputes_organe_df.write.parquet(deputes_organe_parquet_path, mode="overwrite")
    deputes_organe_df.unpersist()
    print("Conversion des fichiers JSON deputes_organe en fichiers Parquet réussie.")

    # Convertir scrutins JSON en Parquet
    scrutins_df = spark.read.json(scrutins_json_path)
    scrutins_parquet_path = os.path.join(f"{scrutins_folder}/json", "parquet")
    scrutins_df.write.parquet(scrutins_parquet_path, mode="overwrite")
    scrutins_df.unpersist()
    print("Conversion des fichiers JSON scrutins en fichiers Parquet réussie.")
    print("Conversion de tous les fichiers JSON en fichiers Parquet réussie.")



# Définir le DAG Airflow
with DAG(
    dag_id="AssembleNationale",
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
    download_deputes_json_zip = PythonOperator(
        task_id='download_deputes_json_zip',
        python_callable=download_zip_file,
        op_kwargs={
            'url' : "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_mandats_actifs_organes/AMO10_deputes_actifs_mandats_actifs_organes.json.zip", # URL de l'API REST qui fournit le fichier ZIP
            'zip_filename' : "Deputes.json.zip" # Nom du fichier ZIP
        }
    )

    download_scrutin_json_zip = PythonOperator(
        task_id='download_scrutin_json_zip',
        python_callable=download_zip_file,
        op_kwargs={
            'url' : "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip",
            'zip_filename' : "Scrutins.json.zip"
        }
    )

    extract_deputes_json_zip = PythonOperator(
        task_id='extract_deputes_json_zip',
        python_callable=extract_zip_file,
        op_kwargs={
            'zip_filename' : "Deputes.json.zip",  # Nom du fichier ZIP
            'extracted_folder' : f"deputes_data/{today}",  # Le dossier 'deputes_data/{date}'
        }
    )

    extract_scrutin_json_zip = PythonOperator(
        task_id='extract_scrutin_json_zip',
        python_callable=extract_zip_file,
        op_kwargs={
            'zip_filename' : "Scrutins.json.zip",  # Nom du fichier ZIP
            'extracted_folder' : f"scrutins_data/{today}", 
        }
    )

    delete_deputes_json_zip = PythonOperator(
        task_id='delete_deputes_json_zip',
        python_callable=delete_zip_file,
        op_kwargs={
            'zip_filename' : "Deputes.json.zip"
        }
    )

    delete_scrutins_json_zip = PythonOperator(
        task_id='delete_scrutins_json_zip',
        python_callable=delete_zip_file,
        op_kwargs={
            'zip_filename' : "Scrutins.json.zip"
        }
    )

    # Tâche pour convertir les fichiers JSON en fichiers Parquet
    spark_json_to_parquet = PythonOperator(
        task_id='spark_json_to_parquet',
        python_callable=spark_json_to_parquet,
        op_kwargs={
            'deputes_folder' : f"deputes_data/{today}",
            'scrutins_folder' : f"scrutins_data/{today}"
        }
    )

    # Ordre d'exécution des tâches
    # download_deputes_json_zip \
    # >> extract_deputes_json_zip \
    # >> delete_deputes_json_zip \
    # >> download_scrutin_json_zip \
    # >> extract_scrutin_json_zip \
    # >> delete_scrutins_json_zip \
    # >> convert_json_to_parquet
    
