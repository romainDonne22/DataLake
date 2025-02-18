from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from Scripts import download_zip_file, extract_zip_file, delete_zip_file, spark_json_to_parquet, spark_save_to_postgress_scrutins, spark_save_to_postgress_deputesActeur, spark_save_to_postgress_deputesOrgane


# Recuperer la date du jour
today = datetime.today().strftime('%Y-%m-%d')  # Format de la date : '2025-02-12'

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

    # # Tâches du DAG
    # download_deputes_json_zip = PythonOperator(
    #     task_id='download_deputes_json_zip',
    #     python_callable=download_zip_file,
    #     op_kwargs={
    #         'url' : "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_mandats_actifs_organes/AMO10_deputes_actifs_mandats_actifs_organes.json.zip", # URL de l'API REST qui fournit le fichier ZIP
    #         'zip_filename' : "Deputes.json.zip" # Nom du fichier ZIP
    #     }
    # )

    # download_scrutin_json_zip = PythonOperator(
    #     task_id='download_scrutin_json_zip',
    #     python_callable=download_zip_file,
    #     op_kwargs={
    #         'url' : "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip",
    #         'zip_filename' : "Scrutins.json.zip"
    #     }
    # )

    # extract_deputes_json_zip = PythonOperator(
    #     task_id='extract_deputes_json_zip',
    #     python_callable=extract_zip_file,
    #     op_kwargs={
    #         'zip_filename' : "Deputes.json.zip",  # Nom du fichier ZIP
    #         'extracted_folder' : f"deputes_data/{today}",  # Le dossier 'deputes_data/{date}'
    #     }
    # )

    # extract_scrutin_json_zip = PythonOperator(
    #     task_id='extract_scrutin_json_zip',
    #     python_callable=extract_zip_file,
    #     op_kwargs={
    #         'zip_filename' : "Scrutins.json.zip",  # Nom du fichier ZIP
    #         'extracted_folder' : f"scrutins_data/{today}", 
    #     }
    # )

    # delete_deputes_json_zip = PythonOperator(
    #     task_id='delete_deputes_json_zip',
    #     python_callable=delete_zip_file,
    #     op_kwargs={
    #         'zip_filename' : "Deputes.json.zip"
    #     }
    # )

    # delete_scrutins_json_zip = PythonOperator(
    #     task_id='delete_scrutins_json_zip',
    #     python_callable=delete_zip_file,
    #     op_kwargs={
    #         'zip_filename' : "Scrutins.json.zip"
    #     }
    # )

    # spark_json_to_parquet = PythonOperator(
    #     task_id='spark_jsontoparquet',
    #     python_callable=spark_json_to_parquet,
    #     op_kwargs={
    #         'deputes_folder' : f"deputes_data/{today}",
    #         'scrutins_folder' : f"scrutins_data/{today}"
    #     }
    # )

    # spark_savetopostgressScrutins = PythonOperator(
    #     task_id='postgress_scrutins',
    #     python_callable=spark_save_to_postgress_scrutins,
    #     op_kwargs={
    #         'date' : today
    #     }
    # )

    # spark_savetopostgressDeputesActeurs = PythonOperator(
    #     task_id='postgress_deputesActeurs',
    #     python_callable=spark_save_to_postgress_deputesActeur,
    #     op_kwargs={
    #         'date' : today
    #     }
    # )

    spark_save_to_postgress_deputesOrgane = PythonOperator(
        task_id='postgress_deputesOrgane',
        python_callable=spark_save_to_postgress_deputesOrgane,
        op_kwargs={
            'date' : today
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
    
