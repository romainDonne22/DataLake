import os
import json
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from jsonpath_ng import jsonpath, parse
from datetime import datetime

# Suppression des données dans la base de données
def delete_previous_data():
    conn = BaseHook.get_connection('postgressRomain')   # Récupération de la connexion à la base de données depuis Airflow
    hook = PostgresHook(postgres_conn_id='postgressRomain')  # Connexion au moteur de la base de données
    hook.run("DELETE FROM scrutin")
    hook.run("DELETE FROM detailscrutin")

# Fonction pour récupérer le sous-dossier le plus récent basé sur la date dans le nom du dossier
def get_latest_extracted_folder(base_folder):
    # Liste les sous-dossiers dans le dossier "scrutins_data" et récupère ceux dont le nom est une date valide
    subfolders = [f.path for f in os.scandir(base_folder) if f.is_dir()]
    date_folders = []

    for folder in subfolders:
        folder_name = os.path.basename(folder)
        try:
            # Tente de convertir le nom du dossier en date au format 'YYYY-MM-DD'
            folder_date = datetime.strptime(folder_name, "%Y-%m-%d")
            date_folders.append((folder_date, folder))
        except ValueError:
            # Ignore les dossiers dont le nom ne correspond pas à une date valide
            continue

    # Trie les sous-dossiers par date (du plus récent au plus ancien)
    date_folders.sort(key=lambda x: x[0], reverse=True)
    return date_folders[0][1] if date_folders else None  # Retourne le chemin du sous-dossier le plus récent

# Charger les fichiers JSON
def load_json_files():
    extracted_folder = get_latest_extracted_folder("/mnt/c/Users/romd3/OneDrive/Documents/Telecom/705Bdd Non relationnelles v2/Projet/scrutins_data")
    if not extracted_folder:
        raise Exception("Aucun sous-dossier trouvé dans scrutins_data")
    else :
        print(f"Chargement des données depuis {extracted_folder}")

    json_folder = os.path.join(extracted_folder, "json")
    data = []
    for filename in os.listdir(json_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(json_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data.append(json.load(f))
    return data

# Insérer les données dans la table scrutin
def insert_scrutin_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='load_json_data')

    jsonpath_scrutin_uid = parse('$.scrutin.uid')
    jsonpath_scrutin_titre = parse('$.scrutin.titre')
    jsonpath_scrutin_dateScrutin = parse('$.scrutin.dateScrutin')
    jsonpath_scrutin_libelleTypeVote = parse('$.scrutin.typeVote.libelleTypeVote')
    jsonpath_scrutin_decompte_nonVotants = parse('$.scrutin.syntheseVote.decompte.nonVotants')
    jsonpath_scrutin_decompte_pour = parse('$.scrutin.syntheseVote.decompte.pour')
    jsonpath_scrutin_decompte_contre = parse('$.scrutin.syntheseVote.decompte.contre')
    jsonpath_scrutin_decompte_abstentions = parse('$.scrutin.syntheseVote.decompte.abstentions')
    jsonpath_scrutin_decompte_nonVotantsVolontaires = parse('$.scrutin.syntheseVote.decompte.nonVotantsVolontaires')

    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    for item in data:
        uid = jsonpath_scrutin_uid.find(item)[0].value
        titre = jsonpath_scrutin_titre.find(item)[0].value
        dateScrutin = jsonpath_scrutin_dateScrutin.find(item)[0].value
        libelleTypeVote = jsonpath_scrutin_libelleTypeVote.find(item)[0].value
        nonVotants = jsonpath_scrutin_decompte_nonVotants.find(item)[0].value
        pour = jsonpath_scrutin_decompte_pour.find(item)[0].value
        contre = jsonpath_scrutin_decompte_contre.find(item)[0].value
        abstentions = jsonpath_scrutin_decompte_abstentions.find(item)[0].value
        nonVotantsVolontaires = jsonpath_scrutin_decompte_nonVotantsVolontaires.find(item)[0].value

        cur.execute(
            "INSERT INTO scrutin (uid, titre, datescrutin, libelletypevote, decompte_nonvotants, decompte_pour, decompte_contre, decompte_abstentions, decompte_nonvotantsvolontaires) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (uid, titre, dateScrutin, libelleTypeVote, nonVotants, pour, contre, abstentions, nonVotantsVolontaires)
        )

    print("Données insérées dans la table 'scrutin'.")
    conn.commit()
    cur.close()
    conn.close()

# Insérer les données dans la table detailscrutin
def insert_detailscrutin_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='load_json_data')

    jsonpath_acteurRef = parse('$.scrutin.ventilationVotes.organe.groupes.groupe[*].vote.decompteNominatif.pours.votant[*]')

    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    for item in data:
        matchesA1 = jsonpath_acteurRef.find(item)
        for match in matchesA1:
            acteur_refs = match.value
            acteurRef = acteur_refs['acteurRef']
            mandatRef = acteur_refs['mandatRef']
            parDelegation = acteur_refs['parDelegation']
            numPlace = acteur_refs['numPlace']
            
            cur.execute(
                "INSERT INTO detailscrutin (uid, acteurref, mandatref, pardelegation, numplace) "
                "VALUES (%s, %s, %s, %s, %s)",
                (item['scrutin']['uid'], acteurRef, mandatRef, parDelegation, numPlace)
            )

    print("Données insérées dans la table 'detailscrutin'.")
    conn.commit()
    cur.close()
    conn.close()

# Définition du DAG Airflow
dag = DAG(
    'postgress_Scrutins',
    default_args={
        'owner': 'romain',
        'start_date': days_ago(1),
        'retries': 1,
    },
    description='DAG pour le traitement des données de scrutin',
    schedule_interval=None,  # À définir selon la fréquence de votre choix
)

# Tâches dans le DAG
delete_data_task = PythonOperator(
    task_id='delete_previous_data',
    python_callable=delete_previous_data,
    dag=dag,
)

load_json_task = PythonOperator(
    task_id='load_json_data',
    python_callable=load_json_files,
    dag=dag,
)

insert_scrutin_task = PythonOperator(
    task_id='insert_scrutin_data',
    python_callable=insert_scrutin_data,
    provide_context=True,  # Pour permettre le passage de données entre les tâches
    dag=dag,
)

insert_detailscrutin_task = PythonOperator(
    task_id='insert_detailscrutin_data',
    python_callable=insert_detailscrutin_data,
    provide_context=True,  # Pour permettre le passage de données entre les tâches
    dag=dag,
)

# Ordre d'exécution des tâches
delete_data_task >> load_json_task >> insert_scrutin_task >> insert_detailscrutin_task