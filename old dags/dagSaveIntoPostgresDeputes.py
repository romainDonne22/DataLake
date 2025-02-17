import os
import json
from jsonpath_ng import jsonpath, parse
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Fonction pour supprimer les données dans la table 'depute'
def clear_previous_data():
    conn = BaseHook.get_connection('postgressRomain')   # Récupération de la connexion à la base de données depuis Airflow
    hook = PostgresHook(postgres_conn_id='postgressRomain')  # Connexion au moteur de la base de données
    
    # Exécution de la suppression des données
    print("Suppression des données précédentes dans la table 'depute'...")
    hook.run("DELETE FROM depute")
    print("Données supprimées avec succès.")

# Fonction pour récupérer le sous-dossier le plus récent basé sur la date dans le nom du dossier
def get_latest_extracted_folder(base_folder):
    # Liste les sous-dossiers dans le dossier "deputes_data" et récupère ceux dont le nom est une date valide
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

# Fonction pour charger les données JSON
def load_json_data():
    extracted_folder = get_latest_extracted_folder("deputes_data")
    json_folder = os.path.join(extracted_folder, "json/acteur")
    print(f"Chargement des données depuis {json_folder}...")
    
    data = []
    for filename in os.listdir(json_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(json_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data.append(json.load(f))
    
    return data

# Fonction pour traiter les données JSON et insérer dans la base de données
def process_and_insert_data():
    data = load_json_data()

    # Définir les chemins JSONPath
    jsonpath_depute_uid = parse('$.acteur.uid."#text"')
    jsonpath_depute_civ = parse('$.acteur.etatCivil.ident.civ')
    jsonpath_depute_prenom = parse('$.acteur.etatCivil.ident.prenom')
    jsonpath_depute_nom = parse('$.acteur.etatCivil.ident.nom')
    jsonpath_depute_indexemandat = parse('$.acteur.mandats.mandat[*]')

    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Traiter chaque élément de données
    for item in data:
        uids = jsonpath_depute_uid.find(item)
        civs = jsonpath_depute_civ.find(item)
        prenoms = jsonpath_depute_prenom.find(item)
        noms = jsonpath_depute_nom.find(item)
        indexmandats = jsonpath_depute_indexemandat.find(item)

        # Extraction des valeurs et insertion dans la table
        for uid in uids:
            uid = uid.value    
        for civ in civs:
            civ = civ.value
        for prenom in prenoms:
            prenom = prenom.value
        for nom in noms:
            nom = nom.value 
        for indexmandat in indexmandats:
            typeOrgane = indexmandat.value['typeOrgane']
            if typeOrgane == "GP":
                organeRef = indexmandat.value['organes']['organeRef']
            if typeOrgane == "ASSEMBLEE":
                placeHemicycle = indexmandat.value['mandature']['placeHemicycle']
                datePriseFonction = indexmandat.value['mandature']['datePriseFonction']
                
        #print(f"{uid} - {civ} / {prenom} / {nom} / {organeRef} / {placeHemicycle} / {datePriseFonction}")
        
        # Insertion des données dans la table depute
        cur.execute(
            "INSERT INTO depute (uid, civ, prenom, nom, organeref, placehemicycle, dateprisefonction) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",   
            (uid, civ, prenom, nom, organeRef, placeHemicycle, datePriseFonction)
        )

    # Commit des transactions et fermeture de la connexion
    conn.commit()
    cur.close()
    conn.close()

    print("Données insérées avec succès dans la table depute.")

# Définir les paramètres du DAG
default_args = {
    'owner': 'romain',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 10),
}

dag = DAG(
    'postgress_Deputes',
    default_args=default_args,
    description='DAG pour importer les données des députés depuis des fichiers JSON',
    schedule_interval=None,  # Déclenchement manuel ou selon planification
)

# Définir les tâches Airflow
clear_task = PythonOperator(
    task_id='clear_previous_data',
    python_callable=clear_previous_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_and_insert_data',
    python_callable=process_and_insert_data,
    dag=dag,
)

# Définir l'ordre des tâches
clear_task >> process_task