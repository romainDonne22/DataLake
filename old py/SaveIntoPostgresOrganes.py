import os
import json
import psycopg2
from jsonpath_ng import jsonpath, parse


# Configuration de la connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="romain",
    host="172.21.208.1", #localhost WSL
    #host="localhost", #localhost WINDOWS
    port=5432
)
cur = conn.cursor()

# Suppression des données précédentes
print("Suppression des données précédentes dans la table 'organe'...")
cur.execute("DELETE FROM organe")

# Charger le fichier JSON
extracted_folder = "deputes_data/json"
json_folder = os.path.join(extracted_folder, "organe")
print(f"Chargement des données depuis {json_folder}...")
data = []
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data.append(json.load(f))

# Définir les chemins JSONPath
jsonpath_organe_uid = parse('$.organe.uid')
jsonpath_organe_codeType = parse('$.organe.codeType')
jsonpath_organe_libelle = parse('$.organe.libelle')
jsonpath_organe_libelleAbrev = parse('$.organe.libelleAbrev')

# Trouver toutes les occurrences correspondant au chemin
for item in data:
    uids = jsonpath_organe_uid.find(item)
    codeTypes = jsonpath_organe_codeType.find(item)
    libelles = jsonpath_organe_libelle.find(item)
    libelleAbrevs = jsonpath_organe_libelleAbrev.find(item) 
    
    # Récupérer les valeurs à injecter à la table scrutin
    for uid in uids:
        uid = uid.value    
    for codeType in codeTypes:
        codeType = codeType.value
    for libelle in libelles:
        libelle = libelle.value
    for libelleAbrev in libelleAbrevs:
        libelleAbrev = libelleAbrev.value

    print(f"{uid} - {codeType} / {libelle} / {libelleAbrev}")
    # Insertion des données dans la table organe
    cur.execute(
        "INSERT INTO organe (uid, codetype, libelle, libelleabrev) VALUES (%s, %s, %s, %s)",(   
        uid, codeType, libelle, libelleAbrev
        )
    )

# Commit des transactions et fermeture de la connexion
conn.commit()
cur.close()
conn.close()

print("Données insérées avec succès dans la base de données PostgreSQL.")
