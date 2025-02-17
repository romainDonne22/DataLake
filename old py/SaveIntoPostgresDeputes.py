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
print("Suppression des données précédentes dans la table 'depute'...")
cur.execute("DELETE FROM depute")

# Charger le fichier JSON
extracted_folder = "deputes_data/json"
json_folder = os.path.join(extracted_folder, "acteur")
print(f"Chargement des données depuis {json_folder}...")
data = []
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data.append(json.load(f))

# Définir les chemins JSONPath
jsonpath_depute_uid = parse('$.acteur.uid."#text"') # on est obligé de mettre "" à cause de #
jsonpath_depute_civ = parse('$.acteur.etatCivil.ident.civ')
jsonpath_depute_prenom = parse('$.acteur.etatCivil.ident.prenom')
jsonpath_depute_nom = parse('$.acteur.etatCivil.ident.nom')
jsonpath_depute_indexemandat = parse('$.acteur.mandats.mandat[*]')

# Trouver toutes les occurrences correspondant au chemin
for item in data:
    uids = jsonpath_depute_uid.find(item)
    civs = jsonpath_depute_civ.find(item)
    prenoms = jsonpath_depute_prenom.find(item)
    noms = jsonpath_depute_nom.find(item)
    indexmandats = jsonpath_depute_indexemandat.find(item)
    
    # Récupérer les valeurs à injecter à la table scrutin
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
            #dateDebutASSEMBLEE = indexmandat.value['dateDebut'] # 2024/07/07 pour la dernière élection des députés
            placeHemicycle = indexmandat.value['mandature']['placeHemicycle']
            datePriseFonction = indexmandat.value['mandature']['datePriseFonction'] # 2024/07/08 pour tous sauf les remplaçants ou ceux déjà présents
            
    print(f"{uid} - {civ} / {prenom} / {nom} / {organeRef} / {placeHemicycle} / {datePriseFonction}")
    
    # Insertion des données dans la table depute
    cur.execute(
        "INSERT INTO depute (uid, civ, prenom, nom, organeref, placehemicycle, dateprisefonction) VALUES (%s, %s, %s, %s, %s, %s, %s)",(   
        uid,civ,prenom,nom,organeRef,placeHemicycle, datePriseFonction
        )
    )

# Commit des transactions et fermeture de la connexion
conn.commit()
cur.close()
conn.close()

print("Données insérées avec succès dans la base de données PostgreSQL.")
