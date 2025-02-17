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
print("Suppression des données précédentes dans la table 'scrutin'...")
cur.execute("DELETE FROM scrutin")
print("Suppression des données précédentes dans la table 'detailscrutin'...")
cur.execute("DELETE FROM detailscrutin")


# Charger le fichier JSON
extracted_folder = "scrutins_data"
json_folder = os.path.join(extracted_folder, "json")
print(f"Chargement des données depuis {json_folder}...")
data = []
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data.append(json.load(f))

# Définir les chemins JSONPath
jsonpath_scrutin_uid = parse('$.scrutin.uid')
jsonpath_scrutin_titre = parse('$.scrutin.titre') 
jsonpath_scrutin_dateScrutin = parse('$.scrutin.dateScrutin') 
jsonpath_scrutin_libelleTypeVote = parse('$.scrutin.typeVote.libelleTypeVote')
jsonpath_scrutin_decompte_nonVotants = parse('$.scrutin.syntheseVote.decompte.nonVotants')
jsonpath_scrutin_decompte_pour = parse('$.scrutin.syntheseVote.decompte.pour')
jsonpath_scrutin_decompte_contre = parse('$.scrutin.syntheseVote.decompte.contre')
jsonpath_scrutin_decompte_abstentions = parse('$.scrutin.syntheseVote.decompte.abstentions')
jsonpath_scrutin_decompte_nonVotantsVolontaires = parse('$.scrutin.syntheseVote.decompte.nonVotantsVolontaires') 

jsonpath_acteurRef = parse('$.scrutin.ventilationVotes.organe.groupes.groupe[*].vote.decompteNominatif.pours.votant[*]')





# Trouver toutes les occurrences correspondant au chemin
for item in data:
    matches = jsonpath_scrutin_uid.find(item)
    matches2 = jsonpath_scrutin_titre.find(item)
    matches3 = jsonpath_scrutin_dateScrutin.find(item)
    matches4 = jsonpath_scrutin_libelleTypeVote.find(item)
    matches5 = jsonpath_scrutin_decompte_nonVotants.find(item)
    matches6 = jsonpath_scrutin_decompte_pour.find(item)
    matches7 = jsonpath_scrutin_decompte_contre.find(item)
    matches8 = jsonpath_scrutin_decompte_abstentions.find(item)
    matches9 = jsonpath_scrutin_decompte_nonVotantsVolontaires.find(item)
    
    matchesA1 = jsonpath_acteurRef.find(item)
    
    # Récupérer les valeurs à injecter à la table scrutin
    for match in matches:
        uid = match.value    
    for match2 in matches2:
        titre = match2.value
    for match3 in matches3:
        dateScrutin = match3.value  
    for match4 in matches4:
        libelleTypeVote = match4.value
    for match5 in matches5:
        nonVotants = match5.value
    for match6 in matches6:
        pour = match6.value
    for match7 in matches7:
        contre = match7.value    
    for match8 in matches8:
        abstentions = match8.value
    for match9 in matches9:
        nonVotantsVolontaires = match9.value
    
    print(uid)
    #print(uid, titre, dateScrutin, libelleTypeVote, nonVotants, pour, contre, abstentions, nonVotantsVolontaires)
    cur.execute(
        "INSERT INTO scrutin (uid, titre, datescrutin, libelletypevote, decompte_nonvotants, decompte_pour, decompte_contre, decompte_abstentions, decompte_nonvotantsvolontaires) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",(   
        uid,titre,dateScrutin,libelleTypeVote,nonVotants,pour,contre,abstentions,nonVotantsVolontaires
        )
    )



    # Récupérer les valeurs à injecter à la table detailscrutin
    for match10 in matchesA1:
        acteur_refs = match10.value
        #if (uid=='VTANR5L17V2') :

        # Extraire les valeurs des clés spécifiques
        acteurRef = acteur_refs['acteurRef']
        mandatRef = acteur_refs['mandatRef']
        parDelegation = acteur_refs['parDelegation']
        numPlace = acteur_refs['numPlace']
        
        # Afficher les valeurs extraites
        #print(f"{uid} --- acteurRef: {acteurRef}, mandatRef: {mandatRef}, parDelegation: {parDelegation}, numPlace: {numPlace}")

        # Insérer les données dans la table detailscrutin
        cur.execute(
            "INSERT INTO detailscrutin (uid, acteurref, mandatref, pardelegation, numplace) VALUES (%s, %s, %s, %s, %s)",
            (uid, acteurRef, mandatRef, parDelegation, numPlace)
        )



# Commit des transactions et fermeture de la connexion
conn.commit()
cur.close()
conn.close()

print("Données insérées avec succès dans la base de données PostgreSQL.")
