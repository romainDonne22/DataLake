import requests
import zipfile
import json
import os

# URL de l'API REST qui fournit le fichier ZIP
url = "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_mandats_actifs_organes/AMO10_deputes_actifs_mandats_actifs_organes.json.zip"

# Nom du fichier ZIP et du dossier de destination pour les députés
zip_filename = "AMO10_deputes_actifs_mandats_actifs_organes.json.zip"
extracted_folder = "/mnt/c/Users/romd3/OneDrive/Documents/Telecom/705Bdd Non relationnelles v2/Projet/deputes_data"

# Étape 1 : Télécharger le fichier ZIP en utilisant REST (requests)
print("Téléchargement du fichier ZIP via REST API...")
response = requests.get(url)

# Vérifie si la requête a réussi (code HTTP 200)
if response.status_code == 200:
    with open(zip_filename, 'wb') as f:
        f.write(response.content)
    print(f"Fichier {zip_filename} téléchargé avec succès.")
else:
    print(f"Erreur lors du téléchargement (code {response.status_code}).")
    exit()

# Étape 2 : Extraire le fichier ZIP
print(f"Extraction du fichier ZIP {zip_filename}...")
with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
    # Créer un dossier pour l'extraction
    os.makedirs(extracted_folder, exist_ok=True)
    zip_ref.extractall(extracted_folder)

# Étape 3 : Charger les données JSON extraites
json_folder = os.path.join(extracted_folder, "json")

print(f"Chargement des données depuis {json_folder}...")
data = []
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data.append(json.load(f))

# Étape 4 : Afficher les premières données pour vérifier
print("Affichage des premières données extraites...")
# for item in data:
#     print(item)

# (Optionnel) Traiter les données : par exemple, afficher une partie spécifique ou analyser les résultats
# Exemple : afficher le nombre d'éléments dans la liste JSON (si c'est une liste)
if isinstance(data, list):
    print(f"Nombre d'éléments dans les données JSON : {len(data)}")
else:
    print(f"Les données JSON ne sont pas au format liste. Structure : {type(data)}")
