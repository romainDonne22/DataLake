from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, from_json, to_date
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt
import requests
import zipfile
import os

# Fonction pour télécharger un fichier ZIP
def download_zip_file(url, zip_filename):
    print("Téléchargement du fichier ZIP via REST API...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(zip_filename, 'wb') as f:
            f.write(response.content)
        print(f"Fichier {zip_filename} téléchargé avec succès.")
    else:
        raise Exception(f"Erreur lors du téléchargement (code {response.status_code}).")

# Fonction pour extraire un fichier ZIP
def extract_zip_file(zip_filename, extracted_folder):
    print(f"Extraction du fichier ZIP {zip_filename}...")
    os.makedirs(extracted_folder, exist_ok=True)  # Création du dossier avec la date du jour sous 'deputes_data'
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extracted_folder)  # Extraction dans le dossier structuré par date
    print(f"Extraction du fichier ZIP dans {extracted_folder} réussie.")

# Fonction pour supprimer un fichier ZIP après extraction
def delete_zip_file(zip_filename):
    print(f"Suppression du fichier ZIP {zip_filename}...")
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
        print(f"Le fichier ZIP {zip_filename} a été supprimé.")
    else:
        print(f"Le fichier ZIP {zip_filename} n'existe pas.")

# Fonction pour convertir des fichiers JSON en fichiers Parquet
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

# Fonction pour sauvegarder les données Parquet des scrutins dans une base de données Postgres
def spark_save_to_postgress_scrutins(date): 
    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Créer une session Spark
    spark = SparkSession.builder.appName("LireParquet").getOrCreate()

    # Scrutins
    dfScrutins = spark.read.parquet(f'scrutins_data/{date}/json/parquet') # Lire le fichier Parquet
    dfScrutins.createOrReplaceTempView("parquet_table") # Créer une vue temporaire à partir du DataFrame
    print(dfScrutins.columns)
    dfScrutins.printSchema() # Vérifier le schéma complet du DataFrame

    # Définir le schéma des données JSON
    votant_schema = ArrayType(StructType([
        StructField("acteurRef", StringType(), True),
        StructField("mandatRef", StringType(), True),
        StructField("parDelegation", StringType(), True),
        StructField("numPlace", StringType(), True)
    ]))

    nonVotants_requete = """
        SELECT 
            scrutin.uid, 
            scrutin.titre, 
            scrutin.dateScrutin, 
            scrutin.typeVote.libelleTypeVote, 
            scrutin.syntheseVote.decompte.nonVotants, 
            scrutin.syntheseVote.decompte.pour, 
            scrutin.syntheseVote.decompte.contre, 
            scrutin.syntheseVote.decompte.abstentions, 
            scrutin.syntheseVote.decompte.nonVotantsVolontaires,
            scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.nonVotants.votant as votant
        FROM parquet_table
    """

    pours_requete = """
        SELECT 
            scrutin.uid, 
            scrutin.titre, 
            scrutin.dateScrutin, 
            scrutin.typeVote.libelleTypeVote, 
            scrutin.syntheseVote.decompte.nonVotants, 
            scrutin.syntheseVote.decompte.pour, 
            scrutin.syntheseVote.decompte.contre, 
            scrutin.syntheseVote.decompte.abstentions, 
            scrutin.syntheseVote.decompte.nonVotantsVolontaires,
            scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.pours.votant as votant
        FROM parquet_table
    """

    contres_requete = """
        SELECT 
            scrutin.uid, 
            scrutin.titre, 
            scrutin.dateScrutin, 
            scrutin.typeVote.libelleTypeVote, 
            scrutin.syntheseVote.decompte.nonVotants, 
            scrutin.syntheseVote.decompte.pour, 
            scrutin.syntheseVote.decompte.contre, 
            scrutin.syntheseVote.decompte.abstentions, 
            scrutin.syntheseVote.decompte.nonVotantsVolontaires,
            scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.contres.votant as votant
        FROM parquet_table
    """

    abstentions_requete = """
        SELECT 
            scrutin.uid, 
            scrutin.titre, 
            scrutin.dateScrutin, 
            scrutin.typeVote.libelleTypeVote, 
            scrutin.syntheseVote.decompte.nonVotants, 
            scrutin.syntheseVote.decompte.pour, 
            scrutin.syntheseVote.decompte.contre, 
            scrutin.syntheseVote.decompte.abstentions, 
            scrutin.syntheseVote.decompte.nonVotantsVolontaires,
            scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.abstentions.votant as votant
        FROM parquet_table
    """

    requetes = [nonVotants_requete, pours_requete, contres_requete, abstentions_requete] # Liste des requêtes SQL
    i=1 # Initialiser un compteur pour les requêtes

    for requete in requetes:
        print(f"requete : {i}")
        
        # Exécuter la requête SQL
        scrutin = spark.sql(requete) 

        # Filtrer les lignes où 'votant' n'est pas NULL
        scrutin = scrutin.filter(F.col("votant").isNotNull())

        # Filtrer les lignes où 'abstentions_votant' est une chaîne de caractères
        string_scrutin = scrutin.filter(F.col("votant").cast(StringType()).isNotNull())
        string_scrutin = string_scrutin.withColumn("votant", explode(col("votant")))

        # Convertir les chaînes JSON en structures JSON
        string_scrutin = string_scrutin.withColumn("votant", from_json(col("votant"), votant_schema))
        string_scrutin = string_scrutin.withColumn("votant", explode(col("votant")))

        # recupérer les résultats des 4 requetes
        if i == 1 :
            string_scrutin = string_scrutin.withColumn("type_vote", F.lit("non_votants"))
            non_votants = string_scrutin
        elif i == 2 :
            string_scrutin = string_scrutin.withColumn("type_vote", F.lit("pours"))
            pours = string_scrutin
        elif i == 3 :
            string_scrutin = string_scrutin.withColumn("type_vote", F.lit("contres"))
            contres = string_scrutin
        else :
            string_scrutin = string_scrutin.withColumn("type_vote", F.lit("abstentions"))
            abstentions = string_scrutin

        i += 1

    # Concaténer les résultats
    scrutinSpark = non_votants.union(pours).union(contres).union(abstentions)

    # Afficher les résultats
    scrutinSpark = scrutinSpark.select("uid", "titre", "datescrutin", "libelletypevote", "nonvotants", "pour", "contre", "abstentions", "nonVotantsVolontaires", "votant.acteurRef","votant.mandatRef", "votant.parDelegation", "votant.numPlace", "type_vote")
    #result.select("uid", "titre", "datescrutin", "libelletypevote", "nonvotants", "pour", "contre", "abstentions", "nonVotantsVolontaires", "votant.acteurRef","votant.mandatRef", "votant.parDelegation", "votant.numPlace", "type_vote").show(n=result.count(), truncate=False)

    # Mettre au bon format les dates et les colonnes numériques
    scrutinSpark = scrutinSpark.withColumn("datescrutin", to_date(col("datescrutin"), "yyyy-MM-dd"))
    scrutinSpark = scrutinSpark.withColumn("nonvotants", col("nonvotants").cast("int"))
    scrutinSpark = scrutinSpark.withColumn("pour", col("pour").cast("int"))
    scrutinSpark = scrutinSpark.withColumn("contre", col("contre").cast("int"))
    scrutinSpark = scrutinSpark.withColumn("abstentions", col("abstentions").cast("int"))
    scrutinSpark = scrutinSpark.withColumn("nonVotantsVolontaires", col("nonVotantsVolontaires").cast("int"))
    scrutinSpark = scrutinSpark.withColumn("numPlace", col("numPlace").cast("int"))

    scrutinSpark.show()
    data = scrutinSpark.collect()

    # Suppression des données précédentes
    print("Suppression des données précédentes dans la table 'scrutin'...")
    cur.execute("DELETE FROM scrutin")

    # Insertion des données dans la table 'scrutin'
    insert_query = """
        INSERT INTO scrutin (uid, titre, datescrutin, libelletypevote, decompte_nonvotants, decompte_pour, decompte_contre, decompte_abstentions, decompte_nonvotantsvolontaires, acteurref, mandatref, pardelegation, numplace, typevote)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_query, data)
    print("Données insérées dans la table 'scrutin'.")

    conn.commit()
    cur.close()
    conn.close()

# Fonction pour sauvegarder les données Parquet des députés acteurs dans une base de données Postgres
def spark_save_to_postgress_deputesActeur(date):
    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Créer une session Spark
    spark = SparkSession.builder.appName("LireParquet").getOrCreate()

    # Deputes Acteurs
    dfDeputesActeurs = spark.read.parquet(f'deputes_data/{date}/json/acteur/parquet') # Lire le fichier Parquet
    dfDeputesActeurs.createOrReplaceTempView("parquet_table") # Créer une vue temporaire à partir du DataFrame
    print(dfDeputesActeurs.columns)
    dfDeputesActeurs.printSchema() # Vérifier le schéma complet du DataFrame

    deputesActeurs_requete = """
        SELECT 
            acteur.uid.`#text` as uid,
            acteur.etatCivil.ident.civ, 
            acteur.etatCivil.ident.prenom,
            acteur.etatCivil.ident.nom,
            explode(acteur.mandats.mandat) as mandat
        FROM parquet_table
    """
    # Exécuter la requête SQL
    deputesActeur = spark.sql(deputesActeurs_requete) 
  
    # Aplatir les colonnes des mandats
    deputesActeur = deputesActeur.select(
        "uid",
        "civ",
        "prenom",
        "nom",
        col("mandat.typeOrgane").alias("typeorgane"),
        col("mandat.organes.organeRef").alias("organeref"),
        col("mandat.mandature.placeHemicycle").alias("placehemicycle"),
        col("mandat.mandature.datePriseFonction").alias("dateprisefonction")
    )

    # Filtrer les données
    deputesActeurfiltre = deputesActeur.filter((col("typeorgane") == "ASSEMBLEE"))
    parti = deputesActeur.filter((col("typeorgane") == "GP"))

    # Jointure entre deputesActeurfiltre et parti sur la clé commune (par exemple, uid)
    deputesActeurfiltre = deputesActeurfiltre.join(parti.select("uid", col("organeref").alias("partiid")), on="uid", how="left")

    # Mettre au bon format les dates et les colonnes numériques
    deputesActeurfiltre = deputesActeurfiltre.withColumn("dateprisefonction", to_date(col("dateprisefonction"), "yyyy-MM-dd"))
    deputesActeurfiltre = deputesActeurfiltre.withColumn("placehemicycle", col("placehemicycle").cast("int"))
    deputesActeurfiltre.show()

    # Collecter les données
    data = [tuple(row) for row in deputesActeurfiltre.collect()]

    # Suppression des données précédentes
    print("Suppression des données précédentes dans la table 'acteur'...")
    cur.execute("DELETE FROM acteur")

    # Insertion des données dans la table 'acteur'
    insert_query = """
        INSERT INTO acteur (uid, civ, prenom, nom, typeorgane, organeref, placehemicycle, dateprisefonction, partiid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_query, data)
    print("Données insérées dans la table 'acteur'.")

    conn.commit()
    cur.close()
    conn.close()

# Fonction pour sauvegarder les données Parquet des députés organes dans une base de données Postgres
def spark_save_to_postgress_deputesOrgane(date):
    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Créer une session Spark
    spark = SparkSession.builder.appName("LireParquet").getOrCreate()

    # Deputes Organes
    dfDeputesOrganes = spark.read.parquet(f'deputes_data/{date}/json/organe/parquet') # Lire le fichier Parquet
    dfDeputesOrganes.createOrReplaceTempView("parquet_table") # Créer une vue temporaire à partir du DataFrame
    print(dfDeputesOrganes.columns)
    dfDeputesOrganes.printSchema() # Vérifier le schéma complet du DataFrame

    deputesOrganes_requete = """
        SELECT 
            organe.uid, 
            organe.codeType,
            organe.libelle,
            organe.libelleAbrev
        FROM parquet_table
    """

    # Exécuter la requête SQL
    deputesOrganes = spark.sql(deputesOrganes_requete) 
    deputesOrganes.show()

    # Collecter les données
    data = [tuple(row) for row in deputesOrganes.collect()]

    # Suppression des données précédentes
    print("Suppression des données précédentes dans la table 'organe'...")
    cur.execute("DELETE FROM organe")

    # Insertion des données dans la table 'organe'
    insert_query = """
        INSERT INTO organe (uid, codetype, libelle, libelleabrev)
        VALUES (%s, %s, %s, %s)
    """
    cur.executemany(insert_query, data)

    print("Données insérées dans la table 'organe'.")

    conn.commit()
    cur.close()
    conn.close()

# Fonction pour analyser les données et calculer les KPI
def analyse():
    # Connexion à la base de données via PostgresHook
    hook = PostgresHook(postgres_conn_id='postgressRomain')
    conn = hook.get_conn()
    cur = conn.cursor()

    # Créer une session Spark
    spark = SparkSession.builder.appName("LireParquet").getOrCreate()

    # Suppression des données précédentes
    cur.execute("DELETE FROM cumulscrutin")
    print("Données supprimées dans la table 'cumulscrutin'.")

    # Requête SQL pour calculer les données cumulées
    cur.execute("""
        	INSERT INTO cumulscrutin (datescrutin, nbscrutin, nbscrutincumul, nbscrutincumuldiff)
			SELECT datescrutin, 
	                nbscrutin, 
	                nbscrutincumul,
	                (MAX(nbscrutincumul) OVER () - nbscrutincumul) + 1 AS nbscrutincumuldiff
	        FROM (
	            SELECT datescrutin, 
	                    nbscrutin,
	                SUM(nbscrutin) OVER (ORDER BY datescrutin) AS nbscrutincumul
	            FROM (
	                SELECT COUNT(*) AS nbscrutin, datescrutin 
	                FROM (
	                    SELECT DISTINCT uid, datescrutin 
	                    FROM scrutin
	                ) AS sous_requete
	                GROUP BY datescrutin
	            ) AS sous_requete_2
	            ORDER BY datescrutin
	        ) AS sous_requete_3
    """
    )
    print("Données insérées dans la table 'cumulscrutin'.")
    

    cur.execute("""
        SELECT a.uid, 
            a.nom, 
            a.prenom, 
            a.dateprisefonction, 
            o.libelle, 
            count(typevote) as nbvote, 
            sc.nbscrutincumuldiff, 
            (count(typevote) * 100 / nbscrutincumuldiff) as ratio
        FROM scrutin s
        JOIN acteur a ON s.acteurref = a.uid
        JOIN organe o ON a.partiid = o.uid
		JOIN cumulscrutin sc ON sc.datescrutin >= a.dateprisefonction
		WHERE NOT EXISTS ( 
			SELECT 1
    		FROM cumulscrutin sc2
    		WHERE sc2.datescrutin >= a.dateprisefonction
      		AND sc2.datescrutin < sc.datescrutin
		)
        GROUP BY a.uid, a.nom, a.prenom, a.dateprisefonction, o.libelle, sc.nbscrutincumuldiff
		ORDER BY ratio desc
    """
    )

    # Récupérer les résultats de la requête
    kpi = cur.fetchall()
    print("KPI calculés avec succès.")

    # Convertir les résultats en DataFrame Pandas
    columns = [desc[0] for desc in cur.description]
    kpi_pandas_df = pd.DataFrame(kpi, columns=columns)

    # Tracer la distribution avec Matplotlib
    plt.figure(figsize=(10, 6))
    plt.hist(kpi_pandas_df['ratio'], bins=30, edgecolor='k', alpha=0.7)
    plt.xlabel('Ratio en % (nombre de votes / nombre de scrutins auxquels le député aurait pu participer)')
    plt.ylabel('Fréquence')
    plt.title('Distribution des pourcentages de votes par député')
    plt.grid(True)

    # Enregistrer le graphique dans un fichier image
    plt.savefig('distribution_kpi_ratios.png')
    print("Graphique enregistré avec succès.")

    conn.commit()
    cur.close()
    conn.close()



