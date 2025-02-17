from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, from_json, to_date
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

import psycopg2

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


# Créer une session Spark
spark = SparkSession.builder.appName("LireParquet").getOrCreate()

# Scrutins
dfScrutins = spark.read.parquet('scrutins_data/2025-02-13/json/parquet') # Lire le fichier Parquet
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
