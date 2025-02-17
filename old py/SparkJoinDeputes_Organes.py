from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder \
    .appName("PostgresJoinExample") \
    .config("spark.jars", "postgresql-42.7.5.jar") \
    .getOrCreate()

# Configuration de la connexion à la base de données PostgreSQL
jdbc_url = "jdbc:postgresql://172.21.208.1:5432/postgres" # host:port/database
properties = {
    "user": "postgres",
    "password": "romain",
    "driver": "org.postgresql.Driver"
}

# Charger les deux tables PostgreSQL dans des DataFrames Spark
depute = spark.read.jdbc(jdbc_url, "depute", properties=properties)
organe = spark.read.jdbc(jdbc_url, "organe", properties=properties)

# Effectuer la jointure
join_depute_organe = depute.join(organe, depute["organeref"] == organe["uid"], "left_outer")
# SELECT * FROM depute d JOIN organe o ON d.organeref = o.uid
# 576 resultats

# Sélectionner les colonnes nécessaires après la jointure
join_depute_organe = join_depute_organe.select(
    depute["uid"],
    depute["civ"],
    depute["prenom"],
    depute["nom"],
    depute["organeref"],
    depute["placehemicycle"],
    depute["dateprisefonction"],
    organe["codetype"],
    organe["libelle"],
    organe["libelleabrev"],
)

# Écrire les résultats de la jointure dans une nouvelle table PostgreSQL
join_depute_organe.write.jdbc(url=jdbc_url, table="join_depute_organe", mode="overwrite", properties=properties)
print("Jointure effectuée avec succès dans la table 'join_depute_organe'.")

# Fermer la session Spark
spark.stop()
