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
scrutin = spark.read.jdbc(jdbc_url, "scrutin", properties=properties)
detailscrutin = spark.read.jdbc(jdbc_url, "detailscrutin", properties=properties)

# Effectuer la jointure
join_scrutin_dscrutin = scrutin.join(detailscrutin, scrutin["uid"] == detailscrutin["uid"], "left_outer")
# SELECT * FROM scrutin s LEFT JOIN detailscrutin ds ON s.uid = ds.uid
# 67530 resultats

# Sélectionner les colonnes nécessaires après la jointure
join_scrutin_dscrutin = join_scrutin_dscrutin.select(
    scrutin["uid"],
    scrutin["titre"],
    scrutin["datescrutin"],
    scrutin["libelletypevote"],
    scrutin["decompte_nonvotants"],
    scrutin["decompte_pour"],
    scrutin["decompte_contre"],
    scrutin["decompte_abstentions"],
    scrutin["decompte_nonvotantsvolontaires"],
    detailscrutin["acteurref"],
    detailscrutin["mandatref"],
    detailscrutin["pardelegation"],
    detailscrutin["numplace"]
)

# Écrire les résultats de la jointure dans une nouvelle table PostgreSQL
join_scrutin_dscrutin.write.jdbc(url=jdbc_url, table="join_scrutin_dscrutin", mode="overwrite", properties=properties)
print("Jointure effectuée avec succès dans la table 'join_scrutin_dscrutin'.")

# Fermer la session Spark
spark.stop()
