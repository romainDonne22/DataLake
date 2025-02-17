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
scrutin_dscrutin = spark.read.jdbc(jdbc_url, "join_scrutin_dscrutin", properties=properties)
depute_organe = spark.read.jdbc(jdbc_url, "join_depute_organe", properties=properties)

# Effectuer la jointure
join_all = scrutin_dscrutin.join(depute_organe, scrutin_dscrutin["acteurref"] == depute_organe["uid"], "left_outer")
# SELECT * FROM join_scrutin_dscrutin sds JOIN join_depute_organe depo ON TRIM(sds.acteurref) = TRIM(depo.uid)
# Le TRIM est nécessaire car les valeurs des colonnes acteurref et uid contiennent des espaces inutiles

# Sélectionner les colonnes nécessaires après la jointure
join_all = join_all.select(
    scrutin_dscrutin["uid"],
    scrutin_dscrutin["titre"],
    scrutin_dscrutin["datescrutin"],
    scrutin_dscrutin["libelletypevote"],
    scrutin_dscrutin["decompte_nonvotants"],
    scrutin_dscrutin["decompte_pour"],
    scrutin_dscrutin["decompte_contre"],
    scrutin_dscrutin["decompte_abstentions"],
    scrutin_dscrutin["decompte_nonvotantsvolontaires"],
    scrutin_dscrutin["acteurref"],
    scrutin_dscrutin["mandatref"],
    scrutin_dscrutin["pardelegation"],
    scrutin_dscrutin["numplace"],
    depute_organe["civ"],
    depute_organe["prenom"],
    depute_organe["nom"],
    depute_organe["organeref"],
    depute_organe["placehemicycle"],
    depute_organe["dateprisefonction"],
    depute_organe["codetype"],
    depute_organe["libelle"],
    depute_organe["libelleabrev"],
)

# Écrire les résultats de la jointure dans une nouvelle table PostgreSQL
join_all.write.jdbc(url=jdbc_url, table="join_all", mode="overwrite", properties=properties)
print("Jointure effectuée avec succès dans la table 'join_all'.")

# Fermer la session Spark
spark.stop()
