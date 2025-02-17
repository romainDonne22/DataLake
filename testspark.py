from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, BooleanType


# Créer une session Spark
spark = SparkSession.builder.appName("LireParquet").getOrCreate()

# Scrutins
dfScrutins = spark.read.parquet('scrutins_data/2025-02-13/json/parquet') # Lire le fichier Parquet
dfScrutins.createOrReplaceTempView("parquet_table") # Créer une vue temporaire à partir du DataFrame
print(dfScrutins.columns)
dfScrutins.printSchema() # Vérifier le schéma complet du DataFrame
requete1 = """
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
        scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.abstentions.votant as abstentions_votant
    FROM parquet_table
    WHERE scrutin.uid = 'VTANR5L17V10'
"""

# scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.contres.votant as contres_votant,
# scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.nonVotants.votant as nonVotants_votant,
# scrutin.ventilationVotes.organe.groupes.groupe.vote.decompteNominatif.pours.votant as pours_votant

scrutin = spark.sql(requete1) # Exécuter la requête SQL


scrutin = scrutin.withColumn("abstentions_votant", F.explode(F.col("abstentions_votant")))
scrutin = scrutin.filter(F.col("abstentions_votant").isNotNull())

#scrutin.show(truncate=False)
scrutin.show() 


