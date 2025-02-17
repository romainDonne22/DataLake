from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime
from airflow.hooks.base_hook import BaseHook


# Fonction qui exécute la logique Spark
def execute_spark_job():
    # Récupérer la connexion Airflow via BaseHook
    conn_id = "postgressRomain"  # Nom de la connexion configurée dans Airflow
    conn = BaseHook.get_connection(conn_id)

    # Extraire les informations de connexion à partir de conn
    host = conn.host
    port = conn.port
    user = conn.login
    password = conn.password
    database = conn.schema

    # Créer une session Spark
    spark = SparkSession.builder \
        .appName("PostgresJoinExample") \
        .config("spark.jars", "postgresql-42.7.5.jar") \
        .getOrCreate()

    # Configuration de la connexion à la base de données PostgreSQL via les infos extraites
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    print("Connexion établie à la base de données : " + jdbc_url)

    # Charger les deux tables PostgreSQL dans des DataFrames Spark
    scrutin_dscrutin = spark.read.jdbc(jdbc_url, "join_scrutin_dscrutin", properties=properties)
    depute_organe = spark.read.jdbc(jdbc_url, "join_depute_organe", properties=properties)

    # Effectuer la jointure avec TRIM sur les colonnes contenant des espaces inutiles
    join_all = scrutin_dscrutin.join(depute_organe, scrutin_dscrutin["acteurref"] == depute_organe["uid"], "left_outer")
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
    print("Ecriture terminée dans la table join_all")

    # Fermer la session Spark
    spark.stop()

# Initialisation du DAG
dag = DAG(
    "Spark_join_all",
    default_args={
        "owner": "romain",
        "start_date": datetime(2025, 2, 11),
        "retries": 1,
    },
    schedule_interval=None,  # Exécution manuelle
    catchup=False,
)

# Création de l'opérateur qui exécute la fonction Spark
spark_task = PythonOperator(
    task_id="execute_spark_job",
    python_callable=execute_spark_job,
    dag=dag,
)

# Définir les dépendances du DAG
spark_task