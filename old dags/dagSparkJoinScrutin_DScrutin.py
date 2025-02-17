from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    scrutin = spark.read.jdbc(jdbc_url, "scrutin", properties=properties)
    detailscrutin = spark.read.jdbc(jdbc_url, "detailscrutin", properties=properties)

    # Effectuer la jointure
    join_scrutin_dscrutin = scrutin.join(detailscrutin, scrutin["uid"] == detailscrutin["uid"], "left_outer")

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
    print("Ecriture terminée dans la table join_scrutin_dscrutin")

    # Fermer la session Spark
    spark.stop()

# Initialisation du DAG
dag = DAG(
    "Spark_join_Scrutin_DScrutin",
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