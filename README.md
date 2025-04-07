# 🤖 Data Lake - Scrutins des députés

Ce projet a pour but de récupérer les informations des scrutins et des députés accessible depuis le site :
🔗 https://data.assemblee-nationale.fr


## ❓ Problématique

**Quel est le ratio de scrutins votés par rapport au nombre total de votes possibles pour chaque député ?**

> ⚠️ *Ce projet vise avant tout à démontrer la mise en place d’un Data Lake avec Apache Airflow. Les résultats obtenus sont présentés à titre illustratif et ne feront l’objet d’aucun commentaire politique ou interprétation ici.*

---

## 🚀 Fonctionnalités

- ✅ Données téléchargées puis extraites
- ✅ Données converties au format parquet
- ✅ Données nettoyées
- ✅ Données enregistrées dans Postgres
- ✅ Calculs distribués avec Spark
- ✅ Résultats présentés sur Elastic Search

---

## 🤖 Workflow entierement automatisé avec Apach AIRFLOW : 

<p align="center">
  <img src="./output/Airflow.png" alt="Workflow Airflow" width="80%">
</p>

---

## 📄 Résultats obtenus

<p align="center">
  <img src="./output/distributionParDeputes.png" alt="Distribution par Députés" width="80%">
  <img src="./output/distributionParPartis.png" alt="Distribution par Partis" width="80%">
</p>

---

## ⚙️ Installation

- Pre-requis : Avoir un serveur Airflow (en local ou sur serveur)
- git clone https://github.com/romainDonne22/DataLake.git
- Puis déplacer les deux fichiers .py contenus dans dags vers votre dossier dag