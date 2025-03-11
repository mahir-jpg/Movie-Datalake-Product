# IMDB Data Pipeline  

## Description  
Ce projet est une solution complète pour l’ingestion, la transformation et le stockage de films et de critiques issues du dataset IMDB. Il automatise le processus de traitement des données en intégrant plusieurs technologies et services.  

## Fonctionnalités  
- Ingestion des données depuis un repository Hugging Face personnel  
- Stockage initial des données brutes dans un bucket S3  
- Nettoyage et structuration des données dans une base MySQL  
- Transformation et enrichissement des données dans Elasticsearch  
- Mise à disposition des données via une API FastAPI  
- Orchestration des différentes étapes du pipeline avec Apache Airflow  

## Technologies utilisées  
- Apache Airflow (orchestration)  
- Amazon S3 (stockage initial)  
- MySQL (base de données relationnelle)  
- Elasticsearch (indexation et enrichissement des données)  
- Hugging Face (source des données)  
- Docker et Docker Compose (déploiement et gestion des services)  
- Python (scripts d’automatisation)  
- FastAPI (exposition des données via une API REST)  

## Prérequis  
Avant de lancer le projet, assurez-vous d’avoir les éléments suivants installés sur votre machine :  
- Docker  
- Docker Compose  
- Python 

## Installation et exécution  

1. Cloner ce repository :  
   ```bash
   git clone https://github.com/mahir-jpg/Movie-Datalake-Product.git  
   cd Movie-Datalake-Product  
   ```  

2. Construire et démarrer les conteneurs Docker :  
   ```bash
   docker compose build  
   docker compose up -d  
   ```  

3. Accéder à l’interface web d’Airflow via un navigateur en se rendant sur `http://localhost:8080`.  

4. Exécuter le DAG `build_process` dans l’interface d’Airflow. Ce DAG réalise les opérations suivantes :  
   - Création du bucket S3  
   - Création de la base de données MySQL  
   - Chargement des données brutes dans le bucket S3  

5. Exécuter le DAG `movie_dag`, qui effectue les opérations suivantes :  
   - Chargement des données depuis le bucket S3 vers MySQL et Elasticsearch  
   - Nettoyage, transformation et enrichissement des données  

Le pipeline est maintenant opérationnel et les données sont disponibles dans MySQL et Elasticsearch.  

## Accès aux APIs  

Le projet inclut une API FastAPI pour interagir avec les données.  

### API de consultation des données  
Pour démarrer l’API permettant d’accéder aux données :  
```bash
cd api  
uvicorn api:app --host 0.0.0.0 --port 8000 --reload  
```  

Endpoints disponibles :  
- **`/raw`** : Accès aux données brutes  
- **`/staging`** : Accès aux données intermédiaires  
- **`/curated`** : Accès aux données finales  
- **`/health`** : Vérification de l’état des services  
- **`/stats`** : Statistiques sur le remplissage des buckets et des bases de données  

### API d’ingestion des données  
Pour démarrer l’API d’ingestion :  
```bash
cd api  
uvicorn ingest:app --host 0.0.0.0 --port 8000 --reload  
```  

### API d’ingestion rapide  
Une version optimisée de l’API d’ingestion est également disponible :  
```bash
cd api  
uvicorn ingest_fast:app --host 0.0.0.0 --port 8000 --reload  
```  

Cette API permet d’accélérer le traitement des données pour des usages nécessitant une faible latence.  

## Test des performances des APIs  
Deux scripts sont disponibles dans le dossier `test_api` pour évaluer les performances des APIs.  

Pour exécuter les tests :  
```bash
cd test_api  
python ingest_test.py  
python ingest_test_fast.py  
```  

Ces scripts permettent de mesurer les temps de réponse et d’évaluer l’efficacité des différentes méthodes d’ingestion et de consultation des données.