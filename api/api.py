from fastapi import FastAPI, HTTPException
import boto3
from botocore.exceptions import ClientError
import mysql.connector
from elasticsearch import Elasticsearch
import os

app = FastAPI()

# --- Configuration ---
# Pour S3 (LocalStack)
S3_ENDPOINT = "http://localhost:4566"
S3_BUCKET = "raw"  # Nom du bucket
AWS_ACCESS_KEY_ID = "root"
AWS_SECRET_ACCESS_KEY = "root"

# Pour MySQL (Staging)
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "staging_db"

# Pour Elasticsearch (Curated)
ES_HOST = "http://localhost:9200"
ES_INDEX = "my_curated_index"

# --- Initialisation des clients ---
# Initialisation du client S3 avec les crédentials et l'endpoint
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Initialisation du client Elasticsearch
es_client = Elasticsearch([ES_HOST])


@app.get("/")
async def read_root():
    return {"message": "Hello World"}


@app.get("/raw")
def get_raw_data():
    """
    Accès aux données brutes depuis le bucket S3 nommé 'raw'
    en utilisant la pagination pour récupérer tous les objets.
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=S3_BUCKET)
        objects = []
        for page in page_iterator:
            if "Contents" in page:
                objects.extend(page["Contents"])
        data = [obj["Key"] for obj in objects]
        return {"message": "Accès aux données brutes", "data": data}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/staging")
def get_staging_data():
    """
    Accès aux données intermédiaires depuis la base MySQL de staging.
    Cet exemple récupère jusqu'à 10 enregistrements depuis les tables 'movies' et 'reviews'.
    """
    try:
        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = cnx.cursor(dictionary=True)
        
        # Récupération des données de la table movies
        cursor.execute("SELECT * FROM movies LIMIT 10;")
        movies = cursor.fetchall()
        
        # Récupération des données de la table reviews
        cursor.execute("SELECT * FROM reviews LIMIT 10;")
        reviews = cursor.fetchall()
        
        cursor.close()
        cnx.close()
        
        return {
            "message": "Accès aux données intermédiaires",
            "data": {
                "movies": movies,
                "reviews": reviews
            }
        }
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))



@app.get("/curated")
def get_curated_data():
    """
    Accès aux données finales depuis l'index Elasticsearch.
    Cet exemple retourne les documents indexés (par défaut, 10 documents).
    """
    try:
        # Exécute une recherche match_all dans l'index
        response = es_client.search(
            index=ES_INDEX,
            body={"query": {"match_all": {}}},
            size=100  # Vous pouvez ajuster ce paramètre pour retourner plus de documents
        )
        # Récupère la liste des hits
        hits = response.get("hits", {}).get("hits", [])
        # Optionnel : vous pouvez extraire uniquement le _source de chaque hit
        documents = [hit["_source"] for hit in hits]
        return {"message": "Accès aux données finales", "data": documents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/health")
def health_check():
    """
    Vérifie l'état des services : S3, MySQL et Elasticsearch.
    """
    health = {}

    # Vérification S3
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        health["raw"] = "OK"
    except Exception as e:
        health["raw"] = f"Error: {e}"

    # Vérification MySQL
    try:
        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = cnx.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        cnx.close()
        health["staging"] = "OK"
    except mysql.connector.Error as err:
        health["staging"] = f"Error: {err}"

    # Vérification Elasticsearch
    try:
        if es_client.ping():
            health["curated"] = "OK"
        else:
            health["curated"] = "Error: Not responding"
    except Exception as e:
        health["curated"] = f"Error: {e}"

    overall_status = "Healthy" if all(status == "OK" for status in health.values()) else "Unhealthy"
    health["status"] = overall_status
    return health


@app.get("/stats")
def get_stats():
    """
    Retourne des métriques sur le remplissage des buckets S3, de la base de staging et de l'index curated.
    Pour la partie MySQL, les enregistrements des tables 'movies' et 'reviews' sont combinés.
    """
    stats = {}

    # Statistiques S3
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=S3_BUCKET)
        objects = []
        for page in page_iterator:
            if "Contents" in page:
                objects.extend(page["Contents"])
        stats["raw_bucket_object_count"] = len(objects)
        stats["raw_bucket_total_size_bytes"] = sum(obj["Size"] for obj in objects)
    except Exception as e:
        stats["raw_bucket"] = f"Error: {e}"

    # Statistiques MySQL : combinaison des enregistrements des tables 'movies' et 'reviews'
    try:
        cnx = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = cnx.cursor()
        # Récupère le nombre d'enregistrements dans movies
        cursor.execute("SELECT COUNT(*) FROM movies;")
        count_movies = cursor.fetchone()[0]
        # Récupère le nombre d'enregistrements dans reviews
        cursor.execute("SELECT COUNT(*) FROM reviews;")
        count_reviews = cursor.fetchone()[0]
        total_count = count_movies + count_reviews

        cursor.close()
        cnx.close()

        stats["staging_db_record_count"] = total_count
        stats["staging_db_record_breakdown"] = {"movies": count_movies, "reviews": count_reviews}
    except mysql.connector.Error as err:
        stats["staging_db"] = f"Error: {err}"

    # Statistiques Elasticsearch (nombre de documents dans l'index curated)
    try:
        response = es_client.count(index=ES_INDEX)
        stats["curated_index_document_count"] = response.get("count", 0)
    except Exception as e:
        stats["curated_index"] = f"Error: {e}"

    return stats

