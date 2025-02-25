import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# -- PARAMÈTRES MYSQL (Staging) --
MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_USER = "user"
MYSQL_PASSWORD = "password"
MYSQL_DATABASE = "staging_db"

# -- PARAMÈTRES ELASTICSEARCH (Gold) --
ES_HOST = "http://elasticsearch:9200"
MOVIES_INDEX = "movies_index"
REVIEWS_INDEX = "reviews_index"

def fetch_data_from_mysql(table_name):
    """
    Se connecte à la base MySQL et récupère toutes les lignes
    de la table spécifiée.
    """
    cnx = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = cnx.cursor(dictionary=True)
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    cnx.close()
    return rows

def transform_movies(data):
    """
    Transforme les données des films si nécessaire.
    Exemple : Création d'un champ 'combined_title'.
    """
    for doc in data:
        if "title" in doc and "year" in doc:
            doc["combined_title"] = f"{doc['title']} ({doc['year']})"
        else:
            doc["combined_title"] = doc.get("title", "")
    return data

def transform_reviews(data):
    """
    Transforme les données des critiques si nécessaire.
    Exemple : Calcul de la longueur du texte de la critique.
    """
    for doc in data:
        doc["review_length"] = len(doc["review_text"]) if "review_text" in doc else 0
    return data

def index_data_into_es(data, index_name):
    """
    Indexe un ensemble de documents dans Elasticsearch via l'API bulk.
    """
    es = Elasticsearch(ES_HOST)
    # Crée l'index s'il n'existe pas déjà
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    # Prépare les actions pour le bulk
    actions = [{"_index": index_name, "_source": doc} for doc in data]
    success, _ = bulk(es, actions)
    print(f"Indexed {success} documents in index '{index_name}'.")

def main():
    # 1) Récupérer les données depuis MySQL pour les films et les critiques
    movies = fetch_data_from_mysql("movies")
    reviews = fetch_data_from_mysql("reviews")
    
    # 2) Transformer les données
    movies = transform_movies(movies)
    reviews = transform_reviews(reviews)
    
    # 3) Indexer les données dans Elasticsearch (Gold)
    index_data_into_es(movies, MOVIES_INDEX)
    index_data_into_es(reviews, REVIEWS_INDEX)

if __name__ == "__main__":
    main()
